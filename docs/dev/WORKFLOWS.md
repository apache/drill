# Workflows

A workflow is a saved query on a schedule, followed by an ordered list of actions. Each action has a condition that decides whether it fires, and can reference the query result and the outputs of earlier actions. Workflows belong to a project and run as the person who owns them.

Workflows replace the older SQL Lab scheduler (see [SCHEDULER.md](SCHEDULER.md)). The design record is [specs/2026-10-20-workflows-design.md](specs/2026-10-20-workflows-design.md) and the build plan is [plans/2026-10-20-workflows-pipeline-core.md](plans/2026-10-20-workflows-pipeline-core.md). This page describes the code as built. For how to use workflows (building them, condition and template syntax, recipes, admin setup, troubleshooting), see the [user and admin guide](WorkflowsGuide.md).

Backend: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/workflow/`. UI: [ui/pages/workflows.md](ui/pages/workflows.md), [ui/pages/workflow-editor.md](ui/pages/workflow-editor.md), [ui/pages/project-workflows.md](ui/pages/project-workflows.md).

## Concepts

### The workflow document

`Workflow.java` is the one document shared by the REST API, the UI and Prospector.

```json
{
  "id": "5f0c...",
  "projectId": "p1",
  "name": "Daily error spike",
  "description": "Alert when errors jump",
  "owner": "alice",
  "savedQueryId": "q42",
  "schedule": {
    "frequency": "daily",
    "timeOfDay": "08:00",
    "timeoutSeconds": 300,
    "enabled": true,
    "expiresAt": "2027-01-18T08:00:00Z"
  },
  "actions": [
    {
      "id": "flag",
      "type": "record_alert",
      "condition": { "expr": "rowCount > 100" },
      "config": { "message": "{{rowCount}} errors, up {{deltaPercent}}%" }
    },
    {
      "id": "mail",
      "type": "email",
      "condition": { "expr": "actions.flag.message contains 'errors'" },
      "config": { "to": ["ops@example.com"], "subject": "Error spike", "body": "{{actions.flag.message}}" }
    }
  ],
  "retentionCount": 30,
  "status": "active",
  "nextRunAt": "2026-10-21T08:00:00Z",
  "lastRunAt": "2026-10-20T08:00:00Z",
  "lastStatus": "success",
  "consecutiveFailures": 0,
  "isRunning": false,
  "runStartedAt": null
}
```

Schedule fields:

| Field | Meaning |
|---|---|
| `frequency` | `hourly`, `daily`, `weekly` or `monthly` |
| `timeOfDay` | `HH:mm`, 24-hour. For `hourly` only the minutes matter. |
| `dayOfWeek` | `0` = Sunday to `6` = Saturday. Required for weekly. |
| `dayOfMonth` | `1` to `28`. Required for monthly. |
| `timeoutSeconds` | `1` to `86400`, default 300. Applies to the saved query (the preview and the `COUNT(*)` each get the full timeout) and to the statements of `save_to_table`, whose action timeout is raised to match (see Pipeline semantics). |
| `enabled`, `expiresAt` | Server managed. Set through the `enabled` and `renew` endpoints. |

Times use the Drillbit's system time zone. `status` is `active`, `paused`, `expired` or `owner_lost_access`. Action ids match `[A-Za-z_][A-Za-z0-9_]*` and are unique within the workflow.

### Conditions

Every action has a `condition` with exactly one meaningful form:

| Form | Example |
|---|---|
| `{ "always": true }` | Fires every run. This is the default. |
| `{ "tree": ... }` | Structured, produced by the builder. Nodes are `{all: [...]}`, `{any: [...]}`, `{not: node}` or a leaf `{field, op, value}`. |
| `{ "expr": "..." }` | Text. The server parses it into `tree` on save, so the stored form is always a tree. |

Expression grammar (`ConditionParser`):

```
expr    := or
or      := and ( "||" and )*
and     := unary ( "&&" unary )*
unary   := "!" unary | cmp
cmp     := "(" expr ")" | path ( OP literal )?
path    := ident ( "." ident | "[*]" )*
literal := number | 'string' | "string" | true | false | null
```

The left side of a comparison is always a field path. A bare path (`actions.flag.sent`) means `path == true`. There is no `exists` operator; use `field != null`.

Operators: `==`, `!=`, `>`, `>=`, `<`, `<=`, `contains`, `startsWith`.

Evaluation rules (`ConditionEvaluator`):

* Numbers compare numerically, including numeric strings (query values are strings). If either side is not numeric, `==` and `!=` compare as text and the ordering operators are false.
* A missing path and a `null` value both resolve to null. Against null, only `==` and `!=` can be true; every other operator is false.
* `rows[*].col` reads column `col` from the preview rows. A condition on it is true if any row matches (any-match). If no row has the column, it is false for every operator, including `!=`.

Field paths:

| Path | Type | Notes |
|---|---|---|
| `status` | string | `success` or `error` for the saved query |
| `rowCount` | number | Exact count, not capped at the preview size |
| `rowCountDelta` | number or null | Versus the previous non-skipped run |
| `deltaPercent` | number or null | Null when the previous count was 0 |
| `durationMs` | number | Query duration |
| `intervalMs` | number | Schedule interval: hourly 3,600,000; daily 86,400,000; weekly 604,800,000; monthly 2,592,000,000 (30 days) |
| `error` | string or null | Query error message |
| `rows[*].<column>` | any-match | Up to 100 preview rows |
| `actions.<id>.<output>` | per action type | Only actions earlier in the list; the output must exist for that type |

Save is rejected with a pathed error (HTTP 422) for a syntax error (the message includes the position), an unknown or misspelled field (`rowcount` is not `rowCount`), a reference to a later action, an unknown operator, a malformed tree, or `null` with an ordering operator. Limits: expressions are capped at 2000 characters and conditions at 50 levels of nesting.

### Templates

Action config strings marked as templates in the action's schema (`x-template`) accept `{{path}}` placeholders using the same paths as conditions. There is no logic and no escaping. A missing value renders as an empty string and a list renders comma-separated. `rows[*].col` renders all values of the column.

A condition or template that references `actions.<id>.*` makes the action depend on `<id>`. Save rejects references to unknown or later actions.

## Action types

| Type | Config | Outputs | Side effects |
|---|---|---|---|
| `record_alert` | `message` (template, required) | `message` (string) | No |
| `save_to_table` | `location` (default `dfs.tmp`), `format` (`parquet`, `json`, `csv`; default `parquet`), `mode` (`overwrite`, `append`, `newTablePerRun`; default `overwrite`) | `path` (string) | Yes |
| `email` | `to` (list of addresses, at least one), `subject` and `body` (templates) | `sent` (boolean) | Yes |

`record_alert` flags the run with a message shown as a badge in run history.

`save_to_table` re-runs the saved query as a CTAS or INSERT and writes the full result, not only the preview. A trailing `;` on the saved SQL is stripped and the query is wrapped with a newline before the closing parenthesis, so a trailing `--` comment cannot swallow it. The target is `<location>.` followed by the backticked path `schedules/<workflowId>` (dashes in the id become underscores). `newTablePerRun` writes `schedules/<id>/run_<epochMillis>` instead. `overwrite` runs `DROP TABLE IF EXISTS` then CTAS, `append` runs `INSERT INTO`, and the other modes run CTAS. The statements run on one session as the workflow owner, starting with `ALTER SESSION SET store.format`. The action validates `location` (`^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)?$`), `format`, `mode` and the workflow id itself before building any SQL, because migrated legacy configs bypass schema validation.

`email` sends through the global SMTP configuration. If SMTP is not configured the action errors with "SMTP is not configured".

### Adding an action type

1. Implement `ActionType` (`id`, `displayName`, `description`, `configSchema`, `outputs`, `hasSideEffects`, `execute`). Override `preview` if the default dry-run output (`wouldSend`, the rendered config) is wrong.
2. Register it in `ActionTypeRegistry.defaults()`.

The editor and the validator read `GET /action-types`, so the schema drives the config form, validation (`SchemaCheck`) and the references offered by autocomplete. Use `x-template: true` on strings that accept placeholders and `x-multiline: true` for text areas. Mark `hasSideEffects` true for anything that touches tables, mail or the network; dry runs then call `preview` instead of `execute`.

## Runner

`WorkflowRunner` starts with the Drillbit (30 second initial delay, then a tick every 60 seconds). On first start it also migrates legacy schedules. Each legacy entry migrates independently: one that cannot be read or converted is logged with its key and skipped, and later entries still migrate. `legacyMigrated` is set only when every entry succeeded, so failed entries are retried on the next start (already migrated ids are skipped). A failed migration never stops the ticker from being scheduled. Each tick catches and logs its own failures (a store error, an unreadable workflow), because an exception escaping a fixed-rate task would cancel every later tick on that Drillbit.

### Tick

For each enabled workflow:

1. If `isRunning` is stale (see below), log a warning and clear `isRunning` and `runStartedAt`.
2. If expired, disable it with status `expired`.
3. If the owner is no longer a member of the project (or the project is gone), disable it with status `owner_lost_access`.
4. If `nextRunAt` is in the future, stop.
5. Claim the slot in `drill.sqllab.workflow_claims` with the key `<workflowId>_<epochMillis of nextRunAt>` and this Drillbit's address. If another Drillbit already holds the claim, normally return without running or changing state. If the slot is older than the longest healthy run (`maxRunSeconds`), the claiming Drillbit died before advancing `nextRunAt`, so advance it to the next slot instead. Every Drillbit computes the same next slot, so this is idempotent. Claims older than 7 days are pruned.
6. If `isRunning` is set, record a `skipped` run ("Skipped: previous run still in progress") and advance `nextRunAt`.
7. Otherwise execute.

Crash recovery. A run sets `isRunning` and `runStartedAt` on a freshly read copy of the workflow (so an edit saved since the tick loaded it is kept) and clears both when it ends. A Drillbit that dies mid-run leaves them set. The flag counts as stale once `runStartedAt` is older than

```
maxRunSeconds = 2 * (timeoutSeconds + 30)
              + actions * (max(actionTimeoutSeconds, timeoutSeconds) + 30)
              + 600
```

which covers the preview query, the `COUNT(*)` query, every action, their 30 second cancel graces, and 10 minutes of slack. A flag with no `runStartedAt` (set before this field existed) is stale immediately. A stale flag is cleared by the tick, ignored by run now, and left out of `/observability/running`.

### Executing a run

1. Check that the saved query is owned by the workflow owner (see Permissions). If not, the query does not run and the run status is `error` with "Saved query is owned by X, workflow runs as Y". Otherwise run the saved query as the workflow owner with a 100-row preview. If the preview is full (100 rows), a second `SELECT COUNT(*)` over the query gives the real `rowCount`. A trailing `;` in the saved SQL is stripped first. The query has `timeoutSeconds` to finish. On timeout the running query is cancelled on the cluster, later statements in that call never start, and the runner waits up to 30 seconds for the cancel to land before releasing the workflow.
2. Compute `rowCountDelta` and `deltaPercent` against the previous non-skipped run.
3. Run the actions in list order (below).
4. Record the run, update `lastRunAt`, `lastStatus`, `consecutiveFailures`, clear `isRunning`, advance `nextRunAt` (scheduled runs only), append to the run log, and trim old runs.

If the saved query fails, no longer exists or has the wrong owner, the run status is `error` but the actions still run, so a condition such as `status == 'error'` can notify someone.

### Pipeline semantics

Each action ends as `fired`, `skipped` or `errored`. An action never stops the ones after it.

| Outcome | Reason or error |
|---|---|
| `skipped` | `condition false` |
| `skipped` | `upstream <id> <outcome>`: the action references `actions.<id>.*` in its condition or config, and `<id>` did not fire (`skipped`, `errored` or `missing`). Its condition is never evaluated against absent outputs. |
| `skipped` | `interrupted` |
| `errored` | The exception message, `Unknown action type: x`, or `Action timed out after N ms` |
| `fired` | The action's outputs are stored on the outcome and visible to later actions |

Each action gets `actionTimeoutSeconds` (default 60) to finish. An action type can ask for more through `ActionType.timeoutMs(workflow, configuredMs)`. `save_to_table` re-runs the whole query, so it gets `max(actionTimeoutSeconds, timeoutSeconds + 35)`: the schedule's query timeout, the 30 second cancel grace and 5 seconds of margin so the query's own timeout fires first.

### Recording, retention and expiry

* Runs are stored in `drill.sqllab.snapshots`, keyed by run id, with `scheduleId` holding the workflow id (the name predates workflows). Run `status` is `success`, `error` or `skipped`; `trigger` is `schedule` or `manual`.
* A stored run keeps 20 preview rows and the column list. ZooKeeper nodes are capped at 1 MB. Actions still see up to 100 rows in memory.
* After each run only the newest `retentionCount` runs (default 30) are kept for that workflow.
* New workflows are created paused and expire `expirationDays` after creation or renewal (default 90) when expiration is enabled. Expired workflows are disabled by the tick, and `renew` restarts the clock. The list page warns `warningDaysBeforeExpiry` days ahead (default 14).
* `owner_lost_access` is set by the tick when the owner leaves the project. An admin clears it with `take-ownership`.

## Permissions

A workflow runs as its owner, so editing someone else's workflow would run SQL with their access. Edit rights are narrow.

| Action | Project member | Project owner | Public-project viewer | Admin |
|---|---|---|---|---|
| See the definition | yes | yes | yes | all projects |
| See run records, health, project run stats | yes | yes | no | yes |
| Create (becomes owner) | yes | yes | no | only as a project member |
| Edit (`PUT`), dry-run a saved workflow | own workflows only | own workflows only | no | any |
| Enable, pause, delete, renew, run now | own workflows only | any in the project | no | any |
| `take-ownership`, `assign-project`, config `PUT`, observability setup, running list | no | no | no | yes |

"Member" is the project owner or someone in `sharedWith`. A public project is readable by anyone, but public visibility alone does not make someone a member. Run records are visible only to members and admins because they contain data fetched under the owner's identity; public-project viewers get 403 on the run, health and run-record endpoints and the UI shows "Run history is visible to project members". Workflows with no project are visible to admins only.

Non-admins may call `GET /workflows` without `projectId`; the result is filtered by view rights. Admin edits never change the run-as identity; `take-ownership` does that explicitly and requires the admin to be a member of the project first.

Saved query ownership. A workflow's saved query must be owned by the workflow owner, the user it runs as. The author of a saved query can change its SQL at any time, so a workflow over someone else's query would let that author run SQL (including CTAS and DDL) with the workflow owner's access. Create, update and validate return a 422 error at `savedQueryId` ("The workflow runs as X, so its saved query must be owned by X"); on update the run-as user is the existing owner, not the admin editing it. A dry run of a draft runs as the caller, so the query must be the caller's. At run time the runner checks again and fails the run with "Saved query is owned by X, workflow runs as Y" without executing anything; this covers migrated workflows and workflows whose owner changed. `take-ownership` is still allowed when the query belongs to the previous owner, but the workflow fails every run until the new owner points it at a saved query they own.

State changes:

* Create and draft dry run return 409 when an admin who is not a member of the project tries them ("Add yourself to the project before creating a workflow"). Such a workflow would be disabled as `owner_lost_access` on its first tick.
* Enabling returns 409 for `owner_lost_access` (the message tells the user an admin can click Take ownership), for an expired workflow (renew first), and for a workflow with no project (assign one first).
* Run now returns 409 if the workflow is already running (a stale flag does not count), if the owner has left the project, or if the workflow has expired.
* `take-ownership` returns 409 if the admin is not a member of the workflow's project ("Add yourself to the project before taking ownership").
* `assign-project` returns 409 if the workflow's saved query is not in the target project ("Add the workflow's saved query to that project first"), and 404 if the project does not exist.

## REST reference

Base path `/api/v1/workflows`. All endpoints need an authenticated user. Errors are `{"error": "<message>"}`. Validation failures are HTTP 422 with `{"errors": [{"path", "message"}]}`.

| Method | Path | Notes |
|---|---|---|
| GET | `?projectId=` | List. With `projectId`, requires view rights on the project (403 otherwise). Without it, returns every workflow the caller can view. |
| POST | `/` | Create. Members and admins. Server sets id, owner, `paused` status, expiry and `nextRunAt`. 201 on success. |
| GET | `/{id}` | Read (view rights). |
| PUT | `/{id}` | Update (owner or admin). Owner, status, enabled flag, expiry and run state are preserved. |
| DELETE | `/{id}` | Delete the workflow and its runs (owner, project owner, admin). 204. |
| POST | `/{id}/enabled` | Body `{"enabled": bool}`. See the 409 rules above. |
| POST | `/validate` | Body is a workflow; returns `{errors: [...]}` (200) without saving. |
| POST | `/parse-condition` | `{expr}` returns `{tree}` or `{error, position}`. |
| POST | `/dry-run` | Dry-run an unsaved draft as the caller. Members and admins. Validates first (422). |
| POST | `/{id}/dry-run` | Dry-run the saved definition. Owner or admin. |
| POST | `/{id}/run` | Run now. Owner, project owner, admin. Returns the recorded run. |
| POST | `/{id}/renew` | Reset expiry. Owner, project owner, admin. |
| POST | `/{id}/take-ownership` | Admin. Sets the owner to the caller; clears `owner_lost_access`. Afterwards, point the workflow at a saved query the admin owns, or every run fails the ownership check. |
| POST | `/{id}/assign-project` | Admin. Body `{"projectId"}`. The workflow's saved query must already be in that project. |
| GET | `/{id}/runs` | Runs, newest first. Members and admins. |
| GET | `/{id}/runs/{runId}` | One run. Members and admins. |
| GET | `/{id}/health` | `{setupRequired, daily, actions}` from the run log, last 30 days. Members and admins. |
| GET | `/action-types` | Registry: `id`, `displayName`, `description`, `configSchema`, `outputs`, `hasSideEffects`. |
| GET / PUT | `/config` | Global settings. PUT is admin only. |
| GET | `/observability/status` | `logPresent`, `workspace`, `format`, `ready`; `logDir` for admins only. |
| POST | `/observability/setup` | Admin. Creates the `dfs.workflow_logs` workspace and `workflow_log` format. |
| GET | `/observability/summary?projectId=` | Per-workflow run counts, successes, average duration and last run (30 days). `projectId` is required for non-admins, who must be members. |
| GET | `/observability/running` | Admin. Workflows with a live `isRunning` flag (stale flags are left out). |

The old `/api/v1/schedules` endpoints no longer exist and there is no alias; the SPA was their only client.

### Dry run

A dry run executes the saved query for real (as the workflow owner for a saved workflow, or as the caller for a draft) and walks the pipeline. Actions with side effects return their `preview` output instead of executing, so nothing is written or sent. A dry run records no run and does not touch run history. From the editor, Dry run always sends the current draft to `POST /dry-run`, even for a saved workflow, so it runs the unsaved edits as the person in the editor.

Dry run requires edit rights (owner or admin) for saved workflows because it executes the query as the owner.

### Config

`GET/PUT /config`, stored in `drill.sqllab.workflow_config` under key `default`:

| Field | Default |
|---|---|
| `expirationEnabled` | `true` |
| `expirationDays` | `90` |
| `warningDaysBeforeExpiry` | `14` |
| `actionTimeoutSeconds` | `60` |
| `legacyMigrated` | Server managed; a PUT cannot change it |

## Storage and observability

| Store | Content |
|---|---|
| `drill.sqllab.workflows` | Workflow documents |
| `drill.sqllab.snapshots` | Run records (reused from the old scheduler; field names stay compatible) |
| `drill.sqllab.workflow_claims` | Per-slot cluster claims |
| `drill.sqllab.workflow_config` | Global settings, key `default` |
| `drill.sqllab.schedules` | Legacy schedules, read only, migration source |
| `drill.sqllab.smtp_config` | SMTP settings, key `default` |

Every run appends one JSON line to `workflow-runs.log` in `DRILL_LOG_DIR` through the `workflow.runs` logger (`distribution/src/main/resources/logback.xml`: daily and 100 MB rolling, 90 days, 5 GB cap). A line holds `ts`, `runId`, `workflowId`, `projectId`, `owner`, `drillbit`, `trigger`, `status`, `rowCount`, `durationMs` and, per action, `id`, `type`, `outcome`, `reason`, `durationMs` and `error`. It never holds action outputs or preview rows.

The Health tab and the list page statistics query this log with Drill SQL. An admin runs the one-time setup (the list page offers a Set up button, or `POST /observability/setup`), which adds the `dfs.workflow_logs` workspace pointing at the log directory with a JSON format for `.log` files. It is the same model as AI Analytics. Until then `health` and `summary` return `setupRequired: true`.

Security note: once setup has run, the `dfs.workflow_logs` workspace makes `workflow-runs.log` queryable by any SQL user. The log contains workflow and project ids, owner, status, durations and per-action outcome, reason and error, but never outputs or preview rows. Admins who need stricter isolation should restrict the workspace.

## Migration from the old scheduler

On first start, `WorkflowRunner.migrateLegacy` converts every entry in `drill.sqllab.schedules` into a workflow with the same id, then, once every entry has migrated, sets `legacyMigrated` in the config so a deleted migrated workflow is never recreated. The legacy store is left untouched.

| Legacy field | Becomes |
|---|---|
| `savedQueryId`, `description`, `frequency`, `timeOfDay`, `dayOfWeek`, `dayOfMonth`, `timeoutSeconds`, `retentionCount`, `expiresAt`, `renewedAt`, `createdAt`, `lastRunAt` | Same-named workflow fields. A weekly schedule without a day gets Monday, legacy `dayOfWeek` 7 (Sunday in `java.time`) becomes 0, a monthly one gets day 1, and the day of month is clamped to 1 to 28. |
| `owner` | Copied. A schedule with no owner (auth disabled) becomes `anonymous`, the user the old scheduler ran it as. On every start the runner also sets `anonymous` on any workflow saved with no owner by an earlier migration, because a null owner fails every membership check. |
| `nextRunAt` | Not copied. Recomputed from the schedule at migration time, so a slot missed while Drill was down for the upgrade does not run (and send mail) on the first tick. |
| name | The saved query's name, else the description, else `Workflow <id>` |
| project | The project containing the saved query. No project means an orphan. |
| `enabled` and not `paused` | `schedule.enabled`, only if the workflow has a project |
| `persistResults` with `resultLocation`, `resultFormat`, `resultMode` | A `save_results` action of type `save_to_table`, writing to the same `schedules/<id>` tables as before |
| enabled `alertRules` (`rowCountAbove`, `rowCountBelow`, `queryFailed`, `durationAbove`, `durationExceedsInterval`) | One `record_alert` action per rule, with an equivalent expression condition |
| `notifyEmails` with `notifyOnFailure` / `notifyOnSuccess` | `email_on_failure` (`status == 'error'`) and `email_on_success` (`status == 'success'`) actions |

Upgrade notes:

* Legacy `notifyEmails` become live `email` actions. They start sending as soon as SMTP is configured, including for schedules that previously never delivered mail. Review them after upgrading.
* Legacy schedule times were UTC. Workflows use the Drillbit's system time zone, so on a server outside UTC every migrated schedule shifts by the zone offset. Hourly schedules now honor the minute in `timeOfDay`. Review migrated times after upgrading.
* A migrated workflow whose saved query belongs to someone other than the schedule owner fails each run until it is pointed at a query the owner owns (see Permissions).

Orphans (schedules whose saved query is in no project) are migrated disabled. An admin assigns them from the "Workflows without a project" card on `/workflows`, which calls `assign-project`. Disabled alert rules and the old AI summary stub are not migrated; they never produced output. Existing run records stay readable, including their legacy `aiSummary`, `triggeredAlerts` and `resultPath` fields.

## SMTP

Email actions use the global SMTP settings from `/api/v1/smtp/config` (the same dialog as the rest of SQL Lab, stored in `drill.sqllab.smtp_config`). `encryption` must be `starttls` (the default), `ssl` or `none`; any other value makes the send fail. Mail is sent with `angus-mail` (`org.eclipse.angus`), a dependency of `exec/java-exec` that is excluded from the shaded `jdbc-all` jar (`exec/jdbc-all/pom.xml`).

## Deviations from the design spec and known gaps

Simplifications made while building, relative to [the spec](specs/2026-10-20-workflows-design.md):

* No `/api/v1/schedules` alias.
* No `exists` operator; use `field != null`.
* The expression editor is a plain input with live parse errors, not Monaco.
* Actions are reordered with up and down buttons, not drag and drop.
* The condition builder handles one level (AND or OR over leaf rows). Deeper trees stay in expression mode.
* Health shows average duration, not p95.
* Stored runs keep 20 preview rows.
* Run now does not take a cluster claim.
* Dry run requires edit rights (owner or admin), because it executes as the owner.
* Public-project viewers can see workflows, but not run records.
* Non-admins may list without `projectId`; results are filtered by view rights.

Crash recovery is bounded by `maxRunSeconds` (see Tick). After a Drillbit dies mid-run, the workflow may skip slots until that bound passes; the next tick after it clears the flag and moves past the abandoned slot. The enable, update and renew endpoints still preserve run state from the store, so they never clear a live flag.
