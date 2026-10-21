# Workflows: user and admin guide

A workflow runs one of your saved queries on a schedule and then does things with the result: flag the run, save the full result to a table, or send an email. Each step can have a condition, so a workflow can act only when something interesting happens, such as "email the on-call list when there are more than 100 errors."

This guide is for the people who build and run workflows, and for the admins who set them up. The technical reference (data model, runner internals, REST API) is [WORKFLOWS.md](WORKFLOWS.md).

Workflows replace the old scheduled queries feature. There is only one scheduling system: if you previously scheduled a query, it is now a workflow (see [Upgrading from scheduled queries](#upgrading-from-scheduled-queries)).

## Contents

* [Key ideas](#key-ideas)
* [Before you start](#before-you-start)
* [Create your first workflow](#create-your-first-workflow)
* [Schedules](#schedules)
* [Actions](#actions)
* [Conditions](#conditions)
* [Templates](#templates)
* [Chaining actions](#chaining-actions)
* [Testing: dry run and run now](#testing-dry-run-and-run-now)
* [Run history and health](#run-history-and-health)
* [Lifecycle: paused, expiring, failing](#lifecycle-paused-expiring-failing)
* [Who can do what](#who-can-do-what)
* [Recipes](#recipes)
* [Admin guide](#admin-guide)
* [Upgrading from scheduled queries](#upgrading-from-scheduled-queries)
* [Troubleshooting](#troubleshooting)
* [Limits](#limits)

## Key ideas

* **A workflow lives in a project.** Open a project and choose **Workflows** in its sidebar.
* **A workflow runs a saved query that you own.** Write and save the query first, add it to the project, then build the workflow on it.
* **A workflow runs as its owner.** Scheduled runs use the owner's identity and data access, even when nobody is logged in. The owner is whoever created the workflow.
* **After the query, actions run in order.** Each action has a **Run when** condition. One action failing or being skipped never stops the others.
* **Actions see the result.** Conditions and messages can use the row count, the change since the last run, the first 100 rows, the error message, and the outputs of earlier actions.
* **Every run is recorded.** Each workflow keeps a run history, and the cluster keeps a log of every run that you can query with SQL.

## Before you start

You need:

1. **A project you are a member of.** You are a member if you own the project or it is shared with you. Viewing a public project is not enough.
2. **A saved query that you own, in that project.** In SQL Lab, write the query and save it. On the **Saved Queries** page, use **Add to project…** if it is not already in the project.
3. **SMTP configured**, if you want email. An admin sets this up once (see [Admin guide](#admin-guide)).

Write the query so its result is what you want to act on. Workflows are simplest when the query returns exactly the rows you care about: for example `WHERE level = 'ERROR' AND ts > NOW() - INTERVAL '1' DAY` for "errors in the last day." Then `rowCount > 0` means "there is something to report."

## Create your first workflow

There are two ways in:

* From the project: **Workflows** in the project sidebar, then **New workflow**.
* From a saved query: on the **Saved Queries** page, select the query, open the **⋯** menu and choose **Create workflow…**. The editor opens with the query already chosen. If the query already has a workflow, the menu says **Open workflow** (or **View workflows** when it has several) instead.

The editor has three tabs: **Definition**, **Runs** and **Health**. Runs and Health appear once the workflow is saved.

On the **Definition** tab:

1. **Name** the workflow and optionally add a description.
2. **Choose the saved query.** The picker lists the project's saved queries. If you choose one you do not own, the editor shows "The workflow runs as you, so its saved query must be owned by you."
3. **Set the schedule**: frequency, time, the day for weekly or monthly runs, and the query timeout. See [Schedules](#schedules).
4. **Add actions** with **Add action**. For each action, set **Run when** and fill in its settings. See [Actions](#actions) and [Conditions](#conditions).
5. **Click Dry run** to see what would happen. The query runs for real, but nothing is sent or written. See [Testing](#testing-dry-run-and-run-now).
6. **Click Save.** Save stays disabled while there are errors, and every error is shown next to its field or in a list above the tabs.
7. **Turn on the Enabled switch.** New workflows are always saved paused, so nothing runs until you enable it.

## Schedules

| Frequency | Runs | Settings |
|---|---|---|
| Hourly | Every hour, at the minute in **Time** (for `00:15`, at 15 past each hour) | Time (only the minutes count) |
| Daily | Once a day at **Time** | Time |
| Weekly | Once a week | Time and day of the week |
| Monthly | Once a month | Time and day of the month, 1 to 28 |

* Times use the Drill server's time zone, in 24-hour `HH:mm` format.
* Days 29 to 31 aren't offered, so a monthly workflow runs every month.
* **Timeout** (seconds, default 300, maximum 86,400) is how long the query may run. A query that runs over is cancelled on the cluster and the run is recorded as an error.
* In a cluster, each scheduled run happens exactly once, on one Drillbit, however many Drillbits there are.
* If a run is still going when the next one is due, the next one is recorded as **skipped** ("previous run still in progress") and the workflow waits for the following slot.
* If Drill is down when a run is due, that run does not happen later. The workflow carries on from its next slot.

## Actions

The **Add action** menu lists every action type the server supports. Actions marked **side effects** write data or send messages. A dry run never executes those; it shows a preview of what they would do.

Each action has an **id** (letters, digits and `_`, starting with a letter or `_`). Later actions use the id to refer to it, as in `actions.flag.message`.

### Record alert

Flags the run with a message. The message shows as an alert badge in run history. Use it to mark runs worth looking at, or as a first step that later actions build on.

| Setting | |
|---|---|
| Alert message | Required. A [template](#templates), for example `{{rowCount}} failed logins` |

Output: `message`, the rendered text.

### Save to table (side effects)

Runs the saved query again and writes the **full result**, not just the first 100 rows, to a table.

| Setting | Default | |
|---|---|---|
| Workspace | `dfs.tmp` | Any writable workspace, as `plugin` or `plugin.workspace` |
| Format | `parquet` | `parquet`, `json` or `csv` |
| Mode | `overwrite` | See below |

| Mode | Result |
|---|---|
| `overwrite` | One table, replaced on every run |
| `append` | One table, with each run's rows added. The first run must create it, so start with `overwrite` or create the table yourself. |
| `newTablePerRun` | A new table for each run, named `run_<timestamp>` |

The table is `` <workspace>.`schedules/<workflow id>` `` (dashes in the id become underscores). With `newTablePerRun` it is `` <workspace>.`schedules/<workflow id>/run_<epoch millis>` ``. Output: `path`, the table that was written. The write runs as the workflow owner, so the owner needs write access to the workspace.

### Email (side effects)

Sends an email through the server's SMTP settings.

| Setting | |
|---|---|
| Recipients | One or more email addresses |
| Subject | A [template](#templates) |
| Body | A [template](#templates); multi-line |

Output: `sent` (true). If SMTP isn't set up, the action fails with "SMTP is not configured" and the rest of the workflow still runs.

More action types (Slack, webhooks, AI summaries) are planned. They will appear in **Add action** when they exist.

## Conditions

Every action has a **Run when** setting with three modes:

* **Always**: the action runs on every run. This is the default.
* **Builder**: a list of rules, matched with **All (AND)** or **Any (OR)**. Each rule is a field, an operator and a value. The field box offers every field you can use.
* **Expression**: the same thing typed as text, for logic the builder can't express (mixing AND with OR, NOT, parentheses). Errors show as you type, with their position.

You can switch between Builder and Expression. A complicated expression stays in Expression mode, and the editor tells you so.

### Fields

| Field | Meaning |
|---|---|
| `status` | `'success'` or `'error'`: did the query run? |
| `rowCount` | Number of rows the query returned (the real total, not capped at 100) |
| `rowCountDelta` | Change in row count since the previous run; empty on the first run |
| `deltaPercent` | Percentage change since the previous run; empty when the previous count was 0 |
| `durationMs` | How long the query took, in milliseconds |
| `intervalMs` | Length of the schedule interval in milliseconds (hourly 3,600,000; daily 86,400,000; weekly 604,800,000; monthly is treated as 30 days) |
| `error` | The query's error message, if it failed |
| `rows[*].<column>` | A column from the first 100 rows (see below) |
| `actions.<id>.<output>` | An output of an earlier action, such as `actions.flag.message` or `actions.mail.sent` |

Field names are case sensitive: `rowCount`, not `rowcount`. The editor rejects names it doesn't know.

### Operators

| Operator | Example |
|---|---|
| `==`, `!=` | `status == 'error'` |
| `>`, `>=`, `<`, `<=` | `rowCount >= 100` |
| `contains` | `error contains 'timeout'` |
| `startsWith` | `rows[*].host startsWith 'prod-'` |

Combine with `&&` (and), `||` (or), `!` (not) and parentheses:

```
status == 'success' && (rowCount > 100 || deltaPercent > 50)
```

### Values

* Text goes in single or double quotes: `'error'` or `"error"`.
* Numbers are written plainly: `100`, `2.5`, `-1`.
* `true`, `false` and `null` are keywords.
* A field on its own means "is true": `actions.mail.sent` is the same as `actions.mail.sent == true`.

### How comparisons behave

* **Numbers compare as numbers**, even though query results arrive as text. `rows[*].count > 9` is true for a value of `10`.
* **Empty values.** A missing field, or one with no value, counts as `null`. Only `== null` and `!= null` can be true for it. `deltaPercent > 50` is false on the first run, when there is no previous run to compare with. To check that a value exists, use `field != null`.
* **Row columns match if any row matches.** `rows[*].status == 'FAILED'` is true if at least one of the first 100 rows has that status. If no row has the column at all (a typo, or zero rows), the rule is false, even with `!=`.

## Templates

Text settings such as the alert message, email subject and email body can include values with `{{field}}`. They use the same field names as conditions:

```
{{rowCount}} errors since yesterday ({{deltaPercent}}% change).
Hosts affected: {{rows[*].host}}
```

* A missing value becomes empty text.
* `{{rows[*].column}}` lists that column's values from the first 100 rows, separated by commas.
* There is no logic in templates (no if or loops). Use conditions to decide whether an action runs.
* The **Insert field** link under each template box lists every field you can use and inserts it at the cursor.

## Chaining actions

Actions always run in order, top to bottom, and you can reorder them with the arrow buttons. An action can use an earlier action's output in its condition or its settings. That makes it **depend** on the earlier action:

* If the earlier action did not run (its condition was false, it failed or it timed out), the dependent action is **skipped** with the reason `upstream <id> skipped` (or `errored`). Its condition isn't even checked.
* An action can only refer to actions above it. Referring to a later or unknown action is an error when you save.

Example: flag first, then email only if the flag fired, reusing its message:

1. `flag` (Record alert), Run when `rowCount > 0`, message `{{rowCount}} new failed logins`.
2. `notify` (Email), Run when **Always**, body `{{actions.flag.message}}`. Because it uses `flag`'s output, it only sends when `flag` fired.

Actions that don't reference each other are independent. If the second of three actions fails, the third still runs.

Each action has 60 seconds by default (an admin can change this). Save to table gets at least the query timeout plus 35 seconds, because it re-runs the query.

## Testing: dry run and run now

**Dry run** (in the editor header) tests what is on screen, including unsaved changes:

* The query really runs, as you.
* Every condition is evaluated and every action that does not have side effects runs.
* Email and Save to table show a preview of what they would send or write. **Nothing is sent or written, and no run is recorded.**
* Results open in a panel: the query status, row count, duration, the first 10 rows, and each action's outcome (fired; skipped, with the reason; or errored, with the message).

Because a dry run runs as you, its result can differ from a scheduled run if you and the workflow's owner can see different data.

**Run now** runs the saved workflow for real, as its owner. Emails are sent, tables are written and the run appears in run history. It is refused if the workflow is already running, has expired, or its owner has left the project.

## Run history and health

The **Runs** tab lists recent runs, newest first, with when it ran, whether it was scheduled or manual, status (success, error or skipped), row count, duration, the messages of any **Record alert** actions that fired, and a count of fired, skipped and errored actions. Expand a run to see its error, a preview of the result (stored runs keep 20 rows) and every action's outcome with its reason, error and outputs. Each workflow keeps its 30 most recent runs.

The **Health** tab covers the last 30 days: the daily success rate, the average duration and each action's outcome counts. Health needs a one-time admin setup; until then it shows a setup hint.

The project's **Workflows** list shows each workflow's schedule, state, last run, 30-day success rate and next run. Failing workflows (two or more failures in a row) sort to the top.

Run history is visible to project members and admins only. People viewing a public project can see that a workflow exists and how it is defined, but not its results, because those results were fetched with the owner's access.

## Lifecycle: paused, expiring, failing

| State | Meaning | What to do |
|---|---|---|
| Paused | Saved but not running. Every new workflow starts here. | Turn on **Enabled** |
| On | Running on schedule | |
| Failing | Two or more failed runs in a row (still on) | Check the Runs tab for the error |
| Expiring soon | Will expire within the warning window (14 days by default) | Click **Renew** |
| Expired | Not renewed in time (90 days by default). It stops running. | Click **Renew** in the editor banner or the list, then turn on **Enabled** |
| Owner lost access | The owner was removed from the project. It stops running. | An admin takes ownership (see [Admin guide](#admin-guide)) |

Expiry stops forgotten workflows running forever. Renewing restarts the clock. Admins can change the period or turn expiry off.

A run where the query fails is still a run. Its status is `error`, but the actions still execute, so an action whose condition is `status == 'error'` can tell someone.

## Who can do what

| | Project member | Project owner | Viewer of a public project | Admin |
|---|---|---|---|---|
| See workflows | Yes | Yes | Yes | All projects |
| See run history and health | Yes | Yes | No | Yes |
| Create a workflow | Yes | Yes | No | In projects they are a member of |
| Edit, dry run | Own workflows | Own workflows | No | Any |
| Enable, pause, run now, renew, delete | Own workflows | Any in the project | No | Any |

Why so strict? A workflow runs as its owner. If someone else could change it, or change the SQL it runs, they could use the owner's access. That's also why the saved query must belong to the workflow's owner.

## Recipes

**Alert on any errors**

* Query: today's errors.
* Action `notify` (Email), Run when `rowCount > 0`, subject `{{rowCount}} errors today`, body `Hosts: {{rows[*].host}}`.

**Spike detection**

* Action `spike` (Record alert), Run when `deltaPercent > 50 && rowCount > 20`, message `Up {{deltaPercent}}% to {{rowCount}}`.
* Action `page` (Email), Run when **Always**, body `{{actions.spike.message}}`. It runs only when `spike` fired.

**Tell me when the job itself breaks**

* Action `broken` (Email), Run when `status == 'error'`, subject `Workflow query failed`, body `{{error}}`.

**Data disappeared**

* Action `empty` (Record alert), Run when `status == 'success' && rowCount == 0`, message `No rows today`.

**Daily snapshot table**

* Action `snapshot` (Save to table), Run when `status == 'success'`, mode `newTablePerRun`, format `parquet`. Query the history later with ``SELECT * FROM dfs.tmp.`schedules/<id>` ``.

**Slow query watch**

* Action `slow` (Record alert), Run when `durationMs > 60000`, message `Took {{durationMs}} ms`.

**Specific value in the results**

* Action `critical` (Email), Run when `rows[*].severity == 'CRITICAL'`, body `Critical rows from {{rows[*].source}}`.

## Admin guide

### Find every workflow

**Admin → All workflows** (`/workflows`) lists workflows across every project, with owner and project columns. It also shows:

* **Running now**: workflows currently mid-run, refreshed every 15 seconds.
* **Workflows without a project**: workflows migrated from old scheduled queries whose saved query was not in any project. Pick a project for each; only projects that already contain the workflow's saved query are offered. These workflows don't run until they have a project.
* **Settings**, described below.

### Set up email

Open **Preferences → SMTP** (subtitle "SMTP for alerts") and enter the mail server, port, credentials, sender address and encryption (`starttls` is the default; `ssl` or `none` also work). Every workflow uses these settings. Until they are set, every email action fails with "SMTP is not configured".

### Set up run history (observability)

The Health tab and the success-rate column read the run log with Drill SQL. An admin enables this once: on the **All workflows** page, click **Set up** in the banner. This creates the `dfs.workflow_logs` workspace pointing at Drill's log directory (`DRILL_LOG_DIR` must be set) and a JSON format for the log files. It works the same way as AI Analytics.

Once set up, the workspace is readable by every SQL user. The log holds workflow and project ids, owners, status, durations, row counts and per-action outcomes and errors. It never holds query results or email contents. Restrict the workspace if that is too open for you.

### Query the run log

Each run adds one JSON line to `workflow-runs.log` in the Drill log directory. The files roll daily and at 100 MB, and are kept for 90 days, up to 5 GB.

```json
{"ts":"2026-10-21T08:00:03Z","runId":"…","workflowId":"…","projectId":"…","owner":"alice",
 "drillbit":"node1:31010","trigger":"schedule","status":"success","rowCount":142,"durationMs":1830,
 "actions":[{"id":"flag","type":"record_alert","outcome":"fired","reason":null,"durationMs":2,"error":null}]}
```

Failures in the last week:

```sql
SELECT ts, workflowId, owner, status
FROM dfs.workflow_logs.`workflow-runs*.log`
WHERE status = 'error' AND ts >= '2026-10-14'
ORDER BY ts DESC
```

Success rate and average duration per workflow:

```sql
SELECT workflowId, COUNT(*) AS runs,
       SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS ok,
       AVG(durationMs) AS avg_ms
FROM dfs.workflow_logs.`workflow-runs*.log`
GROUP BY workflowId
ORDER BY runs DESC
```

Action failures, with their errors:

```sql
SELECT t.a.id AS action, t.a.type AS type, t.a.error AS error, COUNT(*) AS n
FROM (SELECT FLATTEN(actions) AS a FROM dfs.workflow_logs.`workflow-runs*.log`) t
WHERE t.a.outcome = 'errored'
GROUP BY t.a.id, t.a.type, t.a.error
ORDER BY n DESC
```

### Settings

**All workflows → Settings**:

| Setting | Default | |
|---|---|---|
| Expiration enabled | On | Workflows stop after the expiration period unless renewed |
| Expiration days | 90 | |
| Warning days before expiry | 14 | When the "expiring soon" warning appears |
| Action timeout (seconds) | 60 | Per action, per run |

### When an owner leaves

If a workflow's owner is removed from the project, the workflow stops with **Owner lost access**. To keep it:

1. Add yourself to the project.
2. Open the workflow. The **Status: owner lost access** banner has a **Take ownership** button (admins only). From then on it runs as you.
3. Point it at a saved query **you** own (save a copy of the original query). Until you do, every run fails with "Saved query is owned by X, workflow runs as Y."
4. Enable it.

Or delete it if it's no longer needed.

### Editing other people's workflows

Admins can edit any workflow. Editing does not change who it runs as; only **Take ownership** does that. The saved query must still belong to the workflow's owner, so an admin cannot point someone else's workflow at the admin's own query.

## Upgrading from scheduled queries

On first start after the upgrade, every old scheduled query is converted into a workflow automatically. The old schedules aren't deleted, and the conversion happens only once.

| Old setting | Becomes |
|---|---|
| The schedule (frequency, time, day, timeout), owner, retention, expiry | The same on the workflow |
| Save results to a table | A **Save to table** action writing to the same tables as before |
| Enabled alert rules (row count above or below, query failed, duration above, duration over the interval) | One **Record alert** action per rule, with the matching condition |
| Notification emails, on failure and on success | **Email** actions with `status == 'error'` or `status == 'success'` |
| Disabled alert rules, the AI summary placeholder | Dropped. They never produced anything. |

Check your converted workflows after upgrading:

* **Emails will start sending.** The old feature stored recipients but never sent mail. Converted email actions send for real once SMTP is configured.
* **Times may have moved.** Old schedules were in UTC; workflows use the server's time zone. Hourly schedules now respect the minute.
* **Monthly days 29 to 31 became the 28th.**
* **A query owned by someone else.** If the old schedule ran a query owned by another person, the workflow fails each run until it is pointed at a query its owner owns.
* **Queries in no project.** These schedules are converted but disabled, and appear under **Workflows without a project** for an admin to assign.

Old run history is still shown.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Save is disabled | There are errors. Each is shown on its field or in the list above the tabs. |
| "The workflow runs as X, so its saved query must be owned by X" | Choose a saved query the owner owns, or save a copy of the query as the owner. |
| Run fails: "Saved query is owned by X, workflow runs as Y" | The query or the owner changed after the workflow was created. Point the workflow at a query the owner owns. |
| Email action errors: "SMTP is not configured" | An admin needs to set up SMTP. |
| Email action skipped: `upstream flag skipped` | The email uses an output of `flag`, and `flag` did not fire on that run. That is by design. |
| Action skipped: `condition false` | The condition didn't match. Dry run shows the values it saw. |
| Condition on `deltaPercent` never fires | It is empty on the first run and when the previous count was 0. Add a second rule on `rowCount`. |
| Condition on `rows[*].x` never fires | The column name doesn't match (it is case sensitive), or the matching rows are not in the first 100. Sort the query so the important rows come first, or filter in SQL. |
| Run skipped: "previous run still in progress" | The previous run was still going. Shorten the query or schedule less often. |
| Workflow isn't running | Check it is **Enabled** and not Expired or Owner lost access, and check **Next run**. |
| "Run history is visible to project members" | You're viewing a public project you are not a member of. |
| Health tab asks for setup | An admin hasn't set up run history yet. |
| Save to table fails on the first run in `append` mode | Append needs an existing table. Run once with `overwrite`, or create the table. |

## Limits

| Limit | Value |
|---|---|
| Rows actions can see | First 100 (`rowCount` is still the full count) |
| Rows kept per stored run | 20 |
| Runs kept per workflow | 30 |
| Query timeout | 1 to 86,400 seconds, default 300 |
| Action timeout | 60 seconds by default |
| Expression length | 2,000 characters |
| Condition nesting | 50 levels |
| Monthly day | 1 to 28 |
| Run log retention | 90 days, 5 GB |
