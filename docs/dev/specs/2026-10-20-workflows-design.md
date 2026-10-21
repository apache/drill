# Workflows: Scheduled Queries with Action Pipelines — Design

**Date:** 2026-10-20
**Status:** Approved design, pending implementation plan
**Replaces:** the schedule model described in [`../SCHEDULER.md`](../SCHEDULER.md)

## Goal

Let users (and Prospector) build recurring **workflows**: a saved query that runs on a
schedule, followed by an ordered list of **actions** that do something with the results:
rule-based alerts, email, Slack/webhook messages, AI summaries, AI-judged alerting, wiki
reports, persisting results. Workflows live inside projects, are fully creatable by AI,
and are observable per project and globally by admins.

### Requirements (from the user)

- Many post-run possibilities: rule actions, Slack/other messaging, summaries, AI alerting.
- Existing schedule fields are **migrated** into the new model (one system, not two).
- Actions see a **result preview** (first 100 rows + metadata), not the full result set.
- Actions are independent by default but **may reference outputs of earlier actions**.
- Conditions can be written as **structured rules and as expressions**.
- Workflows must be **completely creatable by AI** (Prospector), with matching UI.
- Workflows have a **dedicated page**, are **contained in projects** like saved queries.
- **Admins see all workflows.** Observability exists at project and admin level.

### Facts about the current system that shape the design

- `ScheduleManager` already runs saved queries hourly/daily/weekly/monthly with
  expiration, overlap protection, timeouts, snapshots (100-row preview, row delta),
  optional CTAS persistence, and threshold alert rules.
- `notifyEmails` / `notifyOnSuccess` / `notifyOnFailure` are stored but **never sent**;
  `SmtpConfig` exists but nothing calls it.
- Fired alert rules are only recorded on the snapshot (`triggeredAlerts`).
- `aiSummary` is a stub (`ScheduleManager.java`, "AI summary pending").
- Every Drillbit runs its own `ScheduleManager`, so multi-node clusters run each
  schedule once per node.
- `LlmProvider.streamChatCompletion(config, messages, tools, OutputStream, observer)` is
  plain Java and callable from a background thread; no HTTP self-call needed.
- Prospector tools are defined client-side in `hooks/useProspector.ts` and call REST.
- Projects have `owner` + `sharedWith`; AI Analytics uses a JSONL log queried via Drill SQL.

## Build order (sub-projects)

1. **Pipeline core** — workflow document, migration, runner, conditions (tree + expression),
   `record_alert` / `save_to_table` / `email`, REST, run log, editor + list + observability UI.
2. **Messaging** — `slack`, `webhook`, project-scoped notification credentials.
3. **AI actions** — `ai_summary`, `ai_judge`, `wiki_report`, AI budget caps.
4. **Prospector tools** — workflow tools, "Describe a workflow" entry point.

2–4 depend only on 1. Each gets its own implementation plan.

---

## 1. The workflow document

One JSON shape shared by UI, REST, and Prospector. Replaces `QueryScheduleModel`.

```json
{
  "id": "wf-…",
  "projectId": "proj-…",
  "name": "Failed login watch",
  "description": "…",
  "owner": "cgivre",
  "savedQueryId": "q-…",
  "schedule": {
    "frequency": "daily",
    "timeOfDay": "08:00",
    "dayOfWeek": null,
    "dayOfMonth": null,
    "timeoutSeconds": 300,
    "enabled": true,
    "expiresAt": "…"
  },
  "actions": [
    { "id": "sum", "type": "ai_summary", "condition": { "always": true },
      "config": { "prompt": "Summarize notable changes" } },
    { "id": "judge", "type": "ai_judge", "condition": { "always": true },
      "config": { "question": "Is anything unusual vs. the prior run?" } },
    { "id": "notify", "type": "slack",
      "condition": { "expr": "actions.judge.verdict && rowCount > 0" },
      "config": { "channelRef": "secops-webhook", "message": "{{actions.sum.text}}" } }
  ],
  "retentionCount": 30,
  "status": "active",
  "lastRunAt": "…",
  "nextRunAt": "…"
}
```

### Conditions

Exactly one of:

- `{ "always": true }`
- `{ "tree": <node> }` where `<node>` is
  `{ "all": [node…] }` | `{ "any": [node…] }` | `{ "not": node }` |
  `{ "field": "<path>", "op": "<op>", "value": <literal> }`
- `{ "expr": "<expression>" }`

Ops: `== != > >= < <= contains startsWith exists`.
Field paths: `status`, `rowCount`, `rowCountDelta`, `deltaPercent`, `durationMs`, `intervalMs` (schedule interval), `error`,
`rows[*].<column>` (any-row match), `actions.<id>.<output>`.

Expression grammar (hand-written recursive-descent parser, no dependency):

```
expr    := or
or      := and ( "||" and )*
and     := unary ( "&&" unary )*
unary   := "!" unary | cmp
cmp     := operand ( OP operand )?
operand := literal | path | "(" expr ")"
path    := ident ( "." ident | "[*]" )*
literal := number | 'string' | "string" | true | false | null
```

On save, the server parses `expr` to a tree and stores both (`expr` for display,
`tree` for evaluation). The evaluator only ever sees trees. Janino is not used: it compiles
arbitrary Java and is unsafe for user input.

### Action types

Each action type is a Java class registered in an `ActionTypeRegistry` and declares:

- `id` (e.g. `slack`), display name, description
- `configSchema` — JSON Schema for `config`; drives server validation, the editor form,
  and Prospector's knowledge of the type
- `outputs` — name → type (e.g. `ai_judge`: `verdict: boolean, reason: string, severity: string`);
  drives template autocomplete and reference validation
- `hasSideEffects` — whether dry run must suppress it
- `execute(RunContext ctx, Map<String,Object> config, boolean dryRun) → ActionResult`

### Templates

String config fields may contain `{{path}}` placeholders using the same paths as
conditions. Rendering is plain substitution (no logic); missing paths render as empty
and are a validation error at save time.

### Validation (server-side, on every save / validate / dry-run)

- Unknown action type, duplicate action id, config not matching schema.
- Condition `expr` that fails to parse (error carries character position).
- References (in conditions or templates) to a later action, to the action itself, or to
  an output the referenced type does not declare.
- `savedQueryId` not in the workflow's project.
- `channelRef`/credential refs that do not exist in the project.

Errors are returned as `422 { "errors": [ { "path": "actions[2].config.message", "message": "…" } ] }`.

### Secrets

Configs never hold secrets inline. Fields ending in `Ref` name a project-scoped credential
(Slack webhook URL, webhook URL + headers, etc.). Credential storage is specified in
sub-project 2. SMTP stays global in `SmtpConfig`.

### Storage and migration

- New store `drill.sqllab.workflows`.
- On first start, each `drill.sqllab.schedules` entry is converted once:

| Old field(s) | Becomes |
|---|---|
| `notifyOnFailure` + `notifyEmails` | `email` action, condition `status == "error"` |
| `notifyOnSuccess` + `notifyEmails` | `email` action, condition `status == "success"` |
| each rule in `alertRules` | `record_alert` action with the equivalent tree condition |
| `persistResults` + `result*` | `save_to_table` action |
| `aiSummaryEnabled` + prompt + maxRows | `ai_summary` action |
| `frequency`, `timeOfDay`, `dayOf*`, `timeoutSeconds`, `enabled`, `expiresAt` | `schedule.*` |

- `projectId` is set to the project that contains the saved query. Schedules whose query
  is in no project become **orphans**: visible to admins only, assignable to a project.
- The old store is left untouched (read-only backup). A marker key prevents re-migration.
- Existing snapshots keep working; `scheduleId` is reused as `workflowId`.

---

## 2. Execution pipeline

`ScheduleManager` is renamed `WorkflowRunner` (same daemon thread, same 60 s tick, same
startup/shutdown hooks in `Drillbit`).

For each due workflow:

1. **Claim** — `runClaims.putIfAbsent("<workflowId>:<scheduledTime>", drillbitId)`.
   If false, another Drillbit owns this slot; skip. ZK-backed stores make this atomic
   across the cluster. Claims older than 7 days are pruned on each tick.
2. **Query** — run the saved query as the workflow owner (`effectiveUserName`) with the
   schedule timeout, capturing a 100-row preview. Build
   `RunContext { status, rowCount, rowCountDelta, deltaPercent, durationMs, error, columns, rows }`
   (delta from the previous snapshot). A failed query sets `status = "error"`; actions
   still run.
3. **Actions**, in list order. For each:
   - If it references an action whose outcome is not `fired`: outcome `skipped`,
     reason `upstream <id> <outcome>`.
   - Else evaluate the condition against `RunContext` + prior outputs. False:
     `skipped`, reason `condition false`.
   - Else `execute()` with a per-action timeout (default 60 s; AI actions use the AI
     timeout from config). Success: `fired` with outputs. Exception/timeout: `errored`
     with message. Never aborts later actions.
4. **Record**
   - Snapshot (existing store, retention unchanged) now also holds per-action
     `{id, type, outcome, reason, outputs, error, durationMs}`. Outputs live here because
     they may contain data; snapshots are guarded by project permissions.
   - One JSONL line appended to `workflow-runs.log` in `DRILL_LOG_DIR`:
     `{ts, runId, workflowId, projectId, owner, drillbit, trigger, status, rowCount, durationMs,
       actions:[{id, type, outcome, reason, durationMs, error}]}`.
     No outputs, no preview rows.
   - Update `lastRunAt`, `nextRunAt`, consecutive-failure counter.

**Dry run** — same pipeline with `dryRun = true`: no claim, no snapshot, no log line.
Conditions are evaluated and AI actions do execute (their verdicts are the point).
Actions with `hasSideEffects` return a rendered preview (`wouldSend: …`) instead of acting.
The full result (context + per-action outcomes) is returned to the caller.

**Run now** — real pipeline, `trigger = "manual"`, claim key `<workflowId>:manual:<ts>`.

**Expiration**, overlap protection (`isRunning`), and the 10 s shutdown grace are kept
as-is.

---

## 3. Permissions and REST API

A workflow **runs as its owner**. Editing someone else's workflow would let you run SQL
with their access, so edit rights are narrow.

| Who | View / history / dry-run | Create | Edit | Pause / delete |
|---|---|---|---|---|
| Project member (owner or `sharedWith`) | yes | yes (becomes owner) | own only | own only |
| Project owner | yes | yes | own only | any in project |
| Admin | all projects | yes | any | any |

- Admin edits never change the run-as identity. `take-ownership` does that explicitly.
- If the owner loses project access, the next tick sets `status = "owner_lost_access"` and
  disables the workflow.
- Credential refs resolve only within the workflow's project.

### Endpoints (`/api/v1/workflows`)

| Method | Path | Purpose |
|---|---|---|
| GET | `?projectId=` | List. Non-admins must pass a project they can view; admins may omit for all. |
| POST | `/` | Create (validated). |
| GET / PUT / DELETE | `/{id}` | Read / update (validated) / delete with snapshots. |
| POST | `/validate` | Validate a document without saving. |
| POST | `/parse-condition` | `{expr}` → `{tree}` or `{error, position}`. |
| POST | `/dry-run` | Dry-run an unsaved document. |
| POST | `/{id}/dry-run` | Dry-run a saved workflow. |
| POST | `/{id}/run` | Run now. |
| POST | `/{id}/renew` | Renew expiration. |
| POST | `/{id}/take-ownership` | Admin: become run-as owner. |
| POST | `/{id}/assign-project` | Admin: assign an orphan. |
| GET | `/{id}/runs` | Snapshot list. |
| GET | `/{id}/runs/{runId}` | Snapshot detail with action outcomes and outputs. |
| GET | `/action-types` | Registry: schemas + outputs. |
| GET | `/observability/summary?projectId=` | Health rollup (Drill SQL over the run log). |
| GET | `/observability/running` | Admin: running/queued now. |
| GET / PUT | `/config` | Admin: expiration, timeouts, AI caps. |
| POST | `/observability/setup` | Admin: one-time `dfs` workspace setup for the run log. |

`/api/v1/schedules/*` stays as a deprecated alias for one release, mapping onto workflows.

### Observability queries

`workflow-runs.log` is queried through a `dfs.workflow_logs` workspace with a JSON format
on `.log`, set up once by an admin (same pattern as `AiAnalyticsResources` /
`LogsResources.setupLogsSql()`). The summary endpoint returns per-workflow: runs, success
rate (30 d), consecutive failures, avg/p95 duration, action fire rates; and AI tokens/cost
by joining AI Analytics events where `feature = 'workflow'` and `source = workflowId`.
If the workspace is not set up, the endpoint returns snapshot-derived basics and a
`setupRequired` flag.

---

## 4. UI

### Routes

- `/projects/:id/workflows` — project list (replaces the wrapper; tab always visible).
- `/projects/:id/workflows/new`, `/projects/:id/workflows/:wid` — editor/detail.
- `/workflows` — admin, all projects, under **Administration** in the sidebar.
  Non-admins are redirected to `/projects`.

### List page (project and admin)

- Health strip: total, active, failing (≥ 2 consecutive errors), expiring, AI cost (7 d).
- Table: name, query, schedule ("Daily 08:00"), last status, success rate (30 d),
  next run, action-type icons. Admin view adds project and owner columns and filters.
- Failing and expiring sort to the top.
- "Describe a workflow…" input (project view) → Prospector draft mode (section 5).

### Editor / detail page — tabs

1. **Definition**
   - Name, description, saved-query picker (project queries only), schedule controls.
   - Action list: draggable cards showing type icon, id, one-line condition summary,
     recent fired/skipped counts. "Add action" opens a type picker from `/action-types`.
   - Expanded card:
     - **Condition**: `Always` / `Builder` / `Expression` toggle. Builder edits the tree
       (field/op/value rows, AND/OR groups). Expression uses Monaco with debounced
       `/parse-condition` and inline error markers. Switching Expression → Builder works
       when the parse succeeds; Builder → Expression renders the tree to text.
     - **Config**: form generated from the type's `configSchema`; template fields offer
       `{{…}}` autocomplete from run-context fields and earlier actions' outputs.
   - Server validation errors render inline at their `path`.
   - **Dry run** button → side panel: preview rows, then per action the outcome, skip
     reason, outputs, or `wouldSend` text.
   - **Save** disabled until `/validate` passes. **Enabled** is a separate switch.
2. **Runs** — timeline of snapshots; selecting one shows preview rows and per-action
   outcomes with outputs.
3. **Health** — success rate and duration over time, per-action fire rates, AI
   tokens/cost (ECharts, consistent with AI Analytics).

### Admin extras on `/workflows`

"Running now / queued" panel, overrun list (duration > interval), orphan list with
assign-to-project, link to global config.

### Removed

`ScheduleModal` is deleted. "Schedule this query" from SQL Lab / saved queries navigates to
`/projects/:id/workflows/new?query=<savedQueryId>`.

---

## 5. Prospector integration

New client-side tools in `useProspector.ts`, offered only when the chat has a project
context:

| Tool | Calls | Notes |
|---|---|---|
| `list_workflow_action_types` | `GET /action-types` | Model learns the catalog at runtime. |
| `list_notification_channels` | project credentials (filtered) | Names only, never secrets. |
| `list_workflows`, `get_workflow` | `GET` | Supports editing existing workflows. |
| `create_workflow`, `update_workflow` | `POST` / `PUT` | 422 errors returned as tool result for self-correction; max 3 retries. |
| `dry_run_workflow` | `POST /dry-run` or `/{id}/dry-run` | Model dry-runs before finishing. |

Combined with the existing `save_query`, a single conversation can write the SQL, save it,
and build the workflow.

**Guardrails**

- Prospector-created workflows are always saved with `schedule.enabled = false`. The tool
  result links to the editor; a human enables.
- `update_workflow` on an enabled workflow disables it and says so in the result.
- Prospector acts as the current user; section 3 permissions apply unchanged.

**Draft mode** — the list page's "Describe a workflow…" opens Prospector with a system
addendum (goal: produce a workflow; call `list_workflow_action_types` first; dry-run before
finishing). On completion the editor opens on the draft. `PROSPECTOR.md` dialect notes gain
a workflows section.

---

## 6. Action catalog and AI runtime

| Type | Sub-project | Outputs | Behavior |
|---|---|---|---|
| `record_alert` | 1 | `message` | Templated message recorded on the snapshot; shown as a badge. |
| `save_to_table` | 1 | `path` | CTAS/INSERT, ported from `persistResults`. |
| `email` | 1 | `sent` | Templated subject/body to listed recipients via `SmtpConfig`. |
| `slack` | 2 | `status` | Incoming webhook from project credential; templated text. |
| `webhook` | 2 | `status`, `body` | POST JSON (template, or default RunContext payload); URL + headers from credential. |
| `ai_summary` | 3 | `text` | LLM summary of the preview. |
| `ai_judge` | 3 | `verdict`, `reason`, `severity` | LLM returns strict JSON; malformed output → `errored`, never `true`. |
| `wiki_report` | 3 | `pageId` | Writes a page to the project wiki via the Prospector Reports code. |

Teams, Discord, PagerDuty, etc. use `webhook` until a dedicated type is warranted. A new
type is one class plus its schema.

### AI runtime (sub-project 3)

- Calls `LlmProviderRegistry.get(config.getProvider()).streamChatCompletion(...)` with a
  `ByteArrayOutputStream`, using the global AI config. No AI config → AI actions `errored`
  with a clear message; the rest of the workflow runs.
- Prompt: action prompt/question, columns, preview rows (truncated to ~8K tokens),
  rowCount, delta, and the previous run's `ai_summary` text if present (enables "vs. last
  run").
- Admin caps in `/config`: max tokens per action, max AI calls per workflow per day,
  global daily AI budget. Exceeded → `errored` with `AI budget exceeded`.
- Every call logged via `AiEventLogger` with `feature = "workflow"`, `source = workflowId`.

---

## 7. Testing

- **Unit (Java):** expression parser (precedence, literals, error positions, tree
  round-trip); condition evaluator; reference validation; migration of each old-field
  combination; permission matrix; claim dedup with an in-memory store; template rendering.
- **Pipeline:** a recording test `ActionType` to verify ordering, skip propagation,
  error isolation, dry-run suppression of side effects, per-action timeout.
- **Effects:** local HTTP server for `slack`/`webhook`; SMTP stub or mocked `Transport`
  for `email`; fake `LlmProvider` for `ai_*`, including malformed JSON for `ai_judge`.
- **UI (vitest):** builder ⇄ tree conversion, tree → expression rendering, template
  autocomplete source list. One end-to-end path: create → dry-run → save.
- `mvn checkstyle:check -pl exec/java-exec`, license headers on all new files.

## 8. Documentation

- `docs/dev/WORKFLOWS.md` — backend (replaces `SCHEDULER.md`, which becomes a pointer).
- `docs/dev/ui/pages/workflows.md` (rewrite), `docs/dev/ui/pages/workflow-editor.md` (new),
  `docs/dev/ui/pages/project-workflows.md` (rewrite).
- `docs/dev/PROSPECTOR.md` — workflow tools.
- Root `CLAUDE.md` docs table — add Workflows row.

## Out of scope

- Full-result-set access for actions (use `save_to_table`).
- Workflows chained to other workflows / event triggers (non-time-based).
- Cron expressions beyond hourly/daily/weekly/monthly.
- Per-project AI provider configuration.
