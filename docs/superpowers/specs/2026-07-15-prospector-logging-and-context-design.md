# Prospector: complete interaction logging and project-aware context

Date: 2026-07-15
Status: approved, pending implementation plan

## Problem

Two independent defects in the Prospector AI assistant.

**1. The AI Analytics dashboard does not show all interactions.** Three separate
causes, often mistaken for one:

- *Attribution collapse.* Nine components call `streamChat` without any feature
  tag, so every call lands in the dashboard as `prospector_chat`. Dashboard Q&A,
  wiki generation, and SQL Lab chat are indistinguishable.
- *Double counting.* `query_suggestions`, `explain_query`, and `optimize_query`
  log a client-side event **and** trigger a server-side `prospector_chat` event
  for the same LLM call. `totalCalls`, `totalTokens`, and the cost projection are
  inflated, and they mix real token counts with `Math.ceil(len/4)` estimates.
- *Blind spots.* `/api/v1/ai/config/test` issues a real billable LLM call and
  logs nothing. The `chat` guards (Prospector disabled, unknown provider) and the
  outer catch return before any event is written, so config failures are
  invisible.

Separately, `AiAnalyticsResources.isReady()` returns HTTP 200 with empty arrays
when `DRILL_LOG_DIR` is unset or `ai-events.log` is absent — indistinguishable
from "no AI usage". It stats only the active file, so a rolled log reports zero
while the data sits in archives that the read glob would have matched.

**2. User-authored context never reaches the LLM.** `buildMessages`
(`ProspectorResources.java:532-741`) is a pure function of the JSON the browser
posts. The server never opens the projects store, and the client never sends
`projectId` — `SqlLabPage.tsx:398-405` maps datasets down to
`{type, schema, table, label}`, discarding everything descriptive. So project
descriptions, tags, wiki markdown, and saved-query descriptions are absent from
every chat prompt.

The precedent already exists but is ad-hoc: `ProjectWikiPage.tsx:207` and
`QuerySuggestions.tsx:358` both inline `project.description` into hand-built
prompt strings, bypassing the chat context path entirely.

### Scope decision

Dataset descriptions and column descriptions **do not exist as a feature**.
`DatasetRef` is `{id, type, schema, table, savedQueryId, label}`; there is no
column annotation store anywhere. This spec wires up the metadata that exists
today and does not add authoring UI. Building a dataset/column data dictionary
is a separate project, to be considered after we learn whether context injection
measurably helps.

## Part 1 — Logging: exactly one server-owned event per call

### 1.1 Feature tagging

Add `feature` to `ChatContext` (both `types/ai.ts` and the Java
`ProspectorResources.ChatContext`). Each call site sets it:

| Call site | `feature` |
|---|---|
| `useProspector.ts` (SQL Lab chat) | `sql_lab_chat` |
| `GlobalProspectorTab.tsx` | `global_chat` |
| `QuerySuggestions.tsx` | `query_suggestions` |
| `AiAssistantModal.tsx` (explain) | `explain_query` |
| `AiAssistantModal.tsx` (optimize) | `optimize_query` |
| `AiQnAPanel.tsx` | `dashboard_qna` |
| `ExecutiveSummaryPanel.tsx` | `executive_summary` |
| `NlFilterPanel.tsx` | `nl_filter` |
| `AiAlertsPanel.tsx` | `ai_alerts` |
| `ProjectWikiPage.tsx` | `wiki_generation` |
| `ProfileDetailPage.tsx` | `profile_analysis` |
| `FileSystemForm.tsx` | `filesystem_form` |
| `SqlLabPage.tsx:528` | `sql_lab_optimize` |

`recordEvent` (`ProspectorResources.java:524`) reads `ctx.feature`, defaulting to
`prospector_chat` when absent so old clients degrade rather than break.

The dashboard doc (`docs/dev/ui/pages/ai-analytics.md:43`) advertises a
`format_sql` label that no code emits. Remove it.

### 1.2 Delete client-side chat logging

Remove the `logAiInteraction` calls from `QuerySuggestions.tsx:375,383,392` and
`AiAssistantModal.tsx:107,112,228,233`. Every LLM call goes through
`/api/v1/ai/chat`, which now logs authoritatively with real token counts.

This leaves `aiObservability.ts` (354 lines) and `AiLogsResources`
(`POST /api/v1/ai/logs`) with no callers — verified: those two components are the
only importers of the service, the service is the only caller of the endpoint,
and its `getLogs()` localStorage accessor has no consumers. Delete both, along
with the `?ai_logs=false` / `localStorage.drill_ai_logs_enabled` gates.
Rationale: a client sink that is
opt-outable, batches with `sendBeacon` (dropping pending events when a tab dies),
and reports estimated tokens is strictly worse than the server record it
duplicates. Retaining it as dead code invites future double counting.

### 1.3 Close the blind spots

Log an event at the two `chat` guards (`ProspectorResources.java:347` disabled,
`:355` unknown provider) and in the outer catch (`:409`), with `success=false`
and an `errorClass` of `ProspectorDisabled` / `UnknownProvider` / the exception's
simple name. These are zero-token events; they exist so setup failures appear.

Log `/api/v1/ai/config/test` (`AiConfigResources.java:604`), whose
`provider.validateConfig()` → `probe()` sends a real `max_tokens:1` request.
Feature: `config_test`.

### 1.4 Correctness fixes

**Success must mean success.** `recordEvent` currently sets
`event.success = failure == null` (`ProspectorResources.java:508`). The stream
lambda catches `Exception`, not `Throwable`, so an `Error` (e.g. `OutOfMemoryError`)
leaves `failure` null while `finally` still runs — logging a **successful,
zero-token call**. Change to:

```java
event.success = failure == null && callResult != null;
```

**Cancellations must reconcile.** `success=false` is set for client
cancellations, and `/summary` excludes them from failures, but `totalCalls` is a
bare `COUNT(*)`. Consequently `successCount + failureCount != totalCalls`
whenever cancellations exist, which reads as missing data.

Add a `cancelledCount` to the `/summary` payload and surface it in the dashboard,
so `success + failure + cancelled == total` holds. Do **not** fix this by
excluding cancellations from `totalCalls`: a cancelled call can still have
consumed tokens upstream, so suppressing it would hide real usage and
under-report cost — the opposite of this spec's goal.

**Readiness must be honest.** `isReady()` (`AiAnalyticsResources.java:436`)
gates `/summary` and `/events` and silently yields empty arrays. Two changes:
return an explicit `notConfigured` status in the payload so the UI can say
"analytics not configured" rather than "no usage"; and check for any
`ai-events*.log` (matching the read glob) instead of only the active
`ai-events.log`, so rolled logs still register.

## Part 2 — Context: server reads the stores

### 2.1 Client sends `projectId`

Add `projectId` to `ChatContext` (TS + Java). Populate it in
`SqlLabPage.tsx:381-427` (the prop already exists at `:127` and is currently
unused for AI) and in `GlobalProspectorTab.tsx:30`, which sends `{}` today and
therefore gains project awareness for free.

### 2.2 Server injects a compact project block

`ProspectorResources` already injects `storeProvider` and uses a cached-store
pattern (`getStore()`, `:793`). Add the same for `drill.sqllab.projects`
(`ProjectResources.Project`) and `drill.sqllab.saved_queries`
(`SavedQueryResources.SavedQuery`).

When `ctx.projectId` is set, `buildMessages` appends, after the existing
`projectDatasets` block:

- project name, description, tags
- for each project saved query: name, description, SQL (truncated ~500 chars)
- **wiki page titles only**, with a line stating the full text is available via
  `get_project_docs`

Budget: the whole block is capped (~2000 chars) and truncated with an explicit
marker. This matters because `useProspector` runs up to 15 tool rounds
(`maxToolRounds`, `useProspector.ts:36`) and **re-sends the full system prompt on
every round** — anything inlined is multiplied by up to 15× per question.

Failure to load a project must never fail the chat: log at debug and inject
nothing.

### 2.3 `get_project_docs` tool

Wiki pages are expected to be large or of unknown size, so full markdown is
fetched on demand rather than inlined.

Add to `TOOL_DEFINITIONS` (`useProspector.ts:38-142`) and execute it client-side
(`useProspector.ts:190-289`) like every existing tool, calling the existing
`GET /api/v1/projects/{id}/wiki` via `api/projects.ts`:

```
get_project_docs(pageTitle?: string) -> { title, content }[]
```

No `pageTitle` returns the page list. Content is capped (~8000 chars/page) with a
truncation marker. Listing the titles in the inlined block (2.2) is what makes
the model reach for this tool; a tool the model never learns exists is dead code.

### 2.4 Notebook context (approved fix)

`types/ai.ts:45-56` sends `notebookMode`, `notebookDfName`, `notebookDfShape`,
`notebookColumns`, `notebookCellCode`, `notebookCellError`, populated at
`SqlLabPage.tsx:407-425`. The Java `ChatContext` has no matching fields, so
Jackson discards them and they never reach the LLM. Add the six fields and emit
them in `buildMessages` under a notebook section.

### 2.5 `sendDataToAi` (approved fix)

`useProspector.ts:210` returns `rows: result.rows?.slice(0, 5)` from the
`execute_sql` tool **unconditionally**, ignoring the `sendDataToAi` privacy flag
that the optimize path honours (`SqlLabPage.tsx:508`). Thread the flag into
`useProspector` and omit `rows` (keep columns/types/rowCount) when it is off.
Apply the same gate to `appendDashboardData` (`ProspectorResources.java:767-776`),
which serializes sample rows regardless — gated server-side via a new
`sendDataToAi` boolean on `ChatContext`.

Project descriptions and wiki markdown are user-authored prose, not queried data,
and are injected ungated.

## Testing

Server (`exec/java-exec`, JUnit):

- `recordEvent` uses `ctx.feature`; falls back to `prospector_chat` when absent.
- `success=false` when `callResult` is null even if no exception was thrown
  (the `Error` path).
- Cancellation produces `cancelled=true` and `errorClass=ClientCancelled`; a
  `/summary` over mixed rows satisfies `success + failure + cancelled == total`.
- Both `chat` guards and the outer catch emit an event.
- `buildMessages` with a `projectId` injects name/description/tags, saved-query
  descriptions, and wiki titles; respects the size cap; injects nothing and does
  not throw when the project is missing.
- `buildMessages` emits notebook fields when `notebookMode` is set.
- `appendDashboardData` omits sample rows when `sendDataToAi=false`.
- `isReady()` reports `notConfigured` distinctly from "no events", and detects a
  rolled-only `ai-events-*.log`.

Frontend (`vitest`):

- Each call site sends its expected `feature`.
- `execute_sql` omits `rows` when `sendDataToAi` is off and retains columns/types.
- `get_project_docs` returns the page list with no argument and caps page content.

Manual: run a chat from SQL Lab and from the global tab, confirm one event each
in the dashboard with the right feature and non-estimated tokens, and confirm no
duplicate rows for explain/optimize.

## Out of scope (follow-ups)

- Dataset descriptions and a column-level data dictionary (new stores + UI).
- `AiAnalyticsResources` builds SQL by string interpolation (`appendEqClause`
  `:465`, `buildWhereClause` `:454`) behind an `escapeSql` and an `isIsoLike`
  character sniff. Admin-only and the sniff rejects quotes, but it should be
  parameterized. Tracked separately.
- Partial-output loss: on a mid-stream failure, `callResult` is null, so tokens
  and any text already streamed are lost from the event.
