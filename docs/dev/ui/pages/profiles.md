# Profiles

Covers the query history list and the per-query detail view.

## ProfilesPage

**File:** `src/pages/ProfilesPage.tsx`
**Route:** `/profiles`

### Purpose

Real-time query activity monitor. Lists running and recently-finished queries grouped by day. Filter by user, type, status. Bulk-cancel running queries. Click into [`ProfileDetailPage`](#profiledetailpage) for plan, timing, and AI analysis.

### Data sources

| API | Module |
|---|---|
| `GET /profiles.json` (`getQueryProfiles`) | `api/profile.ts` |
| `GET /profiles/cancel/:queryId` (cancel) | `api/profile.ts` |

`getQueryProfiles` returns `{ runningQueries, finishedQueries }`. The page polls with `refetchInterval: 5000`.

### Child components

- `DayGroup` — sections grouped by date (`Today`, `Yesterday`, weekday names, or absolute dates).
- Per-row layout: time, type, user, SQL snippet (first 80 chars), duration, status pill, optional bulk-select checkbox.
- Status filter chips.

### Key state

- `searchText`, `statusFilter`, `userFilter`, `typeFilter`
- `selectedRunningIds: Set<string>` for bulk cancel
- `isCancelling`
- Derived: `allUsers`, `allTypes` from the response — populate the filter dropdowns

### Behavior

- **Polling.** 5-second auto-refresh keeps the list live.
- **Sorting.** Running queries float to the top (pulsing dot indicator).
- **Bulk cancel.** Selecting checkboxes reveals the bulk action bar; cancellation loops over each id calling `cancel`. Each cancel is independent — partial failures are tolerated.
- **Query-type classification.** `detectQueryType()` parses the first non-comment line and tags SELECT/INSERT/UPDATE/DELETE/CREATE/DROP/ALTER/SHOW/EXPLAIN/USE/SET/Other.
- **Day grouping.** `dayKey()` (`YYYY-M-D`) is the bucket; `dayLabel()` maps to `Today` / `Yesterday` / weekday / date.

### Chrome

Breadcrumb: `Library > Query History`. Toolbar: filter chips, refresh indicator.

### Quirks

- Pulsing dot is pure CSS animation triggered by an `is-running` class.
- Cancellation can race — if a query finishes between fetch and cancel click, the cancel call returns 4xx but the UI just removes it from the running set silently.

---

## ProfileDetailPage

**File:** `src/pages/ProfileDetailPage.tsx`
**Route:** `/profiles/:queryId`

### Purpose

Deep-dive on a single query: plan tree, timing breakdown, memory/CPU metrics, fragment profiles, AI-powered analysis. Export timeline.

### Data sources

| API | Module |
|---|---|
| `GET /profiles/:queryId.json` (`getQueryProfileDetail`) | `api/profile.ts` |
| `POST /api/v1/ai/chat` (`streamChat`, SSE) | `api/ai.ts` |

### Child components

- Plan tree (ECharts tree chart) for the parsed plan.
- Tabs: Metrics, Fragments, Errors.
- AI analysis markdown pane (rendered by `react-markdown`).

### Key state

- `queryResult`, `queryLoading`, `queryError`
- AI: `aiStream` (accumulating markdown), `aiError`, `aiLoading`, `aiAbortController`

### Behavior

- **Credential redaction.** Before sending SQL to the AI, `redactCredentials()` strips API keys, passwords, connection strings, IPs, and emails so they don't leak into the LLM provider.
- **Plan parsing.** The text plan is parsed by indentation depth into a tree (`TreeNode`).
- **Timing.** Nanoseconds via `fmtNanos`, bytes via `fmtBytes`.
- **Fragment table.** Shows operator chains per minor fragment, sortable by duration.

### Chrome

Breadcrumb: `Library > Query History > <queryId>`. Toolbar: Export Timeline, AI Analyze.

### Quirks

- The plan is text, not structured JSON, so the tree parser is line-based; malformed plans may produce a degenerate tree. The raw plan tab is always available as a fallback.
- AI analysis streams; cancelling navigates away and aborts the controller. The hook does not preserve partial responses across navigations.
- Large profiles can be ~MB-scale JSON; the page does not virtualize the fragment table, so very large profiles can be slow.
