# Logs

**File:** `src/pages/LogsPage.tsx`
**Route:** `/logs`

## Purpose

Browse Drillbit log files (raw text) and run pre-canned SQL queries against them via the `dfs.logs` schema. When Prospector is configured, the page links into a conversation in the user's Scratch project (see `conversations.md`); it has no chat of its own.

## Data sources

| API | Module |
|---|---|
| `GET /api/v1/logs` (`getLogFiles`) | `api/logs.ts` |
| `GET /api/v1/logs/{file}` (`getLogContent`) | `api/logs.ts` |
| `getLogDownloadUrl(file)` | `api/logs.ts` |
| `GET /api/v1/logs/sql/status` (`getLogSqlStatus`) | `api/logs.ts` |
| `POST /api/v1/logs/sql/setup` (`setupLogSql`) | `api/logs.ts` |
| `POST /query.json` (`executeQuery`) | `api/queries.ts` |
| `GET /api/v1/ai/status` (`getAiStatus`) | `api/ai.ts` |
| `GET /api/v1/projects/scratch` (via `useScratchProject`) | `api/conversations.ts` |

## Child components

- Tabs: `LogFilesTab`, `LogSqlTab`.
- `AskProspectorButton` (page header, and "Analyze" in the log drawer) and `useAskProspector` (the ERROR-line click). Each navigates to `/projects/<scratch>/chat`; they are disabled, with a message, if the Scratch lookup fails.
- `logSeeds.ts` — builds the seeded first turns below.

### LogFilesTab

- File list with date and size.
- Click to view content in a drawer (paginated for large files).
- Download link per file.
- **Analyze** (drawer header): starts a new Scratch conversation seeded with the last 200 lines of the open file (`analyzeSeed`).
- **Click an ERROR line**: starts a new Scratch conversation seeded with that line and five lines either side (`errorSeed`).
- Both seeds carry `feature: 'log_analysis'`, `logAnalysisMode`, `logFileName` and `logLines`, so the server's log-analysis prompt applies to that first turn. Later turns in the conversation use the ordinary project context, so the lines are not re-sent.
- A seed travels in router state (`{ seed }`) and is consumed once by `ConversationsPage`.

### LogSqlTab

- Pre-canned example queries (recent errors, frequency by hour, level distribution, top loggers).
- Free-form SQL editor against `dfs.logs.*`.
- Results in an AG Grid table.

## Key state

- `selectedLog` — current file
- `logContent` — fetched text
- `searchText`
- `selectedQueryExample` — pre-canned query picker
- `sqlResult` — output of log SQL query

## Behavior

- **`setupLogSql`** registers the `dfs.logs` workspace pointing at the log directory. The page calls `getLogSqlStatus` on mount and offers a one-click setup if it isn't configured.
- **No redaction.** Log lines are sent to the LLM provider as they appear in the file (`analyzeSeed` and `errorSeed` do not filter them), so credentials in a log reach the provider.
- **AI gating.** `getAiStatus()` decides whether the Ask Prospector, Analyze and ERROR-line affordances appear.

## Chrome

Breadcrumb: `Administration > Server Logs`. Toolbar: tab switcher, refresh. Right side: Ask Prospector.

## Quirks

- Large files are loaded as plain text; very large logs can be slow to render. The drawer paginates by line count to keep React responsive.
- The pre-canned queries assume `.drilllog` files in the configured directory; if logs are rotated to a different format, queries silently return zero rows.
