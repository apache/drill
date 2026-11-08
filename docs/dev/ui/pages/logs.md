# Logs

**File:** `src/pages/LogsPage.tsx`
**Route:** `/logs`

## Purpose

Browse Drillbit log files (raw text) and run pre-canned SQL queries against them via the `dfs.logs` schema. When Prospector is configured, an Ask Prospector button opens a conversation in the user's Scratch project; the page has no chat of its own.

## Data sources

| API | Module |
|---|---|
| `GET /api/v1/logs` (`getLogFiles`) | `api/logs.ts` |
| `GET /api/v1/logs/{file}` (`getLogContent`) | `api/logs.ts` |
| `getLogDownloadUrl(file)` | `api/logs.ts` |
| `GET /api/v1/logs/sql/status` (`getLogSqlStatus`) | `api/logs.ts` |
| `POST /api/v1/logs/sql/setup` (`setupLogSql`) | `api/logs.ts` |
| `POST /query.json` (`executeQuery`) | `api/queries.ts` |
| `POST /api/v1/ai/chat` (`streamChat`) | `api/ai.ts` |

## Child components

- Tabs: `LogFilesTab`, `LogSqlTab`.
- `AskProspectorButton` — navigates to `/projects/<scratch>/chat`; disabled with a message if the Scratch lookup fails. The page no longer hosts a chat, and the per-line and Analyze affordances that fed log lines to the dock were removed.

### LogFilesTab

- File list with date and size.
- Click to view content in a drawer (paginated for large files).
- Download link per file.
- AI analyze button → opens a chat with the log content as context.

### LogSqlTab

- Pre-canned example queries (recent errors, frequency by hour, level distribution, top loggers).
- Free-form SQL editor against `dfs.logs.*`.
- Results in an AG Grid table.
- AI analysis of results.

## Key state

- `selectedLog` — current file
- `logContent` — fetched text
- `searchText`
- `selectedQueryExample` — pre-canned query picker
- `sqlResult` — output of log SQL query
- `aiStream` — markdown response accumulator

## Behavior

- **`setupLogSql`** registers the `dfs.logs` workspace pointing at the log directory. The page calls `getLogSqlStatus` on mount and offers a one-click setup if it isn't configured.
- **`redactCredentials()`** sanitizes log lines before sending to the AI to keep secrets out of the LLM provider.
- **AI gating.** `getAiStatus()` decides whether the AI buttons are enabled.

## Chrome

Breadcrumb: `Administration > Server Logs`. Toolbar: tab switcher, refresh. Right side: Ask Prospector.

## Quirks

- Large files are loaded as plain text; very large logs can be slow to render. The drawer paginates by line count to keep React responsive.
- The pre-canned queries assume `.drilllog` files in the configured directory; if logs are rotated to a different format, queries silently return zero rows.
