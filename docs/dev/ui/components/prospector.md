# Prospector (frontend)

Prospector is Drill's chat-based AI assistant — used on the project Conversations page (SQL Lab and Logs hand off to it through Ask Prospector). This doc covers the frontend architecture. For backend (LLM provider registry, REST endpoints, persistent store) see [`../../PROSPECTOR.md`](../../PROSPECTOR.md). For the SQL transpiler used by the "Optimize" flow, see [`../../TRANSPILER.md`](../../TRANSPILER.md).

## Architecture overview

```
┌────────────────────┐
│  ProspectorPanel   │      ← React UI component
│  (chat messages +  │
│   input + actions) │
└─────────┬──────────┘
          │ consumes
          ▼
┌────────────────────┐
│  useProspector     │      ← state + tool-execution orchestrator hook
│  (state machine)   │
└─────────┬──────────┘
          │ calls
          ▼
┌────────────────────┐
│   streamChat       │      ← native-fetch SSE client (api/ai.ts)
└─────────┬──────────┘
          │ HTTP POST SSE
          ▼
┌────────────────────┐
│  ProspectorResources│     ← Java backend (see ../../PROSPECTOR.md)
└────────────────────┘
```

Pages instantiate `useProspector()`, pass its return value plus a `ChatContext` to `ProspectorPanel`. The panel renders the conversation; `useProspector` streams responses, executes tool calls, and loops until the model emits `finish_reason: stop` or hits the tool-rounds cap.

## Components (`src/components/prospector/`)

### ProspectorPanel

**Entry component.** Props: prospector state (from `useProspector()`), `ChatContext`, optional `clearLabel` / `clearIcon` (the header button defaults to a trash can labelled "Clear chat"; the [Conversations page](../pages/conversations.md) relabels it "New conversation" and wires it to create one).

Renders four child elements:

- `ChatMessageList` — message history + streaming bubble
- `ChatInput` — textarea with send / stop buttons (Enter to send, Shift+Enter for newline)
- `QuickActionBar` — context-aware buttons ("Suggest Queries", "Fix Error", "Analyze Data") that vary by `ChatContext` mode
- Usage pill — token count + cost

### ChatMessageBubble

Renders one message. User messages: plain text. Assistant messages: `react-markdown` with code-block rendering. Delegates tool-call display to `ToolCallDisplay`.

### ToolCallDisplay

Expandable card showing tool invocations (`execute_sql`, `create_visualization`, …) with JSON arguments and results. For tools that produce navigable resources (dashboards, visualizations), includes a "View" link.

### ChatMessageList

Scrolls automatically, hides tool messages from the visible flow but collects their results to inject into assistant bubbles.

### ChatInput

Textarea + send/stop. No fancy state — delegates everything to props.

### ProspectorSettingsModal

Admin configuration: provider (OpenAI, Anthropic, Ollama, ...), endpoint, model, temperature, max tokens, system prompt, max tool rounds. Includes a "Test Connection" button. Saves via `updateAiConfig` from `api/ai.ts`.

## useProspector hook

**File:** `src/hooks/useProspector.ts`

The orchestrator. Accepts optional `onSqlGenerated`, `onVisualizationCreated`, `maxToolRounds` (default `DEFAULT_MAX_TOOL_ROUNDS = 15`), `conversationId` and `projectId`, and `onQueryResult`.

The hook is server-primary: with a `conversationId` and `projectId` the server owns the conversation, it is loaded from there and every change is saved there, and `localStorage` is only an offline buffer. The old tab-scoped path (`storageKey`, `tabId`, `prospectorChatKey`, `conversationLengthFor`, and the mirror to `/api/v1/tabs/{id}/conversation`) was removed. Without a conversation the hook keeps messages in memory only. `serverSaveTimerRef` is the one save-debounce timer; with the legacy mirror gone, the server-primary save path is its only user.

`onSqlGenerated` (SQL Lab) routes a suggested query to a new tab unless the active tab is empty, in which case it reuses that tab — see `handleProspectorSql` in `SqlLabPage.tsx`.

Returns:

```ts
{
  messages: ChatMessage[];
  isStreaming: boolean;
  streamingContent: string;
  usage: UsageEvent | null;
  sendMessage(text, context): void;
  stopStreaming(): void;
  clearChat(): void;
  executeToolCall(...): Promise<string>;
  conversationId: string | null;   // null outside server mode
  isLoaded: boolean;               // the server copy for conversationId has been loaded
  saveStatus: 'idle' | 'saving' | 'too-large' | 'conflict' | 'error';
  reloadConversation(): void;      // takes the server's copy over local changes
}
```

Inside, it manages:

- A `tool rounds` counter (`useRef`) that caps multi-step tool execution.
- An `AbortController` for the current stream.
- An accumulating `contentBuffer` and a `tool calls Map`.
- A `doStreamRound()` function that calls `streamChat`, accumulates deltas, executes any tool calls when `finish_reason === 'tool_calls'`, and recurses up to `maxToolRounds`.

Tool definitions are hardcoded in `TOOL_DEFINITIONS` at the top of the file: `execute_sql`, `list_schemas`, `get_schema_info`, `create_visualization`, `create_dashboard`, `save_query`, `create_workflow`, `write_wiki_page`, `get_available_functions`, `get_project_docs`. Each maps to a backend or local API call invoked by `executeToolCall()`.

### create_workflow and activation

`create_workflow` builds a full workflow (schedule, actions, optional per-action `when` condition) and creates it through `createWorkflow`. A workflow cannot exist without a saved query that is in the project and owned by the creator, so the tool takes the query as `sql`, saves it, adds it to the project and then creates the workflow. (`save_query` also adds its query to the project, but a failure there is not fatal the way it is here: the query exists either way, so the result reports `addedToProject: false` with a `projectError` and the card still renders.) `savedQueryId` is accepted for a retry: when the server rejects the workflow with field errors (422), the tool returns them with the saved query's id so the model calls again without saving the query twice.

The tool cannot turn a workflow on. `WorkflowResources` pins every created workflow to `status: paused` and `schedule.enabled: false`, and `WorkflowRunner` never runs a disabled one; the client adds nothing to that. What the client owns is not calling `setWorkflowEnabled` from the tool path. `components/conversation/WorkflowActivationCard.tsx` renders under the workflow's artifact card in the thread, reads the workflow from the server, and offers Activate only while it is paused. Pressing it opens a confirm dialog built from the server's copy of the workflow (name, schedule, each action with its recipients or target, and its condition); `setWorkflowEnabled(id, true)` runs only from that dialog's confirm. A server refusal (expired, owner lost access) is shown under the card and the workflow stays paused. The dialog focuses Cancel by default, shows destination config (`url`, `webhook`, `endpoint`, `to`) for action types it does not recognise, and a failed load that is not a 404 shows a Retry button rather than "no longer exists". Only the Conversations page passes `onOpenArtifact`, so the card (and its Activate control) does not render anywhere else; the `create_workflow` result therefore names the workflow's Workflows page path, never a button. A non-owner project member cannot use the `sql` route: `addSavedQuery` is owner-only, and the tool returns a stop message naming the owner as the remedy instead of a retry. `useProspector.test.tsx` asserts the hook's source never mentions `setWorkflowEnabled`.

### Reports

Three separate paths end at the same wiki page, and all three build it with `buildReportMarkdown` (`src/utils/report.ts`): the assistant's own text, an appendix of the distinct `execute_sql` statements run in the conversation, and a provenance footer (model/provider when known, timestamp, conversation id).

1. **Quick action.** The "Generate Report" button in `QuickActionBar` sends a fixed prompt asking for a standalone, self-contained report (H1 title, executive summary, findings, recommendations) and tags the resulting user message `report_generation`. This is the only step that shapes what Prospector writes; the other two paths just decide whether to keep it.
2. **The chip.** `ChatMessageBubble` calls `looksLikeReport(message.content)` (two or more markdown headings, plus a table or 1500+ characters) on every non-streaming assistant message. A hit shows a "This looks like a report" chip with **Save to project** and **Dismiss**. Save opens `SaveReportModal`, which lets the user pick a project, edit the title, and choose between a new page or overwriting an existing same-titled page under the `Reports` wiki folder (`REPORTS_FOLDER` in `report.ts`). Dismiss is remembered per message via `messageKey()`, a content hash rather than list index, because the server-merged conversation array gets replaced wholesale on mount and positions would then point at different messages.
3. **The tool.** `write_wiki_page` lets Prospector write a wiki page itself when the user agrees in conversation, without the modal's project picker. It replaced `save_report`, which could only create pages in `Reports` and always appended the queries. The backend system prompt (`ProspectorResources.buildMessages`) instructs it to offer after writing a full report and to call the tool only once the user agrees. `executeToolCall`'s `write_wiki_page` case errors if `context.projectId` is unset, otherwise builds the markdown from `messagesRef.current` (a ref mirroring `messages`, so the callback doesn't have to depend on chat state and re-create itself every streamed token). The provenance footer is always written and names the conversation id. `includeTranscript: true` also appends the query appendix. Without `pageId` it creates the page via `createWikiPage` in `folder` (default `REPORTS_FOLDER`), refusing with the existing page's id when that folder already holds a page of the same title (case-insensitive). With `pageId` it calls `updateWikiPage`, leaves the folder alone unless one is given, and reports a deleted page as an error instead of creating a new one. `get_project_docs` lists page ids so the model can update a page it did not write. Conversations stored before the rename still contain `save_report` calls; `autoFollow.ts` and `ArtifactHistory.tsx` keep the old name so their cards still appear.

Reports land as ordinary wiki pages under the `Reports` folder in the project tree (`ProjectWikiPage.tsx`), so they inherit whatever that page already does: markdown rendering, and a **Download as PDF** button that calls `window.print()` against a print-styled view (`wiki-printable` in `index.css`) for a selectable-text PDF via the browser's print dialog.

### get_project_docs

Client-executed, project-scoped: it errors out immediately if `context.projectId` is unset. Called with no arguments it lists the current project's wiki page titles; called with a `pageTitle` it fetches that page (via `getProject`) and returns its body, truncated to `PROJECT_DOC_MAX_CHARS` (8000 characters, suffixed `...[truncated]`) if longer.

This exists because the server-injected project context block (see [`../../PROSPECTOR.md`](../../PROSPECTOR.md#project-context)) lists wiki page **titles only** — bodies are fetched on demand through this tool instead of being inlined into the system prompt, since that prompt is re-sent on every tool round.

## API client

**File:** `src/api/ai.ts`

`streamChat(request, callbacks)` uses native `fetch` for SSE (axios doesn't stream cleanly). It POSTs a `ChatRequest` to `/api/v1/ai/chat`, reads the body as a `ReadableStream`, parses `event: <type>\ndata: <json>` line pairs, and dispatches to callbacks:

- `onDelta(event)` — content / tool_call_start / tool_call_delta / tool_call_end
- `onDone(event)` — `finish_reason: 'stop' | 'tool_calls'`
- `onUsage(event)` — token counts + cost (Anthropic emits incrementally; OpenAI once at end)
- `onError(event)`

Returns the `AbortController` so the caller can cancel.

Other AI endpoints in this module:

| Function | Endpoint | Purpose |
|---|---|---|
| `getAiStatus` | `GET /api/v1/ai/status` | Is Prospector configured? (gates UI affordances). Also carries `sendDataToAi`, mirrored from the admin-only config because every authenticated user can read this endpoint. |
| `transpileSql` | `POST /api/v1/ai/transpile` | sqlglot transpile (see [`../../TRANSPILER.md`](../../TRANSPILER.md)) |
| `formatSql` | `POST /api/v1/ai/formatSql` | LLM-formatted SQL |
| `getAiConfig` / `updateAiConfig` | `GET/PUT /api/v1/ai/config` | Provider config |

## Types

**File:** `src/types/ai.ts`

- `ChatRequest` — `{ messages, tools, context }`
- `ChatMessage` — `{ role: 'user'|'assistant'|'system'|'tool', content, toolCalls?, toolCallId?, name? }`
- `ChatContext` — page / mode signals: `currentSql`, `schema`, `errorMessage`, `conversationMode` (set by the Conversations page; the server then builds the conversation-mode prompt), `logAnalysisMode`, `projectDatasets`, …
- `DeltaEvent` — discriminated union for streamed content / tool calls
- `DoneEvent` — `{ finish_reason }`
- `UsageEvent` — `{ promptTokens?, responseTokens?, totalTokens?, costUsd?, currency? }`
- `ToolCall` — `{ id, name, arguments }` (arguments is a JSON string)

## Context (AiModalContext)

**File:** `src/contexts/AiModalContext.tsx`

Separate from `ChatContext`. Owns whether the global AI assistant **modal** is open (`isOpen`, `mode`, `openModal`, `closeModal`). The Toolbar's AI menu calls `openModal('suggestions' | 'explain' | 'optimize')`; `AiAssistantModal` reads `mode` to decide which UI to render.

ChatContext (the per-message payload) lives in `types/ai.ts` and is constructed at the call site, not provided via Context.

## Integration points

`useProspector` is instantiated by the Conversations page, the only chat surface. SQL Lab and Logs no longer mount a panel; they link into a Scratch project conversation through Ask Prospector.

`AiModalProvider` wraps the whole app in `App.tsx`, so `useAiModal()` works anywhere; it's used by the Toolbar AI menu.

## Quirks

- **SSE streaming is hand-rolled** because axios doesn't expose `ReadableStream`. If you add another streaming endpoint, copy the pattern from `streamChat` rather than trying to fit it into the axios wrapper.
- **CSRF is mandatory.** `streamChat` reads the token from the same meta-tag-or-cookie source as `apiClient` and adds the `X-CSRF-Token` header manually.
- **Tool rounds cap.** Default 15. After the limit a system message is injected ("I've reached the maximum number of tool call rounds.") so the user understands why the conversation stopped.
- **Usage events are cumulative per conversation.** Multi-round tool execution emits a usage event per roundtrip; the hook overwrites with the latest snapshot, so the usage pill shows the running total for the conversation, not just the last message.
- **Quick action prompts** include explicit anti-hallucination instructions ("never make up table or column names — use ONLY real tables and columns from the schemas I have access to"). When adding new quick actions, follow the same pattern.
- **Context scoping is partial.** `projectDatasets` in `ChatContext` lets the backend restrict schema listings, but enforcement is server-side — the frontend does not filter tools by mode.
- **`sendDataToAi` is server config, never a request field.** It lives on `LlmConfig` (set by an admin in Prospector Settings) and gates sample *rows* only — `columns` and `rowCount` are metadata and always sent.
  - Server-side: `appendDashboardData` in `ProspectorResources.java` reads `config.isSendDataToAi()` and is invoked for all three dashboard modes (executive summary, Q&A, alerts), so the **system-prompt** path is gated for every one of them by construction. This does *not* cover the user-message path — see the known gap below.
  - Client-side: `useProspector` reads the flag via the shared `useSendDataToAi()` hook (`src/hooks/useSendDataToAi.ts`) and omits `rows` from the `execute_sql` tool result when it is off. `SqlLabPage` uses the same hook for its optimize-query flow, so the read/default logic lives in one place. `sendDataToAi` is deliberately *not* a hook parameter or a `ChatContext` field on `useProspector`: it used to be one, and the callers that forgot to pass it (the global Prospector tab, the dashboard panels) sent rows regardless of the setting. Reading it inside the hook means no caller can forget.
  - `useSendDataToAi` fetches from `/api/v1/ai/status`, not `/api/v1/ai/config`, because config is admin-only — a non-admin's fetch would 403 and fall back to permissive. Unknown (still loading, or the fetch failed) withholds rows: a privacy flag fails closed.
  - **Known gap:** `AiQnAPanel` and `ExecutiveSummaryPanel` inline sample rows into the *user message*, which no server-side gate can suppress. `ExecutiveSummaryPanel`'s own `includeSampleData` toggle is likewise undermined by its context passing raw `dashboardData`. Pre-existing and not yet fixed.
- **No state in Redux.** Prospector is entirely component-local (hook + props). It does not write to Redux, react-query cache, or shared contexts other than reading config from `AiModalContext`.

## Related docs

- [`../../PROSPECTOR.md`](../../PROSPECTOR.md) — backend: LLM provider registry, REST endpoints, persistent store
- [`../../TRANSPILER.md`](../../TRANSPILER.md) — GraalPy + sqlglot transpiler used by "Optimize"
- [`../../AI_FEATURES.md`](../../AI_FEATURES.md) — overview of all Drill AI features
- [`../pages/sql-lab.md`](../pages/sql-lab.md) — SQL Lab integration
- [`../pages/logs.md`](../pages/logs.md) — Logs integration
- [`../pages/ai-analytics.md`](../pages/ai-analytics.md) — usage analytics dashboard
