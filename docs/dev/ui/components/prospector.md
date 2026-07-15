# Prospector (frontend)

Prospector is Drill's chat-based AI assistant — embedded in SQL Lab, Logs, and the global right inspector. This doc covers the frontend architecture. For backend (LLM provider registry, REST endpoints, persistent store) see [`../../PROSPECTOR.md`](../../PROSPECTOR.md). For the SQL transpiler used by the "Optimize" flow, see [`../../TRANSPILER.md`](../../TRANSPILER.md).

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

**Entry component.** Props: prospector state (from `useProspector()`), `ChatContext`, optional `onInsertCell` (for notebook integration).

Renders four child elements:

- `ChatMessageList` — message history + streaming bubble
- `ChatInput` — textarea with send / stop buttons (Enter to send, Shift+Enter for newline)
- `QuickActionBar` — context-aware buttons ("Suggest Queries", "Fix Error", "Analyze Data") that vary by `ChatContext` mode
- Usage pill — token count + cost

### ChatMessageBubble

Renders one message. User messages: plain text. Assistant messages: `react-markdown` with code-block rendering. In notebook mode, code blocks get an "Insert Cell" button that calls `onInsertCell`. Delegates tool-call display to `ToolCallDisplay`.

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

The orchestrator. Accepts optional `onSqlGenerated`, `onVisualizationCreated`, `maxToolRounds` (default `DEFAULT_MAX_TOOL_ROUNDS = 15`).

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
}
```

Inside, it manages:

- A `tool rounds` counter (`useRef`) that caps multi-step tool execution.
- An `AbortController` for the current stream.
- An accumulating `contentBuffer` and a `tool calls Map`.
- A `doStreamRound()` function that calls `streamChat`, accumulates deltas, executes any tool calls when `finish_reason === 'tool_calls'`, and recurses up to `maxToolRounds`.

Tool definitions are hardcoded in `TOOL_DEFINITIONS` at the top of the file: `execute_sql`, `list_schemas`, `get_schema_info`, `create_visualization`, `create_dashboard`, `save_query`, `get_available_functions`, `get_project_docs`. Each maps to a backend or local API call invoked by `executeToolCall()`.

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
| `getAiStatus` | `GET /api/v1/ai/status` | Is Prospector configured? (gates UI affordances) |
| `transpileSql` | `POST /api/v1/ai/transpile` | sqlglot transpile (see [`../../TRANSPILER.md`](../../TRANSPILER.md)) |
| `formatSql` | `POST /api/v1/ai/formatSql` | LLM-formatted SQL |
| `getAiConfig` / `updateAiConfig` | `GET/PUT /api/v1/ai/config` | Provider config |

## Types

**File:** `src/types/ai.ts`

- `ChatRequest` — `{ messages, tools, context }`
- `ChatMessage` — `{ role: 'user'|'assistant'|'system'|'tool', content, toolCalls?, toolCallId?, name? }`
- `ChatContext` — page / mode signals: `currentSql`, `schema`, `errorMessage`, `notebookMode`, `logAnalysisMode`, `projectDatasets`, …
- `DeltaEvent` — discriminated union for streamed content / tool calls
- `DoneEvent` — `{ finish_reason }`
- `UsageEvent` — `{ promptTokens?, responseTokens?, totalTokens?, costUsd?, currency? }`
- `ToolCall` — `{ id, name, arguments }` (arguments is a JSON string)

## Context (AiModalContext)

**File:** `src/contexts/AiModalContext.tsx`

Separate from `ChatContext`. Owns whether the global AI assistant **modal** is open (`isOpen`, `mode`, `openModal`, `closeModal`). The Toolbar's AI menu calls `openModal('suggestions' | 'explain' | 'optimize')`; `AiAssistantModal` reads `mode` to decide which UI to render.

ChatContext (the per-message payload) lives in `types/ai.ts` and is constructed at the call site, not provided via Context.

## Integration points

`useProspector` is currently instantiated in:

1. **`GlobalProspectorTab`** — always-available right-inspector tab; empty `ChatContext`.
2. **SQL Lab** (via `ProspectorPanel` inside the page) — passes `onSqlGenerated` to update the editor and `onVisualizationCreated` to track new charts.
3. **Logs** — passes `logAnalysisMode: true` in the context so quick actions surface log-analysis prompts.

`AiModalProvider` wraps the whole app in `App.tsx`, so `useAiModal()` works anywhere; it's used by the Toolbar AI menu.

## Quirks

- **SSE streaming is hand-rolled** because axios doesn't expose `ReadableStream`. If you add another streaming endpoint, copy the pattern from `streamChat` rather than trying to fit it into the axios wrapper.
- **CSRF is mandatory.** `streamChat` reads the token from the same meta-tag-or-cookie source as `apiClient` and adds the `X-CSRF-Token` header manually.
- **Tool rounds cap.** Default 15. After the limit a system message is injected ("I've reached the maximum number of tool call rounds.") so the user understands why the conversation stopped.
- **Usage events are cumulative per conversation.** Multi-round tool execution emits a usage event per roundtrip; the hook overwrites with the latest snapshot, so the usage pill shows the running total for the conversation, not just the last message.
- **Quick action prompts** include explicit anti-hallucination instructions ("never make up table or column names — use ONLY real tables and columns from the schemas I have access to"). When adding new quick actions, follow the same pattern.
- **Context scoping is partial.** `projectDatasets` in `ChatContext` lets the backend restrict schema listings, but enforcement is server-side — the frontend does not filter tools by mode.
- **`sendDataToAi: false` suppresses sample rows, not metadata.** When the user turns this off in Prospector Settings, `execute_sql`'s tool result (built here in `executeToolCall`) omits `rows` while still returning `columns` and `rowCount`. The same flag also suppresses `sampleRows` in dashboard-mode chats (executive summary, Q&A, alerts) — but that check (`isSendDataToAi`) is server-side, in `appendDashboardData` in `ProspectorResources.java`, not in this hook. Absent (`undefined`) is treated as opted-in, same as explicit `true`.
- **No state in Redux.** Prospector is entirely component-local (hook + props). It does not write to Redux, react-query cache, or shared contexts other than reading config from `AiModalContext`.

## Related docs

- [`../../PROSPECTOR.md`](../../PROSPECTOR.md) — backend: LLM provider registry, REST endpoints, persistent store
- [`../../TRANSPILER.md`](../../TRANSPILER.md) — GraalPy + sqlglot transpiler used by "Optimize"
- [`../../AI_FEATURES.md`](../../AI_FEATURES.md) — overview of all Drill AI features
- [`../pages/sql-lab.md`](../pages/sql-lab.md) — SQL Lab integration
- [`../pages/logs.md`](../pages/logs.md) — Logs integration
- [`../pages/ai-analytics.md`](../pages/ai-analytics.md) — usage analytics dashboard
