# AI Features in Apache Drill SQL Lab

## Overview

Apache Drill SQL Lab includes a comprehensive suite of AI-powered features designed to help users explore data, generate queries, build dashboards, and gain insights through natural language interaction. All AI features are powered by configurable LLM providers (OpenAI, Anthropic Claude, Ollama, or any OpenAI-compatible endpoint) and are accessed through a secure backend proxy that keeps API keys on the server.

This document covers every AI feature, its configuration, and how the pieces fit together.

---

## Table of Contents

1. [Prospector (AI Chat Assistant)](#1-prospector-ai-chat-assistant)
2. [AI-Powered Dashboard Panels](#2-ai-powered-dashboard-panels)
   - [Executive Summary Panel](#21-executive-summary-panel)
   - [AI Q&A Panel](#22-ai-qa-panel)
   - [AI Alerts Panel](#23-ai-alerts-panel)
   - [Natural Language Filter Panel](#24-natural-language-filter-panel)
3. [SQL Transpiler](#3-sql-transpiler)
4. [Configuration](#4-configuration)
5. [Architecture](#5-architecture)
6. [API Reference](#6-api-reference)

---

## 1. Prospector (AI Chat Assistant)

Prospector is a chat-based AI assistant integrated into SQL Lab's sidebar. It helps users explore schemas, write SQL, create visualizations, build dashboards, and analyze logs through natural language conversation.

### Tools

Prospector has access to seven tools that it calls autonomously during conversations:

| Tool | Description |
|------|-------------|
| `list_schemas` | Lists available schemas/data sources. Respects project scoping. |
| `get_schema_info` | Gets tables in a schema, or columns in a specific table. Understands Drill's hierarchical schema model (e.g., `mysql.store`). |
| `execute_sql` | Executes a SQL query and returns columns, row count, and sample rows. Also populates the SQL editor. |
| `create_visualization` | Creates a chart (bar, line, pie, scatter, table, etc.) from a SQL query. |
| `create_dashboard` | Creates a new dashboard with optional visualization panels and grid positioning. |
| `save_query` | Saves a SQL query for later reuse. |
| `get_available_functions` | Lists all SQL functions available in the Drill instance. |

The AI is instructed to use tools proactively -- it will explore schemas, discover tables and columns, and write queries without asking the user for schema/table names.

### Tool Round Limit

Each user message allows the AI to perform multiple rounds of tool calls (e.g., list schemas, then get tables, then get columns, then execute SQL). The maximum number of rounds is configurable via the **Max Tool Rounds** setting in Prospector Settings (default: 15). This prevents infinite loops while allowing complex multi-step workflows.

### Quick Actions

Context-aware buttons appear above the chat input based on the current mode:

**SQL Lab mode:**
- Generate SQL -- always available
- Explain Query -- when SQL is in the editor
- Fix Error -- when a query error exists
- Suggest Chart -- when query results are available

**Notebook mode:**
- Analyze Data -- analyze the current DataFrame
- Suggest Plot -- generate matplotlib visualization code
- Build Model -- generate scikit-learn model code
- Fix Cell Error -- when a notebook cell has an error

**Log Analysis mode:**
- Summarize Errors -- summarize error patterns
- Find Performance Issues -- identify slow operations
- Explain Errors -- explain error root causes
- Suggest SQL -- generate queries for the `dfs.logs` workspace

### Project Scoping

When Prospector is used within a Project, schema exploration is automatically scoped to the project's datasets:

- `list_schemas` returns only schemas referenced by project datasets
- `availableSchemas` in the context is filtered to project schemas
- The system prompt explicitly instructs the AI to stay within the project's data boundaries
- This prevents accidental exploration of unrelated data sources

### Streaming Protocol

Prospector uses Server-Sent Events (SSE) for real-time streaming:

```
POST /api/v1/ai/chat
Content-Type: application/json
Accept: text/event-stream
```

Events:
- `event: delta` -- content chunks, tool call start/delta/end
- `event: done` -- response complete (finish_reason: `stop` or `tool_calls`)
- `event: error` -- error message

---

## 2. AI-Powered Dashboard Panels

Dashboards support four AI-powered panel types in addition to standard visualization, markdown, image, and title panels.

### 2.1 Executive Summary Panel

An AI-generated summary that analyzes data from all (or selected) visualization panels on a dashboard. The summary updates automatically when dashboard data changes.

#### Summary Templates (A5)

Four built-in templates provide starting prompts tailored to different use cases:

| Template | Focus |
|----------|-------|
| General | Key insights, anomalies, trends, areas needing attention |
| Sales Dashboard | Revenue, pipeline, top products, action items |
| Operations Review | System health, throughput, error rates, SLA, capacity |
| Incident Summary | Timeline, impact, root cause, next steps |

Selecting a template populates the prompt; users can further customize it.

#### Tone/Audience Selector (A1)

Controls the writing style of the generated summary:

| Tone | Style |
|------|-------|
| Executive | Concise, high-level, focused on business impact and strategy |
| Technical | Detailed, includes specific metrics, data points, and technical context |
| Casual | Friendly, conversational, approachable |

#### Pin/Lock Summary (A2)

- A pin toggle button in view mode prevents the summary from auto-regenerating when dashboard data changes
- Pinned summaries display the cached version immediately
- Manual "Regenerate" still works when pinned
- Useful for preserving a summary you want to keep while data refreshes

#### Panel Scope Selector (A3)

- Multi-select dropdown in edit mode lists all sibling visualization panels
- Select specific panels to include in the summary; leave empty for all panels
- Useful when a dashboard has many panels but the summary should focus on a subset

#### Anomaly Detection Emphasis (A4)

- Toggle switch in edit mode
- When enabled, the AI pays special attention to outliers, anomalies, and values deviating from expected ranges
- Adds explicit instructions to the prompt for anomaly-focused analysis

#### Status Icon Sets (A6)

Three icon sets for status indicators in summaries:

| Set | Good | Warning | Critical |
|-----|------|---------|----------|
| Checkmarks (default) | ✅ | ⚠️ | ❌ |
| Traffic Light | 🟢 | 🟡 | 🔴 |
| Weather | ☀️ | ⛅ | 🌧️ |

#### Historical Comparison (A7)

- Stores the last 5 generated summaries with timestamps
- A "Compare" button appears when 2+ summaries exist
- Clicking it regenerates the summary with the previous version included as context
- The AI notes changes, trends, and differences between the current and previous summary

#### PDF Export (A8)

- When exporting a dashboard to PDF, the `pdf-export-mode` CSS class is applied
- Hides interactive elements (buttons, spinners, chat inputs) for clean output
- Applies print-friendly typography

### 2.2 AI Q&A Panel

A mini chat widget embedded in the dashboard for asking questions about the displayed data.

**Features:**
- Scoped to the dashboard's visualization data (panel names, columns, sample rows)
- Lightweight message history with configurable maximum (default: 20 messages)
- Uses the same streaming SSE protocol as Prospector
- Messages render with markdown support
- Auto-scrolls to the latest message

**Edit mode configuration:**
- Max messages -- controls how many messages are retained in the conversation

### 2.3 AI Alerts Panel

Threshold-based alert rules that evaluate against dashboard data, with optional AI-powered analysis.

**Alert Rules:**
- Each rule specifies: name, target panel, column, operator (`>`, `>=`, `<`, `<=`, `=`, `!=`), threshold value, and severity (`info`, `warning`, `critical`)
- Rules are stored as JSON in the panel config
- Rules are evaluated client-side against sample data from visualization panels

**Display:**
- Triggered alerts show with color-coded severity (blue/info, yellow/warning, red/critical)
- Non-triggered rules show as green "OK"
- When AI analysis is enabled, triggered alerts are sent to the AI for contextual explanation and recommended actions

**Edit mode configuration:**
- Add, edit, and remove rules
- Toggle AI analysis on/off

### 2.4 Natural Language Filter Panel

Converts plain-English filter descriptions into structured dashboard cross-filters.

**How it works:**
1. User types a filter request (e.g., "Show only sales greater than 1000 in California")
2. The AI receives available columns with their types from all dashboard panels
3. The AI returns a JSON array of structured filter objects
4. The user sees a preview of the filters as tags
5. Clicking "Apply" adds them as dashboard cross-filters; "Cancel" discards them

**Filter objects include:**
- `column` and `value` -- the basic filter
- `isTemporal` / `rangeStart` / `rangeEnd` -- for date/time filters
- `isNumeric` / `numericOp` -- for numeric comparisons (including `between`)

---

## 3. SQL Transpiler

The SQL Transpiler automatically converts SQL between dialects using [Java sqlglot](https://github.com/gtkcyber/sqlglot_java), a pure Java SQL parsing and transpilation library. No external runtime is required.

**Primary use case:** When users accept AI-optimized SQL, it is silently transpiled from common dialects (e.g., MySQL, PostgreSQL) to Apache Drill's SQL dialect before being placed in the editor.

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/transpile` | POST | Transpile SQL between dialects |
| `/api/v1/transpile/status` | GET | Check transpiler availability |
| `/api/v1/transpile/format` | POST | Pretty-print SQL |
| `/api/v1/transpile/convert-type` | POST | Wrap a column in a CAST expression |
| `/api/v1/transpile/time-grain` | POST | Wrap a column with DATE_TRUNC |

### Supported Dialects

`drill`, `mysql`, `postgres`, `hive`, `spark`, `bigquery`, `snowflake`, `presto`, `trino`, `sqlite`, `tsql`, `oracle`, `duckdb`

---

## 4. Configuration

### Accessing Settings

Open the Prospector sidebar and click the gear icon to open the settings modal. **Admin access is required.**

### Configuration Options

| Setting | Default | Description |
|---------|---------|-------------|
| Enable Prospector | Off | Master toggle for all AI features |
| LLM Provider | OpenAI | `openai` (OpenAI-compatible), `anthropic` (Anthropic Claude) |
| API Endpoint | (auto) | Override the default endpoint URL. Leave empty for the provider's default. Useful for Azure OpenAI, local Ollama, or proxy setups. |
| API Key | -- | Provider API key. Stored securely on the server; never sent to the browser. |
| Model | -- | Model identifier (e.g., `gpt-4o`, `claude-sonnet-4-20250514`, `llama3`) |
| Max Tokens | 4096 | Maximum tokens in the LLM response |
| Temperature | 0.7 | Sampling temperature (0 = deterministic, 2 = most creative) |
| Custom System Prompt | -- | Additional instructions appended to the default system prompt |
| Send Data Samples to AI | On | Include sample query result rows when optimizing queries |
| Max Tool Rounds | 15 | Maximum tool call rounds per message (1-50). Higher values allow more complex multi-step tasks. |

### Test Connection

Click "Test Connection" to validate the provider, endpoint, API key, and model before saving.

### Storage

Configuration is stored in Drill's `PersistentStore` under the key `drill.sqllab.ai_config`. This persists across restarts and uses the same storage backend as other Drill system tables.

---

## 5. Architecture

```
Browser (React)                     Drill Backend (Java)                LLM Provider
+---------------------------+      +----------------------------+      +-----------+
| SqlLabPage                |      | ProspectorResources        |      | OpenAI    |
|   useProspector hook      |      |   buildMessages()          |      | Anthropic |
|   Tool executor           |--SSE-->  System prompt builder    |---->|  Ollama   |
|   Quick action bar        |      |   SSE streaming proxy      |      +-----------+
|                           |      |                            |
| ProspectorPanel           |      | AiConfigResources          |
|   ChatInput               |      |   GET/PUT config           |
|   ChatMessageList         |      |   POST test                |
|   ToolCallDisplay         |      |                            |
|   ProspectorSettingsModal |      | TranspileResources         |
|                           |      |   Transpile, format, cast  |
| Dashboard AI Panels       |      |                            |
|   ExecutiveSummaryPanel   |      | MetadataResources          |
|   AiQnAPanel              |      |   Schemas, tables, columns |
|   AiAlertsPanel           |      |                            |
|   NlFilterPanel           |      | LlmProviderRegistry        |
+---------------------------+      |   OpenAiCompatibleProvider |
                                   |   AnthropicProvider        |
                                   |   LlmConfig (PersistStore) |
                                   +----------------------------+
```

### Data Flow

1. **User sends a message** -- the frontend builds a `ChatContext` with current SQL, schema, errors, results, project datasets, and mode flags
2. **Frontend calls** `POST /api/v1/ai/chat` with messages, tool definitions, and context
3. **Backend builds the system prompt** using context (schema info, mode-specific instructions, project scoping, custom prompt)
4. **Backend streams** the LLM response as SSE events back to the frontend
5. **If tool calls are returned**, the frontend executes them using existing API functions (metadata, query execution, visualization creation) and sends results back
6. **The cycle repeats** until the LLM returns a final text response or the tool round limit is reached

### System Prompt Construction

The backend dynamically builds the system prompt based on context:

1. Base identity and Drill SQL guidance
2. **Project scoping** -- if `projectDatasets` is set, lists available datasets and restricts exploration
3. **Available schemas** -- if no project scope, lists all schemas
4. **Current SQL and errors** -- from the editor
5. **Mode-specific instructions** -- log analysis, dashboard summary, Q&A, NL filter, or alert mode
6. **Tone and anomaly instructions** -- for dashboard summaries
7. **Historical comparison** -- previous summary for comparison
8. **Result summary** -- current query results metadata
9. **Tool usage directives** -- proactive schema exploration, hierarchical schema handling
10. **Custom system prompt** -- user-configured additional instructions

### Hierarchical Schema Handling

Drill uses a hierarchical schema model where plugins contain sub-schemas (e.g., `mysql` has `mysql.store`, `mysql.inventory`). The metadata endpoint handles dot-qualified table names by:

1. Splitting the table name at the last dot (e.g., `store.order_items` becomes schema `mysql.store`, table `order_items`)
2. Querying `INFORMATION_SCHEMA.COLUMNS` with the resolved schema and table name
3. Falling back to the original unsplit values if no results are found (handles file-based plugins with dots in filenames)

---

## 6. API Reference

### AI Chat

```
POST /api/v1/ai/chat
Content-Type: application/json
Accept: text/event-stream

{
  "messages": [{ "role": "user", "content": "..." }],
  "tools": [{ "name": "...", "description": "...", "parameters": {...} }],
  "context": {
    "currentSql": "SELECT ...",
    "currentSchema": "dfs.tmp",
    "availableSchemas": ["dfs", "mysql", ...],
    "projectDatasets": [{ "type": "table", "schema": "mysql.store", "table": "orders", "label": "Orders" }],
    "dashboardSummaryMode": false,
    "dashboardQnAMode": false,
    "dashboardNlFilterMode": false,
    "dashboardAlertMode": false,
    "dashboardTone": "executive",
    "dashboardAnomalyFocus": false,
    "previousSummary": "..."
  }
}
```

### AI Status

```
GET /api/v1/ai/status
Response: { "enabled": true, "configured": true }
```

### AI Configuration (Admin only)

```
GET    /api/v1/ai/config              -- Get config (API key redacted)
PUT    /api/v1/ai/config              -- Update config (partial updates)
POST   /api/v1/ai/config/test         -- Test connection
GET    /api/v1/ai/config/providers    -- List available providers
```

### SQL Transpilation

```
POST   /api/v1/transpile              -- Transpile SQL between dialects
GET    /api/v1/transpile/status       -- Check availability
POST   /api/v1/transpile/format       -- Pretty-print SQL
POST   /api/v1/transpile/convert-type -- Add CAST expression
POST   /api/v1/transpile/time-grain   -- Add DATE_TRUNC
```

---

## Key Files

### Backend (Java)

| File | Purpose |
|------|---------|
| `ProspectorResources.java` | Chat SSE endpoint, system prompt building, context classes |
| `AiConfigResources.java` | Admin configuration endpoints |
| `LlmConfig.java` | Configuration POJO (PersistentStore) |
| `OpenAiCompatibleProvider.java` | OpenAI/Ollama streaming implementation |
| `AnthropicProvider.java` | Anthropic Claude streaming implementation |
| `LlmProviderRegistry.java` | Provider discovery and registration |
| `MetadataResources.java` | Schema/table/column metadata (used by tools) |
| `TranspileResources.java` | SQL transpilation endpoints |

### Frontend (TypeScript/React)

| File | Purpose |
|------|---------|
| `hooks/useProspector.ts` | Core chat hook, tool definitions, tool execution |
| `api/ai.ts` | API client (streamChat, config, transpile) |
| `types/ai.ts` | All AI-related type definitions |
| `components/prospector/ProspectorPanel.tsx` | Main chat UI |
| `components/prospector/ChatInput.tsx` | Message input with send/stop |
| `components/prospector/QuickActionBar.tsx` | Context-aware action buttons |
| `components/prospector/ToolCallDisplay.tsx` | Collapsible tool call panels |
| `components/prospector/ProspectorSettingsModal.tsx` | Admin settings modal |
| `components/dashboard/ExecutiveSummaryPanel.tsx` | AI executive summary |
| `components/dashboard/AiQnAPanel.tsx` | Dashboard Q&A chat |
| `components/dashboard/AiAlertsPanel.tsx` | Threshold alerts + AI analysis |
| `components/dashboard/NlFilterPanel.tsx` | Natural language filters |
