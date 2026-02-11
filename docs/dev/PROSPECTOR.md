# Prospector - AI Assistant for Apache Drill SQL Lab

## Overview

Prospector is an integrated chat-based AI assistant for SQL Lab that helps users explore data, generate SQL queries, create visualizations, and build dashboards using natural language. It connects to LLM providers (OpenAI, Anthropic Claude, Ollama, etc.) through a backend proxy that keeps API keys secure.

## Architecture

```
Browser (React)                    Drill Backend (Java)              LLM Provider
+-------------------+             +----------------------+          +------------+
| ProspectorDrawer  |--POST SSE-->| ProspectorResources  |--HTTP--> | OpenAI     |
| useProspector hook|             | LlmProviderRegistry  |          | Anthropic  |
| Tool Executor     |             | OpenAiCompatProvider  |          | Ollama     |
| Quick Actions     |             | AnthropicProvider     |          +------------+
+-------------------+             | AiConfigResources     |
                                  | PersistentStore       |
                                  +----------------------+
```

**Key design decisions:**
- Backend proxy holds API keys; the frontend never sees them
- SSE streaming via JAX-RS `StreamingOutput` for real-time responses
- Tool calls are returned to the frontend for execution using existing API functions
- No new Maven or npm dependencies (uses OkHttp, Jackson, react-markdown already in the project)
- Configuration stored in `PersistentStoreProvider` (same pattern as visualizations/dashboards)

## Setup & Configuration

### 1. Access Prospector Settings

Click the robot icon in the top navigation bar to open the Prospector Settings modal.

### 2. Configure a Provider

| Provider | API Endpoint | API Key Required | Example Models |
|----------|-------------|-----------------|----------------|
| **OpenAI Compatible** | `https://api.openai.com/v1` (default) | Yes | `gpt-4o`, `gpt-4o-mini` |
| **Anthropic Claude** | `https://api.anthropic.com` (default) | Yes | `claude-sonnet-4-20250514`, `claude-haiku-4-5-20251001` |
| **Ollama (local)** | `http://localhost:11434/v1` | No | `llama3`, `mistral`, `codellama` |
| **Azure OpenAI** | Your Azure endpoint | Yes | Your deployed model name |

### 3. Required Settings

- **Provider**: Select from the dropdown
- **API Key**: Enter your API key (stored securely on the server, never sent to the browser)
- **Model**: Enter the model name
- **Enable Prospector**: Toggle on

### 4. Optional Settings

- **API Endpoint**: Override the default endpoint (useful for Ollama, Azure, or custom proxies)
- **Max Tokens**: Maximum response length (default: 4096)
- **Temperature**: Controls randomness (0 = deterministic, 2 = creative; default: 0.7)
- **Custom System Prompt**: Additional instructions appended to the default system prompt

### 5. Test Connection

Click "Test Connection" to validate your configuration before saving.

## Usage

### Opening Prospector

Click the floating AI button in the bottom-right corner of SQL Lab. The Prospector drawer opens on the right side.

### Asking Questions

Type a question or request in the chat input and press Enter (or click Send). Examples:

- "What tables are available in the dfs.tmp schema?"
- "Write a query to find the top 10 customers by revenue"
- "Explain this SQL query"
- "Create a bar chart showing sales by month"
- "Fix the error in my query"

### Quick Actions

Quick action buttons appear above the chat input:

- **Generate SQL**: Ask Prospector to generate a SQL query
- **Explain Query**: Explain the SQL currently in the editor (shown when editor has SQL)
- **Fix Error**: Fix the current query error (shown when there's an error)
- **Suggest Chart**: Suggest a visualization for the current results (shown when results exist)

### Tool Capabilities

Prospector has access to the following tools:

| Tool | Description |
|------|-------------|
| `execute_sql` | Execute SQL queries against Drill |
| `get_schema_info` | Browse schemas, tables, and columns |
| `create_visualization` | Create chart visualizations |
| `create_dashboard` | Create dashboards |
| `save_query` | Save SQL queries |
| `get_available_functions` | List Drill SQL functions |

When Prospector uses a tool, you'll see a collapsible panel showing the tool name, arguments, and results. Tool calls are executed automatically and Prospector uses the results to continue the conversation.

### Conversation Context

Prospector automatically receives context about your current state:
- The SQL in the editor
- The selected schema
- Available schemas
- Current query error (if any)
- Query result summary (row count, column names and types)

## REST API Reference

### Status Endpoint

```
GET /api/v1/ai/status
```

Returns whether Prospector is enabled and configured.

**Response:**
```json
{
  "enabled": true,
  "configured": true
}
```

### Chat Endpoint

```
POST /api/v1/ai/chat
Content-Type: application/json
```

Streams AI responses via Server-Sent Events (SSE).

**Request body:**
```json
{
  "messages": [
    { "role": "user", "content": "What tables are in dfs.tmp?" }
  ],
  "tools": [...],
  "context": {
    "currentSql": "SELECT * FROM ...",
    "currentSchema": "dfs.tmp",
    "availableSchemas": ["dfs", "dfs.tmp", "sys"],
    "error": null,
    "resultSummary": { "rowCount": 10, "columns": ["id", "name"], "columnTypes": ["INT", "VARCHAR"] }
  }
}
```

**SSE Events:**
```
event: delta
data: {"type":"content","content":"The tables in dfs.tmp are..."}

event: delta
data: {"type":"tool_call_start","id":"call_1","name":"get_schema_info"}

event: delta
data: {"type":"tool_call_delta","id":"call_1","arguments":"{\"schema\":\"dfs.tmp\"}"}

event: delta
data: {"type":"tool_call_end","id":"call_1"}

event: done
data: {"finish_reason":"tool_calls"}

event: error
data: {"message":"API key invalid"}
```

### Configuration Endpoints (Admin Only)

```
GET    /api/v1/ai/config           - Get config (API key redacted)
PUT    /api/v1/ai/config           - Update config (partial updates supported)
POST   /api/v1/ai/config/test      - Test configuration
GET    /api/v1/ai/config/providers  - List available providers
```

## File Structure

### Backend (Java)

```
exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/
  ai/
    LlmProvider.java            - Provider interface
    LlmConfig.java              - Config POJO (PersistentStore)
    ChatMessage.java            - Normalized chat message
    ToolDefinition.java         - Tool schema definition
    ToolCall.java               - Tool call from LLM
    ValidationResult.java       - Config validation result
    OpenAiCompatibleProvider.java - OpenAI/Ollama provider
    AnthropicProvider.java       - Anthropic Claude provider
    LlmProviderRegistry.java    - Provider registry
  ProspectorResources.java      - Chat SSE endpoint
  AiConfigResources.java        - Admin config endpoint
```

### Frontend (TypeScript/React)

```
webapp/src/
  types/ai.ts                   - TypeScript type definitions
  api/ai.ts                     - API client functions
  hooks/useProspector.ts        - Core chat hook with tool execution
  components/prospector/
    ProspectorDrawer.tsx         - Main drawer component
    ChatMessageList.tsx          - Scrollable message list
    ChatMessageBubble.tsx        - Individual message with markdown
    ToolCallDisplay.tsx          - Collapsible tool call panels
    QuickActionBar.tsx           - Quick action buttons
    ChatInput.tsx                - Text input with send/stop
    ProspectorButton.tsx         - Floating action button
    ProspectorSettingsModal.tsx  - Admin settings dialog
    index.ts                     - Barrel exports
```

## Troubleshooting

### Prospector button is disabled
- Check that Prospector is enabled in settings
- Verify an API key and model are configured
- Check the Drill logs for configuration errors

### "Prospector is not enabled" error
- Open Prospector Settings and ensure "Enable Prospector" is toggled on
- Make sure you've saved the configuration after enabling

### Streaming stops or errors
- Check that the API key is valid
- For Ollama, verify the server is running and the model is pulled
- Check Drill server logs for detailed error messages

### Tool calls fail
- Ensure the Drill cluster is running and accessible
- Check that schemas referenced in queries exist
- Verify the user has permissions for the operations
