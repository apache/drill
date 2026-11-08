/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { useState, useCallback, useRef, useEffect } from 'react';
import { message } from 'antd';
import { useQueryClient } from '@tanstack/react-query';
import { streamChat } from '../api/ai';
import { useSendDataToAi } from './useSendDataToAi';
import { executeQuery } from '../api/queries';
import { getSchemas, getTables, getColumns, searchFunctions } from '../api/metadata';
import { createVisualization } from '../api/visualizations';
import {
  getConversation as getServerConversation,
  saveConversationMessages,
} from '../api/conversations';
import {
  addVisualization, addDashboard, addSavedQuery, getProject, createWikiPage, updateWikiPage,
} from '../api/projects';
import { createDashboard } from '../api/dashboards';
import { createSavedQuery } from '../api/savedQueries';
import { createWorkflow, validationErrors } from '../api/workflows';
import { buildReportMarkdown, REPORTS_FOLDER } from '../utils/report';
import { sanitizeTabTitle } from '../utils/sql';
import type {
  ChatMessage,
  ToolCall,
  ToolDefinition,
  ChatContext,
  DeltaEvent,
  DoneEvent,
  UsageEvent,
} from '../types/ai';
import type { ChartType, QueryResult, VisualizationConfig } from '../types';

const DEFAULT_MAX_TOOL_ROUNDS = 15;
const PROJECT_DOC_MAX_CHARS = 8000;

export const TOOL_DEFINITIONS: ToolDefinition[] = [
  {
    name: 'execute_sql',
    description: 'Execute a SQL query against Apache Drill and return results',
    parameters: {
      type: 'object',
      properties: {
        sql: { type: 'string', description: 'The SQL query to execute' },
        limit: { type: 'number', description: 'Max rows (default 100)' },
        title: {
          type: 'string',
          description: 'A 2 to 5 word title naming what this query answers, in the same '
            + 'descriptive spirit as the comment you put at the top of the SQL. Used as the '
            + 'name of the query tab. For example "Top Talkers by Bytes" or '
            + '"Failed Logins by Host". Do not restate the table name alone.',
        },
      },
      required: ['sql'],
    },
  },
  {
    name: 'list_schemas',
    description: 'List all available schemas/data sources in Apache Drill. Call this first to discover what data is available before writing queries.',
    parameters: {
      type: 'object',
      properties: {},
    },
  },
  {
    name: 'get_schema_info',
    description: 'Get tables in a schema, or columns in a specific table. Drill uses hierarchical schemas (e.g. "mysql" has sub-schemas like "mysql.store"). To get tables, pass the full schema path. To get columns, pass the full schema path and just the table name (not dot-qualified).',
    parameters: {
      type: 'object',
      properties: {
        schema: { type: 'string', description: 'Full schema path, e.g. "dfs.tmp" or "mysql.store"' },
        table: { type: 'string', description: 'Table name without schema prefix, e.g. "order_items" (optional — omit to list tables)' },
      },
      required: ['schema'],
    },
  },
  {
    name: 'create_visualization',
    description: 'Create a chart visualization from query results',
    parameters: {
      type: 'object',
      properties: {
        name: { type: 'string' },
        chartType: {
          type: 'string',
          enum: ['bar', 'line', 'pie', 'scatter', 'table', 'heatmap', 'treemap', 'gauge', 'funnel', 'bigNumber'],
        },
        config: {
          type: 'object',
          description: 'VisualizationConfig: xAxis, yAxis, metrics, dimensions, chartOptions',
          properties: {
            xAxis: { type: 'string', description: 'Column name for x-axis' },
            yAxis: { type: 'string', description: 'Column name for y-axis' },
            metrics: { type: 'array', items: { type: 'string' }, description: 'Column names to use as metrics' },
            dimensions: { type: 'array', items: { type: 'string' }, description: 'Column names to use as dimensions' },
          },
        },
        sql: { type: 'string', description: 'The SQL query for this visualization' },
      },
      required: ['name', 'chartType', 'config', 'sql'],
    },
  },
  {
    name: 'create_dashboard',
    description: 'Create a new dashboard, optionally with visualization panels',
    parameters: {
      type: 'object',
      properties: {
        name: { type: 'string' },
        description: { type: 'string' },
        panels: {
          type: 'array',
          description: 'Array of panel objects with type, visualizationId, x, y, width, height',
          items: {
            type: 'object',
            properties: {
              type: { type: 'string', description: 'Panel type, e.g. "visualization"' },
              visualizationId: { type: 'string', description: 'ID of the visualization to display' },
              x: { type: 'number', description: 'Grid x position' },
              y: { type: 'number', description: 'Grid y position' },
              width: { type: 'number', description: 'Grid width' },
              height: { type: 'number', description: 'Grid height' },
            },
          },
        },
      },
      required: ['name'],
    },
  },
  {
    name: 'save_query',
    description: 'Save the current SQL query for later use',
    parameters: {
      type: 'object',
      properties: {
        name: { type: 'string' },
        description: { type: 'string' },
        sql: { type: 'string' },
      },
      required: ['name', 'sql'],
    },
  },
  {
    name: 'create_workflow',
    description: 'Create a scheduled workflow in the current project: a SQL query that runs on a '
      + 'schedule, followed by actions such as an email or an alert. The workflow is always '
      + 'created PAUSED and does nothing until the user activates it themselves. You cannot '
      + 'activate it, so never tell the user it is running; tell them it is ready for them to '
      + 'review and activate, from the Workflows page (the result gives its path). Only call '
      + 'this after the user has agreed to the schedule and the actions. Give the query as sql '
      + '(it is saved into the project for you, so do not call save_query first), or as '
      + 'savedQueryId to reuse a query you already saved with this tool. Do not call '
      + 'create_workflow again for a workflow that was created: it makes a second one. '
      + 'Actions run in order after the query. Action types and their config: '
      + 'email {to: string[] of addresses, subject: string, body: string}; '
      + 'record_alert {message: string}; '
      + 'save_to_table {location: "workspace" such as "dfs.tmp" (optional), format: '
      + '"parquet" | "json" | "csv" (optional), mode: "overwrite" | "append" | "newTablePerRun" '
      + '(optional)}. Text fields may use {{field}} templates: rowCount, rowCountDelta, '
      + 'deltaPercent, status, error, durationMs, and {{actions.<earlierActionId>.<output>}} '
      + '(email has no outputs, record_alert outputs message, save_to_table outputs path). '
      + 'Each action may carry a "when" condition over the same fields, for example '
      + '"rowCount > 0" or "status == \'error\' || deltaPercent > 20"; leave it out to run '
      + 'the action every time. If the server rejects the workflow the error lists each '
      + 'field to fix; call again with the same query passed as savedQueryId.',
    parameters: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Workflow name' },
        description: { type: 'string' },
        sql: {
          type: 'string',
          description: 'The SQL the workflow runs each time. Saved into the project for you.',
        },
        savedQueryId: {
          type: 'string',
          description: 'Id of a saved query already in this project, instead of sql.',
        },
        frequency: { type: 'string', enum: ['hourly', 'daily', 'weekly', 'monthly'] },
        timeOfDay: {
          type: 'string',
          description: '24-hour HH:mm, for example "06:30". Defaults to "08:00". An hourly '
            + 'workflow ignores it.',
        },
        dayOfWeek: {
          type: 'number',
          description: 'Required when frequency is weekly: 0 (Sunday) to 6 (Saturday).',
        },
        dayOfMonth: {
          type: 'number',
          description: 'Required when frequency is monthly: 1 to 28.',
        },
        actions: {
          type: 'array',
          description: 'What to do after the query runs, in order.',
          items: {
            type: 'object',
            properties: {
              id: {
                type: 'string',
                description: 'Letters, digits and underscores, starting with a letter, unique '
                  + 'in the workflow. Defaults to the type and position.',
              },
              type: { type: 'string', enum: ['email', 'record_alert', 'save_to_table'] },
              when: {
                type: 'string',
                description: 'Condition for running this action, e.g. "rowCount > 0". Omit to '
                  + 'always run it.',
              },
              config: { type: 'object', description: 'Settings for the action type.' },
            },
            required: ['type', 'config'],
          },
        },
      },
      required: ['name', 'frequency', 'actions'],
    },
  },
  {
    name: 'write_wiki_page',
    description: 'Write a page into the current project\'s wiki: create a new one, or replace '
      + 'the content of an existing one by passing its pageId. Only call this after the user '
      + 'has agreed to it. A new page goes in the Reports folder unless you give a folder, and '
      + 'creating a page whose title already exists in that folder is refused: pass that '
      + 'page\'s pageId to update it instead. To update a page you did not just write, find '
      + 'its id with get_project_docs. The page always ends with a note that Prospector wrote '
      + 'it.',
    parameters: {
      type: 'object',
      properties: {
        title: { type: 'string', description: 'Title for the page' },
        content: {
          type: 'string',
          description: 'The full page in markdown. When updating, this replaces the existing '
            + 'content, so send the whole page.',
        },
        folder: {
          type: 'string',
          description: 'Wiki folder for the page. Defaults to "Reports" when creating; an '
            + 'update leaves the page where it is unless you give one.',
        },
        includeTranscript: {
          type: 'boolean',
          description: 'Append the SQL queries run in this conversation, so a reader can '
            + 're-run them. Defaults to false. Set it to true for a report of findings.',
        },
        pageId: {
          type: 'string',
          description: 'Id of an existing page to update instead of creating a new one.',
        },
      },
      required: ['title', 'content'],
    },
  },
  {
    name: 'get_available_functions',
    description: 'Look up Apache Drill SQL functions with their signatures and descriptions. '
      + 'Pass a search term to filter by function name or description (e.g. "date", "json", '
      + '"regexp"); omit it only to browse, since the full catalog is large and the result is '
      + 'capped. Use this before assuming a MySQL or Postgres function exists in Drill, and to '
      + 'discover user-defined functions loaded on this cluster.',
    parameters: {
      type: 'object',
      properties: {
        search: {
          type: 'string',
          description: 'Case-insensitive substring matched against function names and '
            + 'descriptions. Prefer a short term over a long phrase.',
        },
      },
    },
  },
  {
    name: 'get_project_docs',
    description: 'Read the current project\'s documentation (wiki) pages. Call with no '
      + 'arguments to list page titles and ids, or with pageTitle to read one page in full. '
      + 'Use this when the project has documentation that may explain the data, its '
      + 'conventions, or the business meaning of a table or column.',
    parameters: {
      type: 'object',
      properties: {
        pageTitle: {
          type: 'string',
          description: 'Title of the page to read. Omit to list available pages.',
        },
      },
    },
  },
];

interface ToolCallInProgress {
  id: string;
  name: string;
  arguments: string;
}

/**
 * Where a server-primary conversation stands with the server. 'idle' means everything
 * on screen is saved; the rest are the ways it is not:
 *  - 'saving':    a write is in flight.
 *  - 'too-large': the server refused the write for size. Hard stop, see sendMessage.
 *  - 'conflict':  another device wrote first. The unsaved turns are kept, not replaced.
 *  - 'error':     the write (or the initial load) failed on the network. Retried.
 */
export type SaveStatus = 'idle' | 'saving' | 'too-large' | 'conflict' | 'error';

const SERVER_SAVE_DEBOUNCE_MS = 2000;

/**
 * Offline buffer for a server-primary conversation: the messages on screen and the
 * server version they were built on. It exists only because the server is primary, so a
 * rejected write is a lost turn, not a lost sync. On the next load, a buffer that still
 * differs from the server is unsaved work: replayed if the version matches, surfaced as
 * a conflict if the server has moved on. It is removed as soon as the server holds the
 * same messages.
 */
interface ConversationBuffer {
  baseVersion: number;
  messages: ChatMessage[];
}

const conversationBufferKey = (projectId: string, cid: string) =>
  `prospector_conversation_buffer_${projectId}_${cid}`;

function readConversationBuffer(key: string): ConversationBuffer | null {
  try {
    const raw = localStorage.getItem(key);
    const parsed = raw ? JSON.parse(raw) : null;
    return parsed && Array.isArray(parsed.messages) && typeof parsed.baseVersion === 'number'
      ? parsed
      : null;
  } catch {
    return null;
  }
}

/**
 * Returns whether the buffer was actually stored. The caller must act on false: the
 * buffer is the only copy of an unsaved turn, so a quota or private-mode failure here
 * means the safety net is gone and the user has to be told.
 */
function writeConversationBuffer(key: string, buffer: ConversationBuffer): boolean {
  try {
    localStorage.setItem(key, JSON.stringify(buffer));
    return true;
  } catch {
    return false;
  }
}

function removeConversationBuffer(key: string): void {
  try {
    localStorage.removeItem(key);
  } catch {
    // Ignore storage errors
  }
}

/**
 * Removes the unanswered tool calls from a trailing assistant message. Anthropic and
 * OpenAI both reject a tool_use with no matching tool_result with a 400, so a conversation
 * holding one cannot take another turn, and one saved to the server stays malformed for
 * good. The prose the model wrote alongside the calls is real content and is kept; only
 * the calls go, and a message with no prose goes entirely.
 *
 * Applied wherever history leaves the hook or comes back from the buffer: the server
 * write, a buffer read on load, and the provider request. One only exists when a tool
 * round was cut off (Stop, a conversation switch, a closed page).
 */
function dropUnmatchedToolUse(messages: ChatMessage[]): ChatMessage[] {
  const last = messages[messages.length - 1];
  if (!last || last.role !== 'assistant' || !last.toolCalls || last.toolCalls.length === 0) {
    return messages;
  }
  const rest = messages.slice(0, -1);
  return last.content ? [...rest, { role: 'assistant', content: last.content }] : rest;
}

export interface UseProspectorReturn {
  messages: ChatMessage[];
  isStreaming: boolean;
  streamingContent: string;
  /** Most recent usage event from the active or just-completed chat call. */
  usage: UsageEvent | null;
  sendMessage: (text: string, context: ChatContext) => void;
  stopStreaming: () => void;
  clearChat: () => void;
  /** Runs a single tool call. Exposed so its behaviour can be asserted directly. */
  executeToolCall: (toolCall: ToolCall, context?: ChatContext) => Promise<string>;
  /** The server-side conversation this hook is bound to, or null when it is not. */
  conversationId: string | null;
  /** True once the bound conversation's messages are in state, so sendMessage will be accepted.
   *  Always false without a conversationId. */
  isLoaded: boolean;
  /** Sync state of a server-primary conversation; always 'idle' without a conversationId. */
  saveStatus: SaveStatus;
  /** Discards unsaved local turns and takes the server's copy. The way out of a 'conflict'
   *  (or a failed load); always an explicit user action, never automatic. */
  reloadConversation: () => void;
}

export function useProspector(
  onSqlGenerated?: (sql: string, title?: string) => void,
  onVisualizationCreated?: (id: string, name: string) => void,
  maxToolRounds?: number,
  // The server-primary conversation this hook is bound to (needs projectId too). The
  // server owns it: it is loaded from there and every change is saved there.
  // localStorage is only an offline buffer, so a write the server refuses or a network
  // that drops mid-save leaves a turn that exists nowhere else. Every status other than
  // 'idle' must therefore reach the user and none may discard a turn.
  conversationId?: string | null,
  projectId?: string | null,
  // Called with the SQL and the FULL result of every execute_sql the model runs. The model
  // sees only a summary (and rows only when sendDataToAi allows). This is for the user's own
  // screen and is not subject to that gate: the SQL is already shown to them through
  // onSqlGenerated, and the rows never go back into the model's messages. Never forward it
  // to the model.
  onQueryResult?: (sql: string, result: QueryResult) => void,
): UseProspectorReturn {
  const serverMode = !!conversationId && !!projectId;
  const effectiveMaxToolRounds = maxToolRounds && maxToolRounds > 0 ? maxToolRounds : DEFAULT_MAX_TOOL_ROUNDS;
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [isStreaming, setIsStreaming] = useState(false);
  const [streamingContent, setStreamingContent] = useState('');
  const [usage, setUsage] = useState<UsageEvent | null>(null);
  const abortRef = useRef<AbortController | null>(null);
  const toolRoundsRef = useRef(0);

  // Server-primary bookkeeping. Refs, because the save pipeline reads them from timers
  // and promise callbacks that must see the latest value, not the render they closed over.
  const [saveStatus, setSaveStatusState] = useState<SaveStatus>('idle');
  const saveStatusRef = useRef<SaveStatus>('idle');
  const setSaveStatus = useCallback((status: SaveStatus) => {
    saveStatusRef.current = status;
    setSaveStatusState(status);
  }, []);
  const activeConversationRef = useRef<string | null>(null);
  activeConversationRef.current = serverMode ? (conversationId ?? null) : null;
  // The conversation whose messages are in state now; null while loading or after a failed load.
  const loadedForRef = useRef<string | null>(null);
  // The same fact as loadedForRef, as state, so a caller can wait for it.
  const [loadedId, setLoadedId] = useState<string | null>(null);
  // Last server version known per conversation. Read when a save RUNS, not when it is
  // scheduled, so turns batched by the debounce never send a stale version.
  const versionsRef = useRef<Record<string, number>>({});
  // The array last known to match the server; messages equal to it have nothing to save.
  const lastSyncedRef = useRef<ChatMessage[] | null>(null);
  const loadTokenRef = useRef(0);
  // Every save runs through this chain, one at a time: a second write started before the
  // first returns would carry the same version and 409 against ourselves.
  const saveChainRef = useRef<Promise<void>>(Promise.resolve());
  const pendingSaveRef = useRef<{ pid: string; cid: string; messages: ChatMessage[] } | null>(null);

  // Read inside executeToolCall, which must not re-create itself on every token.
  const messagesRef = useRef<ChatMessage[]>(messages);
  messagesRef.current = messages;

  // The sendDataToAi privacy setting, read here rather than accepted as a parameter so
  // that every caller of this hook is gated by construction — the previous design had
  // each caller thread it through ChatContext, and the two that forgot (the global
  // Prospector tab, the dashboard panels) silently sent rows with the setting off.
  // useSendDataToAi reads it from /status (not the admin-only /config) and fails closed
  // while loading or on error; see its doc comment for the full rationale. The ref form
  // is used below so executeToolCall's dependency array doesn't churn on every fetch.
  const { sendDataToAiRef } = useSendDataToAi();
  const queryClient = useQueryClient();

  // Identifies the stream round a callback belongs to. Aborting the SSE controller does
  // not reach a continuation parked on `await executeToolCall`, which would later write
  // its (now foreign) messages into whatever conversation is on screen. Bumped by every
  // stop and conversation load, and by every new round; a callback whose epoch is no
  // longer current has been overtaken and must not touch state.
  const streamEpochRef = useRef(0);

  const stopStreaming = useCallback(() => {
    streamEpochRef.current += 1;
    if (abortRef.current) {
      abortRef.current.abort();
      abortRef.current = null;
    }
    setIsStreaming(false);
    setStreamingContent('');
  }, []);

  const clearChat = useCallback(() => {
    if (serverMode) {
      // A server-primary conversation is a durable, named artifact: emptying it would
      // write [] over the server record in one click, including at the size cap where
      // this is the button a user reaches for. The hook refuses regardless of the caller.
      message.info('A saved conversation cannot be cleared. Start a new conversation instead.');
      return;
    }
    stopStreaming();
    setMessages([]);
    setUsage(null);
    toolRoundsRef.current = 0;
  }, [stopStreaming, serverMode]);

  // Every save timer below (the debounce, and the flush/reload paths that cancel it) uses
  // this ref. Only the server-primary path writes conversations, so it is the sole owner.
  const serverSaveTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // ---- Server-primary persistence (see the conversationId parameter) ----

  // Stores the buffer and, once per failure streak, tells the user when it could not be:
  // the buffer is then the only thing standing between an unsaved turn and its loss, and
  // saveStatus alone would read 'idle' or 'saving' while that net is missing.
  const bufferWarnedRef = useRef(false);
  const storeBuffer = useCallback((key: string, buffer: ConversationBuffer) => {
    if (writeConversationBuffer(key, buffer)) {
      bufferWarnedRef.current = false;
      return;
    }
    if (!bufferWarnedRef.current) {
      bufferWarnedRef.current = true;
      message.warning(
        'This browser would not store a local backup of your conversation. If saving to the '
        + 'server fails, your latest messages cannot be recovered, so avoid closing this tab '
        + 'until the conversation shows as saved.',
        10,
      );
    }
  }, []);

  const enqueueSave = useCallback((pid: string, cid: string, saving: ChatMessage[]) => {
    saveChainRef.current = saveChainRef.current.then(async () => {
      const version = versionsRef.current[cid];
      if (version === undefined) {
        // No known version means nothing can be written; never a silent no-op.
        console.error('Cannot save conversation: no known version for', cid);
        if (activeConversationRef.current === cid) {
          setSaveStatus('error');
        }
        return;
      }
      // The conversation can change under a save in flight. The write still goes to the
      // conversation it was made for, but only the active one may touch hook state.
      const isActive = () => activeConversationRef.current === cid;
      if (isActive()) {
        setSaveStatus('saving');
      }
      const key = conversationBufferKey(pid, cid);
      try {
        const result = await saveConversationMessages(
          pid, cid, dropUnmatchedToolUse(saving), version);
        if (result.status === 'ok') {
          if (result.version === undefined) {
            // The write may have landed, but without its version the next one would carry
            // a stale or missing version. Leave the map and the buffer alone and say so.
            throw new Error('Server accepted the save but returned no version');
          }
          const saved = result.version;
          versionsRef.current[cid] = saved;
          const buffer = readConversationBuffer(key);
          // Delete the buffer only when it holds nothing this write did not. The user can
          // add turns while a write is in flight, and a flush after a conversation switch
          // queues a later snapshot behind this one; in both cases the buffer is still the
          // only copy of the extra turns, so it is rebased onto the new version instead.
          const covered = isActive()
            ? messagesRef.current === saving
            : !buffer || JSON.stringify(buffer.messages) === JSON.stringify(saving);
          if (covered) {
            removeConversationBuffer(key);
          } else if (buffer) {
            storeBuffer(key, { ...buffer, baseVersion: saved });
          }
          if (isActive()) {
            lastSyncedRef.current = saving;
            setSaveStatus('idle');
          }
        } else if (isActive()) {
          // Not retried: the same write would be refused again. The turns stay on screen
          // and in the buffer until the user acts.
          setSaveStatus(result.status);
        }
      } catch (err) {
        console.error('Saving conversation failed; keeping it in the local buffer', err);
        if (isActive()) {
          setSaveStatus('error');
        }
      }
    });
  }, [setSaveStatus, storeBuffer]);

  const loadConversation = useCallback((pid: string, cid: string) => {
    const token = ++loadTokenRef.current;
    loadedForRef.current = null;
    setLoadedId(null);
    setSaveStatus('idle');
    const key = conversationBufferKey(pid, cid);
    getServerConversation(pid, cid).then(({ record, messages: serverMessages }) => {
      if (token !== loadTokenRef.current) {
        return;
      }
      versionsRef.current[cid] = record.version;
      lastSyncedRef.current = serverMessages;
      let shown = serverMessages;
      const buffer = readConversationBuffer(key);
      const buffered = buffer ? dropUnmatchedToolUse(buffer.messages) : null;
      if (buffer && JSON.stringify(buffered) !== JSON.stringify(serverMessages)) {
        // Unsaved turns from an earlier session. They are never dropped here: shown as
        // they were, and either replayed (the save effect picks them up) or, when the
        // server has moved on since, held back as a conflict for the user to resolve.
        shown = buffered as ChatMessage[];
        if (buffer.baseVersion !== record.version) {
          // Keep the buffer rooted at the version it was built on; saves are paused, and
          // rebasing it onto the server's version would make the next load overwrite.
          versionsRef.current[cid] = buffer.baseVersion;
          setSaveStatus('conflict');
        }
      } else if (buffer) {
        removeConversationBuffer(key);
      }
      loadedForRef.current = cid;
      setLoadedId(cid);
      setMessages(shown);
      setUsage(null);
      toolRoundsRef.current = 0;
    }).catch((err) => {
      if (token !== loadTokenRef.current) {
        return;
      }
      console.error('Loading conversation failed', err);
      // Show whatever the buffer holds so nothing looks lost, but stay unloaded: with no
      // known version nothing can be saved, and sendMessage refuses until a reload works.
      setMessages(readConversationBuffer(key)?.messages ?? []);
      setSaveStatus('error');
    });
  }, [setSaveStatus]);

  const reloadConversation = useCallback(() => {
    if (!conversationId || !projectId) {
      return;
    }
    // Overtakes any stream or tool round still running for the copy being discarded.
    stopStreaming();
    if (serverSaveTimerRef.current) {
      clearTimeout(serverSaveTimerRef.current);
    }
    pendingSaveRef.current = null;
    removeConversationBuffer(conversationBufferKey(projectId, conversationId));
    loadConversation(projectId, conversationId);
  }, [conversationId, projectId, loadConversation, stopStreaming]);

  // Load on mount and whenever the conversation changes. The previous conversation's
  // stream is stopped first so its late deltas cannot land in the new thread.
  useEffect(() => {
    if (!conversationId || !projectId) {
      return;
    }
    stopStreaming();
    setMessages([]);
    toolRoundsRef.current = 0;
    loadConversation(projectId, conversationId);
    return () => {
      loadTokenRef.current += 1;
    };
  }, [conversationId, projectId, loadConversation, stopStreaming]);

  // Switching away (or unmounting) inside the debounce window would otherwise drop the
  // pending write. Flush it to the conversation it belongs to.
  useEffect(() => {
    if (!conversationId || !projectId) {
      return;
    }
    return () => {
      const pending = pendingSaveRef.current;
      if (pending && pending.cid === conversationId) {
        pendingSaveRef.current = null;
        if (serverSaveTimerRef.current) {
          clearTimeout(serverSaveTimerRef.current);
        }
        enqueueSave(pending.pid, pending.cid, pending.messages);
      }
    };
  }, [conversationId, projectId, enqueueSave]);

  // Keep the offline buffer equal to what is on screen until the server holds it too.
  useEffect(() => {
    if (!conversationId || !projectId || loadedForRef.current !== conversationId) {
      return;
    }
    const key = conversationBufferKey(projectId, conversationId);
    if (messages === lastSyncedRef.current) {
      removeConversationBuffer(key);
    } else {
      storeBuffer(key, {
        baseVersion: versionsRef.current[conversationId] ?? 0,
        messages,
      });
    }
  }, [messages, conversationId, projectId, storeBuffer]);

  // Debounced write, never while streaming (a half-finished tool round is not worth
  // storing) and not once the server has refused: too-large and conflict wait for the user.
  useEffect(() => {
    if (!conversationId || !projectId) {
      return;
    }
    if (loadedForRef.current !== conversationId || messages === lastSyncedRef.current) {
      return;
    }
    if (isStreaming) {
      // The buffer covers this turn; a snapshot scheduled before the stream began is stale.
      pendingSaveRef.current = null;
      return;
    }
    if (saveStatusRef.current === 'too-large' || saveStatusRef.current === 'conflict') {
      return;
    }
    const pending = { pid: projectId, cid: conversationId, messages };
    pendingSaveRef.current = pending;
    const timer = setTimeout(() => {
      pendingSaveRef.current = null;
      enqueueSave(pending.pid, pending.cid, pending.messages);
    }, SERVER_SAVE_DEBOUNCE_MS);
    serverSaveTimerRef.current = timer;
    return () => clearTimeout(timer);
  }, [messages, conversationId, projectId, isStreaming, enqueueSave]);

  // Replay after a network failure as soon as the browser is back online.
  useEffect(() => {
    if (!conversationId || !projectId) {
      return;
    }
    const onOnline = () => {
      if (saveStatusRef.current !== 'error') {
        return;
      }
      if (loadedForRef.current === conversationId) {
        enqueueSave(projectId, conversationId, messagesRef.current);
      } else {
        loadConversation(projectId, conversationId);
      }
    };
    window.addEventListener('online', onOnline);
    return () => window.removeEventListener('online', onOnline);
  }, [conversationId, projectId, enqueueSave, loadConversation]);

  // A ref, so a caller's changing callback never rebuilds executeToolCall mid-conversation.
  const onQueryResultRef = useRef(onQueryResult);
  onQueryResultRef.current = onQueryResult;

  const executeToolCall = useCallback(async (toolCall: ToolCall, context?: ChatContext): Promise<string> => {
    try {
      const args = JSON.parse(toolCall.arguments);

      switch (toolCall.name) {
        case 'execute_sql': {
          const limit = args.limit || 100;
          const result = await executeQuery({
            query: args.sql,
            queryType: 'SQL',
            autoLimitRowCount: limit,
          });
          // Also set the SQL in the editor
          if (onSqlGenerated && args.sql) {
            onSqlGenerated(args.sql as string, sanitizeTabTitle(args.title));
          }
          if (typeof args.sql === 'string') {
            onQueryResultRef.current?.(args.sql, result);
          }
          const rowCount = result.rows?.length ?? 0;
          const summary = {
            columns: result.columns,
            rowCount,
            // Shape (columns, count) is metadata and always safe; the rows themselves
            // are user data and go only when the deployment permits it.
            rows: sendDataToAiRef.current ? result.rows?.slice(0, 5) : undefined,
          };
          return JSON.stringify(summary);
        }

        case 'list_schemas': {
          // When inside a project, return only project-scoped schemas
          if (context?.projectDatasets && context.projectDatasets.length > 0) {
            const projectSchemas = [...new Set(
              context.projectDatasets
                .map((d) => d.schema)
                .filter(Boolean) as string[]
            )];
            return JSON.stringify({ schemas: projectSchemas });
          }
          const schemas = await getSchemas();
          return JSON.stringify({ schemas: schemas.map((s) => s.name) });
        }

        case 'get_schema_info': {
          if (args.table) {
            const columns = await getColumns(args.schema, args.table);
            return JSON.stringify({ columns });
          }
          const tables = await getTables(args.schema);
          return JSON.stringify({ tables });
        }

        case 'create_visualization': {
          const viz = await createVisualization({
            name: args.name,
            chartType: args.chartType as ChartType,
            config: args.config as VisualizationConfig,
            sql: args.sql,
          });

          // Attach it to the active project, as VisualizationBuilder does on its
          // own save path. Without this the visualization is created but orphaned,
          // so it never appears in the project the user is working in.
          let addedToProject = false;
          let projectError: string | undefined;
          if (context?.projectId) {
            try {
              await addVisualization(context.projectId, viz.id);
              addedToProject = true;
            } catch (err) {
              // The visualization itself exists, so don't fail the whole call and
              // report it as never created. Tell the model what actually happened.
              projectError = err instanceof Error ? err.message : 'unknown error';
              console.error('Failed to add visualization to project', err);
            }
          }

          onVisualizationCreated?.(viz.id, viz.name);
          return JSON.stringify({
            id: viz.id,
            name: viz.name,
            addedToProject,
            ...(projectError ? { projectError } : {}),
            message: `Visualization "${viz.name}" created successfully.`,
            viewPath: '/visualizations',
          });
        }

        case 'create_dashboard': {
          const dashboard = await createDashboard({
            name: args.name,
            description: args.description,
            panels: args.panels,
          });

          // Same orphaning problem as create_visualization above.
          let addedToProject = false;
          let projectError: string | undefined;
          if (context?.projectId) {
            try {
              await addDashboard(context.projectId, dashboard.id);
              addedToProject = true;
            } catch (err) {
              projectError = err instanceof Error ? err.message : 'unknown error';
              console.error('Failed to add dashboard to project', err);
            }
          }

          return JSON.stringify({
            id: dashboard.id,
            name: dashboard.name,
            addedToProject,
            ...(projectError ? { projectError } : {}),
            message: `Dashboard "${dashboard.name}" created successfully.`,
            viewPath: '/dashboards',
          });
        }

        case 'save_query': {
          const saved = await createSavedQuery({
            name: args.name,
            description: args.description,
            sql: args.sql,
          });

          // Attach it to the active project, or it is created but missing from the project's
          // Saved Queries tab, which filters on the project's own id list. That tab is how
          // the user checks what Prospector built.
          let addedToProject = false;
          let projectError: string | undefined;
          if (context?.projectId) {
            try {
              await addSavedQuery(context.projectId, saved.id);
              addedToProject = true;
            } catch (err) {
              console.error('Failed to add saved query to project', err);
              // The query itself exists, so this is not reported as a failure: its card and
              // canvas need the id. Only the model's next move needs steering.
              const status = (err as { response?: { status?: number } })?.response?.status;
              projectError = status === 403
                // Only a project's owner can add to it, while any member can save a query.
                // Retrying fails the same way, so say so rather than invite a loop.
                ? 'The query is saved but is not in the project: only the project owner can add a saved query to it. Do not retry. Tell the user the project owner has to add it to the project.'
                : `The query is saved but could not be added to the project (${err instanceof Error ? err.message : 'unknown error'}). Do not call save_query again: that saves a duplicate. Tell the user.`;
            }
            if (addedToProject) {
              await queryClient.invalidateQueries({ queryKey: ['project', context.projectId] });
            }
          }

          return JSON.stringify({
            id: saved.id,
            name: saved.name,
            addedToProject,
            ...(projectError ? { projectError } : {}),
            message: 'Query saved successfully',
          });
        }

        case 'create_workflow': {
          if (!context?.projectId) {
            return JSON.stringify({ error: 'No active project — a workflow can only be '
              + 'created inside a project. Ask the user to open one.' });
          }
          const projectId = context.projectId;
          const hasSql = typeof args.sql === 'string' && args.sql.trim() !== '';
          if (!hasSql && !args.savedQueryId) {
            return JSON.stringify({ error: 'create_workflow needs the query to run: pass sql or '
              + 'savedQueryId.' });
          }
          // The server only accepts a saved query that is in the workflow's project, so a query
          // given as sql is saved and added here, and a failure to add it stops the call.
          let savedQueryId: string | undefined = args.savedQueryId;
          if (hasSql) {
            const saved = await createSavedQuery({
              name: args.name,
              description: args.description,
              sql: args.sql,
            });
            savedQueryId = saved.id;
            try {
              await addSavedQuery(projectId, saved.id);
            } catch (err) {
              console.error('Failed to add saved query to project', err);
              // Only a project's owner can add to it, while any member can create a workflow.
              // For a member neither sql nor savedQueryId can ever work, so say so and stop
              // rather than offering a retry that fails the same way.
              const status = (err as { response?: { status?: number } })?.response?.status;
              if (status === 403) {
                return JSON.stringify({
                  error: 'No workflow was created. A workflow needs its query saved in the project, and only the project owner can add a saved query to it. Do not retry. Tell the user the project owner has to create this workflow, or has to add the saved query to the project first.',
                });
              }
              const reason = err instanceof Error ? err.message : 'unknown error';
              return JSON.stringify({
                error: `No workflow was created: the query was saved but could not be added to the project (${reason}). Calling create_workflow again with sql will work only if the cause was temporary, and the query is saved again. Try that at most once, then tell the user.`,
              });
            }
          }
          try {
            const created = await createWorkflow({
              projectId,
              name: args.name,
              description: args.description,
              savedQueryId: savedQueryId as string,
              schedule: {
                frequency: args.frequency,
                timeOfDay: args.timeOfDay || '08:00',
                dayOfWeek: args.dayOfWeek ?? null,
                dayOfMonth: args.dayOfMonth ?? null,
                timeoutSeconds: 300,
                enabled: false,
              },
              actions: (Array.isArray(args.actions) ? args.actions : []).map(
                (a: Record<string, unknown>, i: number) => ({
                  id: (a.id as string) || `${a.type}_${i + 1}`,
                  type: a.type as string,
                  condition: typeof a.when === 'string' && a.when.trim()
                    ? { expr: a.when }
                    : { always: true },
                  config: (a.config ?? {}) as Record<string, unknown>,
                })),
              retentionCount: 30,
            });
            await queryClient.invalidateQueries({ queryKey: ['project', projectId] });
            await queryClient.invalidateQueries({ queryKey: ['workflows'] });
            const viewPath = `/projects/${projectId}/workflows/${created.id}`;
            return JSON.stringify({
              id: created.id,
              name: created.name,
              savedQueryId,
              status: created.status,
              message: `Workflow "${created.name}" was created paused. It is not running and will not run until the user activates it. They can review and activate it on the Workflows page at ${viewPath}.`,
              viewPath,
            });
          } catch (err) {
            // Handled here rather than by the outer catch so the saved query's id survives
            // for the retry: calling again with sql would save the same query twice.
            const fields = validationErrors(err);
            const reason = fields
              ? fields.map((e) => `${e.path}: ${e.message}`).join('; ')
              : (err instanceof Error ? err.message : 'create_workflow failed');
            console.error('Tool "create_workflow" failed', err);
            return JSON.stringify({
              error: `Workflow not created. ${reason}. Fix this and call create_workflow again${hasSql ? ` with savedQueryId "${savedQueryId}" instead of sql` : ''}.`,
              savedQueryId,
            });
          }
        }

        case 'write_wiki_page': {
          if (!context?.projectId) {
            return JSON.stringify({ error: 'No active project — a wiki page can only be '
              + 'written inside a project. Ask the user to open one.' });
          }
          const projectId = context.projectId;
          if (typeof args.title !== 'string' || !args.title.trim()
              || typeof args.content !== 'string') {
            return JSON.stringify({ error: 'write_wiki_page: title and content are required '
              + 'and must be strings. Call it again with both.' });
          }
          const title = args.title.trim();
          const pageId = args.pageId as string | undefined;
          const folder = args.folder as string | undefined;
          // The footer is always written, so only the query appendix follows
          // includeTranscript. The conversation id is the closed-over one: the conversation
          // that issued this call.
          const markdown = buildReportMarkdown(
            args.content,
            messagesRef.current,
            {
              generatedAt: Date.now(),
              conversationId: (serverMode ? conversationId : null) ?? undefined,
            },
            { includeQueries: args.includeTranscript === true }
          );

          let page;
          if (pageId) {
            try {
              // folder is left out unless asked for: an update must not move the page.
              page = await updateWikiPage(projectId, pageId, {
                title,
                content: markdown,
                ...(folder ? { folder } : {}),
              });
            } catch (err) {
              if ((err as { response?: { status?: number } }).response?.status === 404) {
                return JSON.stringify({ error: `No wiki page with id "${pageId}" in this `
                  + 'project; it may have been deleted. Call write_wiki_page without pageId '
                  + 'to create a new page, or use get_project_docs to find the right id.' });
              }
              throw err;
            }
          } else {
            const targetFolder = folder || REPORTS_FOLDER;
            // Read fresh rather than from the query cache, which may predate a page
            // written earlier in this same conversation.
            const existing = ((await getProject(projectId)).wikiPages || []).find((p) =>
              (p.folder || '') === targetFolder
              && p.title.trim().toLowerCase() === title.toLowerCase());
            if (existing) {
              return JSON.stringify({ error: `A page titled "${existing.title}" already exists `
                + `in ${targetFolder} (id ${existing.id}). Pass pageId "${existing.id}" to `
                + 'update it, or choose a different title.' });
            }
            page = await createWikiPage(projectId, {
              title,
              content: markdown,
              folder: targetFolder,
            });
          }
          // Same collision-check cache that SaveReportModal reads; without this a
          // second write_wiki_page call within the staleTime window would not see this
          // page either.
          await queryClient.invalidateQueries({ queryKey: ['project', projectId] });
          await queryClient.invalidateQueries({ queryKey: ['projects'] });
          return JSON.stringify({
            id: page.id,
            title: page.title,
            updated: !!pageId,
            message: pageId
              ? `Wiki page "${page.title}" updated.`
              : `Wiki page "${page.title}" created in ${page.folder || REPORTS_FOLDER}.`,
            viewPath: `/projects/${projectId}/wiki/${page.id}`,
          });
        }

        case 'get_available_functions': {
          const search = args.search as string | undefined;
          const result = await searchFunctions(search);
          // Details carry the signatures; the bare name list adds nothing on top of them and
          // would double the payload, so it is only used when detail came back empty.
          if (result.details && result.details.length > 0) {
            return JSON.stringify({
              functions: result.details,
              matchCount: result.matchCount,
              truncated: result.truncated,
              ...(result.truncated
                ? { note: 'Results were capped. Narrow the search term to see more.' }
                : {}),
            });
          }
          return JSON.stringify({
            functions: [],
            matchCount: 0,
            note: search
              ? `No Drill function matches "${search}". It may not exist — do not invent one.`
              : 'No functions returned.',
          });
        }

        case 'get_project_docs': {
          if (!context?.projectId) {
            return JSON.stringify({ error: 'No active project — get_project_docs is '
              + 'only available inside a project.' });
          }
          const project = await getProject(context.projectId);
          const pages = project.wikiPages || [];
          if (!args.pageTitle) {
            return JSON.stringify({ pages: pages.map((p) => ({ id: p.id, title: p.title })) });
          }
          const page = pages.find((p) => p.title === args.pageTitle);
          if (!page) {
            return JSON.stringify({
              error: `No documentation page titled "${args.pageTitle}". Available: `
                + `${pages.map((p) => p.title).join(', ') || 'none'}`,
            });
          }
          const content = page.content || '';
          return JSON.stringify({
            id: page.id,
            title: page.title,
            content: content.length > PROJECT_DOC_MAX_CHARS
              ? `${content.slice(0, PROJECT_DOC_MAX_CHARS)}...[truncated]`
              : content,
          });
        }

        default:
          return JSON.stringify({ error: `Unknown tool: ${toolCall.name}` });
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Tool execution failed';
      // The result goes to the model, which may narrate success regardless, so
      // without this the failure is invisible to both the user and the console.
      console.error(`Tool "${toolCall.name}" failed`, err);
      return JSON.stringify({ error: msg });
    }
  }, [onSqlGenerated, onVisualizationCreated, sendDataToAiRef, serverMode, conversationId, queryClient]);

  const doStreamRound = useCallback((
    allMessages: ChatMessage[],
    context: ChatContext,
  ) => {
    const epoch = ++streamEpochRef.current;
    setIsStreaming(true);
    setStreamingContent('');

    let contentBuffer = '';
    const pendingToolCalls: Map<string, ToolCallInProgress> = new Map();

    const controller = streamChat(
      { messages: allMessages, tools: TOOL_DEFINITIONS, context },
      // onDelta
      (event: DeltaEvent) => {
        switch (event.type) {
          case 'content':
            contentBuffer += event.content;
            setStreamingContent(contentBuffer);
            break;
          case 'tool_call_start':
            pendingToolCalls.set(event.id, {
              id: event.id,
              name: event.name,
              arguments: '',
            });
            break;
          case 'tool_call_delta': {
            const tc = pendingToolCalls.get(event.id);
            if (tc) {
              tc.arguments += event.arguments;
            }
            break;
          }
          case 'tool_call_end':
            // Tool call complete, arguments fully assembled
            break;
        }
      },
      // onDone
      async (doneEvent: DoneEvent) => {
        if (epoch !== streamEpochRef.current) {
          return;
        }
        const continuing = doneEvent.finish_reason === 'tool_calls' && pendingToolCalls.size > 0;
        // Stays true through the tool phase (the next round sets it again). The history
        // holds an assistant tool_use with no result until the tool returns, and while
        // isStreaming is true the input is disabled and no save is scheduled, so no turn
        // can be built on, or persisted with, that unmatched tool_use.
        if (!continuing) {
          setIsStreaming(false);
        }
        setStreamingContent('');

        if (continuing) {
          // Build assistant message with tool calls
          // A tool invoked with no arguments (get_available_functions, or
          // get_project_docs listing page titles) streams no argument deltas, leaving
          // this blank. Blank is not valid JSON: it breaks JSON.parse in
          // executeToolCall and serializes to a null tool_use.input, which the
          // Anthropic API rejects.
          const toolCalls: ToolCall[] = Array.from(pendingToolCalls.values()).map((tc) => ({
            id: tc.id,
            name: tc.name,
            arguments: tc.arguments || '{}',
          }));

          const assistantMsg: ChatMessage = {
            role: 'assistant',
            content: contentBuffer || null,
            toolCalls,
          };

          const updatedMessages = [...allMessages, assistantMsg];
          setMessages(updatedMessages);

          // Execute tool calls
          const toolResults: ChatMessage[] = [];
          for (const tc of toolCalls) {
            const result = await executeToolCall(tc, context);
            toolResults.push({
              role: 'tool',
              content: result,
              toolCallId: tc.id,
              name: tc.name,
            });
          }

          if (epoch !== streamEpochRef.current) {
            // Stopped, cleared or switched to another conversation while a tool ran. The
            // results complete the history only if nothing has replaced it; after a switch
            // `prev` is the other conversation and must not be touched. No further round.
            setMessages((prev) => (prev === updatedMessages ? [...prev, ...toolResults] : prev));
            return;
          }

          const messagesWithResults = [...updatedMessages, ...toolResults];
          setMessages(messagesWithResults);

          // Check round limit
          toolRoundsRef.current += 1;
          if (toolRoundsRef.current >= effectiveMaxToolRounds) {
            // Add a system-level note
            const limitMsg: ChatMessage = {
              role: 'assistant',
              content: 'I\'ve reached the maximum number of tool call rounds. Please send another message if you need more help.',
            };
            setMessages((prev) => [...prev, limitMsg]);
            setIsStreaming(false);
            return;
          }

          // Continue the conversation with tool results
          doStreamRound(messagesWithResults, context);
        } else {
          // Normal stop - add assistant message
          if (contentBuffer) {
            const assistantMsg: ChatMessage = {
              role: 'assistant',
              content: contentBuffer,
            };
            setMessages((prev) => [...prev, assistantMsg]);
          }
          toolRoundsRef.current = 0;
        }
      },
      // onError
      (error) => {
        if (epoch !== streamEpochRef.current) {
          return;
        }
        setIsStreaming(false);
        setStreamingContent('');
        const errorMsg: ChatMessage = {
          role: 'assistant',
          content: `Error: ${error.message}`,
        };
        setMessages((prev) => [...prev, errorMsg]);
      },
      // onUsage — keep the most recent counts. During tool-call rounds the
      // server emits multiple usage events; we accumulate by always replacing
      // with the latest cumulative snapshot so the pill reflects "this turn so far".
      (event: UsageEvent) => {
        setUsage(event);
      },
    );

    abortRef.current = controller;
  }, [executeToolCall, effectiveMaxToolRounds]);

  const sendMessage = useCallback((text: string, context: ChatContext) => {
    if (isStreaming) {
      return;
    }

    if (serverMode) {
      // ponytail: at the cap (TabConversationStore.MAX_CONVERSATION_BYTES, 512 KB of
      // serialized messages; tool results carrying sample rows fill it in a long session)
      // a conversation HARD-STOPS. The server refuses the write, the turns already sent
      // stay on screen and in the offline buffer, and no further turn is accepted into a
      // thread that cannot hold it. Chosen over summarizing (lossy, and needs an extra LLM
      // round trip) and spilling to a continuation record (needs a backend pointer and a
      // multi-record load) because it is the only one of the three that cannot lose or
      // rewrite a turn. Upgrade path: spill to a continuation record once the stop is
      // hit often enough to hurt; summarizing older turns is the cheaper alternative.
      const status = saveStatusRef.current;
      if (loadedForRef.current !== conversationId) {
        message.warning('This conversation has not finished loading, so it cannot take a message yet.');
        return;
      }
      if (status === 'too-large') {
        message.warning(
          'This conversation has reached its size limit and cannot take more messages. '
          + 'Start a new conversation to continue.',
          8,
        );
        return;
      }
      if (status === 'conflict') {
        message.warning(
          'This conversation was changed on another device. Reload it to continue; '
          + 'your unsaved messages are kept until you do.',
          8,
        );
        return;
      }
    }

    const userMsg: ChatMessage = {
      role: 'user',
      content: text,
    };

    // Never send a provider an unanswered tool_use (see dropUnmatchedToolUse).
    const updatedMessages = [...dropUnmatchedToolUse(messages), userMsg];
    setMessages(updatedMessages);
    setUsage(null);
    toolRoundsRef.current = 0;

    doStreamRound(updatedMessages, context);
  }, [messages, isStreaming, doStreamRound, serverMode, conversationId]);

  return {
    messages,
    isStreaming,
    streamingContent,
    usage,
    sendMessage,
    stopStreaming,
    clearChat,
    executeToolCall,
    conversationId: serverMode ? (conversationId ?? null) : null,
    isLoaded: serverMode && loadedId === conversationId,
    saveStatus,
    reloadConversation,
  };
}
