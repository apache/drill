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
import { getConversation, putConversation } from '../api/tabs';
import {
  getConversation as getServerConversation,
  saveConversationMessages,
} from '../api/conversations';
import type { StoredChatMessage } from '../api/tabs';
import { addVisualization, addDashboard, getProject, createWikiPage } from '../api/projects';
import { createDashboard } from '../api/dashboards';
import { createSavedQuery } from '../api/savedQueries';
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
import type { ChartType, VisualizationConfig } from '../types';

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
    name: 'save_report',
    description: 'Save a report you have written into the current project\'s wiki, under the '
      + 'Reports folder. Only call this after the user has agreed to save it.',
    parameters: {
      type: 'object',
      properties: {
        title: { type: 'string', description: 'Title for the report' },
        content: { type: 'string', description: 'The full report in markdown' },
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
      + 'arguments to list page titles, or with pageTitle to read one page in full. '
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

// ponytail: chat history persisted to localStorage keyed by project. Fine for the
// current single-user-per-browser model; move to a backend store if chats need to
// follow the user across devices.
function loadChat(key: string | null | undefined): ChatMessage[] {
  if (!key) {
    return [];
  }
  try {
    const raw = localStorage.getItem(key);
    return raw ? JSON.parse(raw) : [];
  } catch {
    return [];
  }
}

function saveChat(key: string, messages: ChatMessage[]): void {
  try {
    localStorage.setItem(key, JSON.stringify(messages));
  } catch {
    // Ignore storage errors (quota, private mode)
  }
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

function writeConversationBuffer(key: string, buffer: ConversationBuffer): void {
  try {
    localStorage.setItem(key, JSON.stringify(buffer));
  } catch {
    // Ignore storage errors (quota, private mode); the in-memory copy is still live.
  }
}

function removeConversationBuffer(key: string): void {
  try {
    localStorage.removeItem(key);
  } catch {
    // Ignore storage errors
  }
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
  /** The localStorage key this conversation is persisted under, or null when unset. */
  storageKey: string | null;
  /** The tab this conversation belongs to, or null when unset. Recorded as provenance
   *  when a report from this conversation is saved. */
  tabId: string | null;
  /** The server-side conversation this hook is bound to, or null when it is not. */
  conversationId: string | null;
  /** Sync state of a server-primary conversation; always 'idle' without a conversationId. */
  saveStatus: SaveStatus;
  /** Discards unsaved local turns and takes the server's copy. The way out of a 'conflict'
   *  (or a failed load); always an explicit user action, never automatic. */
  reloadConversation: () => void;
}

/**
 * localStorage key for one tab's Prospector conversation.
 *
 * Conversations are per tab, not per project: a thread is about the query in front of
 * you, and carrying one across tabs made the assistant answer about the wrong SQL.
 * The schema cache already puts the project's tables in the system prompt
 * (ProspectorResources.buildSchemaCacheBlock), so a fresh conversation per tab does
 * not pay to rediscover the schema.
 *
 * Returns null without a tab id, so nothing is ever written under a shared key.
 *
 * See docs/dev/TabPersistence.md.
 */
export function prospectorChatKey(
  projectId: string | undefined,
  tabId: string | undefined,
): string | null {
  if (!tabId) {
    return null;
  }
  return `prospector_chat_${projectId ?? 'global'}_${tabId}`;
}

/**
 * How many Prospector messages a tab holds, read straight from storage.
 *
 * Deliberately not tied to the hook: the promotion rule has to know whether a tab
 * *being closed* has a conversation, and that is rarely the tab whose conversation is
 * loaded in the panel. Returns 0 for anything unreadable — an undercount only means a
 * tab is not promoted, which is recoverable, whereas throwing here would break the
 * save path.
 */
export function conversationLengthFor(
  projectId: string | undefined,
  tabId: string | undefined,
): number {
  const key = prospectorChatKey(projectId, tabId);
  if (!key) {
    return 0;
  }
  try {
    const raw = localStorage.getItem(key);
    if (!raw) {
      return 0;
    }
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed.length : 0;
  } catch {
    return 0;
  }
}

export function useProspector(
  onSqlGenerated?: (sql: string, title?: string) => void,
  onVisualizationCreated?: (id: string, name: string) => void,
  maxToolRounds?: number,
  // When set (e.g. per project), chat history is loaded from and saved to
  // localStorage under this key, so it survives navigation and unmounts.
  storageKeyArg?: string | null,
  // LEGACY (tab-scoped) path: the conversation is also mirrored to the server so it
  // follows the tab across devices. localStorage stays the write-through buffer there:
  // a failed server write costs sync, never the conversation.
  tabIdArg?: string | null,
  // SERVER-PRIMARY path (needs projectId too). The server owns this conversation: it is
  // loaded from there and every change is saved there. This INVERTS the legacy rule
  // above. localStorage is only an offline buffer, so a write the server refuses or a
  // network that drops mid-save leaves a turn that exists nowhere else. Every status
  // other than 'idle' must therefore reach the user and none may discard a turn.
  // When set, storageKeyArg and tabIdArg are ignored.
  conversationId?: string | null,
  projectId?: string | null,
): UseProspectorReturn {
  const serverMode = !!conversationId && !!projectId;
  const storageKey = serverMode ? null : storageKeyArg;
  const tabId = serverMode ? null : tabIdArg;
  const effectiveMaxToolRounds = maxToolRounds && maxToolRounds > 0 ? maxToolRounds : DEFAULT_MAX_TOOL_ROUNDS;
  const [messages, setMessages] = useState<ChatMessage[]>(() => loadChat(storageKey));
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

  const stopStreaming = useCallback(() => {
    if (abortRef.current) {
      abortRef.current.abort();
      abortRef.current = null;
    }
    setIsStreaming(false);
    setStreamingContent('');
  }, []);

  const clearChat = useCallback(() => {
    stopStreaming();
    setMessages([]);
    setUsage(null);
    toolRoundsRef.current = 0;
    // Clearing makes room, so a conversation stopped at the size cap can be written again.
    if (saveStatusRef.current === 'too-large') {
      setSaveStatus('idle');
    }
  }, [stopStreaming, setSaveStatus]);

  // Swap in the stored history when the project (storageKey) changes.
  const prevKeyRef = useRef(storageKey);
  useEffect(() => {
    if (prevKeyRef.current !== storageKey) {
      prevKeyRef.current = storageKey;
      setMessages(loadChat(storageKey));
      setUsage(null);
      toolRoundsRef.current = 0;
    }
  }, [storageKey]);

  // Pull the server's copy for this tab. The local copy renders immediately; the
  // server copy replaces it only when it holds more history, which is what makes a
  // conversation started on another device show up here without ever discarding
  // messages this device has that the server has not seen yet.
  useEffect(() => {
    if (!tabId || !storageKey) {
      return;
    }
    let cancelled = false;
    getConversation(tabId).then((serverMessages: StoredChatMessage[]) => {
      if (cancelled || serverMessages.length === 0) {
        return;
      }
      setMessages((current) => {
        if (serverMessages.length <= current.length) {
          return current;
        }
        const merged = serverMessages as unknown as ChatMessage[];
        saveChat(storageKey, merged);
        return merged;
      });
    });
    return () => {
      cancelled = true;
    };
  }, [tabId, storageKey]);

  // Persist history for the active tab (including clears, which write []).
  // Unconditional and immediate: this is the copy that must never be lost.
  useEffect(() => {
    if (storageKey) {
      saveChat(storageKey, messages);
    }
  }, [messages, storageKey]);

  // Mirror to the server on a slower beat than the local write, and never while a
  // response is still streaming — a half-written assistant turn is not worth storing,
  // and messages changes on every token.
  const serverSaveTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const oversizeReportedRef = useRef(false);
  useEffect(() => {
    if (!tabId || isStreaming) {
      return;
    }
    if (serverSaveTimerRef.current) {
      clearTimeout(serverSaveTimerRef.current);
    }
    serverSaveTimerRef.current = setTimeout(() => {
      putConversation(tabId, messages as unknown as StoredChatMessage[]).then((result) => {
        // Past the server's size cap this thread has quietly stopped syncing, which
        // the user should hear about once — not on every subsequent keystroke.
        if (result === 'too-large' && !oversizeReportedRef.current) {
          oversizeReportedRef.current = true;
          message.warning(
            'This conversation is too long to save on the server. It stays on this '
            + 'device, but will not follow you to another one. Start a new tab to '
            + 'keep syncing.',
            8,
          );
        }
        if (result === 'ok') {
          oversizeReportedRef.current = false;
        }
      });
    }, 2000);

    return () => {
      if (serverSaveTimerRef.current) {
        clearTimeout(serverSaveTimerRef.current);
      }
    };
  }, [messages, tabId, isStreaming]);

  // ---- Server-primary persistence (see the conversationId parameter) ----

  const enqueueSave = useCallback((pid: string, cid: string, saving: ChatMessage[]) => {
    saveChainRef.current = saveChainRef.current.then(async () => {
      const version = versionsRef.current[cid];
      if (version === undefined) {
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
        const result = await saveConversationMessages(pid, cid, saving, version);
        if (result.status === 'ok') {
          const saved = result.version as number;
          versionsRef.current[cid] = saved;
          if (!isActive() || messagesRef.current === saving) {
            removeConversationBuffer(key);
          } else {
            // The user kept going while this write was in flight: the buffer still holds
            // unsaved turns, now on top of the version this write produced.
            const buffer = readConversationBuffer(key);
            if (buffer) {
              writeConversationBuffer(key, { ...buffer, baseVersion: saved });
            }
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
  }, [setSaveStatus]);

  const loadConversation = useCallback((pid: string, cid: string) => {
    const token = ++loadTokenRef.current;
    loadedForRef.current = null;
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
      if (buffer && JSON.stringify(buffer.messages) !== JSON.stringify(serverMessages)) {
        // Unsaved turns from an earlier session. They are never dropped here: shown as
        // they were, and either replayed (the save effect picks them up) or, when the
        // server has moved on since, held back as a conflict for the user to resolve.
        shown = buffer.messages;
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
    if (serverSaveTimerRef.current) {
      clearTimeout(serverSaveTimerRef.current);
    }
    pendingSaveRef.current = null;
    removeConversationBuffer(conversationBufferKey(projectId, conversationId));
    loadConversation(projectId, conversationId);
  }, [conversationId, projectId, loadConversation]);

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
      writeConversationBuffer(key, {
        baseVersion: versionsRef.current[conversationId] ?? 0,
        messages,
      });
    }
  }, [messages, conversationId, projectId]);

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
          return JSON.stringify({ id: saved.id, name: saved.name, message: 'Query saved successfully' });
        }

        case 'save_report': {
          if (!context?.projectId) {
            return JSON.stringify({ error: 'No active project — a report can only be saved '
              + 'inside a project. Ask the user to open one.' });
          }
          const markdown = buildReportMarkdown(
            args.content as string,
            messagesRef.current,
            { generatedAt: Date.now(), conversationId: tabId ?? undefined }
          );
          const page = await createWikiPage(context.projectId, {
            title: (args.title as string).trim(),
            content: markdown,
            folder: REPORTS_FOLDER,
          });
          // Same collision-check cache that SaveReportModal reads; without this a
          // second save_report call within the staleTime window would not see this
          // page either.
          await queryClient.invalidateQueries({ queryKey: ['project', context.projectId] });
          await queryClient.invalidateQueries({ queryKey: ['projects'] });
          return JSON.stringify({
            id: page.id,
            title: page.title,
            message: `Report saved to the project wiki under ${REPORTS_FOLDER}.`,
            viewPath: `/projects/${context.projectId}/wiki/${page.id}`,
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
            return JSON.stringify({ pages: pages.map((p) => ({ title: p.title })) });
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
  }, [onSqlGenerated, onVisualizationCreated, sendDataToAiRef, tabId, queryClient]);

  const doStreamRound = useCallback((
    allMessages: ChatMessage[],
    context: ChatContext,
  ) => {
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
        setIsStreaming(false);
        setStreamingContent('');

        if (doneEvent.finish_reason === 'tool_calls' && pendingToolCalls.size > 0) {
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

    const updatedMessages = [...messages, userMsg];
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
    storageKey: storageKey ?? null,
    tabId: tabId ?? null,
    conversationId: serverMode ? (conversationId ?? null) : null,
    saveStatus,
    reloadConversation,
  };
}
