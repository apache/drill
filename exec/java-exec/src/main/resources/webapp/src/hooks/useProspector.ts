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
import { useState, useCallback, useRef } from 'react';
import { streamChat } from '../api/ai';
import { executeQuery } from '../api/queries';
import { getSchemas, getTables, getColumns, getFunctions } from '../api/metadata';
import { createVisualization } from '../api/visualizations';
import { createDashboard } from '../api/dashboards';
import { createSavedQuery } from '../api/savedQueries';
import type {
  ChatMessage,
  ToolCall,
  ToolDefinition,
  ChatContext,
  DeltaEvent,
  DoneEvent,
} from '../types/ai';
import type { ChartType, VisualizationConfig } from '../types';

const DEFAULT_MAX_TOOL_ROUNDS = 15;

export const TOOL_DEFINITIONS: ToolDefinition[] = [
  {
    name: 'execute_sql',
    description: 'Execute a SQL query against Apache Drill and return results',
    parameters: {
      type: 'object',
      properties: {
        sql: { type: 'string', description: 'The SQL query to execute' },
        limit: { type: 'number', description: 'Max rows (default 100)' },
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
    name: 'get_available_functions',
    description: 'List available SQL functions in Apache Drill',
    parameters: { type: 'object', properties: {} },
  },
];

interface ToolCallInProgress {
  id: string;
  name: string;
  arguments: string;
}

export interface UseProspectorReturn {
  messages: ChatMessage[];
  isStreaming: boolean;
  streamingContent: string;
  sendMessage: (text: string, context: ChatContext) => void;
  stopStreaming: () => void;
  clearChat: () => void;
}

export function useProspector(
  onSqlGenerated?: (sql: string) => void,
  onVisualizationCreated?: (id: string, name: string) => void,
  maxToolRounds?: number,
): UseProspectorReturn {
  const effectiveMaxToolRounds = maxToolRounds && maxToolRounds > 0 ? maxToolRounds : DEFAULT_MAX_TOOL_ROUNDS;
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [isStreaming, setIsStreaming] = useState(false);
  const [streamingContent, setStreamingContent] = useState('');
  const abortRef = useRef<AbortController | null>(null);
  const toolRoundsRef = useRef(0);

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
    toolRoundsRef.current = 0;
  }, [stopStreaming]);

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
            onSqlGenerated(args.sql);
          }
          const rowCount = result.rows?.length ?? 0;
          const summary = {
            columns: result.columns,
            rowCount,
            rows: result.rows?.slice(0, 5),
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
          onVisualizationCreated?.(viz.id, viz.name);
          return JSON.stringify({
            id: viz.id,
            name: viz.name,
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
          return JSON.stringify({
            id: dashboard.id,
            name: dashboard.name,
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

        case 'get_available_functions': {
          const functions = await getFunctions();
          return JSON.stringify({ functions: functions.slice(0, 100), totalCount: functions.length });
        }

        default:
          return JSON.stringify({ error: `Unknown tool: ${toolCall.name}` });
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Tool execution failed';
      return JSON.stringify({ error: msg });
    }
  }, [onSqlGenerated, onVisualizationCreated]);

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
          const toolCalls: ToolCall[] = Array.from(pendingToolCalls.values()).map((tc) => ({
            id: tc.id,
            name: tc.name,
            arguments: tc.arguments,
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
    );

    abortRef.current = controller;
  }, [executeToolCall]);

  const sendMessage = useCallback((text: string, context: ChatContext) => {
    if (isStreaming) {
      return;
    }

    const userMsg: ChatMessage = {
      role: 'user',
      content: text,
    };

    const updatedMessages = [...messages, userMsg];
    setMessages(updatedMessages);
    toolRoundsRef.current = 0;

    doStreamRound(updatedMessages, context);
  }, [messages, isStreaming, doStreamRound]);

  return {
    messages,
    isStreaming,
    streamingContent,
    sendMessage,
    stopStreaming,
    clearChat,
  };
}
