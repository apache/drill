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

import type { AiFeature } from '../constants/aiFeatures';

export interface ChatMessage {
  role: 'system' | 'user' | 'assistant' | 'tool';
  content: string | null;
  toolCalls?: ToolCall[];
  toolCallId?: string;
  name?: string;
}

export interface ToolCall {
  id: string;
  name: string;
  arguments: string;
}

export interface ToolDefinition {
  name: string;
  description: string;
  parameters: Record<string, unknown>;
}

export interface ChatContext {
  currentSql?: string;
  currentSchema?: string;
  /**
   * Which UI feature originated this call; used for analytics attribution. Typed as
   * the labelled-slug union (not `string`) so an unlabelled or misspelled slug fails
   * `tsc` at the call site instead of silently reaching the server unlabelled.
   */
  feature: AiFeature;
  /**
   * Active project, when the call originates inside one. Tools that create
   * artefacts attach them to this project, matching what the equivalent
   * hand-driven UI does.
   */
  projectId?: string;
  availableSchemas?: string[];
  error?: string;
  resultSummary?: ResultSummary;
  /** Whether the user is currently in the notebook tab */
  notebookMode?: boolean;
  /** Name of the DataFrame variable available in the notebook */
  notebookDfName?: string;
  /** DataFrame shape info (rows x columns) */
  notebookDfShape?: string;
  /** Column names and types from the DataFrame */
  notebookColumns?: string[];
  /** Current cell code the user is working on */
  notebookCellCode?: string;
  /** Error from the last cell execution */
  notebookCellError?: string;
  /** Whether the user is on the logs page */
  logAnalysisMode?: boolean;
  /** Name of the log file being analyzed */
  logFileName?: string;
  /** Log lines for AI analysis context */
  logLines?: string[];
  /** Whether this is a dashboard executive summary request */
  dashboardSummaryMode?: boolean;
  /** Data from dashboard visualization panels for executive summary */
  dashboardData?: DashboardDataContext[];
  /** Project datasets — when set, restricts schema exploration to these datasets only */
  projectDatasets?: ProjectDatasetRef[];
  /** Whether this is a dashboard Q&A chat mode */
  dashboardQnAMode?: boolean;
  /** Whether this is a natural language filter mode */
  dashboardNlFilterMode?: boolean;
  /** Whether this is a dashboard alert analysis mode */
  dashboardAlertMode?: boolean;
  /** Tone/audience for dashboard summary (Executive, Technical, Casual) */
  dashboardTone?: string;
  /** Whether to emphasize anomaly detection in summary */
  dashboardAnomalyFocus?: boolean;
  /** Previous summary for historical comparison */
  previousSummary?: string;
}

export interface ProjectDatasetRef {
  type: 'table' | 'saved_query' | 'plugin' | 'schema' | 'view' | 'materialized_view';
  schema?: string;
  table?: string;
  label: string;
}

export interface DashboardDataContext {
  panelName: string;
  sql: string;
  columns: string[];
  columnTypes: string[];
  rowCount: number;
  sampleRows: Record<string, unknown>[];
}

export interface ResultSummary {
  rowCount: number;
  columns: string[];
  columnTypes: string[];
}

export interface ChatRequest {
  messages: ChatMessage[];
  tools: ToolDefinition[];
  context: ChatContext;
}

export interface AiStatus {
  enabled: boolean;
  configured: boolean;
  /**
   * Whether the deployment permits sample data to reach the model. Mirrored from the
   * admin-only AiConfig onto this endpoint because every authenticated user can read
   * /api/v1/ai/status, and a client that cannot read the setting cannot honour it.
   */
  sendDataToAi: boolean;
}

export interface AiConfig {
  provider: string;
  apiEndpoint: string;
  apiKeySet: boolean;
  model: string;
  maxTokens: number;
  temperature: number;
  enabled: boolean;
  systemPrompt: string;
  sendDataToAi: boolean;
  maxToolRounds: number;

  // Network Configuration
  customHeaders?: Record<string, string>;
  proxyUrl?: string;
  proxyUsername?: string;
  proxyPasswordSet?: boolean;
  connectTimeoutSeconds?: number;
  readTimeoutSeconds?: number;
  writeTimeoutSeconds?: number;

  // SSL/TLS Configuration
  keystorePath?: string;
  keystorePasswordSet?: boolean;
  keystoreType?: string;
  truststorePath?: string;
  truststorePasswordSet?: boolean;
  truststoreType?: string;
  verifySSL?: boolean;

  // Additional Request Parameters
  additionalParameters?: Record<string, unknown>;

  // Custom API Format
  requestTemplate?: string;
  responseMapping?: string;

  // OAuth2 gateway. consumerSecret is write-only; only its presence is returned.
  authUrl?: string;
  consumerKey?: string;
  consumerSecretSet?: boolean;
  clientId?: string;
  usecaseId?: string;
  clientCertPath?: string;
  gatewayHeaders?: Record<string, string>;
}

export interface AiConfigUpdate {
  provider?: string;
  apiEndpoint?: string;
  apiKey?: string;
  model?: string;
  maxTokens?: number;
  temperature?: number;
  enabled?: boolean;
  systemPrompt?: string;
  sendDataToAi?: boolean;
  maxToolRounds?: number;

  // Network Configuration
  customHeaders?: Record<string, string>;
  proxyUrl?: string;
  proxyUsername?: string;
  proxyPassword?: string;
  connectTimeoutSeconds?: number;
  readTimeoutSeconds?: number;
  writeTimeoutSeconds?: number;

  // SSL/TLS Configuration
  keystorePath?: string;
  keystorePassword?: string;
  keystoreType?: string;
  truststorePath?: string;
  truststorePassword?: string;
  truststoreType?: string;
  verifySSL?: boolean;

  // Additional Request Parameters
  additionalParameters?: Record<string, unknown>;

  // Custom API Format
  requestTemplate?: string;
  responseMapping?: string;

  // OAuth2 gateway
  authUrl?: string;
  consumerKey?: string;
  consumerSecret?: string;
  clientId?: string;
  usecaseId?: string;
  clientCertPath?: string;
  gatewayHeaders?: Record<string, string>;
}

export interface AiProvider {
  id: string;
  displayName: string;
}

export interface ValidationResult {
  success: boolean;
  message: string;
  /** Diagnostic detail for failures (URL, HTTP status, response body, exception chain). */
  details?: string;
}

// SSE event types
export interface DeltaContentEvent {
  type: 'content';
  content: string;
}

export interface DeltaToolCallStartEvent {
  type: 'tool_call_start';
  id: string;
  name: string;
}

export interface DeltaToolCallDeltaEvent {
  type: 'tool_call_delta';
  id: string;
  arguments: string;
}

export interface DeltaToolCallEndEvent {
  type: 'tool_call_end';
  id: string;
}

export type DeltaEvent =
  | DeltaContentEvent
  | DeltaToolCallStartEvent
  | DeltaToolCallDeltaEvent
  | DeltaToolCallEndEvent;

export interface DoneEvent {
  finish_reason: 'stop' | 'tool_calls';
}

/**
 * Cumulative token usage emitted by the server during a chat stream.
 * Anthropic emits incrementally (input on message_start, running output on each
 * message_delta); OpenAI emits once at the end. costUsd / currency are present
 * only when the server has pricing configured for the active model.
 */
export interface UsageEvent {
  promptTokens?: number;
  responseTokens?: number;
  totalTokens?: number;
  costUsd?: number;
  currency?: string;
}

export interface ErrorEvent {
  message: string;
}
