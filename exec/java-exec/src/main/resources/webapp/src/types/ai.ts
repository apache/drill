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
  availableSchemas?: string[];
  error?: string;
  resultSummary?: ResultSummary;
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
}

export interface AiProvider {
  id: string;
  displayName: string;
}

export interface ValidationResult {
  success: boolean;
  message: string;
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

export interface ErrorEvent {
  message: string;
}
