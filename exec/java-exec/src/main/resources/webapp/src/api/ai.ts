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
import apiClient from './client';
import type {
  AiStatus,
  AiConfig,
  AiConfigUpdate,
  AiProvider,
  ValidationResult,
  ChatRequest,
  DeltaEvent,
  DoneEvent,
  ErrorEvent,
} from '../types/ai';

const AI_BASE = '/api/v1/ai';
const AI_CONFIG_BASE = '/api/v1/ai/config';

/**
 * Get AI copilot status (enabled/configured)
 */
export async function getAiStatus(): Promise<AiStatus> {
  const response = await apiClient.get<AiStatus>(`${AI_BASE}/status`);
  return response.data;
}

/**
 * Get AI configuration (admin only, API key redacted)
 */
export async function getAiConfig(): Promise<AiConfig> {
  const response = await apiClient.get<AiConfig>(AI_CONFIG_BASE);
  return response.data;
}

/**
 * Update AI configuration (admin only)
 */
export async function updateAiConfig(config: AiConfigUpdate): Promise<AiConfig> {
  const response = await apiClient.put<AiConfig>(AI_CONFIG_BASE, config);
  return response.data;
}

/**
 * Test AI configuration (admin only)
 */
export async function testAiConfig(config: AiConfigUpdate): Promise<ValidationResult> {
  const response = await apiClient.post<ValidationResult>(`${AI_CONFIG_BASE}/test`, config);
  return response.data;
}

/**
 * Get available LLM providers
 */
export async function getAiProviders(): Promise<AiProvider[]> {
  const response = await apiClient.get<{ providers: AiProvider[] }>(`${AI_CONFIG_BASE}/providers`);
  return response.data.providers;
}

/**
 * Get CSRF token for fetch-based requests
 */
function getCsrfToken(): string | null {
  const metaTag = document.querySelector('meta[name="csrf-token"]');
  if (metaTag) {
    return metaTag.getAttribute('content');
  }
  const cookies = document.cookie.split(';');
  for (const cookie of cookies) {
    const [name, value] = cookie.trim().split('=');
    if (name === 'drill.csrf.token') {
      return decodeURIComponent(value);
    }
  }
  return null;
}

/**
 * Transpile SQL from one dialect to another via the backend sqlglot service.
 * Falls back to returning the original SQL if transpilation fails.
 */
export async function transpileSql(
  sql: string,
  sourceDialect: string = 'mysql',
  targetDialect: string = 'drill',
  schemas?: { name: string; tables: { name: string; columns: string[] }[] }[]
): Promise<string> {
  try {
    const response = await apiClient.post<{ sql: string; success: boolean }>(
      '/api/v1/transpile',
      { sql, sourceDialect, targetDialect, schemas }
    );
    return response.data.sql;
  } catch {
    return sql;
  }
}

/**
 * Format/pretty-print a SQL string via the backend sqlglot service.
 */
export async function formatSql(sql: string): Promise<string> {
  try {
    const response = await apiClient.post<{ sql: string; success: boolean }>(
      '/api/v1/transpile/format',
      { sql }
    );
    return response.data.sql;
  } catch {
    return sql;
  }
}

/**
 * Convert a column's data type via the backend sqlglot service.
 * Wraps the column in a CAST() expression using AST manipulation.
 */
export async function convertDataType(
  sql: string,
  columnName: string,
  dataType: string,
  columns?: Record<string, string>
): Promise<{ sql: string; success: boolean; formattedOriginal?: string }> {
  const response = await apiClient.post<{ sql: string; success: boolean; formattedOriginal?: string }>(
    '/api/v1/transpile/convert-type',
    { sql, columnName, dataType, columns }
  );
  return response.data;
}

/**
 * Stream a chat completion via POST SSE.
 * Uses native fetch + ReadableStream since axios doesn't support streaming.
 * Returns an AbortController for cancellation.
 */
export function streamChat(
  request: ChatRequest,
  onDelta: (event: DeltaEvent) => void,
  onDone: (event: DoneEvent) => void,
  onError: (error: ErrorEvent) => void,
): AbortController {
  const controller = new AbortController();

  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
  };
  const csrfToken = getCsrfToken();
  if (csrfToken) {
    headers['X-CSRF-Token'] = csrfToken;
  }

  fetch(`${AI_BASE}/chat`, {
    method: 'POST',
    headers,
    body: JSON.stringify(request),
    signal: controller.signal,
    credentials: 'include',
  })
    .then(async (response) => {
      if (!response.ok) {
        const text = await response.text();
        onError({ message: `HTTP ${response.status}: ${text}` });
        return;
      }

      const reader = response.body?.getReader();
      if (!reader) {
        onError({ message: 'No response body' });
        return;
      }

      const decoder = new TextDecoder();
      let buffer = '';

      // eslint-disable-next-line no-constant-condition
      while (true) {
        const { done, value } = await reader.read();
        if (done) {
          break;
        }

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        // Keep the last incomplete line in buffer
        buffer = lines.pop() || '';

        let currentEvent = '';
        for (const line of lines) {
          const trimmed = line.trim();

          if (trimmed.startsWith('event: ')) {
            currentEvent = trimmed.substring(7);
          } else if (trimmed.startsWith('data: ')) {
            const data = trimmed.substring(6);
            try {
              const parsed = JSON.parse(data);

              if (currentEvent === 'delta') {
                onDelta(parsed as DeltaEvent);
              } else if (currentEvent === 'done') {
                onDone(parsed as DoneEvent);
              } else if (currentEvent === 'error') {
                onError(parsed as ErrorEvent);
              }
            } catch {
              // Skip unparseable lines
            }
            currentEvent = '';
          }
        }
      }
    })
    .catch((err) => {
      if (err.name !== 'AbortError') {
        onError({ message: err.message || 'Stream failed' });
      }
    });

  return controller;
}
