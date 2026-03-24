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

/**
 * Simple token counter using rough estimation
 * (proper counting would require the tokenizer from the model)
 */
function estimateTokens(text: string): number {
  // Rough estimate: ~4 characters per token
  return Math.ceil(text.length / 4);
}

export interface AIInteractionLog {
  id: string;
  timestamp: number;
  type: 'query_suggestions' | 'explain_query' | 'optimize_query' | 'other';
  prompt: string;
  promptTokens: number;
  response: string;
  responseTokens: number;
  totalTokens: number;
  duration: number; // milliseconds
  success: boolean;
  error?: string;
}

const STORAGE_KEY = 'drill_ai_logs';
const MAX_LOGS = 100; // Keep last 100 interactions
const BATCH_SIZE = 5; // Send logs every 5 entries
const BATCH_TIMEOUT = 30000; // Or every 30 seconds

class AIObservability {
  private enabled = true;
  private logs: AIInteractionLog[] = [];
  private pendingLogs: AIInteractionLog[] = [];
  private batchTimeout: ReturnType<typeof setTimeout> | null = null;

  constructor() {
    this.loadLogsFromStorage();

    // Enable/disable via query param or localStorage
    const urlParams = new URLSearchParams(window.location.search);
    if (urlParams.has('ai_logs')) {
      this.enabled = urlParams.get('ai_logs') !== 'false';
    }

    if (localStorage.getItem('drill_ai_logs_enabled') !== null) {
      this.enabled = localStorage.getItem('drill_ai_logs_enabled') === 'true';
    }
  }

  setEnabled(enabled: boolean) {
    this.enabled = enabled;
    localStorage.setItem('drill_ai_logs_enabled', String(enabled));
    console.log(`AI Observability ${enabled ? 'enabled' : 'disabled'}`);

    // Flush pending logs before disabling
    if (!enabled && this.pendingLogs.length > 0) {
      this.flushToBackend();
    }
  }

  isEnabled(): boolean {
    return this.enabled;
  }

  private loadLogsFromStorage() {
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (stored) {
        this.logs = JSON.parse(stored);
      }
    } catch (err) {
      console.warn('Failed to load AI logs from storage:', err);
      this.logs = [];
    }
  }

  private saveLogsToStorage() {
    try {
      // Keep only the last MAX_LOGS entries
      const logsToSave = this.logs.slice(-MAX_LOGS);
      localStorage.setItem(STORAGE_KEY, JSON.stringify(logsToSave));
    } catch (err) {
      console.warn('Failed to save AI logs to storage:', err);
    }
  }

  logAICall(
    type: AIInteractionLog['type'],
    prompt: string,
    response: string,
    duration: number,
    error?: string
  ) {
    if (!this.enabled) {
      return;
    }

    const promptTokens = estimateTokens(prompt);
    const responseTokens = estimateTokens(response);
    const totalTokens = promptTokens + responseTokens;

    const log: AIInteractionLog = {
      id: `${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
      timestamp: Date.now(),
      type,
      prompt,
      promptTokens,
      response,
      responseTokens,
      totalTokens,
      duration,
      success: !error,
      error,
    };

    this.logs.push(log);
    this.saveLogsToStorage();

    // Queue for backend logging
    this.queueForBackend(log);

    // Also log to console for immediate visibility
    console.group(`🤖 AI Call: ${type}`);
    console.log(`Timestamp: ${new Date(log.timestamp).toISOString()}`);
    console.log(`Duration: ${duration}ms`);
    console.log(`Tokens - Input: ${promptTokens}, Output: ${responseTokens}, Total: ${totalTokens}`);
    console.log('Prompt:', prompt);
    console.log('Response:', response);
    if (error) {
      console.error('Error:', error);
    }
    console.groupEnd();
  }

  private queueForBackend(log: AIInteractionLog) {
    this.pendingLogs.push(log);

    // If we've reached batch size, send immediately
    if (this.pendingLogs.length >= BATCH_SIZE) {
      this.flushToBackend();
    } else if (!this.batchTimeout) {
      // Start timeout if not already running
      this.batchTimeout = setTimeout(() => {
        this.flushToBackend();
      }, BATCH_TIMEOUT);
    }
  }

  private flushToBackend() {
    if (this.pendingLogs.length === 0) {
      return;
    }

    if (this.batchTimeout) {
      clearTimeout(this.batchTimeout);
      this.batchTimeout = null;
    }

    const logsToSend = this.pendingLogs.slice();
    this.pendingLogs = [];

    // Send asynchronously, don't block if it fails
    this.sendLogsToBackend(logsToSend).catch((err) => {
      console.warn('Failed to send AI logs to backend:', err);
      // Logs are still stored locally, so we don't lose them
    });
  }

  private async sendLogsToBackend(logs: AIInteractionLog[]) {
    try {
      const payload = logs.map((log) => ({
        timestamp: log.timestamp,
        type: log.type,
        promptTokens: log.promptTokens,
        responseTokens: log.responseTokens,
        totalTokens: log.totalTokens,
        duration: log.duration,
        success: log.success,
        error: log.error,
        prompt: log.prompt,
        response: log.response,
      }));

      console.log(`📤 Sending ${logs.length} AI logs to backend...`, payload);

      const response = await fetch('/api/v1/ai/logs', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-CSRF-Token': this.getCsrfToken() || '',
        },
        credentials: 'include',
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        const text = await response.text();
        console.error(`❌ Backend log request failed: HTTP ${response.status}: ${text}`);
      } else {
        const result = await response.json();
        console.log(`✅ Backend accepted logs:`, result);
      }
    } catch (err) {
      console.error('❌ Error sending AI logs to backend:', err);
    }
  }

  private getCsrfToken(): string | null {
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

  getLogs(): AIInteractionLog[] {
    return [...this.logs];
  }

  getLogsSince(timestamp: number): AIInteractionLog[] {
    return this.logs.filter(log => log.timestamp >= timestamp);
  }

  getLogsByType(type: AIInteractionLog['type']): AIInteractionLog[] {
    return this.logs.filter(log => log.type === type);
  }

  clearLogs() {
    this.logs = [];
    localStorage.removeItem(STORAGE_KEY);
    console.log('AI logs cleared');
  }

  exportLogs(format: 'json' | 'csv' = 'json'): string {
    if (format === 'json') {
      return JSON.stringify(this.logs, null, 2);
    }

    // CSV format
    const headers = ['Timestamp', 'Type', 'Input Tokens', 'Output Tokens', 'Total Tokens', 'Duration (ms)', 'Success'];
    const rows = this.logs.map(log => [
      new Date(log.timestamp).toISOString(),
      log.type,
      log.promptTokens,
      log.responseTokens,
      log.totalTokens,
      log.duration,
      log.success ? 'Yes' : 'No',
    ]);

    const csvContent = [
      headers.join(','),
      ...rows.map(row => row.map(cell => `"${cell}"`).join(',')),
    ].join('\n');

    return csvContent;
  }

  downloadLogs(format: 'json' | 'csv' = 'json') {
    const content = this.exportLogs(format);
    const blob = new Blob([content], { type: format === 'json' ? 'application/json' : 'text/csv' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `drill-ai-logs-${Date.now()}.${format}`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  }

  getStats() {
    const total = this.logs.length;
    const successful = this.logs.filter(l => l.success).length;
    const failed = total - successful;
    const totalInputTokens = this.logs.reduce((sum, l) => sum + l.promptTokens, 0);
    const totalOutputTokens = this.logs.reduce((sum, l) => sum + l.responseTokens, 0);
    const avgDuration = total > 0 ? this.logs.reduce((sum, l) => sum + l.duration, 0) / total : 0;

    return {
      total,
      successful,
      failed,
      totalInputTokens,
      totalOutputTokens,
      totalTokens: totalInputTokens + totalOutputTokens,
      avgDuration,
    };
  }
}

// Singleton instance
export const aiObservability = new AIObservability();

// Expose to window for easy access in console
declare global {
  interface Window {
    __drillAI?: {
      logs: () => AIInteractionLog[];
      stats: () => ReturnType<AIObservability['getStats']>;
      download: (format?: 'json' | 'csv') => void;
      clear: () => void;
      enable: () => void;
      disable: () => void;
    };
  }
}

if (typeof window !== 'undefined') {
  window.__drillAI = {
    logs: () => aiObservability.getLogs(),
    stats: () => aiObservability.getStats(),
    download: (format = 'json') => aiObservability.downloadLogs(format),
    clear: () => aiObservability.clearLogs(),
    enable: () => aiObservability.setEnabled(true),
    disable: () => aiObservability.setEnabled(false),
  };

  // Flush pending logs before page unload
  window.addEventListener('beforeunload', () => {
    // Use sendBeacon for best-effort delivery
    if (aiObservability.getLogs().length > 0) {
      const stats = aiObservability.getStats();
      navigator.sendBeacon('/api/v1/ai/logs', JSON.stringify({
        type: 'page_unload',
        stats: stats,
      }));
    }
  });
}
