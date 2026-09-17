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
import { useEffect, useRef, useState } from 'react';
import ChatMessageBubble from './ChatMessageBubble';
import type { ChatMessage } from '../../types/ai';

interface ChatMessageListProps {
  messages: ChatMessage[];
  streamingContent: string;
  isStreaming: boolean;
  onInsertCell?: (code: string) => void;
  /** Offer to save report-like assistant messages. Omitted when saving is unavailable. */
  onSaveReport?: (content: string) => void;
  /**
   * The conversation's localStorage key (see prospectorChatKey), used to persist which
   * report suggestions were dismissed so they stay hidden across reloads. Dismissals
   * are not persisted when this is null.
   */
  storageKey?: string | null;
}

// ponytail: dismissed report suggestions are keyed by the message's position in
// visibleMessages, since ChatMessage carries no id. That is safe for the normal case
// (messages only ever append), but it can mistarget a dismissal onto a different
// message after the conversation is reset and rebuilt at the same indices, e.g.
// clearChat or the server-side merge on mount. We clear the dismissed set whenever the
// conversation is empty to close the clearChat case; a full fix (content-hash keys)
// would be needed to close the rest.
function dismissedStorageKey(storageKey: string): string {
  return `${storageKey}:dismissedReports`;
}

function loadDismissed(storageKey: string | null | undefined): Set<number> {
  if (!storageKey) {
    return new Set();
  }
  try {
    const raw = localStorage.getItem(dismissedStorageKey(storageKey));
    return raw ? new Set(JSON.parse(raw)) : new Set();
  } catch {
    return new Set();
  }
}

function saveDismissed(storageKey: string, dismissed: Set<number>): void {
  try {
    localStorage.setItem(dismissedStorageKey(storageKey), JSON.stringify(Array.from(dismissed)));
  } catch {
    // Ignore storage errors (quota, private mode)
  }
}

export default function ChatMessageList({
  messages,
  streamingContent,
  isStreaming,
  onInsertCell,
  onSaveReport,
  storageKey,
}: ChatMessageListProps) {
  const bottomRef = useRef<HTMLDivElement>(null);
  const [dismissedReports, setDismissedReports] = useState<Set<number>>(() => loadDismissed(storageKey));

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, streamingContent]);

  // Reload the dismissed set when the conversation (storageKey) changes.
  const prevKeyRef = useRef(storageKey);
  useEffect(() => {
    if (prevKeyRef.current !== storageKey) {
      prevKeyRef.current = storageKey;
      setDismissedReports(loadDismissed(storageKey));
    }
  }, [storageKey]);

  // A cleared conversation restarts message indices from 0; stale dismissals from the
  // previous conversation would otherwise silently apply to unrelated new messages.
  useEffect(() => {
    if (messages.length === 0) {
      setDismissedReports((prev) => (prev.size === 0 ? prev : new Set()));
    }
  }, [messages.length]);

  useEffect(() => {
    if (storageKey) {
      saveDismissed(storageKey, dismissedReports);
    }
  }, [dismissedReports, storageKey]);

  // Collect tool result messages for display in their parent assistant message
  const toolResults = messages.filter((m) => m.role === 'tool');

  // Only show user and assistant messages (not tool results)
  const visibleMessages = messages.filter((m) => m.role === 'user' || m.role === 'assistant');

  return (
    <div className="prospector-message-list">
      {visibleMessages.length === 0 && !isStreaming && (
        <div className="prospector-empty-state">
          <div style={{ fontSize: 32, marginBottom: 12, opacity: 0.3 }}>AI</div>
          <div style={{ color: 'var(--color-text-tertiary)', textAlign: 'center' }}>
            Ask me about your data, generate SQL queries, or create visualizations.
          </div>
        </div>
      )}
      {visibleMessages.map((msg, i) => (
        <ChatMessageBubble
          key={i}
          message={msg}
          toolResults={toolResults}
          onInsertCell={onInsertCell}
          onSaveReport={onSaveReport}
          dismissed={dismissedReports.has(i)}
          onDismissReport={() => setDismissedReports((prev) => new Set(prev).add(i))}
        />
      ))}
      {isStreaming && streamingContent && (
        <ChatMessageBubble
          message={{ role: 'assistant', content: streamingContent }}
          toolResults={[]}
          isStreaming
          onInsertCell={onInsertCell}
        />
      )}
      {isStreaming && !streamingContent && (
        <div className="prospector-message prospector-message-assistant">
          <div className="prospector-message-avatar">
            <span>AI</span>
          </div>
          <div className="prospector-message-bubble prospector-bubble-assistant">
            <div className="prospector-thinking">
              <span className="prospector-dot" />
              <span className="prospector-dot" />
              <span className="prospector-dot" />
            </div>
          </div>
        </div>
      )}
      <div ref={bottomRef} />
    </div>
  );
}
