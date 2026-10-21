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
import { messageKey } from '../../utils/report';

interface ChatMessageListProps {
  messages: ChatMessage[];
  streamingContent: string;
  isStreaming: boolean;
  onInsertCell?: (code: string) => void;
  /**
   * Offer to save report-like assistant messages. Called with the message's index
   * within the full `messages` array (not the filtered, visible-only list), so the
   * query appendix can be built from only the history up to that point.
   */
  onSaveReport?: (content: string, messageIndex: number) => void;
  /**
   * The conversation's localStorage key (see prospectorChatKey), used to persist which
   * report suggestions were dismissed so they stay hidden across reloads. Dismissals
   * are not persisted when this is null.
   */
  storageKey?: string | null;
}

// Dismissed report suggestions are keyed by a hash of the message's content
// (messageKey, from utils/report), not its position in the conversation. Content is a
// stable handle that survives the array being wholesale-replaced (clearChat, the
// server-merge-on-mount path) and survives reordering; two identical report messages
// sharing one dismissal is the correct outcome, not a bug.
function dismissedStorageKey(storageKey: string): string {
  return `${storageKey}:dismissedReports`;
}

function loadDismissed(storageKey: string | null | undefined): Set<string> {
  if (!storageKey) {
    return new Set();
  }
  try {
    const raw = localStorage.getItem(dismissedStorageKey(storageKey));
    if (!raw) {
      return new Set();
    }
    const parsed = JSON.parse(raw);
    // Guard against a stale array from the previous (index-keyed) format: only strings
    // are valid keys here, so anything else is dropped rather than trusted.
    if (!Array.isArray(parsed) || !parsed.every((v) => typeof v === 'string')) {
      return new Set();
    }
    return new Set(parsed);
  } catch {
    return new Set();
  }
}

function saveDismissed(storageKey: string, dismissed: Set<string>): void {
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
  const [dismissedReports, setDismissedReports] = useState<Set<string>>(() => loadDismissed(storageKey));

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

  useEffect(() => {
    if (storageKey) {
      saveDismissed(storageKey, dismissedReports);
    }
  }, [dismissedReports, storageKey]);

  // Collect tool result messages for display in their parent assistant message
  const toolResults = messages.filter((m) => m.role === 'tool');

  // Only show user and assistant messages (not tool results). Each entry keeps its
  // index within the full `messages` array so a report save can slice the query
  // appendix at the right point rather than at its position in this filtered list.
  const visibleMessages = messages
    .map((m, index) => ({ m, index }))
    .filter(({ m }) => m.role === 'user' || m.role === 'assistant');

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
      {visibleMessages.map(({ m: msg, index }, i) => {
        const key = messageKey(msg.content ?? '');
        return (
          <ChatMessageBubble
            key={i}
            message={msg}
            toolResults={toolResults}
            onInsertCell={onInsertCell}
            onSaveReport={onSaveReport}
            messageIndex={index}
            dismissed={dismissedReports.has(key)}
            onDismissReport={() => setDismissedReports((prev) => new Set(prev).add(key))}
          />
        );
      })}
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
