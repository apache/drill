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
import type { ArtifactKind } from '../conversation/ArtifactCanvas';

interface ChatMessageListProps {
  messages: ChatMessage[];
  streamingContent: string;
  isStreaming: boolean;
  /**
   * Offer to save report-like assistant messages. Called with the message's index
   * within the full `messages` array (not the filtered, visible-only list), so the
   * query appendix can be built from only the history up to that point.
   */
  onSaveReport?: (content: string, messageIndex: number) => void;
  /** Cards for the artifacts a message's tool calls created, opened through this. */
  onOpenArtifact?: (kind: ArtifactKind, id: string) => void;
  /** The artifact on the canvas now, so its card can show as live. */
  liveArtifact?: { kind: ArtifactKind; id: string } | null;
}

// Dismissed report suggestions are keyed by a hash of the message's content
// (messageKey, from utils/report), not its position in the conversation. Content is a
// stable handle that survives the array being wholesale-replaced and survives
// reordering; two identical report messages sharing one dismissal is the correct
// outcome, not a bug. The set lives in memory: a reload shows the suggestion again.

export default function ChatMessageList({
  messages,
  streamingContent,
  isStreaming,
  onSaveReport,
  onOpenArtifact,
  liveArtifact,
}: ChatMessageListProps) {
  const bottomRef = useRef<HTMLDivElement>(null);
  const [dismissedReports, setDismissedReports] = useState<Set<string>>(() => new Set());

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, streamingContent]);

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
            onSaveReport={onSaveReport}
            messageIndex={index}
            dismissed={dismissedReports.has(key)}
            onDismissReport={() => setDismissedReports((prev) => new Set(prev).add(key))}
            onOpenArtifact={onOpenArtifact}
            liveArtifact={liveArtifact}
          />
        );
      })}
      {isStreaming && streamingContent && (
        <ChatMessageBubble
          message={{ role: 'assistant', content: streamingContent }}
          toolResults={[]}
          isStreaming
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
