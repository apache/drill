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
import { useEffect, useRef } from 'react';
import ChatMessageBubble from './ChatMessageBubble';
import type { ChatMessage } from '../../types/ai';

interface ChatMessageListProps {
  messages: ChatMessage[];
  streamingContent: string;
  isStreaming: boolean;
  onInsertCell?: (code: string) => void;
}

export default function ChatMessageList({
  messages,
  streamingContent,
  isStreaming,
  onInsertCell,
}: ChatMessageListProps) {
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, streamingContent]);

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
