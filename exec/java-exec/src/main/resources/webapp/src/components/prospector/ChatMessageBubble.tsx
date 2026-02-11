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
import Markdown from 'react-markdown';
import { UserOutlined, RobotOutlined } from '@ant-design/icons';
import type { ChatMessage } from '../../types/ai';
import ToolCallDisplay from './ToolCallDisplay';

interface ChatMessageBubbleProps {
  message: ChatMessage;
  toolResults: ChatMessage[];
  isStreaming?: boolean;
}

export default function ChatMessageBubble({
  message,
  toolResults,
  isStreaming,
}: ChatMessageBubbleProps) {
  const isUser = message.role === 'user';
  const isError = message.content?.startsWith('Error:');

  return (
    <div className={`prospector-message ${isUser ? 'prospector-message-user' : 'prospector-message-assistant'}`}>
      <div className="prospector-message-avatar">
        {isUser ? <UserOutlined /> : <RobotOutlined />}
      </div>
      <div
        className={`prospector-message-bubble ${isUser ? 'prospector-bubble-user' : 'prospector-bubble-assistant'} ${isError ? 'prospector-bubble-error' : ''}`}
      >
        {message.content && (
          <div className="prospector-message-content">
            {isUser ? (
              <span>{message.content}</span>
            ) : (
              <Markdown>{message.content}</Markdown>
            )}
          </div>
        )}
        {isStreaming && <span className="prospector-cursor" />}
        {message.toolCalls && message.toolCalls.length > 0 && (
          <ToolCallDisplay
            toolCalls={message.toolCalls}
            toolResults={toolResults}
          />
        )}
      </div>
    </div>
  );
}
