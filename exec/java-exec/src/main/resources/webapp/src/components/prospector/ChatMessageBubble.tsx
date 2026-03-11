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
import { useCallback } from 'react';
import Markdown from 'react-markdown';
import { Button, Tooltip } from 'antd';
import { UserOutlined, RobotOutlined, PlusSquareOutlined } from '@ant-design/icons';
import type { ChatMessage } from '../../types/ai';
import ToolCallDisplay from './ToolCallDisplay';

interface ChatMessageBubbleProps {
  message: ChatMessage;
  toolResults: ChatMessage[];
  isStreaming?: boolean;
  onInsertCell?: (code: string) => void;
}

export default function ChatMessageBubble({
  message,
  toolResults,
  isStreaming,
  onInsertCell,
}: ChatMessageBubbleProps) {
  const isUser = message.role === 'user';
  const isError = message.content?.startsWith('Error:');

  const handleInsertCode = useCallback((code: string) => {
    onInsertCell?.(code);
  }, [onInsertCell]);

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
              <Markdown
                components={onInsertCell ? {
                  code({ className, children, ref: _ref, ...props }) {
                    const match = /language-(\w+)/.exec(className || '');
                    const lang = match?.[1];
                    const codeStr = String(children).replace(/\n$/, '');
                    const isBlock = lang || codeStr.includes('\n');

                    if (isBlock && (!lang || lang === 'python' || lang === 'py')) {
                      return (
                        <div style={{ position: 'relative' }}>
                          <code className={className} {...props}>{children}</code>
                          <Tooltip title="Insert as notebook cell">
                            <Button
                              size="small"
                              type="link"
                              icon={<PlusSquareOutlined />}
                              onClick={() => handleInsertCode(codeStr)}
                              style={{
                                position: 'absolute',
                                top: 2,
                                right: 2,
                                fontSize: 12,
                                opacity: 0.7,
                              }}
                            >
                              Insert Cell
                            </Button>
                          </Tooltip>
                        </div>
                      );
                    }
                    return <code className={className} {...props}>{children}</code>;
                  },
                  pre({ children, ...props }) {
                    return <pre {...props} style={{ position: 'relative' }}>{children}</pre>;
                  },
                } : undefined}
              >
                {message.content}
              </Markdown>
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
