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
import { useCallback, useEffect, useRef, useState } from 'react';
import { Alert, InputNumber, Spin, Typography } from 'antd';
import Markdown from 'react-markdown';
import rehypeRaw from 'rehype-raw';
import { getAiStatus, streamChat } from '../../api/ai';
import ChatInput from '../prospector/ChatInput';
import type { ChatMessage, DashboardDataContext, DeltaEvent } from '../../types/ai';

const { Text } = Typography;

interface AiQnAPanelProps {
  config?: Record<string, string>;
  editMode: boolean;
  darkMode?: boolean;
  dashboardData: DashboardDataContext[];
  onConfigChange: (config: Record<string, string>) => void;
}

export default function AiQnAPanel({
  config,
  editMode,
  darkMode,
  dashboardData,
  onConfigChange,
}: AiQnAPanelProps) {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [isStreaming, setIsStreaming] = useState(false);
  const [aiAvailable, setAiAvailable] = useState<boolean | null>(null);
  const abortRef = useRef<AbortController | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const maxMessages = parseInt(config?.maxMessages || '20', 10);

  useEffect(() => {
    getAiStatus()
      .then((status) => setAiAvailable(status.enabled && status.configured))
      .catch(() => setAiAvailable(false));
  }, []);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages]);

  const handleSend = useCallback((text: string) => {
    const userMsg: ChatMessage = { role: 'user', content: text };
    setMessages((prev) => {
      const next = [...prev, userMsg];
      if (next.length > maxMessages) {
        return next.slice(next.length - maxMessages);
      }
      return next;
    });

    setIsStreaming(true);
    let accumulated = '';

    const dataContext = dashboardData.map((d, i) => {
      let section = `### Panel ${i + 1}: ${d.panelName}\n`;
      section += `- Columns: ${d.columns.join(', ')}\n`;
      section += `- Row count: ${d.rowCount}\n`;
      if (d.sql) {
        section += `**Query:**\n\`\`\`sql\n${d.sql}\n\`\`\`\n`;
      }
      if (d.sampleRows.length > 0) {
        section += '**Sample Data:**\n```json\n' + JSON.stringify(d.sampleRows.slice(0, 5), null, 2) + '\n```\n';
      }
      return section;
    }).join('\n');

    const briefingInstruction = 'Keep responses concise (1-2 sentences). Focus on direct answers to the question.';
    const chatMessages: ChatMessage[] = [
      ...messages,
      userMsg,
      ...(dataContext ? [{ role: 'user' as const, content: `[Dashboard Data Context]\n${dataContext}\n\n${briefingInstruction}` }] : [{ role: 'user' as const, content: briefingInstruction }]),
    ];

    const controller = streamChat(
      {
        messages: chatMessages,
        tools: [],
        context: {
          dashboardQnAMode: true,
          dashboardData,
        },
      },
      (event: DeltaEvent) => {
        if (event.type === 'content') {
          accumulated += event.content;
          setMessages((prev) => {
            const last = prev[prev.length - 1];
            if (last?.role === 'assistant') {
              return [...prev.slice(0, -1), { role: 'assistant', content: accumulated }];
            }
            return [...prev, { role: 'assistant', content: accumulated }];
          });
        }
      },
      () => {
        setIsStreaming(false);
      },
      () => {
        setIsStreaming(false);
      },
    );

    abortRef.current = controller;
  }, [messages, dashboardData, maxMessages]);

  const handleStop = useCallback(() => {
    if (abortRef.current) {
      abortRef.current.abort();
    }
    setIsStreaming(false);
  }, []);

  useEffect(() => {
    return () => {
      if (abortRef.current) {
        abortRef.current.abort();
      }
    };
  }, []);

  if (editMode) {
    return (
      <div style={{ padding: 8, display: 'flex', flexDirection: 'column', gap: 8 }}>
        <Text type="secondary" style={{ fontSize: 12 }}>
          AI Q&amp;A panel lets users ask questions about the dashboard data in a mini chat interface.
        </Text>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <Text style={{ fontSize: 12 }}>Max messages:</Text>
          <InputNumber
            size="small"
            min={5}
            max={100}
            value={maxMessages}
            onChange={(val) => onConfigChange({ ...(config || {}), maxMessages: String(val || 20) })}
          />
        </div>
      </div>
    );
  }

  if (aiAvailable === false) {
    return (
      <div style={{ padding: 16 }}>
        <Alert
          message="AI Not Configured"
          description="Configure the Prospector AI assistant to use the Q&A panel."
          type="warning"
          showIcon
        />
      </div>
    );
  }

  if (aiAvailable === null) {
    return (
      <div style={{ padding: 16, display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
        <Spin tip="Checking AI availability..." />
      </div>
    );
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', overflow: 'hidden' }}>
      <div
        ref={scrollRef}
        style={{
          flex: 1,
          overflow: 'auto',
          padding: '8px 12px',
          color: darkMode ? '#e0e0e0' : undefined,
        }}
      >
        {messages.length === 0 && (
          <Text type="secondary" style={{ display: 'block', textAlign: 'center', marginTop: 16 }}>
            Ask a question about the dashboard data...
          </Text>
        )}
        {messages.map((msg, i) => (
          <div
            key={i}
            style={{
              marginBottom: 8,
              padding: '4px 8px',
              borderRadius: 6,
              background: msg.role === 'user'
                ? (darkMode ? 'rgba(24,144,255,0.15)' : 'rgba(24,144,255,0.06)')
                : 'transparent',
            }}
          >
            <Text strong style={{ fontSize: 11, color: msg.role === 'user' ? '#1890ff' : '#52c41a' }}>
              {msg.role === 'user' ? 'You' : 'AI'}
            </Text>
            <div style={{ fontSize: 13, lineHeight: 1.5 }}>
              {msg.role === 'assistant' ? (
                <Markdown rehypePlugins={[rehypeRaw]}>{msg.content || ''}</Markdown>
              ) : (
                <span>{msg.content}</span>
              )}
            </div>
          </div>
        ))}
        {isStreaming && messages[messages.length - 1]?.role !== 'assistant' && (
          <Spin size="small" style={{ display: 'block', margin: '8px auto' }} />
        )}
      </div>
      <div style={{ padding: '4px 8px 8px', borderTop: '1px solid var(--color-border, #f0f0f0)' }}>
        <ChatInput
          onSend={handleSend}
          onStop={handleStop}
          isStreaming={isStreaming}
          disabled={dashboardData.length === 0}
          placeholder="Ask about this dashboard..."
        />
      </div>
    </div>
  );
}
