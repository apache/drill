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
import { Modal, Tabs, Empty, Button } from 'antd';
import { BulbOutlined, FileTextOutlined, RocketOutlined } from '@ant-design/icons';
import { useState, useRef } from 'react';
import ReactMarkdown from 'react-markdown';
import { streamChat } from '../../api/ai';
import { aiObservability } from '../../services/aiObservability';
import QuerySuggestions from '../project/QuerySuggestions';
import type { DatasetRef, Project } from '../../types';
import type { ChatMessage, DeltaEvent } from '../../types/ai';
import { useAiModal } from '../../contexts/AiModalContext';

interface AiAssistantModalProps {
  projectId: string;
  datasets: DatasetRef[];
  savedQueryCount: number;
  project?: Project;
  currentSql?: string;
  onSelectSql: (sql: string) => void;
}

function ExplainQueryPanel({ sql, onApplySql }: { sql: string; onApplySql?: (sql: string) => void }) {
  const [response, setResponse] = useState<string>('');
  const [isLoading, setIsLoading] = useState(false);
  const abortRef = useRef<AbortController | null>(null);

  if (!sql.trim()) {
    return <Empty description="Enter a query to explain it" />;
  }

  // Extract SQL code blocks from the response
  const extractSqlFromResponse = (text: string): string | null => {
    // Try to match markdown code blocks first (```sql ... ```)
    const blockMatch = text.match(/```(?:sql)?\s*\n?([\s\S]*?)\n?```/);
    if (blockMatch) {
      return blockMatch[1].trim();
    }

    // Try to find a SELECT/INSERT/UPDATE/DELETE statement
    const sqlMatch = text.match(/(SELECT|INSERT|UPDATE|DELETE|WITH)\s+[\s\S]*?(?=\n\n|\n[A-Z]|$)/i);
    if (sqlMatch) {
      return sqlMatch[0].trim();
    }

    return null;
  };

  const extractedSql = extractSqlFromResponse(response);

  // Debug logging for SQL extraction
  if (response && !extractedSql) {
    console.log('⚠️ Could not extract SQL from response:', response.substring(0, 200));
  } else if (extractedSql) {
    console.log('✅ Extracted SQL:', extractedSql.substring(0, 100));
  }

  const handleExplain = () => {
    if (abortRef.current) {
      abortRef.current.abort();
    }

    setIsLoading(true);
    setResponse('');

    const prompt = `You are an Apache Drill SQL expert. Explain this SQL query in detail:

\`\`\`sql
${sql}
\`\`\`

Provide:
1. What the query does (in plain English)
2. What data it returns
3. Any key operations (joins, aggregations, filters)
4. Any potential performance considerations`;

    const messages: ChatMessage[] = [{ role: 'user', content: prompt }];
    let accumulated = '';
    const startTime = Date.now();

    abortRef.current = streamChat(
      { messages, tools: [], context: {} },
      (event: DeltaEvent) => {
        if (event.type === 'content') {
          accumulated += event.content;
          setResponse(accumulated);
        }
      },
      () => {
        const duration = Date.now() - startTime;
        aiObservability.logAICall('explain_query', prompt, accumulated, duration);
        setIsLoading(false);
      },
      (err) => {
        const duration = Date.now() - startTime;
        aiObservability.logAICall('explain_query', prompt, accumulated, duration, err.message);
        setIsLoading(false);
      }
    );
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <Button type="primary" onClick={handleExplain} loading={isLoading}>
        Explain This Query
      </Button>
      {response && (
        <>
          <div style={{
            padding: 12,
            border: '1px solid var(--color-border)',
            borderRadius: 4,
            background: 'var(--color-bg-container)',
            maxHeight: 400,
            overflow: 'auto',
          }}>
            <ReactMarkdown>{response}</ReactMarkdown>
          </div>
          {onApplySql && (
            <Button
              onClick={() => {
                if (extractedSql) {
                  onApplySql(extractedSql);
                } else {
                  console.warn('⚠️ Could not extract SQL from the response. Please copy it manually.');
                }
              }}
              type="primary"
              disabled={!extractedSql}
              title={extractedSql ? 'Apply this query to the editor' : 'No SQL found in response'}
            >
              Use This Query {extractedSql ? '✓' : '(no SQL detected)'}
            </Button>
          )}
        </>
      )}
    </div>
  );
}

function OptimizeQueryPanel({ sql, onApplySql }: { sql: string; onApplySql?: (sql: string) => void }) {
  const [response, setResponse] = useState<string>('');
  const [isLoading, setIsLoading] = useState(false);
  const abortRef = useRef<AbortController | null>(null);

  if (!sql.trim()) {
    return <Empty description="Enter a query to optimize it" />;
  }

  // Extract SQL code blocks from the response
  const extractSqlFromResponse = (text: string): string | null => {
    // Try to match markdown code blocks first (```sql ... ```)
    const blockMatch = text.match(/```(?:sql)?\s*\n?([\s\S]*?)\n?```/);
    if (blockMatch) {
      return blockMatch[1].trim();
    }

    // Try to find a SELECT/INSERT/UPDATE/DELETE statement
    const sqlMatch = text.match(/(SELECT|INSERT|UPDATE|DELETE|WITH)\s+[\s\S]*?(?=\n\n|\n[A-Z]|$)/i);
    if (sqlMatch) {
      return sqlMatch[0].trim();
    }

    return null;
  };

  const extractedSql = extractSqlFromResponse(response);

  // Debug logging for SQL extraction
  if (response && !extractedSql) {
    console.log('⚠️ Could not extract SQL from response:', response.substring(0, 200));
  } else if (extractedSql) {
    console.log('✅ Extracted SQL:', extractedSql.substring(0, 100));
  }

  const handleOptimize = () => {
    if (abortRef.current) {
      abortRef.current.abort();
    }

    setIsLoading(true);
    setResponse('');

    const prompt = `You are an Apache Drill SQL expert specializing in query optimization. Analyze this SQL query and suggest optimizations:

\`\`\`sql
${sql}
\`\`\`

Provide:
1. Current performance characteristics
2. 3-5 specific optimization suggestions with reasons
3. Potential rewritten query (if applicable)
4. When these optimizations would help most

Be practical and focus on changes that can actually improve performance in Apache Drill.`;

    const messages: ChatMessage[] = [{ role: 'user', content: prompt }];
    let accumulated = '';
    const startTime = Date.now();

    abortRef.current = streamChat(
      { messages, tools: [], context: {} },
      (event: DeltaEvent) => {
        if (event.type === 'content') {
          accumulated += event.content;
          setResponse(accumulated);
        }
      },
      () => {
        const duration = Date.now() - startTime;
        aiObservability.logAICall('optimize_query', prompt, accumulated, duration);
        setIsLoading(false);
      },
      (err) => {
        const duration = Date.now() - startTime;
        aiObservability.logAICall('optimize_query', prompt, accumulated, duration, err.message);
        setIsLoading(false);
      }
    );
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <Button type="primary" onClick={handleOptimize} loading={isLoading}>
        Optimize This Query
      </Button>
      {response && (
        <>
          <div style={{
            padding: 12,
            border: '1px solid var(--color-border)',
            borderRadius: 4,
            background: 'var(--color-bg-container)',
            maxHeight: 400,
            overflow: 'auto',
          }}>
            <ReactMarkdown>{response}</ReactMarkdown>
          </div>
          {onApplySql && (
            <Button
              onClick={() => {
                if (extractedSql) {
                  onApplySql(extractedSql);
                } else {
                  console.warn('⚠️ Could not extract SQL from the response. Please copy it manually.');
                }
              }}
              type="primary"
              disabled={!extractedSql}
              title={extractedSql ? 'Apply this query to the editor' : 'No SQL found in response'}
            >
              Use This Query {extractedSql ? '✓' : '(no SQL detected)'}
            </Button>
          )}
        </>
      )}
    </div>
  );
}

export default function AiAssistantModal({
  projectId,
  datasets,
  savedQueryCount,
  project,
  currentSql,
  onSelectSql,
}: AiAssistantModalProps) {
  const { isOpen, mode, closeModal, openModal } = useAiModal();

  const tabItems = [
    {
      key: 'suggestions',
      label: (
        <span>
          <BulbOutlined style={{ marginRight: 8 }} />
          Query Suggestions
        </span>
      ),
      children: (
        <QuerySuggestions
          projectId={projectId}
          datasets={datasets}
          savedQueryCount={savedQueryCount}
          project={project}
          onSelectSql={(sql) => {
            onSelectSql(sql);
            closeModal();
          }}
        />
      ),
    },
    {
      key: 'explain',
      label: (
        <span>
          <FileTextOutlined style={{ marginRight: 8 }} />
          Explain Query
        </span>
      ),
      children: (
        <ExplainQueryPanel
          sql={currentSql || ''}
          onApplySql={(sql) => {
            onSelectSql(sql);
            closeModal();
          }}
        />
      ),
    },
    {
      key: 'optimize',
      label: (
        <span>
          <RocketOutlined style={{ marginRight: 8 }} />
          Optimize Query
        </span>
      ),
      children: (
        <OptimizeQueryPanel
          sql={currentSql || ''}
          onApplySql={(sql) => {
            onSelectSql(sql);
            closeModal();
          }}
        />
      ),
    },
  ];

  return (
    <Modal
      title="AI Assistant"
      open={isOpen}
      onCancel={closeModal}
      width={900}
      footer={null}
      bodyStyle={{ padding: '24px' }}
    >
      <Tabs
        defaultActiveKey={mode}
        activeKey={mode}
        onChange={(key) => openModal(key as 'suggestions' | 'explain' | 'optimize')}
        items={tabItems}
      />
    </Modal>
  );
}
