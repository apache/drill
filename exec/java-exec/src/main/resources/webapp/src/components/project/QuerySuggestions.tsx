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
import { useState, useEffect, useRef } from 'react';
import { Card, Row, Col, Button, Space, Typography, Collapse, Spin, Empty, Tooltip } from 'antd';
import { BulbOutlined, ReloadOutlined, PlayCircleOutlined } from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { getAiStatus, streamChat } from '../../api/ai';
import type { DatasetRef } from '../../types';
import type { ChatMessage, DeltaEvent } from '../../types/ai';

const { Text, Paragraph } = Typography;

interface QuerySuggestion {
  title: string;
  description: string;
  sql: string;
}

interface QuerySuggestionsProps {
  projectId: string;
  datasets: DatasetRef[];
  savedQueryCount: number;
  onSelectSql: (sql: string) => void;
}

function getCacheKey(projectId: string): string {
  return `drill.querySuggestions.${projectId}`;
}

function loadCachedSuggestions(projectId: string): QuerySuggestion[] | null {
  try {
    const raw = localStorage.getItem(getCacheKey(projectId));
    if (raw) {
      return JSON.parse(raw) as QuerySuggestion[];
    }
  } catch {
    // Ignore corrupt cache
  }
  return null;
}

function saveCachedSuggestions(projectId: string, suggestions: QuerySuggestion[]): void {
  try {
    localStorage.setItem(getCacheKey(projectId), JSON.stringify(suggestions));
  } catch {
    // Ignore storage errors
  }
}

/**
 * Parse JSON from an AI response that may be wrapped in markdown code blocks.
 */
function parseJsonFromResponse(text: string): QuerySuggestion[] {
  // Try to extract JSON from markdown code blocks first
  const codeBlockMatch = text.match(/```(?:json)?\s*\n?([\s\S]*?)\n?\s*```/);
  const jsonStr = codeBlockMatch ? codeBlockMatch[1].trim() : text.trim();

  const parsed = JSON.parse(jsonStr);
  if (!Array.isArray(parsed)) {
    throw new Error('Expected a JSON array');
  }
  return parsed.map((item: Record<string, unknown>) => ({
    title: String(item.title || ''),
    description: String(item.description || ''),
    sql: String(item.sql || ''),
  }));
}

function buildPrompt(datasets: DatasetRef[], savedQueryCount: number): string {
  const datasetDescriptions = datasets.map((ds) => {
    if (ds.type === 'table') {
      return `- Table: ${ds.schema ? ds.schema + '.' : ''}${ds.table || ds.label} (type: ${ds.type})`;
    }
    if (ds.type === 'schema') {
      return `- Schema: ${ds.schema || ds.label}`;
    }
    if (ds.type === 'saved_query') {
      return `- Saved Query: ${ds.label}`;
    }
    return `- ${ds.type}: ${ds.label}`;
  }).join('\n');

  return `You are an Apache Drill SQL expert. A project has the following datasets:

${datasetDescriptions}

The project has ${savedQueryCount} saved queries.

Generate 3-5 useful SQL queries that would help explore and analyze this data.
For each query, provide a title, brief description, and the SQL.

Return ONLY a JSON array with objects having these fields: "title", "description", "sql".
Do not include any other text outside the JSON array. Example format:
[
  {"title": "...", "description": "...", "sql": "..."}
]

Make the queries practical and varied - include aggregations, joins (if multiple tables), filtering, and exploration queries. Use Apache Drill SQL syntax.`;
}

export default function QuerySuggestions({
  projectId,
  datasets,
  savedQueryCount,
  onSelectSql,
}: QuerySuggestionsProps) {
  const [suggestions, setSuggestions] = useState<QuerySuggestion[] | null>(null);
  const [isGenerating, setIsGenerating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const { data: aiStatus } = useQuery({
    queryKey: ['aiStatus'],
    queryFn: getAiStatus,
    staleTime: 60_000,
  });

  // Load cached suggestions on mount
  useEffect(() => {
    const cached = loadCachedSuggestions(projectId);
    if (cached) {
      setSuggestions(cached);
    }
  }, [projectId]);

  // Cleanup abort controller on unmount
  useEffect(() => {
    return () => {
      if (abortRef.current) {
        abortRef.current.abort();
      }
    };
  }, []);

  const handleGenerate = () => {
    if (abortRef.current) {
      abortRef.current.abort();
    }

    setIsGenerating(true);
    setError(null);
    setSuggestions(null);

    const prompt = buildPrompt(datasets, savedQueryCount);
    const messages: ChatMessage[] = [
      { role: 'user', content: prompt },
    ];

    let accumulated = '';

    abortRef.current = streamChat(
      { messages, tools: [], context: {} },
      (event: DeltaEvent) => {
        if (event.type === 'content') {
          accumulated += event.content;
        }
      },
      () => {
        // Done
        try {
          const parsed = parseJsonFromResponse(accumulated);
          setSuggestions(parsed);
          saveCachedSuggestions(projectId, parsed);
          setError(null);
        } catch {
          setError('Failed to parse AI response. Please try again.');
        }
        setIsGenerating(false);
      },
      (err) => {
        setError(err.message || 'Failed to generate suggestions.');
        setIsGenerating(false);
      },
    );
  };

  // Don't render if AI is not enabled/configured
  if (!aiStatus?.enabled || !aiStatus?.configured) {
    return null;
  }

  const renderContent = () => {
    if (isGenerating) {
      return (
        <div style={{ textAlign: 'center', padding: '24px 0' }}>
          <Spin size="large" />
          <div style={{ marginTop: 12 }}>
            <Text type="secondary">Generating query suggestions...</Text>
          </div>
        </div>
      );
    }

    if (error) {
      return (
        <Empty
          description={error}
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        >
          <Button type="primary" onClick={handleGenerate}>
            Try Again
          </Button>
        </Empty>
      );
    }

    if (!suggestions) {
      return (
        <div style={{ textAlign: 'center', padding: '24px 0' }}>
          <Paragraph type="secondary">
            Let AI analyze your project datasets and suggest useful queries.
          </Paragraph>
          <Button
            type="primary"
            icon={<BulbOutlined />}
            onClick={handleGenerate}
          >
            Generate Suggestions
          </Button>
        </div>
      );
    }

    if (suggestions.length === 0) {
      return (
        <Empty description="No suggestions generated.">
          <Button onClick={handleGenerate} icon={<ReloadOutlined />}>
            Retry
          </Button>
        </Empty>
      );
    }

    return (
      <div>
        <div style={{ marginBottom: 12, textAlign: 'right' }}>
          <Tooltip title="Regenerate suggestions">
            <Button
              size="small"
              icon={<ReloadOutlined />}
              onClick={handleGenerate}
            >
              Refresh
            </Button>
          </Tooltip>
        </div>
        <Row gutter={[12, 12]}>
          {suggestions.map((suggestion, index) => (
            <Col key={index} xs={24} sm={12} lg={8}>
              <Card
                size="small"
                title={<Text strong>{suggestion.title}</Text>}
                actions={[
                  <Button
                    key="use"
                    type="link"
                    icon={<PlayCircleOutlined />}
                    onClick={() => onSelectSql(suggestion.sql)}
                  >
                    Use This Query
                  </Button>,
                ]}
              >
                <Paragraph
                  type="secondary"
                  ellipsis={{ rows: 2 }}
                  style={{ marginBottom: 8 }}
                >
                  {suggestion.description}
                </Paragraph>
                <pre
                  style={{
                    fontSize: 11,
                    background: '#f5f5f5',
                    padding: 8,
                    borderRadius: 4,
                    maxHeight: 80,
                    overflow: 'hidden',
                    whiteSpace: 'pre-wrap',
                    wordBreak: 'break-all',
                    margin: 0,
                  }}
                >
                  {suggestion.sql}
                </pre>
              </Card>
            </Col>
          ))}
        </Row>
      </div>
    );
  };

  return (
    <Collapse
      defaultActiveKey={suggestions ? ['suggestions'] : []}
      style={{ marginBottom: 12 }}
      items={[
        {
          key: 'suggestions',
          label: (
            <Space>
              <BulbOutlined />
              <Text strong>AI Query Suggestions</Text>
            </Space>
          ),
          children: renderContent(),
        },
      ]}
    />
  );
}
