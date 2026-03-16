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
import { Alert, Button, Input, Space, Spin, Tag, Typography } from 'antd';
import { getAiStatus, streamChat } from '../../api/ai';
import type { DashboardFilter } from '../../types';
import type { ChatMessage, DashboardDataContext, DeltaEvent } from '../../types/ai';

const { Text } = Typography;

interface NlFilterPanelProps {
  config?: Record<string, string>;
  editMode: boolean;
  darkMode?: boolean;
  dashboardData: DashboardDataContext[];
  onConfigChange: (config: Record<string, string>) => void;
  onApplyFilters?: (filters: DashboardFilter[]) => void;
}

export default function NlFilterPanel({
  editMode,
  darkMode,
  dashboardData,
  onApplyFilters,
}: NlFilterPanelProps) {
  const [query, setQuery] = useState('');
  const [isProcessing, setIsProcessing] = useState(false);
  const [previewFilters, setPreviewFilters] = useState<DashboardFilter[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [aiAvailable, setAiAvailable] = useState<boolean | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    getAiStatus()
      .then((s) => setAiAvailable(s.enabled && s.configured))
      .catch(() => setAiAvailable(false));
  }, []);

  useEffect(() => {
    return () => {
      if (abortRef.current) {
        abortRef.current.abort();
      }
    };
  }, []);

  const availableColumns = dashboardData.flatMap((d) =>
    d.columns.map((col, i) => `${col} (${d.columnTypes[i] || 'unknown'}) from "${d.panelName}"`)
  );

  const handleSearch = useCallback((value: string) => {
    if (!value.trim() || !aiAvailable) {
      return;
    }
    if (abortRef.current) {
      abortRef.current.abort();
    }

    setIsProcessing(true);
    setError(null);
    setPreviewFilters(null);

    let accumulated = '';

    const messages: ChatMessage[] = [{
      role: 'user',
      content: `Convert this natural language filter request into structured filters:\n\n"${value}"\n\nAvailable columns:\n${availableColumns.join('\n')}`,
    }];

    const controller = streamChat(
      {
        messages,
        tools: [],
        context: {
          dashboardNlFilterMode: true,
          dashboardData,
        },
      },
      (event: DeltaEvent) => {
        if (event.type === 'content') {
          accumulated += event.content;
        }
      },
      () => {
        setIsProcessing(false);
        try {
          // Extract JSON array from response (may be wrapped in markdown code block)
          const jsonMatch = accumulated.match(/\[[\s\S]*\]/);
          if (!jsonMatch) {
            setError('Could not parse filter response from AI');
            return;
          }
          const parsed = JSON.parse(jsonMatch[0]) as Array<{
            column: string;
            value: string;
            isTemporal?: boolean;
            isNumeric?: boolean;
            numericOp?: string;
            rangeStart?: string;
            rangeEnd?: string;
          }>;

          const filters: DashboardFilter[] = parsed.map((f) => ({
            id: crypto.randomUUID(),
            column: f.column,
            value: f.value,
            label: `${f.column} = ${f.value}`,
            isTemporal: f.isTemporal,
            isNumeric: f.isNumeric,
            numericOp: f.numericOp as DashboardFilter['numericOp'],
            rangeStart: f.rangeStart,
            rangeEnd: f.rangeEnd,
          }));
          setPreviewFilters(filters);
        } catch {
          setError('Failed to parse AI response into filters');
        }
      },
      (err) => {
        setIsProcessing(false);
        setError(err.message);
      },
    );

    abortRef.current = controller;
  }, [aiAvailable, availableColumns, dashboardData]);

  const handleApply = useCallback(() => {
    if (previewFilters && onApplyFilters) {
      onApplyFilters(previewFilters);
      setPreviewFilters(null);
      setQuery('');
    }
  }, [previewFilters, onApplyFilters]);

  const handleCancel = useCallback(() => {
    setPreviewFilters(null);
  }, []);

  if (editMode) {
    return (
      <div style={{ padding: 8 }}>
        <Text type="secondary" style={{ fontSize: 12 }}>
          Natural Language Filter panel lets users describe filters in plain English. The AI converts
          the description into structured dashboard filters.
        </Text>
      </div>
    );
  }

  if (aiAvailable === false) {
    return (
      <div style={{ padding: 16 }}>
        <Alert
          message="AI Not Configured"
          description="Configure the Prospector AI assistant to use the NL Filter panel."
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
    <div style={{ padding: '8px 12px', color: darkMode ? '#e0e0e0' : undefined }}>
      <Input.Search
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        onSearch={handleSearch}
        placeholder='e.g. "Show only sales > 1000 in California"'
        enterButton="Filter"
        loading={isProcessing}
        disabled={dashboardData.length === 0}
      />

      {error && (
        <Alert
          message={error}
          type="error"
          showIcon
          closable
          onClose={() => setError(null)}
          style={{ marginTop: 8 }}
        />
      )}

      {previewFilters && previewFilters.length > 0 && (
        <div style={{ marginTop: 8 }}>
          <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 4 }}>
            Preview filters:
          </Text>
          <div style={{ marginBottom: 8 }}>
            {previewFilters.map((f) => (
              <Tag key={f.id} color="blue" style={{ marginBottom: 4 }}>
                {f.column} = {f.value}
                {f.isTemporal && f.rangeStart && ` (${f.rangeStart})`}
                {f.isNumeric && f.numericOp && f.numericOp !== '=' && ` ${f.numericOp}`}
              </Tag>
            ))}
          </div>
          <Space>
            <Button type="primary" size="small" onClick={handleApply}>Apply</Button>
            <Button size="small" onClick={handleCancel}>Cancel</Button>
          </Space>
        </div>
      )}

      {previewFilters && previewFilters.length === 0 && (
        <Text type="secondary" style={{ fontSize: 12, display: 'block', marginTop: 8 }}>
          No matching filters found. Try rephrasing your query.
        </Text>
      )}
    </div>
  );
}
