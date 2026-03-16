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
import { Alert, Button, Input, Select, Space, Spin, Switch, Typography } from 'antd';
import { PushpinFilled, PushpinOutlined, ReloadOutlined, HistoryOutlined } from '@ant-design/icons';
import Markdown from 'react-markdown';
import rehypeRaw from 'rehype-raw';
import { getAiStatus } from '../../api/ai';
import { streamChat } from '../../api/ai';
import type { ChatMessage, DashboardDataContext } from '../../types/ai';

const { TextArea } = Input;
const { Text } = Typography;

const DEFAULT_PROMPT =
  'You are an executive analyst. Summarize the key insights from this dashboard data. ' +
  'Highlight any anomalies, trends, or areas needing attention. Use status indicators ' +
  '(✅ for good, ⚠️ for warning, ❌ for critical) and format your response with clear sections. ' +
  'If appropriate, include relevant images using markdown image syntax.';

// A5: Summary Templates
const SUMMARY_TEMPLATES: { key: string; label: string; prompt: string }[] = [
  { key: 'general', label: 'General', prompt: DEFAULT_PROMPT },
  {
    key: 'sales',
    label: 'Sales Dashboard',
    prompt:
      'You are a sales analyst. Summarize key sales metrics, revenue trends, top performers, ' +
      'and areas of concern. Use status indicators (✅ ⚠️ ❌) and format with clear sections for ' +
      'Revenue, Pipeline, Top Products, and Action Items.',
  },
  {
    key: 'operations',
    label: 'Operations Review',
    prompt:
      'You are an operations analyst. Summarize system health, throughput, error rates, and ' +
      'resource utilization. Highlight any SLA breaches or capacity concerns. Use status indicators ' +
      'and organize by Infrastructure, Performance, Incidents, and Recommendations.',
  },
  {
    key: 'incident',
    label: 'Incident Summary',
    prompt:
      'You are an incident responder. Provide a concise incident summary including timeline, ' +
      'impact assessment, root cause indicators, and recommended next steps. Use severity indicators ' +
      'and organize by Status, Impact, Timeline, and Action Items.',
  },
];

// A1: Tone/Audience Options
const TONE_OPTIONS: { key: string; label: string; instruction: string }[] = [
  { key: 'executive', label: 'Executive', instruction: 'Write in a concise, high-level executive style. Focus on business impact and strategic implications.' },
  { key: 'technical', label: 'Technical', instruction: 'Write in a detailed technical style. Include specific metrics, data points, and technical context.' },
  { key: 'casual', label: 'Casual', instruction: 'Write in a friendly, conversational tone. Keep it approachable and easy to understand.' },
];

// A6: Status Icon Sets
const STATUS_ICON_SETS: { key: string; label: string; good: string; warning: string; critical: string }[] = [
  { key: 'checkmarks', label: 'Checkmarks (default)', good: '✅', warning: '⚠️', critical: '❌' },
  { key: 'traffic', label: 'Traffic Light', good: '🟢', warning: '🟡', critical: '🔴' },
  { key: 'weather', label: 'Weather', good: '☀️', warning: '⛅', critical: '🌧️' },
];

interface ExecutiveSummaryPanelProps {
  content: string;
  config?: Record<string, string>;
  editMode: boolean;
  darkMode?: boolean;
  refreshKey?: number;
  dashboardData: DashboardDataContext[];
  allPanelNames?: { id: string; name: string }[];
  onContentChange: (content: string) => void;
  onConfigChange: (config: Record<string, string>) => void;
}

export default function ExecutiveSummaryPanel({
  content,
  config,
  editMode,
  darkMode,
  refreshKey,
  dashboardData,
  allPanelNames,
  onContentChange,
  onConfigChange,
}: ExecutiveSummaryPanelProps) {
  const [summary, setSummary] = useState(config?.cachedSummary || '');
  const [isGenerating, setIsGenerating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [aiAvailable, setAiAvailable] = useState<boolean | null>(null);
  const abortRef = useRef<AbortController | null>(null);
  const lastDataHashRef = useRef<string>('');
  const prompt = content || DEFAULT_PROMPT;
  const includeSampleData = config?.includeSampleData !== 'false';
  const pinned = config?.pinned === 'true';
  const selectedTemplate = config?.template || 'general';
  const selectedTone = config?.tone || 'executive';
  const anomalyFocus = config?.anomalyFocus === 'true';
  const selectedIconSet = config?.statusIconSet || 'checkmarks';
  const selectedPanelIds = config?.selectedPanelIds ? config.selectedPanelIds.split(',').filter(Boolean) : [];

  // A7: Parse summary history
  const summaryHistory: { timestamp: number; summary: string }[] = (() => {
    try {
      return config?.summaryHistory ? JSON.parse(config.summaryHistory) : [];
    } catch {
      return [];
    }
  })();

  // Check AI availability on mount
  useEffect(() => {
    getAiStatus()
      .then((status) => setAiAvailable(status.enabled && status.configured))
      .catch(() => setAiAvailable(false));
  }, []);

  const buildDataContext = useCallback((): string => {
    if (dashboardData.length === 0) {
      return 'No visualization data is currently available on this dashboard.';
    }

    return dashboardData
      .map((d, i) => {
        let section = `### Panel ${i + 1}: ${d.panelName}\n`;
        section += `- **Query**: \`${d.sql}\`\n`;
        section += `- **Columns**: ${d.columns.map((c, j) => `${c} (${d.columnTypes[j] || 'unknown'})`).join(', ')}\n`;
        section += `- **Row count**: ${d.rowCount}\n`;
        if (includeSampleData && d.sampleRows.length > 0) {
          section += `- **Sample data** (first ${d.sampleRows.length} rows):\n`;
          section += '```json\n' + JSON.stringify(d.sampleRows, null, 2) + '\n```\n';
        }
        return section;
      })
      .join('\n');
  }, [dashboardData, includeSampleData]);

  const generateSummary = useCallback((previousSummaryForComparison?: string) => {
    if (!aiAvailable) {
      return;
    }

    // Abort any in-progress generation
    if (abortRef.current) {
      abortRef.current.abort();
    }

    setIsGenerating(true);
    setError(null);
    setSummary('');

    const dataContext = buildDataContext();
    let accumulated = '';

    // Build prompt with tone instruction (A1)
    let fullPrompt = prompt;
    const tone = TONE_OPTIONS.find((t) => t.key === selectedTone);
    if (tone) {
      fullPrompt = `${tone.instruction}\n\n${fullPrompt}`;
    }

    // Append anomaly focus instruction (A4)
    if (anomalyFocus) {
      fullPrompt += '\n\nPay special attention to anomalies, outliers, and unexpected patterns in the data. ' +
        'Highlight any values that deviate significantly from expected ranges or historical norms.';
    }

    // A6: Status icon instruction
    const iconSet = STATUS_ICON_SETS.find((s) => s.key === selectedIconSet);
    if (iconSet) {
      fullPrompt += `\n\nUse these status indicators: ${iconSet.good} for good/healthy, ${iconSet.warning} for warning/attention needed, ${iconSet.critical} for critical/problem.`;
    }

    // A7: Historical comparison
    if (previousSummaryForComparison) {
      fullPrompt += '\n\nHere is the previous summary for comparison. Please note any changes or trends:\n\n' +
        '---\nPREVIOUS SUMMARY:\n' + previousSummaryForComparison + '\n---';
    }

    const messages: ChatMessage[] = [
      {
        role: 'user',
        content:
          `${fullPrompt}\n\n---\n\n## Dashboard Data\n\n${dataContext}`,
      },
    ];

    const controller = streamChat(
      {
        messages,
        tools: [],
        context: {
          dashboardSummaryMode: true,
          dashboardData,
          dashboardTone: selectedTone,
          dashboardAnomalyFocus: anomalyFocus,
          previousSummary: previousSummaryForComparison,
        },
      },
      (event) => {
        if (event.type === 'content') {
          accumulated += event.content;
          setSummary(accumulated);
        }
      },
      () => {
        setIsGenerating(false);
        // Cache the result and push to history (A7)
        const newHistory = [...summaryHistory, { timestamp: Date.now(), summary: accumulated }].slice(-5);
        onConfigChange({
          ...(config || {}),
          cachedSummary: accumulated,
          summaryHistory: JSON.stringify(newHistory),
        });
      },
      (err) => {
        setIsGenerating(false);
        setError(err.message);
      },
    );

    abortRef.current = controller;
  }, [aiAvailable, prompt, dashboardData, buildDataContext, config, onConfigChange, selectedTone, anomalyFocus, selectedIconSet, summaryHistory]);

  // Generate summary when data changes or on refresh (respecting pin - A2)
  useEffect(() => {
    if (editMode || !aiAvailable || pinned) {
      return;
    }

    // Create a hash of the data to detect changes
    const dataHash = JSON.stringify(
      dashboardData.map((d) => ({ name: d.panelName, rows: d.rowCount, cols: d.columns })),
    );

    if (dataHash !== lastDataHashRef.current && dashboardData.length > 0) {
      lastDataHashRef.current = dataHash;
      generateSummary();
    }
  }, [editMode, aiAvailable, dashboardData, generateSummary, pinned]);

  // Respond to external refresh (refresh interval or manual)
  useEffect(() => {
    if (refreshKey && refreshKey > 0 && !editMode && aiAvailable && dashboardData.length > 0) {
      generateSummary();
    }
    // Only trigger on refreshKey changes
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [refreshKey]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (abortRef.current) {
        abortRef.current.abort();
      }
    };
  }, []);

  // A2: Toggle pin
  const handleTogglePin = useCallback(() => {
    onConfigChange({ ...(config || {}), pinned: pinned ? 'false' : 'true' });
  }, [config, onConfigChange, pinned]);

  // A7: Compare with previous
  const handleCompareWithPrevious = useCallback(() => {
    if (summaryHistory.length >= 2) {
      const previous = summaryHistory[summaryHistory.length - 2];
      generateSummary(previous.summary);
    }
  }, [summaryHistory, generateSummary]);

  // Edit mode: show prompt editor with enhancement controls
  if (editMode) {
    return (
      <div className="executive-summary-panel" style={{ padding: 8, height: '100%', display: 'flex', flexDirection: 'column', gap: 8, overflow: 'auto' }}>
        <Text type="secondary" style={{ fontSize: 12 }}>
          Customize the AI prompt used to generate the executive summary. The AI will receive data from
          visualization panels on this dashboard.
        </Text>

        {/* A5: Template selector */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <Text style={{ fontSize: 12, whiteSpace: 'nowrap' }}>Template:</Text>
          <Select
            size="small"
            style={{ flex: 1 }}
            value={selectedTemplate}
            onChange={(val) => {
              const tmpl = SUMMARY_TEMPLATES.find((t) => t.key === val);
              if (tmpl) {
                onContentChange(tmpl.prompt);
                onConfigChange({ ...(config || {}), template: val });
              }
            }}
            options={SUMMARY_TEMPLATES.map((t) => ({ label: t.label, value: t.key }))}
          />
        </div>

        {/* A1: Tone selector */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <Text style={{ fontSize: 12, whiteSpace: 'nowrap' }}>Tone:</Text>
          <Select
            size="small"
            style={{ flex: 1 }}
            value={selectedTone}
            onChange={(val) => onConfigChange({ ...(config || {}), tone: val })}
            options={TONE_OPTIONS.map((t) => ({ label: t.label, value: t.key }))}
          />
        </div>

        {/* A6: Status icon set */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <Text style={{ fontSize: 12, whiteSpace: 'nowrap' }}>Status Icons:</Text>
          <Select
            size="small"
            style={{ flex: 1 }}
            value={selectedIconSet}
            onChange={(val) => onConfigChange({ ...(config || {}), statusIconSet: val })}
            options={STATUS_ICON_SETS.map((s) => ({ label: `${s.good} ${s.warning} ${s.critical} ${s.label}`, value: s.key }))}
          />
        </div>

        {/* A3: Panel scope selector */}
        {allPanelNames && allPanelNames.length > 0 && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <Text style={{ fontSize: 12, whiteSpace: 'nowrap' }}>Scope:</Text>
            <Select
              size="small"
              mode="multiple"
              style={{ flex: 1 }}
              value={selectedPanelIds}
              onChange={(vals: string[]) => onConfigChange({ ...(config || {}), selectedPanelIds: vals.join(',') })}
              options={allPanelNames.map((p) => ({ label: p.name, value: p.id }))}
              placeholder="All panels (default)"
              allowClear
            />
          </div>
        )}

        <TextArea
          value={prompt}
          onChange={(e) => onContentChange(e.target.value)}
          placeholder={DEFAULT_PROMPT}
          style={{ flex: 1, resize: 'none', fontFamily: 'monospace', fontSize: 12 }}
        />
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <Switch
            size="small"
            checked={includeSampleData}
            onChange={(checked) =>
              onConfigChange({ ...(config || {}), includeSampleData: checked ? 'true' : 'false' })
            }
          />
          <Text style={{ fontSize: 12 }}>Include sample data rows in AI context</Text>
        </div>
        {/* A4: Anomaly detection emphasis */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <Switch
            size="small"
            checked={anomalyFocus}
            onChange={(checked) =>
              onConfigChange({ ...(config || {}), anomalyFocus: checked ? 'true' : 'false' })
            }
          />
          <Text style={{ fontSize: 12 }}>Emphasize anomaly detection</Text>
        </div>
      </div>
    );
  }

  // AI not available
  if (aiAvailable === false) {
    return (
      <div className="executive-summary-panel" style={{ padding: 16 }}>
        <Alert
          message="AI Not Configured"
          description="The Prospector AI assistant must be configured to use the Executive Summary panel. Please configure an AI provider in the SQL Lab settings."
          type="warning"
          showIcon
        />
      </div>
    );
  }

  // Loading AI status
  if (aiAvailable === null) {
    return (
      <div className="executive-summary-panel" style={{ padding: 16, display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
        <Spin tip="Checking AI availability..." />
      </div>
    );
  }

  // Error state
  if (error && !summary) {
    return (
      <div className="executive-summary-panel" style={{ padding: 16 }}>
        <Alert
          message="Summary Generation Failed"
          description={error}
          type="error"
          showIcon
          action={
            <Button size="small" onClick={() => generateSummary()}>
              Retry
            </Button>
          }
        />
      </div>
    );
  }

  // No data yet
  if (!summary && !isGenerating && dashboardData.length === 0) {
    return (
      <div className="executive-summary-panel" style={{ padding: 16, display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
        <Text type="secondary">No visualization data available. Add visualization panels to this dashboard to generate an executive summary.</Text>
      </div>
    );
  }

  // View mode: show summary
  return (
    <div
      className="executive-summary-panel"
      style={{
        padding: '8px 16px',
        height: '100%',
        overflow: 'auto',
        color: darkMode ? '#e0e0e0' : undefined,
      }}
    >
      {isGenerating && !summary && (
        <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
          <Spin tip="Generating executive summary..." />
        </div>
      )}
      {summary && (
        <>
          <div className="executive-summary-content" style={{ fontSize: 14, lineHeight: 1.6 }}>
            <Markdown rehypePlugins={[rehypeRaw]}>{summary}</Markdown>
          </div>
          <div style={{ marginTop: 8, display: 'flex', justifyContent: 'flex-end', gap: 8, alignItems: 'center' }}>
            {isGenerating && <Spin size="small" />}
            {error && <Text type="danger" style={{ fontSize: 12 }}>{error}</Text>}
            <Space>
              {/* A2: Pin toggle */}
              <Button
                size="small"
                type="text"
                icon={pinned ? <PushpinFilled style={{ color: '#1890ff' }} /> : <PushpinOutlined />}
                onClick={handleTogglePin}
                title={pinned ? 'Unpin summary (allow auto-regeneration)' : 'Pin summary (prevent auto-regeneration)'}
              />
              {/* A7: Compare with previous */}
              {summaryHistory.length >= 2 && (
                <Button
                  size="small"
                  type="text"
                  icon={<HistoryOutlined />}
                  onClick={handleCompareWithPrevious}
                  disabled={isGenerating}
                >
                  Compare
                </Button>
              )}
              <Button
                size="small"
                type="text"
                icon={<ReloadOutlined />}
                onClick={() => generateSummary()}
                disabled={isGenerating}
              >
                Regenerate
              </Button>
            </Space>
          </div>
        </>
      )}
    </div>
  );
}
