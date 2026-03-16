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
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Alert, Button, Card, Input, InputNumber, Select, Space, Switch, Typography } from 'antd';
import { DeleteOutlined, PlusOutlined } from '@ant-design/icons';
import Markdown from 'react-markdown';
import rehypeRaw from 'rehype-raw';
import { getAiStatus, streamChat } from '../../api/ai';
import type { ChatMessage, DashboardDataContext, DeltaEvent } from '../../types/ai';

const { Text } = Typography;

interface AlertRule {
  id: string;
  name: string;
  panelId: string;
  column: string;
  operator: '>' | '>=' | '<' | '<=' | '=' | '!=';
  value: number;
  severity: 'info' | 'warning' | 'critical';
}

interface TriggeredAlert {
  rule: AlertRule;
  actualValue: number;
  panelName: string;
}

interface AiAlertsPanelProps {
  config?: Record<string, string>;
  editMode: boolean;
  darkMode?: boolean;
  dashboardData: DashboardDataContext[];
  onConfigChange: (config: Record<string, string>) => void;
}

const OPERATOR_OPTIONS = [
  { label: '>', value: '>' },
  { label: '>=', value: '>=' },
  { label: '<', value: '<' },
  { label: '<=', value: '<=' },
  { label: '=', value: '=' },
  { label: '!=', value: '!=' },
];

const SEVERITY_COLORS: Record<string, string> = {
  info: '#1890ff',
  warning: '#faad14',
  critical: '#ff4d4f',
};

function evaluateRule(rule: AlertRule, data: DashboardDataContext[]): TriggeredAlert | null {
  const panel = data.find((d) => d.panelName === rule.panelId);
  if (!panel || !panel.sampleRows.length) {
    return null;
  }

  for (const row of panel.sampleRows) {
    const rawVal = row[rule.column];
    const num = Number(rawVal);
    if (isNaN(num)) {
      continue;
    }

    let triggered = false;
    switch (rule.operator) {
      case '>': triggered = num > rule.value; break;
      case '>=': triggered = num >= rule.value; break;
      case '<': triggered = num < rule.value; break;
      case '<=': triggered = num <= rule.value; break;
      case '=': triggered = num === rule.value; break;
      case '!=': triggered = num !== rule.value; break;
    }

    if (triggered) {
      return { rule, actualValue: num, panelName: panel.panelName };
    }
  }
  return null;
}

export default function AiAlertsPanel({
  config,
  editMode,
  darkMode,
  dashboardData,
  onConfigChange,
}: AiAlertsPanelProps) {
  const [aiAnalysis, setAiAnalysis] = useState('');
  const [analyzing, setAnalyzing] = useState(false);
  const [aiAvailable, setAiAvailable] = useState<boolean | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const rules: AlertRule[] = useMemo(() => {
    try {
      return config?.alertRules ? JSON.parse(config.alertRules) : [];
    } catch {
      return [];
    }
  }, [config?.alertRules]);

  const aiAnalysisEnabled = config?.aiAnalysis === 'true';

  useEffect(() => {
    getAiStatus()
      .then((s) => setAiAvailable(s.enabled && s.configured))
      .catch(() => setAiAvailable(false));
  }, []);

  const saveRules = useCallback((newRules: AlertRule[]) => {
    onConfigChange({ ...(config || {}), alertRules: JSON.stringify(newRules) });
  }, [config, onConfigChange]);

  const addRule = useCallback(() => {
    const panelName = dashboardData.length > 0 ? dashboardData[0].panelName : '';
    const column = dashboardData.length > 0 && dashboardData[0].columns.length > 0
      ? dashboardData[0].columns[0]
      : '';
    const newRule: AlertRule = {
      id: crypto.randomUUID(),
      name: `Rule ${rules.length + 1}`,
      panelId: panelName,
      column,
      operator: '>',
      value: 0,
      severity: 'warning',
    };
    saveRules([...rules, newRule]);
  }, [rules, dashboardData, saveRules]);

  const updateRule = useCallback((ruleId: string, update: Partial<AlertRule>) => {
    saveRules(rules.map((r) => r.id === ruleId ? { ...r, ...update } : r));
  }, [rules, saveRules]);

  const removeRule = useCallback((ruleId: string) => {
    saveRules(rules.filter((r) => r.id !== ruleId));
  }, [rules, saveRules]);

  const triggeredAlerts = useMemo(() => {
    return rules
      .map((rule) => evaluateRule(rule, dashboardData))
      .filter((a): a is TriggeredAlert => a !== null);
  }, [rules, dashboardData]);

  const runAiAnalysis = useCallback(() => {
    if (!aiAvailable || triggeredAlerts.length === 0) {
      return;
    }
    if (abortRef.current) {
      abortRef.current.abort();
    }
    setAnalyzing(true);
    setAiAnalysis('');
    let accumulated = '';

    const alertSummary = triggeredAlerts.map((a) =>
      `- [${a.rule.severity.toUpperCase()}] "${a.rule.name}" on panel "${a.panelName}": ${a.rule.column} ${a.rule.operator} ${a.rule.value} (actual: ${a.actualValue})`
    ).join('\n');

    const messages: ChatMessage[] = [{
      role: 'user',
      content: `The following dashboard alerts have been triggered:\n\n${alertSummary}\n\nPlease explain these alert conditions and suggest actions to address them.`,
    }];

    const controller = streamChat(
      {
        messages,
        tools: [],
        context: { dashboardAlertMode: true, dashboardData },
      },
      (event: DeltaEvent) => {
        if (event.type === 'content') {
          accumulated += event.content;
          setAiAnalysis(accumulated);
        }
      },
      () => setAnalyzing(false),
      () => setAnalyzing(false),
    );
    abortRef.current = controller;
  }, [aiAvailable, triggeredAlerts, dashboardData]);

  useEffect(() => {
    if (aiAnalysisEnabled && triggeredAlerts.length > 0 && aiAvailable && !editMode) {
      runAiAnalysis();
    }
  }, [triggeredAlerts.length, aiAnalysisEnabled, aiAvailable, editMode]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    return () => {
      if (abortRef.current) {
        abortRef.current.abort();
      }
    };
  }, []);

  const allColumns = useMemo(() => {
    const cols: string[] = [];
    dashboardData.forEach((d) => d.columns.forEach((c) => {
      if (!cols.includes(c)) {
        cols.push(c);
      }
    }));
    return cols;
  }, [dashboardData]);

  const panelOptions = useMemo(() =>
    dashboardData.map((d) => ({ label: d.panelName, value: d.panelName })),
  [dashboardData]);

  if (editMode) {
    return (
      <div style={{ padding: 8, height: '100%', overflow: 'auto', display: 'flex', flexDirection: 'column', gap: 8 }}>
        <Text type="secondary" style={{ fontSize: 12 }}>
          Configure alert rules that evaluate against dashboard data. Alerts trigger when conditions are met.
        </Text>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <Switch
            size="small"
            checked={aiAnalysisEnabled}
            onChange={(checked) => onConfigChange({ ...(config || {}), aiAnalysis: checked ? 'true' : 'false' })}
          />
          <Text style={{ fontSize: 12 }}>Enable AI analysis of triggered alerts</Text>
        </div>
        {rules.map((rule) => (
          <Card key={rule.id} size="small" style={{ marginBottom: 4 }}>
            <Space direction="vertical" style={{ width: '100%' }} size={4}>
              <Input
                size="small"
                value={rule.name}
                onChange={(e) => updateRule(rule.id, { name: e.target.value })}
                placeholder="Rule name"
                addonBefore="Name"
              />
              <Space wrap>
                <Select
                  size="small"
                  style={{ width: 160 }}
                  value={rule.panelId}
                  onChange={(val) => updateRule(rule.id, { panelId: val })}
                  options={panelOptions}
                  placeholder="Panel"
                />
                <Select
                  size="small"
                  style={{ width: 120 }}
                  value={rule.column}
                  onChange={(val) => updateRule(rule.id, { column: val })}
                  options={allColumns.map((c) => ({ label: c, value: c }))}
                  placeholder="Column"
                />
                <Select
                  size="small"
                  style={{ width: 70 }}
                  value={rule.operator}
                  onChange={(val) => updateRule(rule.id, { operator: val })}
                  options={OPERATOR_OPTIONS}
                />
                <InputNumber
                  size="small"
                  style={{ width: 90 }}
                  value={rule.value}
                  onChange={(val) => updateRule(rule.id, { value: val ?? 0 })}
                />
                <Select
                  size="small"
                  style={{ width: 100 }}
                  value={rule.severity}
                  onChange={(val) => updateRule(rule.id, { severity: val })}
                  options={[
                    { label: 'Info', value: 'info' },
                    { label: 'Warning', value: 'warning' },
                    { label: 'Critical', value: 'critical' },
                  ]}
                />
                <Button
                  size="small"
                  danger
                  icon={<DeleteOutlined />}
                  onClick={() => removeRule(rule.id)}
                />
              </Space>
            </Space>
          </Card>
        ))}
        <Button size="small" icon={<PlusOutlined />} onClick={addRule}>
          Add Rule
        </Button>
      </div>
    );
  }

  if (rules.length === 0) {
    return (
      <div style={{ padding: 16, textAlign: 'center' }}>
        <Text type="secondary">No alert rules configured. Edit this panel to add rules.</Text>
      </div>
    );
  }

  return (
    <div style={{ padding: 8, height: '100%', overflow: 'auto', color: darkMode ? '#e0e0e0' : undefined }}>
      {triggeredAlerts.length === 0 && (
        <Alert message="All Clear" description="No alerts triggered." type="success" showIcon style={{ marginBottom: 8 }} />
      )}
      {triggeredAlerts.map((alert) => (
        <Alert
          key={alert.rule.id}
          message={alert.rule.name}
          description={`${alert.rule.column} ${alert.rule.operator} ${alert.rule.value} (actual: ${alert.actualValue}) on "${alert.panelName}"`}
          type={alert.rule.severity === 'critical' ? 'error' : alert.rule.severity === 'warning' ? 'warning' : 'info'}
          showIcon
          style={{
            marginBottom: 8,
            borderLeft: `4px solid ${SEVERITY_COLORS[alert.rule.severity]}`,
          }}
        />
      ))}
      {rules.filter((r) => !triggeredAlerts.find((a) => a.rule.id === r.id)).map((rule) => (
        <Alert
          key={rule.id}
          message={rule.name}
          description={`${rule.column} ${rule.operator} ${rule.value} — OK`}
          type="success"
          showIcon
          style={{ marginBottom: 8, opacity: 0.7 }}
        />
      ))}
      {aiAnalysis && (
        <div style={{ marginTop: 8, padding: 8, borderTop: '1px solid var(--color-border, #f0f0f0)' }}>
          <Text strong style={{ fontSize: 12 }}>AI Analysis</Text>
          <div style={{ fontSize: 13, lineHeight: 1.5 }}>
            <Markdown rehypePlugins={[rehypeRaw]}>{aiAnalysis}</Markdown>
          </div>
        </div>
      )}
      {analyzing && !aiAnalysis && (
        <div style={{ textAlign: 'center', marginTop: 8 }}>
          <Text type="secondary" style={{ fontSize: 12 }}>Analyzing alerts...</Text>
        </div>
      )}
    </div>
  );
}
