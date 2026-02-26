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
import { useState, useCallback, useRef, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  Card,
  Row,
  Col,
  Tabs,
  Button,
  Space,
  Spin,
  message,
  Typography,
  Divider,
  Table,
  Empty,
} from 'antd';
import {
  ArrowLeftOutlined,
  RobotOutlined,
  DownloadOutlined,
  PlayCircleOutlined,
  LoadingOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  StopOutlined,
} from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import ReactECharts from 'echarts-for-react';
import Markdown from 'react-markdown';
import { getQueryProfileDetail } from '../api/queries';
import { getAiStatus, streamChat } from '../api/ai';
import type { ColumnsType } from 'antd/es/table';
import type { EChartsOption } from 'echarts';
import type { ChatContext } from '../types/ai';

const { Title, Text, Paragraph } = Typography;

// Helper functions
const fmtNanos = (ns: number): string => {
  if (ns > 1e9) return `${(ns / 1e9).toFixed(2)}s`;
  return `${(ns / 1e6).toFixed(1)}ms`;
};

const fmtBytes = (bytes: number): string => {
  if (bytes > 1e9) return `${(bytes / 1e9).toFixed(1)}GB`;
  return `${(bytes / 1e6).toFixed(0)}MB`;
};

// Parse plan text into tree structure for ECharts
interface TreeNode {
  name: string;
  children?: TreeNode[];
}

const parsePlanText = (plan: string): TreeNode => {
  const lines = plan.split('\n').filter((l) => l.trim());
  const root: TreeNode = { name: 'Query Plan', children: [] };

  if (lines.length === 0) return root;

  const stack: Array<{ level: number; node: TreeNode }> = [{ level: -1, node: root }];

  for (const line of lines) {
    const match = line.match(/^(\s*)/);
    const level = match ? match[1].length / 2 : 0;
    const content = line.trim();

    if (!content) continue;

    const newNode: TreeNode = { name: content };

    // Pop stack until we find the parent (last node with smaller level)
    while (stack.length > 1 && stack[stack.length - 1].level >= level) {
      stack.pop();
    }

    const parent = stack[stack.length - 1].node;
    if (!parent.children) parent.children = [];
    parent.children.push(newNode);

    stack.push({ level, node: newNode });
  }

  return root;
};

// Operator type color
const operatorTypeColor = (type: string): string => {
  if (type.includes('SCAN')) return '#1890ff';
  if (type.includes('JOIN')) return '#722ed1';
  if (type.includes('AGG')) return '#fa8c16';
  if (type.includes('SORT')) return '#eb2f96';
  if (type.includes('EXCHANGE')) return '#52c41a';
  return '#8c8c8c';
};

// Flatten operators from fragments
interface FlatOperator {
  majorId: number;
  minorId: number;
  operatorId: number;
  operatorTypeName: string;
  setupNanos: number;
  processNanos: number;
  waitNanos: number;
  peakLocalMemoryAllocated: number;
  totalRecords: number;
  totalBatches: number;
}

const flattenOperators = (fragments: Array<any>): FlatOperator[] => {
  const operators: FlatOperator[] = [];
  fragments.forEach((major) => {
    major.minorFragmentProfile?.forEach((minor: any) => {
      minor.operatorProfile?.forEach((op: any) => {
        const totalRecords = op.inputProfile?.reduce((sum: number, p: any) => sum + (p.records || 0), 0) || 0;
        const totalBatches = op.inputProfile?.reduce((sum: number, p: any) => sum + (p.batches || 0), 0) || 0;
        operators.push({
          majorId: major.majorFragmentId,
          minorId: minor.minorFragmentId,
          operatorId: op.operatorId,
          operatorTypeName: op.operatorTypeName,
          setupNanos: op.setupNanos,
          processNanos: op.processNanos,
          waitNanos: op.waitNanos,
          peakLocalMemoryAllocated: op.peakLocalMemoryAllocated,
          totalRecords,
          totalBatches,
        });
      });
    });
  });
  return operators;
};

// Status badge styling
const getStatusConfig = (state: string) => {
  switch (state) {
    case 'Succeeded':
      return { color: '#52c41a', bgColor: '#f6ffed', icon: <CheckCircleOutlined /> };
    case 'Failed':
      return { color: '#ff4d4f', bgColor: '#fff1f0', icon: <CloseCircleOutlined /> };
    case 'Running':
      return { color: '#1890ff', bgColor: '#e6f7ff', icon: <LoadingOutlined /> };
    case 'Cancelled':
    case 'Cancellation_Requested':
      return { color: '#faad14', bgColor: '#fffbe6', icon: <StopOutlined /> };
    default:
      return { color: '#000000', bgColor: '#f5f5f5', icon: null };
  }
};

// Main component
function ProfileDetailPage() {
  const { queryId } = useParams<{ queryId: string }>();
  const navigate = useNavigate();
  const [aiContent, setAiContent] = useState('');
  const [aiStreaming, setAiStreaming] = useState(false);
  const [activeTab, setActiveTab] = useState('overview');
  const abortRef = useRef<AbortController | null>(null);
  const hasAutoRun = useRef(false);

  const { data: profile, isLoading, error } = useQuery({
    queryKey: ['profileDetail', queryId],
    queryFn: () => (queryId ? getQueryProfileDetail(queryId) : Promise.reject('No query ID')),
    enabled: !!queryId,
  });

  const { data: aiStatus } = useQuery({
    queryKey: ['aiStatus'],
    queryFn: getAiStatus,
    staleTime: 5 * 60 * 1000,
  });

  const aiAvailable = aiStatus?.configured;

  // Auto-run AI analysis on page load
  useEffect(() => {
    if (profile && !hasAutoRun.current && aiAvailable) {
      hasAutoRun.current = true;
      runAiAnalysis('summary');
    }
  }, [profile, aiAvailable]);

  const buildAiContext = useCallback((): ChatContext => {
    if (!profile) return {};

    const planMs = profile.planEnd - profile.start;
    const queueMs = profile.queueWaitEnd - profile.planEnd;
    const execMs = profile.end - profile.queueWaitEnd;

    const topOps = flattenOperators(profile.fragmentProfile)
      .sort((a, b) => b.processNanos - a.processNanos)
      .slice(0, 8);

    const contextSummary = `
Query: ${profile.query}

State: ${profile.state} | Duration: ${(profile.end - profile.start) / 1000}ms
(Planning: ${planMs / 1000}ms, Queue Wait: ${queueMs / 1000}ms, Execution: ${execMs / 1000}ms)
Cost: ${profile.totalCost?.toFixed(0)} | Fragments: ${profile.finishedFragments}/${profile.totalFragments}
Scanned: ${profile.scannedPlugins?.join(', ') || 'unknown'}

Physical Plan:
${profile.plan?.substring(0, 3000) ?? '(unavailable)'}

Top Operators by Process Time:
${topOps
  .map(
    (o) =>
      `- [${o.majorId}-${o.minorId}] ${o.operatorTypeName}: ${fmtNanos(o.processNanos)} process, ${fmtNanos(o.waitNanos)} wait, ${fmtBytes(o.peakLocalMemoryAllocated)} peak mem, ${o.totalRecords.toLocaleString()} records`
  )
  .join('\n')}
${profile.error ? `\nError: ${profile.error}` : ''}
    `.trim();

    return {
      currentSql: profile.query,
      error: profile.error,
      resultSummary: contextSummary as any,
    };
  }, [profile]);

  const buildPrompt = (promptType: string): string => {
    switch (promptType) {
      case 'summary':
        return `In 3-5 bullet points, describe: what this query does, how it performed, and any issues I should know about.`;
      case 'bottlenecks':
        return `Identify the top 3 bottlenecks in this query execution. For each, explain why it's slow and the impact.`;
      case 'optimize':
        return `Give me 3-5 concrete, actionable optimization suggestions for this query. Include SQL rewrites where applicable.`;
      case 'explain-plan':
        return `Walk me through this physical query plan step by step, explaining each operator's role and how data flows through the pipeline.`;
      case 'explain-tab':
        if (activeTab === 'overview') {
          return `Interpret the execution timeline. Is the queue wait or planning time unusually long? What might cause this?`;
        } else if (activeTab === 'plan') {
          return `Explain this physical plan in detail.`;
        } else if (activeTab === 'fragments') {
          return `Analyze this fragment execution. Are fragments well-balanced? Any stragglers?`;
        } else if (activeTab === 'operators') {
          return `Which operators are most expensive and why? What would reduce their cost?`;
        }
        return 'Analyze the current view.';
      default:
        return '';
    }
  };

  const runAiAnalysis = useCallback(
    (promptType: string) => {
      if (!profile || !aiAvailable) return;
      if (abortRef.current) abortRef.current.abort();

      setAiContent('');
      setAiStreaming(true);

      const userMessage = buildPrompt(promptType);
      const context = buildAiContext();

      abortRef.current = streamChat(
        { messages: [{ role: 'user', content: userMessage }], tools: [], context },
        (event) => {
          if (event.type === 'content') {
            setAiContent((prev) => prev + event.content);
          }
        },
        () => {
          setAiStreaming(false);
        },
        (err) => {
          setAiStreaming(false);
          message.error(`AI error: ${err.message}`);
        }
      );
    },
    [profile, aiAvailable, buildAiContext, buildPrompt, activeTab]
  );

  const stopStreaming = useCallback(() => {
    abortRef.current?.abort();
    setAiStreaming(false);
  }, []);

  const handleRunInSqlLab = useCallback(() => {
    if (profile?.query) {
      navigate('/query', { state: { loadQuery: { sql: profile.query } } });
    }
  }, [profile, navigate]);

  const handleDownloadJson = useCallback(() => {
    if (profile) {
      const json = JSON.stringify(profile, null, 2);
      const blob = new Blob([json], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `profile-${queryId}.json`;
      a.click();
      URL.revokeObjectURL(url);
    }
  }, [profile, queryId]);

  if (isLoading) {
    return (
      <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
        <Spin size="large" />
      </div>
    );
  }

  if (error || !profile) {
    return (
      <div style={{ padding: '24px' }}>
        <Button type="text" onClick={() => navigate(-1)}>
          <ArrowLeftOutlined /> Back
        </Button>
        <Empty description="Profile not found" />
      </div>
    );
  }

  // Calculate timing
  const planMs = (profile.planEnd - profile.start) / 1000;
  const queueMs = (profile.queueWaitEnd - profile.planEnd) / 1000;
  const execMs = (profile.end - profile.queueWaitEnd) / 1000;
  const totalMs = (profile.end - profile.start) / 1000;

  const statusConfig = getStatusConfig(profile.state);

  // Execution waterfall chart
  const waterfallOption: EChartsOption = {
    tooltip: { trigger: 'axis' },
    xAxis: { type: 'value', name: 'ms' },
    yAxis: { type: 'category', data: ['Duration'] },
    series: [
      { name: 'Planning', type: 'bar', stack: 'total', data: [planMs], itemStyle: { color: '#1890ff' } },
      { name: 'Queue Wait', type: 'bar', stack: 'total', data: [queueMs], itemStyle: { color: '#fa8c16' } },
      { name: 'Execution', type: 'bar', stack: 'total', data: [execMs], itemStyle: { color: '#52c41a' } },
    ],
  };

  // Plan tree
  const planTree = parsePlanText(profile.plan);
  const planTreeOption: EChartsOption = {
    tooltip: { trigger: 'item' },
    series: [
      {
        type: 'tree',
        data: [planTree],
        layout: 'orthogonal' as const,
        orient: 'TB' as const,
        symbol: 'rect',
        symbolSize: [100, 40],
        label: { position: 'inside', fontSize: 11 },
        lineStyle: { color: '#ccc' },
        itemStyle: { color: '#e6f7ff' },
      },
    ],
  };

  // Fragment Gantt chart
  const fragmentData: Array<{ value: [number, number]; name: string; majorId: number }> = [];
  const fragmentLabels: string[] = [];

  profile.fragmentProfile.forEach((major) => {
    major.minorFragmentProfile.forEach((minor) => {
      const label = `${major.majorFragmentId}-${minor.minorFragmentId}`;
      fragmentLabels.push(label);
      fragmentData.push({
        value: [minor.startTime, minor.endTime],
        name: label,
        majorId: major.majorFragmentId,
      });
    });
  });

  const majorFragmentColors = [
    '#1890ff',
    '#722ed1',
    '#fa8c16',
    '#52c41a',
    '#eb2f96',
    '#13c2c2',
    '#f5222d',
  ];

  const ganttOption: EChartsOption = {
    tooltip: { trigger: 'axis' },
    xAxis: { type: 'time' },
    yAxis: { type: 'category', data: fragmentLabels },
    series: [
      {
        type: 'bar',
        data: fragmentData.map((d) => ({
          value: d.value,
          itemStyle: { color: majorFragmentColors[d.majorId % majorFragmentColors.length] },
        })),
      },
    ],
  };

  // Operator metrics
  const operators = flattenOperators(profile.fragmentProfile).sort((a, b) => b.processNanos - a.processNanos);

  const topOperatorsOption: EChartsOption = {
    tooltip: { trigger: 'axis' },
    xAxis: { type: 'value' },
    yAxis: { type: 'category', data: operators.slice(0, 10).map((o) => o.operatorTypeName) },
    series: [
      {
        type: 'bar',
        data: operators.slice(0, 10).map((o) => (o.processNanos / 1e6).toFixed(1)),
        itemStyle: { color: '#1890ff' },
      },
    ],
  };

  const operatorColumns: ColumnsType<FlatOperator> = [
    {
      title: 'Fragment',
      key: 'fragment',
      width: 100,
      render: (_, record) => `${record.majorId}-${record.minorId}`,
      sorter: (a, b) => {
        const aVal = `${a.majorId}-${a.minorId}`;
        const bVal = `${b.majorId}-${b.minorId}`;
        return aVal.localeCompare(bVal);
      },
    },
    {
      title: 'Operator Type',
      dataIndex: 'operatorTypeName',
      key: 'operatorTypeName',
      width: 150,
      render: (text) => (
        <span style={{ color: operatorTypeColor(text), fontWeight: 500 }}>{text}</span>
      ),
      sorter: (a, b) => a.operatorTypeName.localeCompare(b.operatorTypeName),
    },
    {
      title: 'Process Time',
      key: 'processNanos',
      width: 120,
      render: (_, record) => fmtNanos(record.processNanos),
      sorter: (a, b) => a.processNanos - b.processNanos,
    },
    {
      title: 'Wait Time',
      key: 'waitNanos',
      width: 120,
      render: (_, record) => fmtNanos(record.waitNanos),
      sorter: (a, b) => a.waitNanos - b.waitNanos,
    },
    {
      title: 'Setup Time',
      key: 'setupNanos',
      width: 120,
      render: (_, record) => fmtNanos(record.setupNanos),
      sorter: (a, b) => a.setupNanos - b.setupNanos,
    },
    {
      title: 'Peak Memory',
      key: 'peakLocalMemoryAllocated',
      width: 120,
      render: (_, record) => fmtBytes(record.peakLocalMemoryAllocated),
      sorter: (a, b) => a.peakLocalMemoryAllocated - b.peakLocalMemoryAllocated,
    },
    {
      title: 'Input Records',
      key: 'totalRecords',
      width: 130,
      render: (_, record) => record.totalRecords.toLocaleString(),
      sorter: (a, b) => a.totalRecords - b.totalRecords,
    },
    {
      title: 'Input Batches',
      key: 'totalBatches',
      width: 120,
      render: (_, record) => record.totalBatches.toLocaleString(),
      sorter: (a, b) => a.totalBatches - b.totalBatches,
    },
  ];

  return (
    <div style={{ padding: '24px', height: '100%', overflow: 'auto' }}>
      {/* Header */}
      <div style={{ marginBottom: '24px' }}>
        <Space>
          <Button type="text" onClick={() => navigate(-1)}>
            <ArrowLeftOutlined /> Back
          </Button>
          <Title level={3} style={{ margin: 0 }}>
            {queryId}
          </Title>
        </Space>
      </div>

      {/* SQL Query */}
      <Card style={{ marginBottom: '16px' }} title="Query">
        <pre
          style={{
            backgroundColor: '#f5f5f5',
            padding: '12px',
            borderRadius: '4px',
            overflow: 'auto',
            fontSize: '12px',
            maxHeight: '150px',
            margin: 0,
          }}
        >
          {profile.query}
        </pre>
      </Card>

      {/* Status & Actions bar */}
      <Card style={{ marginBottom: '24px' }}>
        <Space>
          <span
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: '6px',
              padding: '4px 12px',
              borderRadius: '4px',
              backgroundColor: statusConfig.bgColor,
              color: statusConfig.color,
              fontWeight: 500,
            }}
          >
            {statusConfig.icon}
            {profile.state}
          </span>
          <Divider type="vertical" />
          <Text strong>User:</Text>
          <Text>{profile.user}</Text>
          <Divider type="vertical" />
          <Text strong>Foreman:</Text>
          <Text>{profile.foreman?.address || 'Unknown'}</Text>
          <Divider type="vertical" />
          <Text strong>Queue:</Text>
          <Text>{profile.queueName || '-'}</Text>
          <div style={{ marginLeft: 'auto' }}>
            <Space>
              <Button type="primary" onClick={handleRunInSqlLab}>
                <PlayCircleOutlined /> Run in SQL Lab
              </Button>
              <Button onClick={handleDownloadJson}>
                <DownloadOutlined /> Download JSON
              </Button>
            </Space>
          </div>
        </Space>
      </Card>

      {/* Stat cards - evenly distributed */}
      <Row gutter={[16, 16]} style={{ marginBottom: '24px' }}>
        <Col xs={24} sm={12} lg={6}>
          <Card style={{ height: '100%' }}>
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: '12px', color: '#666', marginBottom: '12px', fontWeight: 500 }}>Status</div>
              <span
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  gap: '6px',
                  padding: '6px 16px',
                  borderRadius: '4px',
                  backgroundColor: statusConfig.bgColor,
                  color: statusConfig.color,
                  fontWeight: 600,
                  fontSize: '14px',
                }}
              >
                {statusConfig.icon}
                {profile.state}
              </span>
            </div>
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card style={{ height: '100%' }}>
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: '12px', color: '#666', marginBottom: '12px', fontWeight: 500 }}>Total Duration</div>
              <div style={{ fontSize: '28px', fontWeight: 'bold', color: '#1890ff', marginBottom: '4px' }}>
                {totalMs.toFixed(1)}ms
              </div>
              <div style={{ fontSize: '12px', color: '#999' }}>
                Plan: {planMs.toFixed(0)}ms | Queue: {queueMs.toFixed(0)}ms | Exec: {execMs.toFixed(0)}ms
              </div>
            </div>
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card style={{ height: '100%' }}>
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: '12px', color: '#666', marginBottom: '12px', fontWeight: 500 }}>Total Cost</div>
              <div style={{ fontSize: '28px', fontWeight: 'bold', color: '#52c41a', marginBottom: '4px' }}>
                {profile.totalCost?.toFixed(2) || 'N/A'}
              </div>
            </div>
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card style={{ height: '100%' }}>
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: '12px', color: '#666', marginBottom: '12px', fontWeight: 500 }}>Fragments</div>
              <div style={{ fontSize: '28px', fontWeight: 'bold', color: '#faad14', marginBottom: '4px' }}>
                {profile.finishedFragments}/{profile.totalFragments}
              </div>
              <div style={{ fontSize: '12px', color: '#999' }}>
                {Math.round((profile.finishedFragments / profile.totalFragments) * 100)}% complete
              </div>
            </div>
          </Card>
        </Col>
      </Row>

      {/* Two column layout */}
      <Row gutter={24}>
        {/* Left column - Tabs */}
        <Col span={16}>
          <Tabs
            activeKey={activeTab}
            onChange={setActiveTab}
            items={[
              {
                key: 'overview',
                label: 'Overview',
                children: (
                  <div>
                    <Card title="Execution Timeline" style={{ marginBottom: '16px' }}>
                      <ReactECharts option={waterfallOption} style={{ height: '300px' }} />
                    </Card>
                    <Card title="Query Metadata">
                      <div style={{ fontSize: '12px' }}>
                        <div style={{ marginBottom: '8px' }}>
                          <Text strong>Query ID:</Text> {profile.queryId}
                        </div>
                        <div style={{ marginBottom: '8px' }}>
                          <Text strong>State:</Text> {profile.state}
                        </div>
                        <div style={{ marginBottom: '8px' }}>
                          <Text strong>User:</Text> {profile.user}
                        </div>
                        <div style={{ marginBottom: '8px' }}>
                          <Text strong>Total Cost:</Text> {profile.totalCost?.toFixed(2)}
                        </div>
                        <div style={{ marginBottom: '8px' }}>
                          <Text strong>Scanned Plugins:</Text> {profile.scannedPlugins?.join(', ') || 'None'}
                        </div>
                        <div style={{ marginBottom: '8px' }}>
                          <Text strong>Queue Name:</Text> {profile.queueName || '-'}
                        </div>
                      </div>
                    </Card>
                  </div>
                ),
              },
              {
                key: 'plan',
                label: 'Plan Visualization',
                children: (
                  <div>
                    <Card title="Physical Plan Tree" style={{ marginBottom: '16px' }}>
                      <ReactECharts option={planTreeOption} style={{ height: '400px' }} />
                    </Card>
                    <Card title="Plan Text">
                      <pre
                        style={{
                          backgroundColor: '#f5f5f5',
                          padding: '12px',
                          borderRadius: '4px',
                          overflow: 'auto',
                          fontSize: '11px',
                          maxHeight: '300px',
                        }}
                      >
                        {profile.plan}
                      </pre>
                    </Card>
                  </div>
                ),
              },
              {
                key: 'fragments',
                label: 'Fragment Analysis',
                children: (
                  <div>
                    <Card title="Fragment Execution Timeline" style={{ marginBottom: '16px' }}>
                      <ReactECharts option={ganttOption} style={{ height: '400px' }} />
                    </Card>
                    <Card title="Fragment Details">
                      <Table
                        dataSource={fragmentData.map((d, i) => ({ key: i, ...d }))}
                        columns={[
                          { title: 'Fragment', dataIndex: 'name', key: 'name' },
                          {
                            title: 'Start',
                            key: 'start',
                            render: (_, record) => new Date(record.value[0]).toLocaleTimeString(),
                          },
                          {
                            title: 'End',
                            key: 'end',
                            render: (_, record) => new Date(record.value[1]).toLocaleTimeString(),
                          },
                          {
                            title: 'Duration',
                            key: 'duration',
                            render: (_, record) => fmtNanos((record.value[1] - record.value[0]) * 1e6),
                          },
                        ]}
                        pagination={false}
                        size="small"
                      />
                    </Card>
                  </div>
                ),
              },
              {
                key: 'operators',
                label: 'Operator Metrics',
                children: (
                  <div>
                    <Card title="Top Operators by Process Time" style={{ marginBottom: '16px' }}>
                      <ReactECharts option={topOperatorsOption} style={{ height: '300px' }} />
                    </Card>
                    <Card title="All Operators">
                      <Table
                        dataSource={operators.map((op, i) => ({ ...op, key: i }))}
                        columns={operatorColumns}
                        pagination={{ pageSize: 20 }}
                        size="small"
                      />
                    </Card>
                  </div>
                ),
              },
              ...(profile.state === 'Failed'
                ? [
                    {
                      key: 'error',
                      label: 'Error Details',
                      children: (
                        <div>
                          <Card title="Error Information">
                            {profile.error && (
                              <div style={{ marginBottom: '16px' }}>
                                <Text strong>Error:</Text>
                                <Paragraph
                                  style={{
                                    backgroundColor: '#fff1f0',
                                    padding: '12px',
                                    borderRadius: '4px',
                                    marginTop: '8px',
                                  }}
                                >
                                  {profile.error}
                                </Paragraph>
                              </div>
                            )}
                            {profile.verboseError && (
                              <div>
                                <Text strong>Verbose Error:</Text>
                                <Paragraph
                                  style={{
                                    backgroundColor: '#fafafa',
                                    padding: '12px',
                                    borderRadius: '4px',
                                    marginTop: '8px',
                                    maxHeight: '300px',
                                    overflow: 'auto',
                                  }}
                                >
                                  <pre style={{ whiteSpace: 'pre-wrap', wordWrap: 'break-word' }}>
                                    {profile.verboseError}
                                  </pre>
                                </Paragraph>
                              </div>
                            )}
                          </Card>
                        </div>
                      ),
                    },
                  ]
                : []),
            ]}
          />
        </Col>

        {/* Right column - AI Advisor */}
        <Col span={8}>
          <Card
            title={
              <Space>
                <span>✦ Query Advisor</span>
                <RobotOutlined />
              </Space>
            }
            style={{
              backgroundColor: '#fafafa',
              height: 'fit-content',
              maxHeight: 'calc(100vh - 200px)',
              overflow: 'auto',
            }}
          >
            {!aiAvailable ? (
              <div style={{ padding: '12px 0' }}>
                <Text type="secondary">
                  AI Query Advisor requires Prospector to be configured. Visit Admin → Prospector Settings.
                </Text>
              </div>
            ) : aiStreaming ? (
              <div style={{ paddingBottom: '12px' }}>
                <Markdown>{aiContent}</Markdown>
                <div style={{ color: '#999', fontSize: '11px', marginTop: '12px', animation: 'pulse 1s infinite' }}>
                  ▌
                </div>
              </div>
            ) : aiContent ? (
              <Markdown>{aiContent}</Markdown>
            ) : (
              <Text type="secondary">Loading analysis...</Text>
            )}

            <Divider />

            <Space wrap style={{ width: '100%' }}>
              {!aiStreaming ? (
                <>
                  <Button
                    size="small"
                    onClick={() => runAiAnalysis('bottlenecks')}
                    disabled={aiStreaming || !aiAvailable}
                  >
                    ⚡ Bottlenecks
                  </Button>
                  <Button
                    size="small"
                    onClick={() => runAiAnalysis('optimize')}
                    disabled={aiStreaming || !aiAvailable}
                  >
                    🔧 Optimize
                  </Button>
                  <Button
                    size="small"
                    onClick={() => runAiAnalysis('explain-plan')}
                    disabled={aiStreaming || !aiAvailable}
                  >
                    📋 Explain Plan
                  </Button>
                  <Button
                    size="small"
                    onClick={() => runAiAnalysis('explain-tab')}
                    disabled={aiStreaming || !aiAvailable}
                  >
                    📊 Explain Tab
                  </Button>
                </>
              ) : (
                <Button size="small" type="primary" danger onClick={stopStreaming}>
                  ■ Stop
                </Button>
              )}
            </Space>
          </Card>
        </Col>
      </Row>
    </div>
  );
}

export default ProfileDetailPage;
