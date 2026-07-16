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
import { useEffect, useMemo, useState } from 'react';
import {
  Alert,
  AutoComplete,
  Button,
  Card,
  Col,
  Collapse,
  DatePicker,
  Drawer,
  Form,
  Input,
  InputNumber,
  Modal,
  Popconfirm,
  Row,
  Select,
  Space,
  Spin,
  Statistic,
  Table,
  Tag,
  Typography,
  message,
} from 'antd';
import { PlusOutlined, DeleteOutlined, EditOutlined, ReloadOutlined } from '@ant-design/icons';
import dayjs, { type Dayjs } from 'dayjs';
import ReactECharts from 'echarts-for-react';
import type { EChartsOption } from 'echarts';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  getAnalyticsStatus,
  setupAnalytics,
  getAnalyticsSummary,
  getAnalyticsEvents,
  listPricing,
  upsertPricing,
  deletePricing,
  getProviders,
  getModelsForProvider,
  type AiAnalyticsSummary,
  type AiPricingEntry,
  type ProviderInfo,
} from '../api/aiAnalytics';
import { useCurrentUser } from '../hooks/useCurrentUser';
import { FEATURE_LABEL, featureLabel } from '../constants/aiFeatures';

const { Title, Text } = Typography;
const { RangePicker } = DatePicker;

function fmtNum(v: unknown): string {
  if (v === null || v === undefined || v === '') {
    return '—';
  }
  const n = typeof v === 'number' ? v : Number(v);
  if (!Number.isFinite(n)) {
    return String(v);
  }
  return n.toLocaleString();
}

function fmtMoney(v: number, currency: string = 'USD'): string {
  if (!Number.isFinite(v)) {
    return '—';
  }
  return v.toLocaleString(undefined, { style: 'currency', currency, maximumFractionDigits: 4 });
}

function num(v: unknown): number {
  if (v === null || v === undefined || v === '') {
    return 0;
  }
  const n = typeof v === 'number' ? v : Number(v);
  return Number.isFinite(n) ? n : 0;
}

/**
 * Estimated cost for a row that already has provider, model, and token columns,
 * using the pricing snapshot. Returns 0 when no pricing exists for the model.
 */
function estimateCost(
  row: Record<string, unknown>,
  pricing: Record<string, AiPricingEntry>,
): number {
  const key = `${row.provider ?? ''}:${row.model ?? ''}`;
  const p = pricing[key];
  if (!p) {
    return 0;
  }
  const inputCost = (num(row.inputTokens) / 1_000_000) * p.inputPricePerMTokens;
  const outputCost = (num(row.outputTokens) / 1_000_000) * p.outputPricePerMTokens;
  return inputCost + outputCost;
}

/**
 * Tiny line chart drawn behind a stat card to show direction of travel.
 * No axes, no tooltip — purely a visual hint. Renders as SVG so it stays crisp
 * at the small size and doesn't cost a Canvas per card.
 */
function Sparkline({ data, color }: { data: number[]; color: string }) {
  if (!data.length || data.every((v) => v === 0)) {
    return null;
  }
  const option: EChartsOption = {
    grid: { left: 0, right: 0, top: 4, bottom: 0 },
    xAxis: { type: 'category', show: false, data: data.map((_, i) => i) },
    yAxis: { type: 'value', show: false, scale: true },
    series: [{
      type: 'line',
      data,
      smooth: true,
      showSymbol: false,
      lineStyle: { width: 1.5, color },
      areaStyle: { color, opacity: 0.12 },
    }],
    tooltip: { show: false },
    animation: false,
  };
  return (
    <ReactECharts
      option={option}
      style={{ height: 32, marginTop: 8 }}
      opts={{ renderer: 'svg' }}
    />
  );
}

export default function AiAnalyticsPage() {
  const { user, isLoading: userLoading } = useCurrentUser();
  const queryClient = useQueryClient();

  const [range, setRange] = useState<[Dayjs, Dayjs]>(() => [
    dayjs().startOf('day').subtract(6, 'day'),
    dayjs().endOf('day'),
  ]);

  const fromIso = range[0].toISOString();
  const toIso = range[1].toISOString();

  const status = useQuery({ queryKey: ['ai-analytics', 'status'], queryFn: getAnalyticsStatus });

  const setup = useMutation({
    mutationFn: setupAnalytics,
    onSuccess: (r) => {
      message.success(r.message || 'Configured');
      queryClient.invalidateQueries({ queryKey: ['ai-analytics', 'status'] });
    },
    onError: (e: Error) => message.error(e.message),
  });

  const summary = useQuery({
    queryKey: ['ai-analytics', 'summary', fromIso, toIso],
    queryFn: () => getAnalyticsSummary(fromIso, toIso),
    enabled: status.data?.ready === true,
  });

  if (userLoading) {
    return (
      <div className="route-fallback">
        <Spin size="large" />
      </div>
    );
  }

  if (user && user.authEnabled && !user.isAdmin) {
    return (
      <div style={{ padding: 24 }}>
        <Alert
          type="warning"
          message="Admin access required"
          description="The AI analytics dashboard is only available to administrators."
        />
      </div>
    );
  }

  const ready = status.data?.ready === true;

  return (
    <div style={{ padding: 24 }}>
      <Space direction="vertical" size="large" style={{ width: '100%' }}>
        <Space style={{ justifyContent: 'space-between', width: '100%' }}>
          <Title level={3} style={{ margin: 0 }}>AI Analytics</Title>
          <Space>
            <RangePicker
              value={range}
              onChange={(v) => {
                if (v && v[0] && v[1]) {
                  setRange([v[0], v[1]]);
                }
              }}
              showTime={false}
              allowClear={false}
            />
            <Button
              icon={<ReloadOutlined />}
              onClick={() => {
                queryClient.invalidateQueries({ queryKey: ['ai-analytics'] });
              }}
            >
              Refresh
            </Button>
          </Space>
        </Space>

        {!status.data?.logDirConfigured && (
          <Alert
            type="error"
            showIcon
            message="DRILL_LOG_DIR is not set"
            description="Set DRILL_LOG_DIR on the Drill server so AI events can be persisted to disk."
          />
        )}

        {status.data?.logDirConfigured && !ready && (
          <Alert
            type="info"
            showIcon
            message="One-time setup required"
            description={
              <Space direction="vertical">
                <Text>
                  Register the Drill workspace and JSON format that lets the dashboard query
                  ai-events.log via SQL. This is a one-time admin action.
                </Text>
                <Button type="primary" loading={setup.isPending} onClick={() => setup.mutate()}>
                  Configure dfs.ai_logs workspace
                </Button>
              </Space>
            }
          />
        )}

        {/*
          Genuine misconfiguration is already reported by the two alerts above
          (DRILL_LOG_DIR unset, workspace/format not registered). Once `ready` is true,
          the only thing left that can set notConfigured is a missing ai-events*.log —
          i.e. the deployment is set up correctly and simply hasn't made an AI call yet.
        */}
        {ready && summary.data?.notConfigured && (
          <Alert
            type="info"
            showIcon
            message="No AI events captured yet"
            description="AI analytics is set up, but no event log has been written. Once Prospector or the SQL transpiler is used, events will appear here."
          />
        )}

        {ready && summary.data && (
          <SummarySection summary={summary.data} loading={summary.isFetching} />
        )}

        {ready && (
          <EventsSection from={fromIso} to={toIso} pricing={summary.data?.pricing ?? {}} />
        )}

        <PricingSection />
      </Space>
    </div>
  );
}

interface SummarySectionProps {
  summary: AiAnalyticsSummary;
  loading: boolean;
}

function SummarySection({ summary, loading }: SummarySectionProps) {
  const totals = summary.totals ?? {};
  const calls = num(totals.totalCalls);
  const cancelled = num(totals.cancelledCount);
  const failures = num(totals.failureCount);
  // Exclude cancellations from the denominator — a user closing the SSE stream
  // is not a real failure and should not drag down the success rate.
  const ratable = Math.max(0, calls - cancelled);
  const successRate = ratable > 0 ? (num(totals.successCount) / ratable) * 100 : 0;

  // Rough cost: sum cost across byModel rows using pricing snapshot
  const totalCost = useMemo(() => {
    return (summary.byModel ?? []).reduce((sum, row) => sum + estimateCost(row, summary.pricing), 0);
  }, [summary.byModel, summary.pricing]);

  // Per-day arrays driving the sparklines that sit behind the headline cards.
  const callsByDay = useMemo(
    () => (summary.series ?? []).map((r) => num(r.calls)),
    [summary.series],
  );
  const tokensByDay = useMemo(
    () => (summary.series ?? []).map((r) => num(r.inputTokens) + num(r.outputTokens)),
    [summary.series],
  );
  const successRateByDay = useMemo(
    () => (summary.series ?? []).map((r) => {
      const c = num(r.calls);
      return c > 0 ? (num(r.successes) / c) * 100 : 0;
    }),
    [summary.series],
  );

  const seriesOption = useMemo<EChartsOption>(() => {
    const days = (summary.series ?? []).map((r) => String(r.day ?? ''));
    const callsArr = (summary.series ?? []).map((r) => num(r.calls));
    const tokensArr = (summary.series ?? []).map((r) => num(r.inputTokens) + num(r.outputTokens));
    return {
      tooltip: { trigger: 'axis' },
      legend: { data: ['Calls', 'Tokens'] },
      xAxis: { type: 'category', data: days },
      yAxis: [
        { type: 'value', name: 'Calls' },
        { type: 'value', name: 'Tokens' },
      ],
      grid: { left: 50, right: 60, top: 30, bottom: 30 },
      series: [
        { name: 'Calls', type: 'bar', data: callsArr, yAxisIndex: 0, itemStyle: { color: '#1677ff' } },
        { name: 'Tokens', type: 'line', data: tokensArr, yAxisIndex: 1, itemStyle: { color: '#52c41a' } },
      ],
    };
  }, [summary.series]);

  return (
    <Spin spinning={loading}>
      <Row gutter={[16, 16]}>
        <Col xs={24} md={6}>
          <Card>
            <Statistic title="Total calls" value={fmtNum(calls)} />
            <Sparkline data={callsByDay} color="#1677ff" />
          </Card>
        </Col>
        <Col xs={24} md={6}>
          <Card>
            <Statistic
              title="Success rate"
              value={successRate.toFixed(1)}
              suffix="%"
              valueStyle={{ color: successRate >= 95 ? '#52c41a' : successRate >= 80 ? '#faad14' : '#ff4d4f' }}
            />
            <Sparkline data={successRateByDay} color="#52c41a" />
          </Card>
        </Col>
        <Col xs={24} md={6}>
          <Card>
            <Statistic
              title="Avg latency"
              value={fmtNum(Math.round(num(totals.avgDurationMs)))}
              suffix="ms"
            />
          </Card>
        </Col>
        <Col xs={24} md={6}>
          <Card>
            <Statistic
              title="Failures"
              value={fmtNum(failures)}
              valueStyle={{ color: failures > 0 ? '#ff4d4f' : undefined }}
            />
          </Card>
        </Col>
        <Col xs={24} md={6}>
          <Card>
            <Statistic
              title="Cancelled"
              value={fmtNum(cancelled)}
              valueStyle={{ color: cancelled > 0 ? '#faad14' : undefined }}
            />
            <Text type="secondary" style={{ fontSize: 12 }}>
              Client closed the stream
            </Text>
          </Card>
        </Col>
        <Col xs={24} md={6}>
          <Card>
            <Statistic title="Total tokens" value={fmtNum(num(totals.totalTokens))} />
            <Sparkline data={tokensByDay} color="#722ed1" />
          </Card>
        </Col>
        <Col xs={24} md={6}>
          <Card>
            <Statistic title="Unique users" value={fmtNum(num(totals.uniqueUsers))} />
          </Card>
        </Col>
        <Col xs={24} md={6}>
          <Card>
            <Statistic
              title="Cost (range)"
              value={fmtMoney(totalCost)}
              valueStyle={{ color: totalCost > 0 ? '#1677ff' : undefined }}
            />
            <Text type="secondary" style={{ fontSize: 12 }}>
              Based on configured pricing
            </Text>
          </Card>
        </Col>
        {summary.projection && (summary.projection.mtdCostUsd > 0
          || summary.projection.projectedMonthEndCostUsd > 0) && (
          <Col xs={24} md={6}>
            <Card>
              <Statistic
                title="Projected this month"
                value={fmtMoney(summary.projection.projectedMonthEndCostUsd)}
                valueStyle={{ color: '#1677ff' }}
              />
              <Text type="secondary" style={{ fontSize: 12 }}>
                {fmtMoney(summary.projection.mtdCostUsd)} so far · day {summary.projection.daysElapsed} of {summary.projection.daysInMonth}
              </Text>
            </Card>
          </Col>
        )}
      </Row>

      <Card title="Calls and tokens over time" style={{ marginTop: 16 }}>
        {summary.series && summary.series.length > 0 ? (
          <ReactECharts option={seriesOption} style={{ height: 320 }} />
        ) : (
          <Text type="secondary">No events in this date range.</Text>
        )}
      </Card>

      <Row gutter={[16, 16]} style={{ marginTop: 16 }}>
        <Col xs={24} lg={12}>
          <Card title="By feature">
            <Table
              size="small"
              rowKey={(r) => String(r.feature ?? Math.random())}
              dataSource={summary.byFeature}
              pagination={false}
              columns={[
                { title: 'Feature', dataIndex: 'feature', render: (v: string) => featureLabel(v) },
                { title: 'Calls', dataIndex: 'calls', align: 'right', render: fmtNum },
                { title: 'Tokens', dataIndex: 'tokens', align: 'right', render: fmtNum },
                { title: 'Avg ms', dataIndex: 'avgDurationMs', align: 'right',
                  render: (v) => fmtNum(Math.round(num(v))) },
              ]}
            />
          </Card>
        </Col>
        <Col xs={24} lg={12}>
          <Card title="By provider / model">
            <Table
              size="small"
              rowKey={(r) => `${r.provider ?? ''}:${r.model ?? ''}`}
              dataSource={summary.byModel}
              pagination={false}
              columns={[
                { title: 'Provider', dataIndex: 'provider' },
                { title: 'Model', dataIndex: 'model' },
                { title: 'Calls', dataIndex: 'calls', align: 'right', render: fmtNum },
                { title: 'In tokens', dataIndex: 'inputTokens', align: 'right', render: fmtNum },
                { title: 'Out tokens', dataIndex: 'outputTokens', align: 'right', render: fmtNum },
                {
                  title: 'Est. cost',
                  align: 'right',
                  render: (_v, row) => fmtMoney(estimateCost(row, summary.pricing)),
                },
              ]}
            />
          </Card>
        </Col>
        <Col xs={24} lg={12}>
          <Card title="Top users">
            <Table
              size="small"
              rowKey={(r) => String(r.user ?? Math.random())}
              dataSource={summary.byUser}
              pagination={false}
              columns={[
                { title: 'User', dataIndex: 'user' },
                { title: 'Calls', dataIndex: 'calls', align: 'right', render: fmtNum },
                { title: 'Tokens', dataIndex: 'tokens', align: 'right', render: fmtNum },
              ]}
            />
          </Card>
        </Col>
        <Col xs={24} lg={12}>
          <Card title="Latency by model">
            <Table
              size="small"
              rowKey={(r) => `${r.provider ?? ''}:${r.model ?? ''}`}
              dataSource={summary.latencyByModel}
              pagination={false}
              columns={[
                { title: 'Provider', dataIndex: 'provider' },
                { title: 'Model', dataIndex: 'model' },
                { title: 'Avg ms', dataIndex: 'avgDurationMs', align: 'right',
                  render: (v) => fmtNum(Math.round(num(v))) },
                { title: 'Min ms', dataIndex: 'minDurationMs', align: 'right', render: fmtNum },
                { title: 'Max ms', dataIndex: 'maxDurationMs', align: 'right', render: fmtNum },
              ]}
            />
          </Card>
        </Col>
      </Row>
    </Spin>
  );
}

interface EventsSectionProps {
  from: string;
  to: string;
  pricing: Record<string, AiPricingEntry>;
}

function EventsSection({ from, to, pricing }: EventsSectionProps) {
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);
  const [filters, setFilters] = useState<{ user?: string; feature?: string; success?: string }>({});
  const [selected, setSelected] = useState<Record<string, string> | null>(null);

  const events = useQuery({
    queryKey: ['ai-analytics', 'events', from, to, page, pageSize, filters],
    queryFn: () =>
      getAnalyticsEvents({
        from,
        to,
        user: filters.user,
        feature: filters.feature,
        success: filters.success === 'true'
          || filters.success === 'false'
          || filters.success === 'cancelled'
          ? (filters.success as 'true' | 'false' | 'cancelled')
          : undefined,
        limit: pageSize,
        offset: (page - 1) * pageSize,
      }),
  });

  return (
    <Card
      title="Recent events"
      extra={
        <Space>
          <Input
            placeholder="user"
            allowClear
            style={{ width: 140 }}
            onChange={(e) => {
              setFilters((f) => ({ ...f, user: e.target.value }));
              setPage(1);
            }}
          />
          <Select
            placeholder="feature"
            allowClear
            style={{ width: 180 }}
            value={filters.feature}
            onChange={(v) => {
              setFilters((f) => ({ ...f, feature: v }));
              setPage(1);
            }}
            options={Object.entries(FEATURE_LABEL).map(([k, v]) => ({ value: k, label: v }))}
          />
          <Select
            placeholder="status"
            allowClear
            style={{ width: 130 }}
            value={filters.success}
            onChange={(v) => {
              setFilters((f) => ({ ...f, success: v }));
              setPage(1);
            }}
            options={[
              { value: 'true', label: 'Success' },
              { value: 'false', label: 'Failure' },
              { value: 'cancelled', label: 'Cancelled' },
            ]}
          />
        </Space>
      }
    >
      <Table
        size="small"
        loading={events.isFetching}
        rowKey={(r, i) => `${r.ts}-${i}`}
        dataSource={events.data?.rows ?? []}
        locale={{
          // notConfigured means the event log doesn't exist yet, which is a different
          // empty than "the log exists but nothing matched these filters".
          emptyText: events.data?.notConfigured
            ? 'No AI event log yet — events appear here once an AI feature is used.'
            : 'No events match the selected range and filters.',
        }}
        onRow={(record) => ({
          onClick: () => setSelected(record),
          style: { cursor: 'pointer' },
        })}
        pagination={{
          current: page,
          pageSize,
          showSizeChanger: true,
          pageSizeOptions: ['25', '50', '100', '250'],
          onChange: (p, ps) => {
            setPage(p);
            setPageSize(ps);
          },
          // Server-side pagination — Ant total drives "Next" button availability.
          // We don't know the absolute total without a count query, so show "many".
          total: ((events.data?.rows.length ?? 0) === pageSize) ? page * pageSize + pageSize : page * pageSize,
        }}
        columns={[
          { title: 'When', dataIndex: 'ts', width: 200,
            render: (v: string) => v ? new Date(v).toLocaleString() : '—' },
          { title: 'User', dataIndex: 'user', width: 140 },
          { title: 'Feature', dataIndex: 'feature', width: 160,
            render: (v: string) => featureLabel(v) },
          { title: 'Provider', dataIndex: 'provider', width: 110 },
          { title: 'Model', dataIndex: 'model', width: 200, ellipsis: true },
          { title: 'In', dataIndex: 'promptTokens', align: 'right', width: 70, render: fmtNum },
          { title: 'Out', dataIndex: 'responseTokens', align: 'right', width: 70, render: fmtNum },
          { title: 'ms', dataIndex: 'durationMs', align: 'right', width: 80, render: fmtNum },
          {
            title: 'Cost',
            align: 'right',
            width: 100,
            render: (_v, row) => fmtMoney(estimateCost({
              provider: row.provider,
              model: row.model,
              inputTokens: row.promptTokens,
              outputTokens: row.responseTokens,
            }, pricing)),
          },
          { title: 'Status', dataIndex: 'success', width: 90,
            render: (v: string, row: Record<string, string>) => {
              if (row.cancelled === 'true') {
                return <Tag color="orange">cancelled</Tag>;
              }
              return v === 'true'
                ? <Tag color="green">ok</Tag>
                : <Tag color="red">fail</Tag>;
            } },
        ]}
      />
      <Drawer
        title="AI event"
        open={selected !== null}
        onClose={() => setSelected(null)}
        width={720}
      >
        {selected && (
          <Space direction="vertical" size="middle" style={{ width: '100%' }}>
            <Row gutter={[12, 12]}>
              <Col span={12}><Text type="secondary">Time</Text><div>{selected.ts}</div></Col>
              <Col span={12}><Text type="secondary">User</Text><div>{selected.user}</div></Col>
              <Col span={12}><Text type="secondary">Feature</Text><div>{featureLabel(selected.feature)}</div></Col>
              <Col span={12}><Text type="secondary">Provider / model</Text><div>{selected.provider} / {selected.model}</div></Col>
              <Col span={6}><Text type="secondary">In tokens</Text><div>{fmtNum(selected.promptTokens)}</div></Col>
              <Col span={6}><Text type="secondary">Out tokens</Text><div>{fmtNum(selected.responseTokens)}</div></Col>
              <Col span={6}><Text type="secondary">Duration</Text><div>{fmtNum(selected.durationMs)} ms</div></Col>
              <Col span={6}>
                <Text type="secondary">Status</Text>
                <div>
                  {selected.cancelled === 'true'
                    ? 'cancelled'
                    : selected.success === 'true' ? 'ok' : 'failed'}
                </div>
              </Col>
            </Row>
            {selected.error && (
              <Alert type="error" message={selected.errorClass || 'Error'} description={selected.error} />
            )}
            <div>
              <Text type="secondary">User message</Text>
              <pre style={{ background: 'var(--color-bg-elevated)', padding: 12, borderRadius: 4, whiteSpace: 'pre-wrap', marginTop: 4 }}>
                {selected.userMessage || '—'}
              </pre>
            </div>
            <div>
              <Text type="secondary">Full prompt sent to LLM</Text>
              <pre style={{ background: 'var(--color-bg-elevated)', padding: 12, borderRadius: 4, whiteSpace: 'pre-wrap', maxHeight: 400, overflow: 'auto', marginTop: 4 }}>
                {selected.prompt || '—'}
              </pre>
            </div>
            <div>
              <Text type="secondary">Response</Text>
              <pre style={{ background: 'var(--color-bg-elevated)', padding: 12, borderRadius: 4, whiteSpace: 'pre-wrap', maxHeight: 400, overflow: 'auto', marginTop: 4 }}>
                {selected.response || '—'}
              </pre>
            </div>
          </Space>
        )}
      </Drawer>
    </Card>
  );
}

function PricingSection() {
  const queryClient = useQueryClient();
  const pricing = useQuery({ queryKey: ['ai-pricing'], queryFn: listPricing });
  const [editing, setEditing] = useState<AiPricingEntry | null>(null);

  const save = useMutation({
    mutationFn: upsertPricing,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['ai-pricing'] });
      queryClient.invalidateQueries({ queryKey: ['ai-analytics', 'summary'] });
      setEditing(null);
    },
    onError: (e: Error) => message.error(e.message),
  });

  const del = useMutation({
    mutationFn: ({ provider, model }: { provider: string; model: string }) => deletePricing(provider, model),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['ai-pricing'] });
      queryClient.invalidateQueries({ queryKey: ['ai-analytics', 'summary'] });
    },
  });

  return (
    <>
      <Collapse
        items={[
          {
            key: 'pricing',
            label: 'Model pricing',
            extra: (
              <Button
                size="small"
                type="primary"
                icon={<PlusOutlined />}
                onClick={(e) => {
                  e.stopPropagation();
                  setEditing({ provider: '', model: '', inputPricePerMTokens: 0, outputPricePerMTokens: 0, currency: 'USD' });
                }}
              >
                Add model
              </Button>
            ),
            children: (
              <>
                <Text type="secondary">
                  Per-million-token pricing used to estimate cost in this dashboard.
                  Costs are computed at query time, so changing prices retroactively updates totals.
                </Text>
                <Table
                  size="small"
                  style={{ marginTop: 12 }}
                  rowKey={(r) => `${r.provider}:${r.model}`}
                  dataSource={pricing.data ?? []}
                  loading={pricing.isLoading}
                  pagination={false}
                  columns={[
                    { title: 'Provider', dataIndex: 'provider' },
                    { title: 'Model', dataIndex: 'model' },
                    { title: 'Input / 1M', dataIndex: 'inputPricePerMTokens', align: 'right',
                      render: (v: number, row) => fmtMoney(v, row.currency) },
                    { title: 'Output / 1M', dataIndex: 'outputPricePerMTokens', align: 'right',
                      render: (v: number, row) => fmtMoney(v, row.currency) },
                    { title: 'Currency', dataIndex: 'currency', width: 90 },
                    {
                      title: '', width: 100, align: 'right',
                      render: (_v, row) => (
                        <Space size="small">
                          <Button size="small" icon={<EditOutlined />} onClick={() => setEditing(row)} />
                          <Popconfirm
                            title="Delete pricing entry?"
                            onConfirm={() => del.mutate({ provider: row.provider, model: row.model })}
                          >
                            <Button size="small" danger icon={<DeleteOutlined />} />
                          </Popconfirm>
                        </Space>
                      ),
                    },
                  ]}
                />
              </>
            ),
          },
        ]}
      />
      {editing && (
        <PricingEditor
          entry={editing}
          onCancel={() => setEditing(null)}
          onSave={(e) => save.mutate(e)}
          saving={save.isPending}
        />
      )}
    </>
  );
}

interface PricingEditorProps {
  entry: AiPricingEntry;
  onCancel: () => void;
  onSave: (e: AiPricingEntry) => void;
  saving: boolean;
}

function PricingEditor({ entry, onCancel, onSave, saving }: PricingEditorProps) {
  const [form] = Form.useForm<AiPricingEntry>();
  const [providers, setProviders] = useState<ProviderInfo[]>([]);
  const [models, setModels] = useState<string[]>([]);

  useEffect(() => {
    getProviders().then(setProviders).catch(() => setProviders([]));
  }, []);

  const handleProviderChange = (value: string) => {
    form.setFieldValue('provider', value);
    setModels([]);
    if (value) {
      getModelsForProvider(value)
        .then(setModels)
        .catch(() => setModels([]));
    }
  };

  const providerOptions = providers.map((p) => ({
    label: p.displayName,
    value: p.id,
  }));

  const modelOptions = models.map((m) => ({
    label: m,
    value: m,
  }));

  return (
    <Modal
      title={entry.provider && entry.model ? `Edit ${entry.provider}/${entry.model}` : 'Add model pricing'}
      open
      onCancel={onCancel}
      onOk={() => {
        form.validateFields().then((v) => onSave({ ...entry, ...v }));
      }}
      confirmLoading={saving}
    >
      <Form layout="vertical" form={form} initialValues={entry}>
        <Form.Item label="Provider" name="provider" rules={[{ required: true }]}>
          <AutoComplete
            options={providerOptions}
            placeholder="Select or enter provider (openai, anthropic, ollama, sqlglot, …)"
            onChange={handleProviderChange}
            filterOption={(inputValue, option) =>
              (option?.label ?? '').toLowerCase().includes(inputValue.toLowerCase()) ||
              (option?.value ?? '').toLowerCase().includes(inputValue.toLowerCase())
            }
          />
        </Form.Item>
        <Form.Item label="Model" name="model" rules={[{ required: true }]}>
          <AutoComplete
            options={modelOptions}
            placeholder="Select or enter model (gpt-4o, claude-sonnet-4, …)"
            filterOption={(inputValue, option) =>
              (option?.label ?? '').toLowerCase().includes(inputValue.toLowerCase())
            }
          />
        </Form.Item>
        <Form.Item label="Input price per 1M tokens" name="inputPricePerMTokens" rules={[{ required: true }]}>
          <InputNumber min={0} step={0.01} style={{ width: '100%' }} />
        </Form.Item>
        <Form.Item label="Output price per 1M tokens" name="outputPricePerMTokens" rules={[{ required: true }]}>
          <InputNumber min={0} step={0.01} style={{ width: '100%' }} />
        </Form.Item>
        <Form.Item label="Currency" name="currency" initialValue="USD">
          <Input />
        </Form.Item>
      </Form>
    </Modal>
  );
}
