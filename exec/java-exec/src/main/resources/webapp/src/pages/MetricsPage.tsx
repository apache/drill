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
import { useMemo, useState, useRef } from 'react';
import {
  Card,
  Row,
  Col,
  Table,
  Progress,
  Statistic,
  Typography,
  Spin,
  Empty,
  Input,
  Space,
  Tag,
  Tooltip,
  Collapse,
} from 'antd';
import {
  SearchOutlined,
  DashboardOutlined,
  ClockCircleOutlined,
  HddOutlined,
  ThunderboltOutlined,
  ArrowUpOutlined,
  ArrowDownOutlined,
  MinusOutlined,
  WarningOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  ApiOutlined,
  FileOutlined,
} from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import ReactECharts from 'echarts-for-react';
import type { EChartsOption } from 'echarts';
import { getMetrics } from '../api/metrics';
import type { MetricsResponse } from '../api/metrics';
import type { ColumnsType } from 'antd/es/table';

const { Text, Title } = Typography;

// ── Helpers ──

const round = (val: number, n: number): number =>
  Math.round(val * Math.pow(10, n)) / Math.pow(10, n);

const fmtBytes = (bytes: number): string => {
  if (bytes >= 1e9) return `${round(bytes / 1e9, 2)} GB`;
  if (bytes >= 1e6) return `${round(bytes / 1e6, 1)} MB`;
  if (bytes >= 1e3) return `${round(bytes / 1e3, 1)} KB`;
  return `${bytes} B`;
};

/** Color thresholds: green < 70%, yellow 70–85%, red > 85% */
function thresholdColor(percent: number): string {
  if (percent >= 85) return '#ff4d4f';
  if (percent >= 70) return '#faad14';
  return '#52c41a';
}

function thresholdStatus(percent: number): 'success' | 'normal' | 'exception' {
  if (percent >= 85) return 'exception';
  if (percent >= 70) return 'normal';
  return 'success';
}

/** Gauge helper */
function gval(gauges: Record<string, { value: number | string | unknown[] }>, key: string): number {
  const v = gauges[key]?.value;
  return typeof v === 'number' ? v : 0;
}

// ── Trend tracking ──

interface TrendData {
  direction: 'up' | 'down' | 'stable';
  delta: number;
}

function getTrend(current: number, previous: number | undefined): TrendData {
  if (previous === undefined) return { direction: 'stable', delta: 0 };
  const delta = current - previous;
  if (Math.abs(delta) < 0.0001) return { direction: 'stable', delta: 0 };
  return { direction: delta > 0 ? 'up' : 'down', delta };
}

function TrendIndicator({ trend, invertColors }: { trend: TrendData; invertColors?: boolean }) {
  if (trend.direction === 'stable') {
    return <MinusOutlined style={{ color: '#8c8c8c', fontSize: 11 }} />;
  }
  const isUp = trend.direction === 'up';
  let color: string;
  if (invertColors) {
    color = isUp ? '#ff4d4f' : '#52c41a';
  } else {
    color = isUp ? '#52c41a' : '#ff4d4f';
  }
  return isUp
    ? <ArrowUpOutlined style={{ color, fontSize: 11 }} />
    : <ArrowDownOutlined style={{ color, fontSize: 11 }} />;
}

// ── Memory Section ──

interface MemoryBar {
  label: string;
  used: number;
  max: number;
  percent: number;
}

function getMemoryBars(gauges: Record<string, { value: number | string | unknown[] }>): MemoryBar[] {
  const g = (k: string) => gval(gauges, k);
  const bar = (label: string, used: number, max: number): MemoryBar => ({
    label,
    used,
    max,
    percent: max > 0 ? round((used / max) * 100, 1) : 0,
  });
  return [
    bar('Heap', g('memory.heap.used'), g('memory.heap.max')),
    bar('Non-Heap', g('memory.non-heap.used'), g('memory.non-heap.max')),
    bar('Total', g('memory.total.used'), g('memory.total.max')),
    bar('Direct (Allocator)', g('drill.allocator.root.used'), g('drill.allocator.root.peak') || g('drill.allocator.root.used')),
  ];
}

/** Gauge key for each memory bar label */
function memBarGaugeKey(label: string): string {
  switch (label) {
    case 'Heap': return 'memory.heap.used';
    case 'Non-Heap': return 'memory.non-heap.used';
    case 'Total': return 'memory.total.used';
    case 'Direct (Allocator)': return 'drill.allocator.root.used';
    default: return '';
  }
}

interface RpcBufferInfo {
  label: string;
  used: number;
  peak: number;
  usedKey: string;
}

function getRpcBuffers(gauges: Record<string, { value: number | string | unknown[] }>): RpcBufferInfo[] {
  const g = (k: string) => gval(gauges, k);
  const channels = [
    { label: 'User RPC', prefix: 'netty.rpc.bit.user' },
    { label: 'Control RPC', prefix: 'netty.rpc.bit.control' },
    { label: 'Data RPC', prefix: 'netty.rpc.bit.data' },
  ];
  return channels
    .map((ch) => ({
      label: ch.label,
      used: g(`${ch.prefix}.used`),
      peak: g(`${ch.prefix}.peak`),
      usedKey: `${ch.prefix}.used`,
    }))
    .filter((b) => b.used > 0 || b.peak > 0);
}

function MemorySection({ gauges, prevGauges }: {
  gauges: Record<string, { value: number | string | unknown[] }>;
  prevGauges: Record<string, { value: number | string | unknown[] }> | undefined;
}) {
  const bars = getMemoryBars(gauges);
  const rpcBuffers = getRpcBuffers(gauges);
  const g = (k: string) => gval(gauges, k);
  const pg = (k: string) => prevGauges ? gval(prevGauges, k) : undefined;

  const directCapacity = g('jvm.direct.capacity');
  const directCount = g('jvm.direct.count');

  return (
    <Card
      title={<span><HddOutlined style={{ marginRight: 8 }} />Memory</span>}
      size="small"
    >
      <Row gutter={[16, 16]}>
        {bars.map((bar) => (
          <Col xs={24} sm={12} lg={6} key={bar.label}>
            <div style={{ marginBottom: 4, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <Text strong style={{ fontSize: 13 }}>{bar.label}</Text>
              <TrendIndicator
                trend={getTrend(bar.used, pg(memBarGaugeKey(bar.label)))}
                invertColors
              />
            </div>
            <Progress
              percent={bar.percent}
              strokeColor={thresholdColor(bar.percent)}
              status={thresholdStatus(bar.percent)}
              format={() => `${bar.percent}%`}
              size="small"
            />
            <Text type="secondary" style={{ fontSize: 11 }}>
              {fmtBytes(bar.used)} / {fmtBytes(bar.max)}
            </Text>
          </Col>
        ))}
      </Row>

      {/* RPC Buffer Memory */}
      {rpcBuffers.length > 0 && (
        <>
          <div style={{ marginTop: 16, marginBottom: 8 }}>
            <Text strong style={{ fontSize: 13 }}><ApiOutlined style={{ marginRight: 4 }} />RPC Buffer Memory</Text>
          </div>
          <Row gutter={[16, 12]}>
            {rpcBuffers.map((buf) => {
              const percent = buf.peak > 0 ? round((buf.used / buf.peak) * 100, 1) : 0;
              return (
                <Col xs={24} sm={8} key={buf.label}>
                  <div style={{ marginBottom: 4, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <Text style={{ fontSize: 12 }}>{buf.label}</Text>
                    <TrendIndicator trend={getTrend(buf.used, pg(buf.usedKey))} invertColors />
                  </div>
                  <Progress
                    percent={percent}
                    strokeColor={thresholdColor(percent)}
                    status={thresholdStatus(percent)}
                    format={() => `${percent}%`}
                    size="small"
                  />
                  <Text type="secondary" style={{ fontSize: 11 }}>
                    {fmtBytes(buf.used)} / {fmtBytes(buf.peak)} peak
                  </Text>
                </Col>
              );
            })}
          </Row>
        </>
      )}

      <div style={{ marginTop: 12, display: 'flex', gap: 24, fontSize: 12, color: 'var(--color-text-secondary)' }}>
        <span>Direct Buffers: <Text strong>{directCount}</Text></span>
        <span>Direct Capacity: <Text strong>{fmtBytes(directCapacity)}</Text></span>
      </div>
    </Card>
  );
}

// ── Queries Section ──

function QueriesSection({ counters, prevCounters }: {
  counters: Record<string, { count: number }>;
  prevCounters: Record<string, { count: number }> | undefined;
}) {
  const stats = useMemo(() => {
    const c = (k: string) => counters[`drill.queries.${k}`]?.count || 0;
    const pc = (k: string) => prevCounters?.[`drill.queries.${k}`]?.count;
    return [
      { key: 'succeeded', label: 'Succeeded', count: c('succeeded'), trend: getTrend(c('succeeded'), pc('succeeded')), color: '#52c41a', icon: <CheckCircleOutlined /> },
      { key: 'failed', label: 'Failed', count: c('failed'), trend: getTrend(c('failed'), pc('failed')), color: '#ff4d4f', icon: <CloseCircleOutlined /> },
      { key: 'running', label: 'Running', count: c('running'), trend: getTrend(c('running'), pc('running')), color: '#3b82f6', icon: <ThunderboltOutlined /> },
      { key: 'canceled', label: 'Canceled', count: c('canceled'), trend: getTrend(c('canceled'), pc('canceled')), color: '#faad14', icon: <WarningOutlined /> },
      { key: 'completed', label: 'Completed', count: c('completed'), trend: getTrend(c('completed'), pc('completed')), color: '#13c2c2', icon: <CheckCircleOutlined /> },
      { key: 'enqueued', label: 'Enqueued', count: c('enqueued'), trend: getTrend(c('enqueued'), pc('enqueued')), color: '#722ed1', icon: <ClockCircleOutlined /> },
    ];
  }, [counters, prevCounters]);

  const pieData = stats.filter((s) => s.count > 0);

  const pieOption: EChartsOption = {
    tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
    series: [{
      type: 'pie',
      radius: ['45%', '75%'],
      data: pieData.map((s) => ({
        name: s.label,
        value: s.count,
        itemStyle: { color: s.color },
      })),
      label: { formatter: '{b}: {c}', fontSize: 11 },
      emphasis: { itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0,0,0,0.2)' } },
    }],
  };

  const connEncrypted = (counters['drill.connections.rpc.user.encrypted']?.count || 0)
    + (counters['drill.connections.rpc.control.encrypted']?.count || 0)
    + (counters['drill.connections.rpc.data.encrypted']?.count || 0);
  const connUnencrypted = (counters['drill.connections.rpc.user.unencrypted']?.count || 0)
    + (counters['drill.connections.rpc.control.unencrypted']?.count || 0)
    + (counters['drill.connections.rpc.data.unencrypted']?.count || 0);

  return (
    <Card
      title={<span><DashboardOutlined style={{ marginRight: 8 }} />Queries & Connections</span>}
      size="small"
    >
      <Row gutter={[16, 16]}>
        <Col xs={24} lg={14}>
          <Row gutter={[12, 12]}>
            {stats.map((s) => (
              <Col xs={8} sm={4} key={s.key}>
                <div style={{ textAlign: 'center' }}>
                  <div style={{ fontSize: 11, color: 'var(--color-text-secondary)', marginBottom: 4 }}>
                    {s.label}
                  </div>
                  <div style={{ fontSize: 24, fontWeight: 700, color: s.color }}>
                    {s.count}
                  </div>
                  <TrendIndicator trend={s.trend} invertColors={s.key === 'failed'} />
                </div>
              </Col>
            ))}
          </Row>
          <div style={{ marginTop: 12, display: 'flex', gap: 24, fontSize: 12, color: 'var(--color-text-secondary)' }}>
            <span>Connections Encrypted: <Text strong>{connEncrypted}</Text></span>
            <span>Connections Unencrypted: <Text strong>{connUnencrypted}</Text></span>
          </div>
        </Col>
        <Col xs={24} lg={10}>
          {pieData.length > 0 ? (
            <ReactECharts option={pieOption} style={{ height: 200 }} />
          ) : (
            <div style={{ textAlign: 'center', padding: 40 }}>
              <Text type="secondary">No query activity yet</Text>
            </div>
          )}
        </Col>
      </Row>
    </Card>
  );
}

// ── Threads Section ──

function ThreadsSection({ gauges, prevGauges }: {
  gauges: Record<string, { value: number | string | unknown[] }>;
  prevGauges: Record<string, { value: number | string | unknown[] }> | undefined;
}) {
  const g = (k: string) => gval(gauges, k);
  const pg = (k: string) => prevGauges ? gval(prevGauges, k) : undefined;

  // Try both thread key prefixes (some Drill versions use "threads.", others "cached-threads.")
  const prefix = g('threads.count') > 0 ? 'threads' : 'cached-threads';
  const count = g(`${prefix}.count`);
  const runnable = g(`${prefix}.runnable.count`);
  const waiting = g(`${prefix}.waiting.count`);
  const timedWaiting = g(`${prefix}.timed_waiting.count`);
  const blocked = g(`${prefix}.blocked.count`);
  const deadlocks = g(`${prefix}.deadlock.count`);
  const peak = g(`${prefix}.peak.count`);
  const daemon = g(`${prefix}.daemon.count`);

  const threadStates = [
    { name: 'Runnable', value: runnable, color: '#52c41a' },
    { name: 'Waiting', value: waiting, color: '#3b82f6' },
    { name: 'Timed Waiting', value: timedWaiting, color: '#fa8c16' },
    { name: 'Blocked', value: blocked, color: '#ff4d4f' },
  ].filter((s) => s.value > 0);

  const chartOption: EChartsOption = {
    tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
    series: [{
      type: 'pie',
      radius: ['45%', '75%'],
      data: threadStates.map((s) => ({
        name: s.name,
        value: s.value,
        itemStyle: { color: s.color },
      })),
      label: { formatter: '{b}: {c}', fontSize: 11 },
    }],
  };

  return (
    <Card
      title={<span><ThunderboltOutlined style={{ marginRight: 8 }} />Threads</span>}
      size="small"
    >
      <Row gutter={[16, 16]}>
        <Col xs={24} lg={14}>
          <Row gutter={[12, 12]}>
            <Col xs={8}>
              <Statistic
                title="Total"
                value={count}
                suffix={<TrendIndicator trend={getTrend(count, pg(`${prefix}.count`))} invertColors />}
                valueStyle={{ fontSize: 20 }}
              />
            </Col>
            <Col xs={8}>
              <Statistic title="Peak" value={peak} valueStyle={{ fontSize: 20 }} />
            </Col>
            <Col xs={8}>
              <Statistic title="Daemon" value={daemon} valueStyle={{ fontSize: 20 }} />
            </Col>
          </Row>
          <div style={{ marginTop: 16 }}>
            <Row gutter={[8, 8]}>
              {threadStates.map((s) => (
                <Col key={s.name}>
                  <Tag color={s.color === '#52c41a' ? 'green' : s.color === '#3b82f6' ? 'blue' : s.color === '#fa8c16' ? 'orange' : 'red'}>
                    {s.name}: {s.value}
                  </Tag>
                </Col>
              ))}
            </Row>
          </div>
          {deadlocks > 0 && (
            <div style={{ marginTop: 12, padding: '8px 12px', background: '#fff1f0', borderRadius: 4 }}>
              <Text type="danger" strong>
                <WarningOutlined style={{ marginRight: 4 }} />
                {deadlocks} deadlock{deadlocks > 1 ? 's' : ''} detected!
              </Text>
            </div>
          )}
        </Col>
        <Col xs={24} lg={10}>
          <ReactECharts option={chartOption} style={{ height: 200 }} />
        </Col>
      </Row>
    </Card>
  );
}

// ── GC Section ──

function GCSection({ gauges, prevGauges }: {
  gauges: Record<string, { value: number | string | unknown[] }>;
  prevGauges: Record<string, { value: number | string | unknown[] }> | undefined;
}) {
  const g = (k: string) => gval(gauges, k);
  const pg = (k: string) => prevGauges ? gval(prevGauges, k) : undefined;

  const youngCount = g('G1-Young-Generation.count');
  const youngTime = g('G1-Young-Generation.time');
  const oldCount = g('G1-Old-Generation.count');
  const oldTime = g('G1-Old-Generation.time');
  const avgYoung = youngCount > 0 ? round(youngTime / youngCount, 1) : 0;
  const avgOld = oldCount > 0 ? round(oldTime / oldCount, 1) : 0;

  return (
    <Card
      title={<span><ClockCircleOutlined style={{ marginRight: 8 }} />Garbage Collection</span>}
      size="small"
    >
      <Row gutter={[24, 16]}>
        <Col xs={24} sm={12}>
          <Text strong style={{ display: 'block', marginBottom: 8 }}>Young Generation (G1)</Text>
          <Row gutter={[16, 8]}>
            <Col span={8}>
              <Statistic
                title="Collections"
                value={youngCount}
                suffix={<TrendIndicator trend={getTrend(youngCount, pg('G1-Young-Generation.count'))} invertColors />}
                valueStyle={{ fontSize: 18 }}
              />
            </Col>
            <Col span={8}>
              <Statistic title="Total Time" value={`${youngTime}ms`} valueStyle={{ fontSize: 18 }} />
            </Col>
            <Col span={8}>
              <Statistic title="Avg Time" value={`${avgYoung}ms`} valueStyle={{ fontSize: 18 }} />
            </Col>
          </Row>
        </Col>
        <Col xs={24} sm={12}>
          <Text strong style={{ display: 'block', marginBottom: 8 }}>Old Generation (G1)</Text>
          <Row gutter={[16, 8]}>
            <Col span={8}>
              <Statistic
                title="Collections"
                value={oldCount}
                suffix={<TrendIndicator trend={getTrend(oldCount, pg('G1-Old-Generation.count'))} invertColors />}
                valueStyle={{ fontSize: 18, color: oldCount > 0 ? '#faad14' : undefined }}
              />
            </Col>
            <Col span={8}>
              <Statistic title="Total Time" value={`${oldTime}ms`} valueStyle={{ fontSize: 18 }} />
            </Col>
            <Col span={8}>
              <Statistic title="Avg Time" value={`${avgOld}ms`} valueStyle={{ fontSize: 18 }} />
            </Col>
          </Row>
        </Col>
      </Row>
    </Card>
  );
}

// ── Allocations Section ──

function AllocationsSection({ histograms }: {
  histograms: Record<string, { count: number; max: number; mean: number; min: number; p50: number; p75: number; p95: number; p99: number; p999: number; stddev: number }>;
}) {
  const entries = Object.entries(histograms);
  if (entries.length === 0) return null;

  const chartOption: EChartsOption = {
    tooltip: { trigger: 'axis' },
    grid: { top: 10, right: 20, bottom: 30, left: 60, containLabel: true },
    xAxis: {
      type: 'category',
      data: entries.map(([name]) => name.replace('drill.allocator.', '').replace('.hist', '')),
      axisLabel: { fontSize: 11 },
    },
    yAxis: { type: 'log', name: 'bytes', axisLabel: { fontSize: 11 } },
    series: [
      { name: 'p50', type: 'bar', data: entries.map(([, h]) => h.p50 || null), itemStyle: { color: '#3b82f6' } },
      { name: 'p95', type: 'bar', data: entries.map(([, h]) => h.p95 || null), itemStyle: { color: '#fa8c16' } },
      { name: 'p99', type: 'bar', data: entries.map(([, h]) => h.p99 || null), itemStyle: { color: '#ff4d4f' } },
      { name: 'max', type: 'bar', data: entries.map(([, h]) => h.max || null), itemStyle: { color: '#722ed1' } },
    ],
    legend: { data: ['p50', 'p95', 'p99', 'max'], bottom: 0, textStyle: { fontSize: 11 } },
  };

  return (
    <Card
      title="Allocations"
      size="small"
    >
      <Row gutter={[16, 16]}>
        <Col xs={24} lg={12}>
          {entries.map(([name, h]) => (
            <div key={name} style={{ marginBottom: 12 }}>
              <Text code style={{ fontSize: 12 }}>{name}</Text>
              <div style={{ marginTop: 4, display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                <Tag>Count: {h.count.toLocaleString()}</Tag>
                <Tag>Mean: {fmtBytes(h.mean)}</Tag>
                <Tag>p50: {fmtBytes(h.p50)}</Tag>
                <Tag color="orange">p95: {fmtBytes(h.p95)}</Tag>
                <Tag color="red">p99: {fmtBytes(h.p99)}</Tag>
                <Tag color="purple">Max: {fmtBytes(h.max)}</Tag>
              </div>
            </div>
          ))}
        </Col>
        <Col xs={24} lg={12}>
          <ReactECharts option={chartOption} style={{ height: 220 }} />
        </Col>
      </Row>
    </Card>
  );
}

// ── Performance / Timers Section ──

function PerformanceSection({ timers }: {
  timers: Record<string, { count: number; max: number; mean: number; min: number; p50: number; p95: number; p99: number; p999: number; stddev: number; m1_rate: number; m5_rate: number; m15_rate: number; mean_rate: number; duration_units: string; rate_units: string }>;
}) {
  const entries = Object.entries(timers);
  if (entries.length === 0) return null;

  return (
    <Card title={<span><ClockCircleOutlined style={{ marginRight: 8 }} />Performance Timers</span>} size="small">
      {entries.map(([name, t]) => {
        const shortName = name.split('.').pop() || name;
        return (
          <div key={name} style={{ marginBottom: 16, padding: '8px 12px', background: 'var(--color-bg-elevated)', borderRadius: 4 }}>
            <Tooltip title={name}>
              <Text strong style={{ fontSize: 13 }}>{shortName}</Text>
            </Tooltip>
            <Text type="secondary" style={{ fontSize: 11, marginLeft: 8 }}>{name}</Text>
            <div style={{ marginTop: 6, display: 'flex', gap: 8, flexWrap: 'wrap' }}>
              <Tag>Calls: {t.count}</Tag>
              <Tag>Mean: {round(t.mean, 2)} {t.duration_units}</Tag>
              <Tag color="blue">p50: {round(t.p50, 2)}</Tag>
              <Tag color="orange">p95: {round(t.p95, 2)}</Tag>
              <Tag color="red">p99: {round(t.p99, 2)}</Tag>
              <Tag color="purple">Max: {round(t.max, 2)}</Tag>
            </div>
            <div style={{ marginTop: 4, fontSize: 11, color: 'var(--color-text-secondary)' }}>
              Rate: {round(t.mean_rate, 4)} {t.rate_units}
              <span style={{ marginLeft: 12 }}>1m: {round(t.m1_rate, 4)}</span>
              <span style={{ marginLeft: 8 }}>5m: {round(t.m5_rate, 4)}</span>
              <span style={{ marginLeft: 8 }}>15m: {round(t.m15_rate, 4)}</span>
            </div>
          </div>
        );
      })}
    </Card>
  );
}

// ── Meters Section ──

function MetersSection({ meters }: {
  meters: Record<string, { count: number; m1_rate: number; m5_rate: number; m15_rate: number; mean_rate: number; units: string }>;
}) {
  const entries = Object.entries(meters);
  if (entries.length === 0) {
    return null;
  }

  const chartOption: EChartsOption = {
    tooltip: { trigger: 'axis' },
    grid: { top: 30, right: 20, bottom: 30, left: 60, containLabel: true },
    xAxis: {
      type: 'category',
      data: entries.map(([name]) => {
        const parts = name.split('.');
        return parts.length > 2 ? parts.slice(-2).join('.') : name;
      }),
      axisLabel: { fontSize: 11, rotate: entries.length > 4 ? 30 : 0 },
    },
    yAxis: { type: 'value', name: 'rate', axisLabel: { fontSize: 11 } },
    legend: { data: ['1m rate', '5m rate', '15m rate'], top: 0, textStyle: { fontSize: 11 } },
    series: [
      { name: '1m rate', type: 'bar', data: entries.map(([, m]) => round(m.m1_rate, 4)), itemStyle: { color: '#3b82f6' } },
      { name: '5m rate', type: 'bar', data: entries.map(([, m]) => round(m.m5_rate, 4)), itemStyle: { color: '#fa8c16' } },
      { name: '15m rate', type: 'bar', data: entries.map(([, m]) => round(m.m15_rate, 4)), itemStyle: { color: '#722ed1' } },
    ],
  };

  return (
    <Card title={<span><DashboardOutlined style={{ marginRight: 8 }} />Meters</span>} size="small">
      <Row gutter={[16, 16]}>
        <Col xs={24} lg={12}>
          {entries.map(([name, m]) => {
            const shortName = name.split('.').pop() || name;
            return (
              <div key={name} style={{ marginBottom: 16, padding: '8px 12px', background: 'var(--color-bg-elevated)', borderRadius: 4 }}>
                <Tooltip title={name}>
                  <Text strong style={{ fontSize: 13 }}>{shortName}</Text>
                </Tooltip>
                <Text type="secondary" style={{ fontSize: 11, marginLeft: 8 }}>{name}</Text>
                <div style={{ marginTop: 6, display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                  <Tag>Count: {m.count.toLocaleString()}</Tag>
                  <Tag>Mean Rate: {round(m.mean_rate, 4)}</Tag>
                  <Tag color="blue">1m: {round(m.m1_rate, 4)}</Tag>
                  <Tag color="orange">5m: {round(m.m5_rate, 4)}</Tag>
                  <Tag color="purple">15m: {round(m.m15_rate, 4)}</Tag>
                </div>
                <div style={{ marginTop: 4, fontSize: 11, color: 'var(--color-text-secondary)' }}>
                  Units: {m.units}
                </div>
              </div>
            );
          })}
        </Col>
        <Col xs={24} lg={12}>
          <ReactECharts option={chartOption} style={{ height: Math.max(200, entries.length * 40) }} />
        </Col>
      </Row>
    </Card>
  );
}

// ── Unified Metrics Table ──

interface UnifiedRow {
  key: string;
  name: string;
  type: 'gauge' | 'counter' | 'histogram' | 'timer' | 'meter';
  domain: string;
  value: string;
  detail: string;
}

function categorizeDomain(name: string, type: string): string {
  if (name.startsWith('memory.') || name.startsWith('jvm.')) return 'Memory';
  if (name.startsWith('drill.allocator.')) return 'Allocator';
  if (name.startsWith('drill.queries.')) return 'Queries';
  if (name.startsWith('drill.connections.')) return 'Connections';
  if (name.startsWith('drill.fragments.')) return 'Fragments';
  if (name.startsWith('drillbit.') || name.startsWith('os.')) return 'Drillbit';
  if (name.includes('threads') || name.includes('cached-threads')) return 'Threads';
  if (name.startsWith('G1-') || name.includes('Generation')) return 'GC';
  if (name.startsWith('netty.rpc.')) return 'RPC';
  if (name.startsWith('class.') || name.startsWith('fd.')) return 'System';
  if (type === 'histogram') return 'Allocations';
  if (type === 'timer') return 'Performance';
  if (type === 'meter') return 'Meters';
  return 'Other';
}

function buildUnifiedRows(metrics: MetricsResponse): UnifiedRow[] {
  const rows: UnifiedRow[] = [];

  Object.entries(metrics.gauges).forEach(([name, g]) => {
    const val = typeof g.value === 'number' ? round(g.value, 6) : g.value;
    rows.push({
      key: `gauge:${name}`,
      name,
      type: 'gauge',
      domain: categorizeDomain(name, 'gauge'),
      value: String(val),
      detail: '',
    });
  });

  Object.entries(metrics.counters).forEach(([name, c]) => {
    rows.push({
      key: `counter:${name}`,
      name,
      type: 'counter',
      domain: categorizeDomain(name, 'counter'),
      value: c.count.toLocaleString(),
      detail: '',
    });
  });

  Object.entries(metrics.histograms).forEach(([name, h]) => {
    rows.push({
      key: `histogram:${name}`,
      name,
      type: 'histogram',
      domain: categorizeDomain(name, 'histogram'),
      value: `count: ${h.count.toLocaleString()}`,
      detail: `mean: ${round(h.mean, 2)}, p50: ${round(h.p50, 2)}, p95: ${round(h.p95, 2)}, p99: ${round(h.p99, 2)}, max: ${round(h.max, 2)}`,
    });
  });

  Object.entries(metrics.timers).forEach(([name, t]) => {
    rows.push({
      key: `timer:${name}`,
      name,
      type: 'timer',
      domain: categorizeDomain(name, 'timer'),
      value: `${t.count} calls`,
      detail: `mean: ${round(t.mean, 2)} ${t.duration_units}, p95: ${round(t.p95, 2)}, rate: ${round(t.mean_rate, 4)} ${t.rate_units}`,
    });
  });

  Object.entries(metrics.meters).forEach(([name, m]) => {
    rows.push({
      key: `meter:${name}`,
      name,
      type: 'meter',
      domain: categorizeDomain(name, 'meter'),
      value: `${m.count.toLocaleString()} events`,
      detail: `mean: ${round(m.mean_rate, 4)} ${m.units}, 1m: ${round(m.m1_rate, 4)}, 5m: ${round(m.m5_rate, 4)}, 15m: ${round(m.m15_rate, 4)}`,
    });
  });

  return rows;
}

function UnifiedTable({ metrics, searchText }: { metrics: MetricsResponse; searchText: string }) {
  const data = useMemo(() => {
    const rows = buildUnifiedRows(metrics);
    if (!searchText) return rows;
    const lower = searchText.toLowerCase();
    return rows.filter((r) => r.name.toLowerCase().includes(lower));
  }, [metrics, searchText]);

  const domains = useMemo(() => Array.from(new Set(data.map((d) => d.domain))).sort(), [data]);

  const typeColors: Record<string, string> = { gauge: 'blue', counter: 'green', histogram: 'orange', timer: 'purple', meter: 'cyan' };
  const domainColors: Record<string, string> = {
    Memory: 'blue', Allocator: 'orange', Queries: 'green', Connections: 'cyan',
    Threads: 'purple', GC: 'red', Fragments: 'magenta', Drillbit: 'lime',
    System: 'default', Allocations: 'orange', Performance: 'purple',
    RPC: 'geekblue', Meters: 'cyan',
  };

  const columns: ColumnsType<UnifiedRow> = [
    {
      title: 'Metric',
      dataIndex: 'name',
      key: 'name',
      sorter: (a, b) => a.name.localeCompare(b.name),
      render: (name: string) => <Text code style={{ fontSize: 12 }}>{name}</Text>,
    },
    {
      title: 'Type',
      dataIndex: 'type',
      key: 'type',
      width: 100,
      filters: ['gauge', 'counter', 'histogram', 'timer', 'meter'].map((t) => ({ text: t, value: t })),
      onFilter: (value, record) => record.type === value,
      render: (type: string) => <Tag color={typeColors[type]}>{type}</Tag>,
    },
    {
      title: 'Domain',
      dataIndex: 'domain',
      key: 'domain',
      width: 130,
      filters: domains.map((d) => ({ text: d, value: d })),
      onFilter: (value, record) => record.domain === value,
      render: (domain: string) => <Tag color={domainColors[domain] || 'default'}>{domain}</Tag>,
    },
    {
      title: 'Value',
      dataIndex: 'value',
      key: 'value',
      width: 180,
      sorter: (a, b) => {
        const av = parseFloat(a.value.replace(/[^0-9.-]/g, ''));
        const bv = parseFloat(b.value.replace(/[^0-9.-]/g, ''));
        if (!isNaN(av) && !isNaN(bv)) return av - bv;
        return a.value.localeCompare(b.value);
      },
      render: (val: string) => <Text strong>{val}</Text>,
    },
    {
      title: 'Details',
      dataIndex: 'detail',
      key: 'detail',
      render: (detail: string) => detail ? <Text type="secondary" style={{ fontSize: 11 }}>{detail}</Text> : null,
    },
  ];

  return (
    <Table
      dataSource={data}
      columns={columns}
      pagination={{ pageSize: 25, showSizeChanger: true, showTotal: (total) => `${total} metrics` }}
      size="small"
      scroll={{ x: 800 }}
    />
  );
}

// ── Main Page ──

export default function MetricsPage() {
  const [searchText, setSearchText] = useState('');
  const prevMetricsRef = useRef<MetricsResponse | undefined>(undefined);

  const { data: metrics, isLoading, error } = useQuery({
    queryKey: ['metrics'],
    queryFn: getMetrics,
    refetchInterval: 3000,
  });

  // Track previous metrics for trend indicators
  const prevMetrics = prevMetricsRef.current;
  if (metrics && metrics !== prevMetricsRef.current) {
    prevMetricsRef.current = metrics;
  }

  if (isLoading) {
    return (
      <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
        <Spin size="large" />
      </div>
    );
  }

  if (error || !metrics) {
    return (
      <div style={{ padding: 24 }}>
        <Card>
          <Empty description={<Text type="danger">Failed to load metrics: {(error as Error)?.message || 'Unknown error'}</Text>} />
        </Card>
      </div>
    );
  }

  const g = (k: string) => gval(metrics.gauges, k);

  const uptimeMs = g('drillbit.uptime');
  const uptimeStr = uptimeMs > 0
    ? `${Math.floor(uptimeMs / 3600000)}h ${Math.floor((uptimeMs % 3600000) / 60000)}m`
    : '-';

  const totalMetrics = Object.keys(metrics.gauges).length
    + Object.keys(metrics.counters).length
    + Object.keys(metrics.histograms).length
    + Object.keys(metrics.meters).length
    + Object.keys(metrics.timers).length;

  const fdUsage = g('fd.usage');
  const fdPercent = round(fdUsage * 100, 1);
  const osLoadAvg = g('os.load.avg');
  const processLoadAvg = g('drillbit.load.avg');

  return (
    <div style={{ padding: 24, overflow: 'auto', flex: 1 }}>
      <Space direction="vertical" style={{ width: '100%' }} size="large">
        {/* Header */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Title level={3} style={{ margin: 0 }}>
            <DashboardOutlined style={{ marginRight: 8 }} />
            System Metrics
          </Title>
          <Space size="middle">
            <Tag>{totalMetrics} metrics</Tag>
            <Text type="secondary" style={{ fontSize: 12 }}>Auto-refreshing every 3s</Text>
          </Space>
        </div>

        {/* Top-level stats */}
        <Row gutter={[16, 16]}>
          <Col xs={12} sm={6} lg={4}>
            <Card size="small">
              <Statistic title="Uptime" value={uptimeStr} prefix={<ClockCircleOutlined />} valueStyle={{ fontSize: 20 }} />
            </Card>
          </Col>
          <Col xs={12} sm={6} lg={4}>
            <Card size="small">
              <Statistic title="Running Fragments" value={g('drill.fragments.running')} prefix={<ThunderboltOutlined />} valueStyle={{ fontSize: 20 }} />
            </Card>
          </Col>
          <Col xs={12} sm={6} lg={4}>
            <Card size="small">
              <Statistic title="System Load" value={round(osLoadAvg, 2)} prefix={<HddOutlined />} valueStyle={{ fontSize: 20 }} />
            </Card>
          </Col>
          <Col xs={12} sm={6} lg={4}>
            <Card size="small">
              <Statistic title="Process CPU" value={round(processLoadAvg, 4)} prefix={<ThunderboltOutlined />} valueStyle={{ fontSize: 20 }} />
            </Card>
          </Col>
          <Col xs={12} sm={6} lg={4}>
            <Card size="small">
              <Tooltip title={`${fdPercent}% of file descriptors in use`}>
                <div>
                  <div style={{ fontSize: 12, color: 'var(--color-text-secondary)', marginBottom: 4 }}>
                    <FileOutlined style={{ marginRight: 4 }} />File Descriptors
                  </div>
                  <Progress
                    percent={fdPercent}
                    strokeColor={thresholdColor(fdPercent)}
                    status={thresholdStatus(fdPercent)}
                    format={() => `${fdPercent}%`}
                    size="small"
                  />
                </div>
              </Tooltip>
            </Card>
          </Col>
          <Col xs={12} sm={6} lg={4}>
            <Card size="small">
              <Statistic title="Classes Loaded" value={g('class.loaded')} valueStyle={{ fontSize: 20 }} />
            </Card>
          </Col>
        </Row>

        {/* Domain sections */}
        <MemorySection gauges={metrics.gauges} prevGauges={prevMetrics?.gauges} />

        <QueriesSection counters={metrics.counters} prevCounters={prevMetrics?.counters} />

        <Row gutter={[16, 16]}>
          <Col xs={24} lg={12}>
            <ThreadsSection gauges={metrics.gauges} prevGauges={prevMetrics?.gauges} />
          </Col>
          <Col xs={24} lg={12}>
            <GCSection gauges={metrics.gauges} prevGauges={prevMetrics?.gauges} />
          </Col>
        </Row>

        {Object.keys(metrics.histograms).length > 0 && (
          <AllocationsSection histograms={metrics.histograms} />
        )}

        {Object.keys(metrics.timers).length > 0 && (
          <PerformanceSection timers={metrics.timers} />
        )}

        {Object.keys(metrics.meters).length > 0 && (
          <MetersSection meters={metrics.meters} />
        )}

        {/* Unified raw metrics table */}
        <Collapse
          items={[{
            key: 'all-metrics',
            label: <span>All Metrics ({totalMetrics})</span>,
            children: (
              <>
                <div style={{ marginBottom: 16 }}>
                  <Input
                    placeholder="Filter metrics by name..."
                    prefix={<SearchOutlined />}
                    value={searchText}
                    onChange={(e) => setSearchText(e.target.value)}
                    allowClear
                    style={{ maxWidth: 400 }}
                  />
                </div>
                <UnifiedTable metrics={metrics} searchText={searchText} />
              </>
            ),
          }]}
        />
      </Space>
    </div>
  );
}
