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
import { useState } from 'react';
import { Card, Tag, Row, Col, Descriptions, Input, Empty } from 'antd';
import ReactECharts from 'echarts-for-react';
import type { ColumnProfileData } from '../../types/profile';

const TYPE_COLORS: Record<string, string> = {
  numeric: '#1890ff',
  string: '#52c41a',
  temporal: '#722ed1',
  boolean: '#fa8c16',
  other: '#8c8c8c',
};

function fmt(v: number | undefined): string {
  if (v == null || !isFinite(v)) { return '-'; }
  if (Number.isInteger(v)) { return v.toLocaleString(); }
  return v.toLocaleString(undefined, { maximumFractionDigits: 4 });
}

function HistogramChart({ data }: { data: { bin: string; count: number }[] }) {
  const option = {
    grid: { top: 8, right: 8, bottom: 24, left: 40 },
    xAxis: {
      type: 'category' as const,
      data: data.map((d) => d.bin),
      axisLabel: { fontSize: 9, rotate: 45 },
    },
    yAxis: { type: 'value' as const, axisLabel: { fontSize: 9 } },
    series: [{
      type: 'bar' as const,
      data: data.map((d) => d.count),
      itemStyle: { color: '#1890ff' },
    }],
    tooltip: { trigger: 'axis' as const },
  };
  return <ReactECharts option={option} style={{ height: 160 }} />;
}

function FrequencyChart({ data }: { data: { value: string; count: number; pct: number }[] }) {
  const option = {
    grid: { top: 8, right: 8, bottom: 8, left: 100 },
    xAxis: { type: 'value' as const, axisLabel: { fontSize: 9 } },
    yAxis: {
      type: 'category' as const,
      data: data.map((d) => d.value.length > 15 ? d.value.slice(0, 15) + '...' : d.value).reverse(),
      axisLabel: { fontSize: 9 },
    },
    series: [{
      type: 'bar' as const,
      data: data.map((d) => d.count).reverse(),
      itemStyle: { color: '#52c41a' },
    }],
    tooltip: {
      trigger: 'axis' as const,
      formatter: (params: { name: string; value: number }[]) => {
        const p = params[0];
        return `${p.name}: ${p.value}`;
      },
    },
  };
  return <ReactECharts option={option} style={{ height: 160 }} />;
}

function ColumnCard({ col }: { col: ColumnProfileData }) {
  const uniquePct = col.count > 0 ? ((col.distinct / col.count) * 100).toFixed(1) : '0';

  return (
    <Card
      size="small"
      title={
        <span>
          <strong>{col.name}</strong>{' '}
          <Tag color={TYPE_COLORS[col.dataType]} style={{ marginLeft: 4 }}>{col.type}</Tag>
        </span>
      }
      style={{ height: '100%' }}
    >
      <Row gutter={12}>
        <Col span={12}>
          <Descriptions size="small" column={1} labelStyle={{ fontSize: 12 }} contentStyle={{ fontSize: 12 }}>
            <Descriptions.Item label="Count">{fmt(col.count)}</Descriptions.Item>
            <Descriptions.Item label="Distinct">{fmt(col.distinct)} ({uniquePct}%)</Descriptions.Item>
            <Descriptions.Item label="Missing">{fmt(col.missing)} ({col.missingPct.toFixed(1)}%)</Descriptions.Item>
            {col.dataType === 'numeric' && (
              <>
                <Descriptions.Item label="Mean">{fmt(col.mean)}</Descriptions.Item>
                <Descriptions.Item label="Std Dev">{fmt(col.stddev)}</Descriptions.Item>
                <Descriptions.Item label="Min">{fmt(col.min)}</Descriptions.Item>
                <Descriptions.Item label="Median">{fmt(col.median)}</Descriptions.Item>
                <Descriptions.Item label="Max">{fmt(col.max)}</Descriptions.Item>
                <Descriptions.Item label="P25 / P75">{fmt(col.p25)} / {fmt(col.p75)}</Descriptions.Item>
              </>
            )}
            {col.dataType === 'string' && (
              <>
                <Descriptions.Item label="Min Length">{fmt(col.minLength)}</Descriptions.Item>
                <Descriptions.Item label="Max Length">{fmt(col.maxLength)}</Descriptions.Item>
                <Descriptions.Item label="Avg Length">{fmt(col.avgLength)}</Descriptions.Item>
              </>
            )}
            {col.dataType === 'temporal' && (
              <>
                <Descriptions.Item label="Min">{String(col.min ?? '-')}</Descriptions.Item>
                <Descriptions.Item label="Max">{String(col.max ?? '-')}</Descriptions.Item>
              </>
            )}
          </Descriptions>
        </Col>
        <Col span={12}>
          {col.histogram && col.histogram.length > 0 && (
            <HistogramChart data={col.histogram} />
          )}
          {col.topValues && col.topValues.length > 0 && !col.histogram && (
            <FrequencyChart data={col.topValues} />
          )}
          {!col.histogram && !col.topValues && (
            <div style={{ height: 160, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#8c8c8c' }}>
              No distribution data
            </div>
          )}
        </Col>
      </Row>
    </Card>
  );
}

interface Props {
  columns: ColumnProfileData[];
}

export default function ColumnProfile({ columns }: Props) {
  const [search, setSearch] = useState('');

  const filtered = search
    ? columns.filter((c) => c.name.toLowerCase().includes(search.toLowerCase()))
    : columns;

  if (columns.length === 0) {
    return <Empty description="No column data" />;
  }

  return (
    <div>
      <Input.Search
        placeholder="Filter columns..."
        onChange={(e) => setSearch(e.target.value)}
        style={{ marginBottom: 16, maxWidth: 300 }}
        allowClear
      />
      <Row gutter={[12, 12]}>
        {filtered.map((col) => (
          <Col key={col.name} xs={24} lg={12} xxl={8}>
            <ColumnCard col={col} />
          </Col>
        ))}
      </Row>
    </div>
  );
}
