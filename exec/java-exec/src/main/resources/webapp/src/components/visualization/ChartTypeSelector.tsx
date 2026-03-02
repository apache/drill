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
import React from 'react';
import { Card, Typography, Row, Col, Tooltip } from 'antd';
import {
  AreaChartOutlined,
  BarChartOutlined,
  LineChartOutlined,
  PieChartOutlined,
  DotChartOutlined,
  TableOutlined,
  HeatMapOutlined,
  FundOutlined,
  DashboardOutlined,
  FilterOutlined,
  GlobalOutlined,
  FieldNumberOutlined,
  BranchesOutlined,
  RadarChartOutlined,
  ExperimentOutlined,
  RiseOutlined,
  SunOutlined,
  StockOutlined,
  CalendarOutlined,
  ClusterOutlined,
  ApartmentOutlined,
} from '@ant-design/icons';
import type { ChartType } from '../../types';

const { Text } = Typography;

interface ChartTypeOption {
  type: ChartType;
  name: string;
  description: string;
  icon: React.ReactNode;
  category: 'basic' | 'advanced';
}

const chartTypes: ChartTypeOption[] = [
  {
    type: 'bar',
    name: 'Bar Chart',
    description: 'Compare values across categories',
    icon: <BarChartOutlined style={{ fontSize: 24 }} />,
    category: 'basic',
  },
  {
    type: 'line',
    name: 'Line Chart',
    description: 'Show trends over time',
    icon: <LineChartOutlined style={{ fontSize: 24 }} />,
    category: 'basic',
  },
  {
    type: 'area',
    name: 'Area Chart',
    description: 'Show trends with filled regions',
    icon: <AreaChartOutlined style={{ fontSize: 24 }} />,
    category: 'basic',
  },
  {
    type: 'pie',
    name: 'Pie Chart',
    description: 'Show proportions of a whole',
    icon: <PieChartOutlined style={{ fontSize: 24 }} />,
    category: 'basic',
  },
  {
    type: 'scatter',
    name: 'Scatter Plot',
    description: 'Show relationships between variables',
    icon: <DotChartOutlined style={{ fontSize: 24 }} />,
    category: 'basic',
  },
  {
    type: 'table',
    name: 'Data Table',
    description: 'Display data in tabular format',
    icon: <TableOutlined style={{ fontSize: 24 }} />,
    category: 'basic',
  },
  {
    type: 'bigNumber',
    name: 'Big Number',
    description: 'Display a single key metric with optional trend',
    icon: <FieldNumberOutlined style={{ fontSize: 24 }} />,
    category: 'basic',
  },
  {
    type: 'heatmap',
    name: 'Heatmap',
    description: 'Show data density with colors',
    icon: <HeatMapOutlined style={{ fontSize: 24 }} />,
    category: 'advanced',
  },
  {
    type: 'treemap',
    name: 'Treemap',
    description: 'Hierarchical data visualization',
    icon: <FundOutlined style={{ fontSize: 24 }} />,
    category: 'advanced',
  },
  {
    type: 'gauge',
    name: 'Gauge',
    description: 'Show KPI or progress',
    icon: <DashboardOutlined style={{ fontSize: 24 }} />,
    category: 'advanced',
  },
  {
    type: 'funnel',
    name: 'Funnel',
    description: 'Show conversion stages',
    icon: <FilterOutlined style={{ fontSize: 24 }} />,
    category: 'advanced',
  },
  {
    type: 'map',
    name: 'Geographic Map',
    description: 'Visualize location data',
    icon: <GlobalOutlined style={{ fontSize: 24 }} />,
    category: 'advanced',
  },
  {
    type: 'sankey',
    name: 'Sankey',
    description: 'Show flow and quantity between nodes',
    icon: <BranchesOutlined style={{ fontSize: 24 }} />,
    category: 'advanced',
  },
  {
    type: 'radar',
    name: 'Radar',
    description: 'Compare multiple metrics across categories',
    icon: <RadarChartOutlined style={{ fontSize: 24 }} />,
    category: 'advanced',
  },
  {
    type: 'boxplot',
    name: 'Box Plot',
    description: 'Show distribution, quartiles, and outliers',
    icon: <ExperimentOutlined style={{ fontSize: 24 }} />,
    category: 'advanced',
  },
  {
    type: 'waterfall',
    name: 'Waterfall',
    description: 'Show incremental changes and running totals',
    icon: <RiseOutlined style={{ fontSize: 24 }} />,
    category: 'advanced',
  },
  {
    type: 'sunburst',
    name: 'Sunburst',
    description: 'Hierarchical data as concentric rings',
    icon: <SunOutlined style={{ fontSize: 24 }} />,
    category: 'advanced',
  },
  {
    type: 'candlestick',
    name: 'Candlestick',
    description: 'OHLC chart for financial or time-series data',
    icon: <StockOutlined style={{ fontSize: 24 }} />,
    category: 'advanced',
  },
  {
    type: 'calendar',
    name: 'Calendar',
    description: 'Daily values on a calendar grid (like GitHub activity)',
    icon: <CalendarOutlined style={{ fontSize: 24 }} />,
    category: 'advanced',
  },
  {
    type: 'bubble',
    name: 'Bubble',
    description: 'Scatter plot with a third dimension as bubble size',
    icon: <ClusterOutlined style={{ fontSize: 24 }} />,
    category: 'advanced',
  },
  {
    type: 'parallel',
    name: 'Parallel',
    description: 'Compare rows across many numeric dimensions',
    icon: <ApartmentOutlined style={{ fontSize: 24 }} />,
    category: 'advanced',
  },
];

interface ChartTypeSelectorProps {
  value?: ChartType;
  onChange?: (type: ChartType) => void;
  compact?: boolean;
}

export default function ChartTypeSelector({ value, onChange, compact }: ChartTypeSelectorProps) {
  const basicCharts = chartTypes.filter((c) => c.category === 'basic');
  const advancedCharts = chartTypes.filter((c) => c.category === 'advanced');

  const renderChartCard = (chart: ChartTypeOption) => (
    <Col key={chart.type} xs={compact ? 8 : 12} sm={compact ? 8 : 8} md={compact ? 8 : 6} lg={compact ? 8 : 4}>
      <Tooltip title={chart.description}>
        <Card
          hoverable
          size="small"
          onClick={() => onChange?.(chart.type)}
          style={{
            textAlign: 'center',
            border: value === chart.type ? '2px solid #3b82f6' : '1px solid var(--color-border)',
            backgroundColor: value === chart.type ? 'var(--color-bg-hover)' : undefined,
            padding: compact ? 0 : undefined,
          }}
          styles={compact ? { body: { padding: '6px 4px' } } : undefined}
        >
          <div style={{ color: value === chart.type ? '#3b82f6' : 'var(--color-text-secondary)', fontSize: compact ? 18 : 24 }}>
            {compact
              ? React.cloneElement(chart.icon as React.ReactElement, { style: { fontSize: 18 } })
              : chart.icon}
          </div>
          <Text
            strong={value === chart.type}
            style={{ fontSize: compact ? 10 : 12, display: 'block', marginTop: compact ? 2 : 4 }}
          >
            {chart.name}
          </Text>
        </Card>
      </Tooltip>
    </Col>
  );

  if (compact) {
    return (
      <Row gutter={[4, 4]}>
        {[...basicCharts, ...advancedCharts].map(renderChartCard)}
      </Row>
    );
  }

  return (
    <div>
      <Text type="secondary" style={{ display: 'block', marginBottom: 8 }}>
        Standard Charts
      </Text>
      <Row gutter={[8, 8]} style={{ marginBottom: 16 }}>
        {basicCharts.map(renderChartCard)}
      </Row>

      <Text type="secondary" style={{ display: 'block', marginBottom: 8 }}>
        Advanced Charts
      </Text>
      <Row gutter={[8, 8]}>
        {advancedCharts.map(renderChartCard)}
      </Row>
    </div>
  );
}
