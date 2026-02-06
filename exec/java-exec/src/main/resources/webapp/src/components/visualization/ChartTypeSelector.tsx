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
import { Card, Typography, Row, Col, Tooltip } from 'antd';
import {
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
];

interface ChartTypeSelectorProps {
  value?: ChartType;
  onChange?: (type: ChartType) => void;
}

export default function ChartTypeSelector({ value, onChange }: ChartTypeSelectorProps) {
  const basicCharts = chartTypes.filter((c) => c.category === 'basic');
  const advancedCharts = chartTypes.filter((c) => c.category === 'advanced');

  const renderChartCard = (chart: ChartTypeOption) => (
    <Col key={chart.type} xs={12} sm={8} md={6} lg={4}>
      <Tooltip title={chart.description}>
        <Card
          hoverable
          size="small"
          onClick={() => onChange?.(chart.type)}
          style={{
            textAlign: 'center',
            border: value === chart.type ? '2px solid #1890ff' : '1px solid #d9d9d9',
            backgroundColor: value === chart.type ? '#e6f7ff' : undefined,
          }}
        >
          <div style={{ color: value === chart.type ? '#1890ff' : '#595959' }}>
            {chart.icon}
          </div>
          <Text
            strong={value === chart.type}
            style={{ fontSize: 12, display: 'block', marginTop: 4 }}
          >
            {chart.name}
          </Text>
        </Card>
      </Tooltip>
    </Col>
  );

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
