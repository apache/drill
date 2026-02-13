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
import { Form, Select, Switch, Typography, Space, Tag, Divider } from 'antd';
import { FieldNumberOutlined, FieldStringOutlined, ClockCircleOutlined, FieldTimeOutlined } from '@ant-design/icons';
import type { ChartType, VisualizationConfig } from '../../types';
import { isTemporalType } from '../../utils/sqlTransformations';
import type { TimeGrain, AggregationFunction } from '../../utils/sqlTransformations';

const { Text } = Typography;

interface ColumnInfo {
  name: string;
  type: string;
}

interface ColumnMapperProps {
  columns: ColumnInfo[];
  chartType: ChartType;
  config: VisualizationConfig;
  onChange: (config: VisualizationConfig) => void;
}

// Determine if a column type is numeric
function isNumericType(type: string): boolean {
  const numericTypes = ['int', 'bigint', 'float', 'double', 'decimal', 'numeric', 'smallint', 'tinyint'];
  return numericTypes.some((t) => type.toLowerCase().includes(t));
}

const TIME_GRAIN_OPTIONS: { value: TimeGrain | ''; label: string }[] = [
  { value: '', label: 'None' },
  { value: 'SECOND', label: 'Second' },
  { value: 'MINUTE', label: 'Minute' },
  { value: 'HOUR', label: 'Hour' },
  { value: 'DAY', label: 'Day' },
  { value: 'WEEK', label: 'Week' },
  { value: 'MONTH', label: 'Month' },
  { value: 'QUARTER', label: 'Quarter' },
  { value: 'YEAR', label: 'Year' },
];

const AGGREGATION_OPTIONS: { value: AggregationFunction; label: string }[] = [
  { value: 'SUM', label: 'SUM' },
  { value: 'AVG', label: 'AVG' },
  { value: 'MIN', label: 'MIN' },
  { value: 'MAX', label: 'MAX' },
  { value: 'COUNT', label: 'COUNT' },
];

// Get required fields for each chart type
function getRequiredFields(chartType: ChartType): { field: string; label: string; multi?: boolean; numeric?: boolean }[] {
  switch (chartType) {
    case 'bar':
    case 'line':
    case 'area':
      return [
        { field: 'xAxis', label: 'X-Axis (Category)', numeric: false },
        { field: 'metrics', label: 'Y-Axis (Values)', multi: true, numeric: true },
      ];
    case 'pie':
      return [
        { field: 'dimensions', label: 'Labels', multi: false, numeric: false },
        { field: 'metrics', label: 'Values', multi: false, numeric: true },
      ];
    case 'scatter':
      return [
        { field: 'xAxis', label: 'X-Axis', numeric: true },
        { field: 'yAxis', label: 'Y-Axis', numeric: true },
        { field: 'dimensions', label: 'Group By (Optional)', multi: false, numeric: false },
      ];
    case 'heatmap':
      return [
        { field: 'xAxis', label: 'X-Axis', numeric: false },
        { field: 'yAxis', label: 'Y-Axis', numeric: false },
        { field: 'metrics', label: 'Value', multi: false, numeric: true },
      ];
    case 'treemap':
      return [
        { field: 'dimensions', label: 'Categories', multi: true, numeric: false },
        { field: 'metrics', label: 'Size', multi: false, numeric: true },
      ];
    case 'gauge':
      return [
        { field: 'metrics', label: 'Value', multi: false, numeric: true },
      ];
    case 'funnel':
      return [
        { field: 'dimensions', label: 'Stages', multi: false, numeric: false },
        { field: 'metrics', label: 'Values', multi: false, numeric: true },
      ];
    case 'map':
      return [
        { field: 'dimensions', label: 'Location', multi: false, numeric: false },
        { field: 'metrics', label: 'Value', multi: false, numeric: true },
      ];
    case 'bigNumber':
      return [
        { field: 'metrics', label: 'Metric', multi: false, numeric: true },
        { field: 'xAxis', label: 'Order By (Optional)', numeric: false },
      ];
    case 'table':
      return [
        { field: 'dimensions', label: 'Columns to Display', multi: true, numeric: false },
      ];
    default:
      return [];
  }
}

export default function ColumnMapper({ columns, chartType, config, onChange }: ColumnMapperProps) {
  const requiredFields = getRequiredFields(chartType);

  const handleFieldChange = (field: string, value: string | string[]) => {
    const newConfig = { ...config, chartOptions: { ...config.chartOptions } };
    if (field === 'xAxis') {
      newConfig.xAxis = value as string;
      // Clear time grain if x-axis changes to non-temporal column
      const selectedCol = columns.find((c) => c.name === value);
      if (!selectedCol || !isTemporalType(selectedCol.type)) {
        delete newConfig.chartOptions.timeGrain;
        delete newConfig.chartOptions.metricAggregations;
      }
    } else if (field === 'yAxis') {
      newConfig.yAxis = value as string;
    } else if (field === 'metrics') {
      const newMetrics = Array.isArray(value) ? value : [value];
      newConfig.metrics = newMetrics;
      // Remove stale aggregation entries for removed metrics
      const aggregations = newConfig.chartOptions.metricAggregations as Record<string, string> | undefined;
      if (aggregations) {
        const cleaned: Record<string, string> = {};
        for (const m of newMetrics) {
          if (aggregations[m]) {
            cleaned[m] = aggregations[m];
          }
        }
        if (Object.keys(cleaned).length > 0) {
          newConfig.chartOptions.metricAggregations = cleaned;
        } else {
          delete newConfig.chartOptions.metricAggregations;
        }
      }
    } else if (field === 'dimensions') {
      newConfig.dimensions = Array.isArray(value) ? value : [value];
    }
    onChange(newConfig);
  };

  const renderColumnOption = (col: ColumnInfo) => {
    let icon = <FieldStringOutlined style={{ color: '#1890ff' }} />;
    if (isNumericType(col.type)) {
      icon = <FieldNumberOutlined style={{ color: '#52c41a' }} />;
    } else if (isTemporalType(col.type)) {
      icon = <FieldTimeOutlined style={{ color: '#722ed1' }} />;
    }
    return (
      <Select.Option key={col.name} value={col.name}>
        <Space>
          {icon}
          <span>{col.name}</span>
          <Tag style={{ fontSize: 10 }}>{col.type}</Tag>
        </Space>
      </Select.Option>
    );
  };

  // Filter columns based on type requirements
  const getFilteredColumns = (numericOnly?: boolean) => {
    if (numericOnly === undefined) {
      return columns;
    }
    return columns.filter((col) =>
      numericOnly ? isNumericType(col.type) : !isNumericType(col.type)
    );
  };

  const getCurrentValue = (field: string): string | string[] | undefined => {
    switch (field) {
      case 'xAxis':
        return config.xAxis;
      case 'yAxis':
        return config.yAxis;
      case 'metrics':
        return config.metrics;
      case 'dimensions':
        return config.dimensions;
      default:
        return undefined;
    }
  };

  // Time grain computed values
  const xAxisCol = columns.find((c) => c.name === config.xAxis);
  const xAxisIsTemporal = !!xAxisCol && isTemporalType(xAxisCol.type);
  const showTimeGrain = (chartType === 'line' || chartType === 'area') && xAxisIsTemporal;
  const currentTimeGrain = (config.chartOptions?.timeGrain as TimeGrain | undefined) || undefined;
  const currentAggregations = (config.chartOptions?.metricAggregations as Record<string, AggregationFunction> | undefined) || {};

  const handleTimeGrainChange = (grain: TimeGrain | '') => {
    const newOptions = { ...config.chartOptions };
    if (grain) {
      newOptions.timeGrain = grain;
    } else {
      delete newOptions.timeGrain;
      delete newOptions.metricAggregations;
    }
    onChange({ ...config, chartOptions: newOptions });
  };

  const handleAggregationChange = (metricName: string, aggregation: AggregationFunction | '') => {
    const aggregations = { ...(config.chartOptions?.metricAggregations as Record<string, string> || {}) };
    if (aggregation) {
      aggregations[metricName] = aggregation;
    } else {
      delete aggregations[metricName];
    }
    const newOptions = { ...config.chartOptions };
    if (Object.keys(aggregations).length > 0) {
      newOptions.metricAggregations = aggregations;
    } else {
      delete newOptions.metricAggregations;
    }
    onChange({ ...config, chartOptions: newOptions });
  };

  if (columns.length === 0) {
    return (
      <div style={{ padding: 16, textAlign: 'center' }}>
        <Text type="secondary">
          Run a query first to see available columns
        </Text>
      </div>
    );
  }

  return (
    <Form layout="vertical" size="small">
      {requiredFields.map(({ field, label, multi, numeric }) => {
        // For table type, show all columns
        const availableColumns = chartType === 'table'
          ? columns
          : getFilteredColumns(numeric);

        return (
          <Form.Item key={field} label={label}>
            <Select
              mode={multi ? 'multiple' : undefined}
              placeholder={`Select ${label.toLowerCase()}`}
              value={getCurrentValue(field)}
              onChange={(value) => handleFieldChange(field, value)}
              style={{ width: '100%' }}
              allowClear
              showSearch
              optionFilterProp="children"
            >
              {availableColumns.map(renderColumnOption)}
            </Select>
            {numeric !== undefined && (
              <Text type="secondary" style={{ fontSize: 11 }}>
                {numeric ? 'Numeric columns recommended' : 'Text/category columns recommended'}
              </Text>
            )}
          </Form.Item>
        );
      })}
      {showTimeGrain && (
        <>
          <Divider style={{ margin: '8px 0' }} />
          <Form.Item label={<Space><ClockCircleOutlined /> Time Grain</Space>}>
            <Select
              placeholder="Select time grain"
              value={currentTimeGrain || undefined}
              onChange={(value) => handleTimeGrainChange(value as TimeGrain | '')}
              allowClear
              onClear={() => handleTimeGrainChange('')}
              style={{ width: '100%' }}
            >
              {TIME_GRAIN_OPTIONS.filter((o) => o.value !== '').map((o) => (
                <Select.Option key={o.value} value={o.value}>{o.label}</Select.Option>
              ))}
            </Select>
            <Text type="secondary" style={{ fontSize: 11 }}>
              Aggregate temporal data by time period
            </Text>
          </Form.Item>
          {currentTimeGrain && config.metrics && config.metrics.length > 0 && (
            <>
              <Text strong style={{ fontSize: 12 }}>Metric Aggregations</Text>
              {config.metrics.map((metric) => (
                <Form.Item
                  key={metric}
                  label={metric}
                  style={{ marginBottom: 8 }}
                  validateStatus={currentAggregations[metric] ? undefined : 'warning'}
                  help={currentAggregations[metric] ? undefined : 'Select an aggregation function'}
                >
                  <Select
                    placeholder="Select aggregation"
                    value={currentAggregations[metric] || undefined}
                    onChange={(value) => handleAggregationChange(metric, value as AggregationFunction | '')}
                    allowClear
                    onClear={() => handleAggregationChange(metric, '')}
                    style={{ width: '100%' }}
                  >
                    {AGGREGATION_OPTIONS.map((o) => (
                      <Select.Option key={o.value} value={o.value}>{o.label}</Select.Option>
                    ))}
                  </Select>
                </Form.Item>
              ))}
            </>
          )}
        </>
      )}
      {chartType === 'bigNumber' && (
        <>
          <Form.Item label="Show Sparkline">
            <Switch
              checked={config.chartOptions?.showSparkline !== false}
              onChange={(checked) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, showSparkline: checked },
              })}
            />
          </Form.Item>
          <Form.Item label="Show Trend">
            <Switch
              checked={config.chartOptions?.showTrend !== false}
              onChange={(checked) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, showTrend: checked },
              })}
            />
          </Form.Item>
        </>
      )}
    </Form>
  );
}
