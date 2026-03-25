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
import { Form, Select, Switch, Radio, Typography, Space, Tag, Divider, Slider, InputNumber, Button } from 'antd';
import { FieldNumberOutlined, FieldStringOutlined, ClockCircleOutlined, FieldTimeOutlined } from '@ant-design/icons';
import type { ChartType, VisualizationConfig, PredictionMethod } from '../../types';
import { isTemporalType } from '../../utils/sqlTransformations';
import type { TimeGrain, AggregationFunction } from '../../utils/sqlTransformations';
import { GEO_SCOPE_OPTIONS, getMapDef } from '../../utils/geoMapRegistry';

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

const MARKER_SHAPE_OPTIONS: { value: string; label: string }[] = [
  { value: 'circle', label: 'Circle' },
  { value: 'rect', label: 'Square' },
  { value: 'roundRect', label: 'Rounded Square' },
  { value: 'triangle', label: 'Triangle' },
  { value: 'diamond', label: 'Diamond' },
  { value: 'pin', label: 'Pin' },
  { value: 'arrow', label: 'Arrow' },
  { value: 'none', label: 'None (hidden)' },
];

const DATE_FORMAT_OPTIONS: { value: string; label: string }[] = [
  { value: 'auto', label: 'Auto' },
  { value: 'YYYY-MM-DD', label: 'YYYY-MM-DD' },
  { value: 'MM/DD/YYYY', label: 'MM/DD/YYYY' },
  { value: 'DD/MM/YYYY', label: 'DD/MM/YYYY' },
  { value: 'YYYY-MM-DD HH:mm', label: 'YYYY-MM-DD HH:mm' },
  { value: 'MM/DD/YYYY HH:mm', label: 'MM/DD/YYYY HH:mm' },
  { value: 'MMM DD, YYYY', label: 'MMM DD, YYYY' },
  { value: 'MMMM DD, YYYY', label: 'MMMM DD, YYYY' },
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
        { field: 'dimensions', label: 'Group By / Series (Optional)', multi: false, numeric: false },
      ];
    case 'pie':
      return [
        { field: 'dimensions', label: 'Labels', multi: false, numeric: false },
        { field: 'metrics', label: 'Values', multi: true, numeric: true },
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
        { field: 'xAxis', label: 'Longitude', numeric: true },
        { field: 'yAxis', label: 'Latitude', numeric: true },
        { field: 'metrics', label: 'Size Value (Optional)', multi: false, numeric: true },
        { field: 'dimensions', label: 'Label (Optional)', multi: false, numeric: false },
      ];
    case 'choropleth':
      return [
        { field: 'dimensions', label: 'Location Code (ISO-2, ISO-3, or country name)', multi: false, numeric: false },
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
    case 'pivot':
      return [
        { field: 'xAxis', label: 'Row Dimension', numeric: false },
        { field: 'yAxis', label: 'Column Pivot (creates headers)', numeric: false },
        { field: 'metrics', label: 'Value', multi: false, numeric: true },
      ];
    case 'sankey':
      return [
        { field: 'xAxis', label: 'Source', numeric: false },
        { field: 'yAxis', label: 'Target', numeric: false },
        { field: 'metrics', label: 'Flow Value', multi: false, numeric: true },
      ];
    case 'radar':
      return [
        { field: 'metrics', label: 'Metrics (Axes)', multi: true, numeric: true },
        { field: 'dimensions', label: 'Series / Group By (Optional)', multi: false, numeric: false },
      ];
    case 'boxplot':
      return [
        { field: 'xAxis', label: 'Category (Group By)', numeric: false },
        { field: 'metrics', label: 'Value Column', multi: false, numeric: true },
      ];
    case 'waterfall':
      return [
        { field: 'xAxis', label: 'Category', numeric: false },
        { field: 'metrics', label: 'Change Value', multi: false, numeric: true },
      ];
    case 'sunburst':
      return [
        { field: 'dimensions', label: 'Hierarchy Levels (outermost → innermost)', multi: true, numeric: false },
        { field: 'metrics', label: 'Size', multi: false, numeric: true },
      ];
    case 'candlestick':
      return [
        { field: 'xAxis', label: 'Date / Time', numeric: false },
        { field: 'metrics', label: 'OHLC Columns (select 4: Open, Close, Low, High)', multi: true, numeric: true },
      ];
    case 'calendar':
      return [
        { field: 'xAxis', label: 'Date Column', numeric: false },
        { field: 'metrics', label: 'Value', multi: false, numeric: true },
      ];
    case 'bubble':
      return [
        { field: 'xAxis', label: 'X Axis', numeric: true },
        { field: 'yAxis', label: 'Y Axis', numeric: true },
        { field: 'metrics', label: 'Bubble Size (Optional)', multi: false, numeric: true },
        { field: 'dimensions', label: 'Group By / Label (Optional)', multi: false, numeric: false },
      ];
    case 'parallel':
      return [
        { field: 'metrics', label: 'Axes (select multiple numeric columns)', multi: true, numeric: true },
        { field: 'dimensions', label: 'Color By (Optional)', multi: false, numeric: false },
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
    let icon = <FieldStringOutlined style={{ color: '#3b82f6' }} />;
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
  const showTimeGrain = xAxisIsTemporal;
  const showDateFormat = (chartType === 'bar' || chartType === 'line' || chartType === 'area') && xAxisIsTemporal;
  const currentDateFormat = (config.chartOptions?.dateFormat as string) || 'auto';
  const currentTimeGrain = (config.chartOptions?.timeGrain as TimeGrain | undefined) || undefined;
  const currentAggregations = (config.chartOptions?.metricAggregations as Record<string, AggregationFunction> | undefined) || {};

  const handleTimeGrainChange = (grain: TimeGrain | '') => {
    const newOptions = { ...config.chartOptions };
    if (grain) {
      newOptions.timeGrain = grain;
    } else {
      delete newOptions.timeGrain;
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
            {field === 'metrics' && chartType !== 'pivot' && config.metrics && config.metrics.length > 0 && (
              <div style={{ marginTop: 8 }}>
                <Text type="secondary" style={{ fontSize: 11, display: 'block', marginBottom: 4 }}>
                  Aggregation (optional — groups results automatically)
                </Text>
                {config.metrics.map((metric) => (
                  <div key={metric} style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
                    <Text style={{ fontSize: 12, minWidth: 80 }}>{metric}</Text>
                    <Select
                      size="small"
                      placeholder="None"
                      value={currentAggregations[metric] || undefined}
                      onChange={(v) => handleAggregationChange(metric, v)}
                      allowClear
                      onClear={() => handleAggregationChange(metric, '')}
                      style={{ flex: 1 }}
                      options={AGGREGATION_OPTIONS}
                    />
                  </div>
                ))}
              </div>
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
        </>
      )}
      {showDateFormat && (
        <>
          <Divider style={{ margin: '8px 0' }} />
          <Form.Item label={<Space><ClockCircleOutlined /> Date Format</Space>}>
            <Select
              value={currentDateFormat}
              onChange={(value) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, dateFormat: value },
              })}
              style={{ width: '100%' }}
            >
              {DATE_FORMAT_OPTIONS.map((o) => (
                <Select.Option key={o.value} value={o.value}>{o.label}</Select.Option>
              ))}
            </Select>
            <Text type="secondary" style={{ fontSize: 11 }}>
              Format for date values on the X-axis
            </Text>
          </Form.Item>
        </>
      )}
      {(chartType === 'line' || chartType === 'area') && (
        <>
          <Divider style={{ margin: '8px 0' }} />
          <Form.Item label="Smooth Lines">
            <Switch
              checked={config.chartOptions?.smoothLine !== false}
              onChange={(checked) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, smoothLine: checked },
              })}
            />
          </Form.Item>
          <Form.Item label="Show Data Point Values">
            <Switch
              checked={config.chartOptions?.showDataLabels === true}
              onChange={(checked) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, showDataLabels: checked },
              })}
            />
          </Form.Item>
          {chartType === 'line' && (
            <>
              <Form.Item label="Marker Shape">
                <Select
                  value={(config.chartOptions?.markerShape as string) || 'circle'}
                  onChange={(value) => onChange({
                    ...config,
                    chartOptions: { ...config.chartOptions, markerShape: value },
                  })}
                  style={{ width: '100%' }}
                >
                  {MARKER_SHAPE_OPTIONS.map((o) => (
                    <Select.Option key={o.value} value={o.value}>{o.label}</Select.Option>
                  ))}
                </Select>
              </Form.Item>
              {(config.chartOptions?.markerShape || 'circle') !== 'none' && (
                <Form.Item label={`Marker Size (${Number(config.chartOptions?.markerSize) || 4}px)`}>
                  <Slider
                    min={2}
                    max={20}
                    value={Number(config.chartOptions?.markerSize) || 4}
                    onChange={(value) => onChange({
                      ...config,
                      chartOptions: { ...config.chartOptions, markerSize: value },
                    })}
                  />
                </Form.Item>
              )}
            </>
          )}
          {chartType === 'area' && (
            <Form.Item label="Gradient Fill">
              <Switch
                checked={config.chartOptions?.gradientArea === true}
                onChange={(checked) => onChange({
                  ...config,
                  chartOptions: { ...config.chartOptions, gradientArea: checked },
                })}
              />
            </Form.Item>
          )}
        </>
      )}
      {(chartType === 'line' || chartType === 'area') && (
        <>
          <Divider style={{ margin: '8px 0' }}>Trend Line</Divider>
          <Form.Item label="Show Trend Line">
            <Switch
              checked={config.chartOptions?.showTrendLine === true}
              onChange={(checked) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, showTrendLine: checked },
              })}
            />
            <Text type="secondary" style={{ fontSize: 11, display: 'block', marginTop: 4 }}>
              Overlay a best-fit line on the historical data
            </Text>
          </Form.Item>
          {config.chartOptions?.showTrendLine === true && (
            <>
              <Form.Item label="Trend Line Method">
                <Select
                  value={(config.chartOptions?.trendLineMethod as string) || 'linear'}
                  onChange={(value) => onChange({
                    ...config,
                    chartOptions: { ...config.chartOptions, trendLineMethod: value },
                  })}
                  style={{ width: '100%' }}
                >
                  <Select.Option value="linear">Linear</Select.Option>
                  <Select.Option value="polynomial">Polynomial</Select.Option>
                  <Select.Option value="movingAverage">Moving Average</Select.Option>
                </Select>
              </Form.Item>
              {(config.chartOptions?.trendLineMethod as string) === 'polynomial' && (
                <Form.Item label={`Polynomial Order (${Number(config.chartOptions?.trendLinePolynomialOrder) || 2})`}>
                  <Slider
                    min={2}
                    max={5}
                    value={Number(config.chartOptions?.trendLinePolynomialOrder) || 2}
                    onChange={(value) => onChange({
                      ...config,
                      chartOptions: { ...config.chartOptions, trendLinePolynomialOrder: value },
                    })}
                  />
                </Form.Item>
              )}
              {(config.chartOptions?.trendLineMethod as string) === 'movingAverage' && (
                <Form.Item label={`Window Size (${Number(config.chartOptions?.trendLineWindow) || 3})`}>
                  <Slider
                    min={2}
                    max={10}
                    value={Number(config.chartOptions?.trendLineWindow) || 3}
                    onChange={(value) => onChange({
                      ...config,
                      chartOptions: { ...config.chartOptions, trendLineWindow: value },
                    })}
                  />
                </Form.Item>
              )}
            </>
          )}
          <Divider style={{ margin: '8px 0' }}>Predictive Analytics</Divider>
          <Form.Item label="Enable Forecasting">
            <Switch
              checked={config.predictiveAnalytics?.enabled === true}
              onChange={(checked) => {
                const pa = checked
                  ? { enabled: true, method: 'linear' as PredictionMethod, periods: 5, confidenceLevel: 0.95 }
                  : { ...config.predictiveAnalytics!, enabled: false };
                onChange({ ...config, predictiveAnalytics: pa });
              }}
            />
          </Form.Item>
          {config.predictiveAnalytics?.enabled && (
            <>
              <Form.Item label="Prediction Method">
                <Select
                  value={config.predictiveAnalytics.method}
                  onChange={(value: PredictionMethod) => onChange({
                    ...config,
                    predictiveAnalytics: { ...config.predictiveAnalytics!, method: value },
                  })}
                  style={{ width: '100%' }}
                >
                  <Select.Option value="linear">Linear Regression</Select.Option>
                  <Select.Option value="polynomial">Polynomial Regression</Select.Option>
                  <Select.Option value="movingAverage">Moving Average</Select.Option>
                </Select>
                <Text type="secondary" style={{ fontSize: 11 }}>
                  {config.predictiveAnalytics.method === 'linear' && 'Best for data with a constant rate of change'}
                  {config.predictiveAnalytics.method === 'polynomial' && 'Best for data with curves or acceleration'}
                  {config.predictiveAnalytics.method === 'movingAverage' && 'Best for noisy data; projects the recent average'}
                </Text>
              </Form.Item>
              <Form.Item label={`Forecast Periods (${config.predictiveAnalytics.periods})`}>
                <Slider
                  min={1}
                  max={20}
                  value={config.predictiveAnalytics.periods}
                  onChange={(value) => onChange({
                    ...config,
                    predictiveAnalytics: { ...config.predictiveAnalytics!, periods: value },
                  })}
                />
              </Form.Item>
              {config.predictiveAnalytics.method === 'polynomial' && (
                <Form.Item label={`Polynomial Order (${config.predictiveAnalytics.polynomialOrder ?? 2})`}>
                  <Slider
                    min={2}
                    max={5}
                    value={config.predictiveAnalytics.polynomialOrder ?? 2}
                    onChange={(value) => onChange({
                      ...config,
                      predictiveAnalytics: { ...config.predictiveAnalytics!, polynomialOrder: value },
                    })}
                  />
                </Form.Item>
              )}
              {config.predictiveAnalytics.method === 'movingAverage' && (
                <Form.Item label={`Window Size (${config.predictiveAnalytics.movingAverageWindow ?? 3})`}>
                  <Slider
                    min={2}
                    max={10}
                    value={config.predictiveAnalytics.movingAverageWindow ?? 3}
                    onChange={(value) => onChange({
                      ...config,
                      predictiveAnalytics: { ...config.predictiveAnalytics!, movingAverageWindow: value },
                    })}
                  />
                </Form.Item>
              )}
              <Form.Item label="Confidence Level">
                <Select
                  value={config.predictiveAnalytics.confidenceLevel ?? 0.95}
                  onChange={(value) => onChange({
                    ...config,
                    predictiveAnalytics: { ...config.predictiveAnalytics!, confidenceLevel: value },
                  })}
                  style={{ width: '100%' }}
                  options={[
                    { value: 0.80, label: '80%' },
                    { value: 0.90, label: '90%' },
                    { value: 0.95, label: '95%' },
                  ]}
                />
              </Form.Item>
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
      {chartType === 'map' && (
        <>
          <Divider style={{ margin: '8px 0' }} />
          <Form.Item label={`Point Size (${Number(config.chartOptions?.pointSize) || 8}px)`}>
            <Slider
              min={4}
              max={30}
              value={Number(config.chartOptions?.pointSize) || 8}
              onChange={(value) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, pointSize: value },
              })}
            />
          </Form.Item>
          {config.metrics && config.metrics.length > 0 && (
            <Form.Item label="Scale Points by Value">
              <Switch
                checked={config.chartOptions?.scaleByValue === true}
                onChange={(checked) => onChange({
                  ...config,
                  chartOptions: { ...config.chartOptions, scaleByValue: checked },
                })}
              />
            </Form.Item>
          )}
          <Form.Item label="Show Country Borders">
            <Switch
              checked={config.chartOptions?.showBorders !== false}
              onChange={(checked) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, showBorders: checked },
              })}
            />
          </Form.Item>
          <Form.Item label="Enable Zoom/Pan">
            <Switch
              checked={config.chartOptions?.enableRoam !== false}
              onChange={(checked) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, enableRoam: checked },
              })}
            />
          </Form.Item>
        </>
      )}
      {chartType === 'choropleth' && (
        <>
          <Divider style={{ margin: '8px 0' }} />
          <Form.Item label="Geographic Scope">
            {(() => {
              // Determine if multi-select is enabled for current scope
              const currentScope = (config.chartOptions?.mapScope as string) || 'world';
              const currentScopes = (config.chartOptions?.mapScopes as string[]) || [];
              const mapDef = getMapDef(currentScope);
              const isMultiSelectScope = mapDef?.multiSelectAllowed || false;

              // Get state ZIP code options for multi-select
              const stateZipOptions = GEO_SCOPE_OPTIONS
                .find((opt) => opt.label === 'US ZIP Codes (by State)')
                ?.options || [];

              if (isMultiSelectScope && stateZipOptions.length > 0) {
                return (
                  <div>
                    <Select
                      mode="multiple"
                      placeholder="Select one or more states (recommended for performance)"
                      value={currentScopes.length > 0 ? currentScopes : [currentScope]}
                      onChange={(values) => onChange({
                        ...config,
                        chartOptions: {
                          ...config.chartOptions,
                          mapScopes: values,
                          mapScope: values[0] || 'world', // Keep single mapScope for fallback
                        },
                      })}
                      options={stateZipOptions}
                    />
                    <div style={{ fontSize: '12px', color: '#999', marginTop: '8px' }}>
                      💡 Tip: Select specific states for best performance. Each state file is ~1-3 MB.
                    </div>
                  </div>
                );
              }

              // Single-select for all other scopes
              return (
                <Select
                  value={currentScope}
                  onChange={(value) => onChange({
                    ...config,
                    chartOptions: { ...config.chartOptions, mapScope: value, mapScopes: [] },
                  })}
                  options={GEO_SCOPE_OPTIONS}
                />
              );
            })()}
          </Form.Item>
          <Form.Item label="Color Scale">
            <Select
              value={(config.chartOptions?.choroplethColorScale as string) || 'blue'}
              onChange={(value) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, choroplethColorScale: value },
              })}
              options={[
                { value: 'blue', label: 'Blue (light → dark)' },
                { value: 'green', label: 'Green' },
                { value: 'red', label: 'Red' },
                { value: 'heat', label: 'Heat (yellow → red)' },
                { value: 'diverging', label: 'Diverging (blue → red)' },
              ]}
            />
          </Form.Item>
          <Form.Item label="Enable Pan/Zoom">
            <Switch
              checked={config.chartOptions?.enableRoam !== false}
              onChange={(checked) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, enableRoam: checked },
              })}
            />
          </Form.Item>
          <Divider style={{ margin: '8px 0' }} />
          <Form.Item label="Starting Zoom" style={{ marginBottom: 8 }}>
            <InputNumber
              min={1} max={20} step={0.5}
              placeholder="Map default"
              value={(config.chartOptions?.choroplethZoom as number) ?? undefined}
              onChange={(val) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, choroplethZoom: val ?? undefined },
              })}
              style={{ width: '100%' }}
            />
          </Form.Item>
          <Form.Item label="Starting Center" style={{ marginBottom: 4 }}>
            <Space.Compact style={{ width: '100%' }}>
              <InputNumber
                placeholder="Longitude" min={-180} max={180} step={0.1}
                value={(config.chartOptions?.choroplethCenter as [number,number])?.[0] ?? undefined}
                onChange={(lon) => {
                  const prev = (config.chartOptions?.choroplethCenter as [number, number]) ?? [0, 0];
                  const next: [number, number] = [...prev];
                  next[0] = lon ?? 0;
                  onChange({ ...config, chartOptions: { ...config.chartOptions, choroplethCenter: next } });
                }}
                style={{ width: '50%' }}
              />
              <InputNumber
                placeholder="Latitude" min={-90} max={90} step={0.1}
                value={(config.chartOptions?.choroplethCenter as [number,number])?.[1] ?? undefined}
                onChange={(lat) => {
                  const prev = (config.chartOptions?.choroplethCenter as [number, number]) ?? [0, 0];
                  const next: [number, number] = [...prev];
                  next[1] = lat ?? 0;
                  onChange({ ...config, chartOptions: { ...config.chartOptions, choroplethCenter: next } });
                }}
                style={{ width: '50%' }}
              />
            </Space.Compact>
          </Form.Item>
          <Button
            size="small" type="link"
            onClick={() => onChange({
              ...config,
              chartOptions: { ...config.chartOptions, choroplethZoom: undefined, choroplethCenter: undefined },
            })}
          >
            Reset to map defaults
          </Button>
        </>
      )}
      {chartType === 'radar' && (
        <>
          <Divider style={{ margin: '8px 0' }} />
          <Form.Item label="Fill Areas">
            <Switch
              checked={config.chartOptions?.radarFill === true}
              onChange={(checked) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, radarFill: checked },
              })}
            />
          </Form.Item>
        </>
      )}
      {chartType === 'waterfall' && (
        <>
          <Divider style={{ margin: '8px 0' }} />
          <Form.Item label="Show Connectors">
            <Switch
              checked={config.chartOptions?.waterfallConnectors !== false}
              onChange={(checked) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, waterfallConnectors: checked },
              })}
            />
          </Form.Item>
        </>
      )}
      {chartType === 'sunburst' && (
        <>
          <Divider style={{ margin: '8px 0' }} />
          <Form.Item label="Show Labels">
            <Switch
              checked={config.chartOptions?.sunburstLabels !== false}
              onChange={(checked) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, sunburstLabels: checked },
              })}
            />
          </Form.Item>
        </>
      )}
      {chartType === 'bubble' && (
        <>
          <Divider style={{ margin: '8px 0' }} />
          <Form.Item label={`Max Bubble Size (${Number(config.chartOptions?.maxBubbleSize ?? 60)}px)`}>
            <Slider
              min={20}
              max={120}
              value={Number(config.chartOptions?.maxBubbleSize ?? 60)}
              onChange={(value) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, maxBubbleSize: value },
              })}
            />
          </Form.Item>
        </>
      )}
      {chartType === 'parallel' && (
        <>
          <Divider style={{ margin: '8px 0' }} />
          <Form.Item label={`Line Opacity (${Number(config.chartOptions?.parallelOpacity ?? 0.4).toFixed(1)})`}>
            <Slider
              min={0.1}
              max={1}
              step={0.1}
              value={Number(config.chartOptions?.parallelOpacity ?? 0.4)}
              onChange={(value) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, parallelOpacity: value },
              })}
            />
          </Form.Item>
        </>
      )}
      {chartType === 'candlestick' && (
        <>
          <Divider style={{ margin: '8px 0' }} />
          <Text type="secondary" style={{ fontSize: 11, display: 'block', marginBottom: 8 }}>
            Select exactly 4 OHLC columns in order: Open, Close, Low, High
          </Text>
        </>
      )}
      {chartType === 'sankey' && (
        <>
          <Divider style={{ margin: '8px 0' }} />
          <Form.Item label="Orientation">
            <Radio.Group
              value={(config.chartOptions?.sankeyOrient as string) || 'horizontal'}
              onChange={(e) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, sankeyOrient: e.target.value },
              })}
              optionType="button"
              buttonStyle="solid"
              size="small"
            >
              <Radio.Button value="horizontal">Horizontal</Radio.Button>
              <Radio.Button value="vertical">Vertical</Radio.Button>
            </Radio.Group>
          </Form.Item>
          <Form.Item label="Show Node Labels">
            <Switch
              checked={config.chartOptions?.sankeyShowLabels !== false}
              onChange={(checked) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, sankeyShowLabels: checked },
              })}
            />
          </Form.Item>
          <Form.Item label={`Link Curveness (${Number(config.chartOptions?.sankeyCurveness ?? 0.5).toFixed(1)})`}>
            <Slider
              min={0}
              max={1}
              step={0.1}
              value={Number(config.chartOptions?.sankeyCurveness ?? 0.5)}
              onChange={(value) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, sankeyCurveness: value },
              })}
            />
          </Form.Item>
          <Form.Item label="Node Width (px)">
            <InputNumber
              min={8}
              max={40}
              value={Number(config.chartOptions?.sankeyNodeWidth ?? 20)}
              onChange={(value) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, sankeyNodeWidth: value ?? 20 },
              })}
              style={{ width: '100%' }}
            />
          </Form.Item>
        </>
      )}
      {chartType === 'pivot' && (
        <>
          <Divider style={{ margin: '8px 0' }} />
          <Form.Item label="Aggregation">
            <Select
              value={(config.chartOptions?.pivotAggregation as string) || 'SUM'}
              onChange={(v) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, pivotAggregation: v },
              })}
              size="small"
              style={{ width: '100%' }}
              options={AGGREGATION_OPTIONS}
            />
          </Form.Item>
          <Form.Item label="Row Totals">
            <Switch
              checked={config.chartOptions?.showRowTotals !== false}
              onChange={(checked) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, showRowTotals: checked },
              })}
            />
          </Form.Item>
          <Form.Item label="Column Totals">
            <Switch
              checked={config.chartOptions?.showColumnTotals === true}
              onChange={(checked) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, showColumnTotals: checked },
              })}
            />
          </Form.Item>
        </>
      )}
      {chartType === 'pie' && (
        <>
          <Divider style={{ margin: '8px 0' }} />
          <Form.Item label="Pie Style">
            <Radio.Group
              value={(config.chartOptions?.pieStyle as string) || 'donut'}
              onChange={(e) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, pieStyle: e.target.value },
              })}
              optionType="button"
              buttonStyle="solid"
              size="small"
            >
              <Radio.Button value="pie">Pie</Radio.Button>
              <Radio.Button value="donut">Donut</Radio.Button>
              <Radio.Button value="nightingale">Nightingale</Radio.Button>
            </Radio.Group>
            <Text type="secondary" style={{ fontSize: 11, display: 'block', marginTop: 4 }}>
              {(config.chartOptions?.pieStyle as string) === 'nightingale'
                ? 'Nightingale (rose) chart — slice radius encodes value'
                : (config.chartOptions?.pieStyle as string) === 'pie'
                  ? 'Standard pie chart — no center hole'
                  : 'Donut chart — hollow center'
              }
            </Text>
          </Form.Item>
          <Form.Item label="Show Labels">
            <Switch
              checked={config.chartOptions?.showPieLabels !== false}
              onChange={(checked) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, showPieLabels: checked },
              })}
            />
          </Form.Item>
          <Form.Item label="Legend Position">
            <Select
              value={(config.chartOptions?.legendPosition as string) || 'right'}
              onChange={(value) => onChange({
                ...config,
                chartOptions: { ...config.chartOptions, legendPosition: value },
              })}
              style={{ width: '100%' }}
              options={[
                { value: 'top', label: 'Top' },
                { value: 'bottom', label: 'Bottom' },
                { value: 'left', label: 'Left' },
                { value: 'right', label: 'Right' },
                { value: 'hidden', label: 'Hidden' },
              ]}
            />
          </Form.Item>
        </>
      )}
    </Form>
  );
}
