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
import { Form, Select, Typography, Space, Tag } from 'antd';
import { FieldNumberOutlined, FieldStringOutlined } from '@ant-design/icons';
import type { ChartType, VisualizationConfig } from '../../types';

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

// Get required fields for each chart type
function getRequiredFields(chartType: ChartType): { field: string; label: string; multi?: boolean; numeric?: boolean }[] {
  switch (chartType) {
    case 'bar':
    case 'line':
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
    const newConfig = { ...config };
    if (field === 'xAxis') {
      newConfig.xAxis = value as string;
    } else if (field === 'yAxis') {
      newConfig.yAxis = value as string;
    } else if (field === 'metrics') {
      newConfig.metrics = Array.isArray(value) ? value : [value];
    } else if (field === 'dimensions') {
      newConfig.dimensions = Array.isArray(value) ? value : [value];
    }
    onChange(newConfig);
  };

  const renderColumnOption = (col: ColumnInfo) => (
    <Select.Option key={col.name} value={col.name}>
      <Space>
        {isNumericType(col.type) ? (
          <FieldNumberOutlined style={{ color: '#52c41a' }} />
        ) : (
          <FieldStringOutlined style={{ color: '#1890ff' }} />
        )}
        <span>{col.name}</span>
        <Tag style={{ fontSize: 10 }}>{col.type}</Tag>
      </Space>
    </Select.Option>
  );

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
    </Form>
  );
}
