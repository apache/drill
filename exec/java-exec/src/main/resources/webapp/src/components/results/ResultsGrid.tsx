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
import { useMemo, useCallback, useRef, useState } from 'react';
import { AgGridReact } from 'ag-grid-react';
import { Button, Space, Dropdown, Typography, Alert, Empty, Modal, Select } from 'antd';
import {
  DownloadOutlined,
  CopyOutlined,
  BarChartOutlined,
  ExpandOutlined,
  SettingOutlined,
  RobotOutlined,
} from '@ant-design/icons';
import type { ColDef, GridReadyEvent, GridApi } from 'ag-grid-community';
import type { MenuProps } from 'antd';
import type { QueryResult, QueryError } from '../../types';
import JsonCellRenderer from './JsonCellRenderer';

import 'ag-grid-community/styles/ag-grid.css';
import 'ag-grid-community/styles/ag-theme-alpine.css';

const { Text } = Typography;

export interface ResultsSettings {
  timestampDisplayFormat: 'locale' | 'iso' | 'utc' | 'epoch';
}

export const DEFAULT_RESULTS_SETTINGS: ResultsSettings = {
  timestampDisplayFormat: 'locale',
};

interface ResultsGridProps {
  results?: QueryResult;
  error?: QueryError;
  isLoading?: boolean;
  onCreateVisualization?: () => void;
  resultsSettings?: ResultsSettings;
  onResultsSettingsChange?: (settings: ResultsSettings) => void;
  onFixWithProspector?: () => void;
  prospectorAvailable?: boolean;
}

export default function ResultsGrid({
  results,
  error,
  isLoading,
  onCreateVisualization,
  resultsSettings,
  onResultsSettingsChange,
  onFixWithProspector,
  prospectorAvailable,
}: ResultsGridProps) {
  const gridRef = useRef<AgGridReact>(null);
  const gridApiRef = useRef<GridApi | null>(null);
  const [settingsModalOpen, setSettingsModalOpen] = useState(false);

  const timestampFormat = resultsSettings?.timestampDisplayFormat ?? DEFAULT_RESULTS_SETTINGS.timestampDisplayFormat;

  // Generate column definitions from results with type-aware filters
  const columnDefs = useMemo<ColDef[]>(() => {
    if (!results?.columns) return [];

    return results.columns.map((col, index) => {
      const metadataType = results.metadata?.[index];
      const config = getColumnConfig(metadataType);
      const isDateColumn = metadataType && (metadataType.toUpperCase().includes('DATE') || metadataType.toUpperCase().includes('TIMESTAMP'));

      const colDef: ColDef = {
        field: col,
        headerName: col,
        sortable: true,
        resizable: true,
        minWidth: 100,
        filter: config.filter,
        filterParams: config.filterParams,
        ...(config.filterValueGetter ? { filterValueGetter: config.filterValueGetter } : {}),
      };

      if (isDateColumn) {
        colDef.valueGetter = (params) => {
          const raw = params.data?.[col];
          if (raw == null) {
            return null;
          }
          if (typeof raw === 'number') {
            return new Date(raw).toISOString();
          }
          return raw;
        };
        colDef.valueFormatter = (params) => {
          if (params.value == null) {
            return '';
          }
          return formatTimestamp(params.value, timestampFormat);
        };
      }

      return colDef;
    });
  }, [results, timestampFormat]);

  // Convert row data — stringify nested objects/arrays so AG Grid doesn't show [object Object]
  const rowData = useMemo(() => {
    if (!results?.rows) return [];
    return results.rows.map((row) => {
      const processed: Record<string, unknown> = {};
      for (const key of Object.keys(row)) {
        const val = row[key];
        if (val !== null && typeof val === 'object') {
          processed[key] = JSON.stringify(val);
        } else {
          processed[key] = val;
        }
      }
      return processed;
    });
  }, [results]);

  const onGridReady = useCallback((params: GridReadyEvent) => {
    gridApiRef.current = params.api;
    params.api.sizeColumnsToFit();
  }, []);

  // Export functions
  const exportToCsv = useCallback(() => {
    gridApiRef.current?.exportDataAsCsv({
      fileName: `drill-query-${results?.queryId || 'results'}.csv`,
    });
  }, [results?.queryId]);

  const exportToJson = useCallback(() => {
    if (!results) return;

    const data = JSON.stringify(results.rows, null, 2);
    const blob = new Blob([data], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `drill-query-${results.queryId || 'results'}.json`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  }, [results]);

  const copyToClipboard = useCallback(async () => {
    if (!results) return;

    const header = results.columns.join('\t');
    const rows = results.rows
      .map((row) => results.columns.map((col) => formatCellValue(row[col])).join('\t'))
      .join('\n');
    const text = `${header}\n${rows}`;

    try {
      await navigator.clipboard.writeText(text);
    } catch (err) {
      console.error('Failed to copy:', err);
    }
  }, [results]);

  const exportMenuItems: MenuProps['items'] = [
    {
      key: 'csv',
      label: 'Export as CSV',
      onClick: exportToCsv,
    },
    {
      key: 'json',
      label: 'Export as JSON',
      onClick: exportToJson,
    },
  ];

  // Show error state
  if (error) {
    return (
      <div style={{ padding: 16 }}>
        <Alert
          message="Query Error"
          description={error.message}
          type="error"
          showIcon
        />
        {prospectorAvailable && onFixWithProspector && (
          <Button
            icon={<RobotOutlined />}
            onClick={onFixWithProspector}
            style={{ marginTop: 12 }}
            type="primary"
            ghost
          >
            Fix with Prospector
          </Button>
        )}
      </div>
    );
  }

  // Show empty state
  if (!results && !isLoading) {
    return (
      <div style={{ padding: 40, textAlign: 'center' }}>
        <Empty
          description={
            <span>
              Run a query to see results
              <br />
              <Text type="secondary">Press Ctrl/Cmd + Enter to execute</Text>
            </span>
          }
        />
      </div>
    );
  }

  // Show loading state
  if (isLoading) {
    return (
      <div style={{ padding: 40, textAlign: 'center' }}>
        <Text type="secondary">Executing query...</Text>
      </div>
    );
  }

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      {/* Results Toolbar */}
      <div className="results-toolbar">
        <Space>
          <Text strong>
            {rowData.length.toLocaleString()} row{rowData.length !== 1 ? 's' : ''}
          </Text>
          {results?.queryId && (
            <Text type="secondary">Query ID: {results.queryId}</Text>
          )}
        </Space>

        <Space>
          <Button
            icon={<CopyOutlined />}
            onClick={copyToClipboard}
            size="small"
          >
            Copy
          </Button>
          <Dropdown menu={{ items: exportMenuItems }} trigger={['click']}>
            <Button icon={<DownloadOutlined />} size="small">
              Export
            </Button>
          </Dropdown>
          <Button
            icon={<BarChartOutlined />}
            onClick={onCreateVisualization}
            size="small"
          >
            Create Chart
          </Button>
          <Button
            icon={<ExpandOutlined />}
            onClick={() => gridApiRef.current?.sizeColumnsToFit()}
            size="small"
          >
            Fit Columns
          </Button>
          <Button
            icon={<SettingOutlined />}
            onClick={() => setSettingsModalOpen(true)}
            size="small"
          >
            Settings
          </Button>
        </Space>
      </div>

      {/* AG Grid */}
      <div className="ag-theme-alpine" style={{ flex: 1, width: '100%' }}>
        <AgGridReact
          ref={gridRef}
          columnDefs={columnDefs}
          rowData={rowData}
          onGridReady={onGridReady}
          defaultColDef={{
            sortable: true,
            resizable: true,
            floatingFilter: true,
            cellRenderer: JsonCellRenderer,
          }}
          animateRows={true}
          pagination={true}
          paginationPageSize={100}
          paginationPageSizeSelector={[50, 100, 500, 1000]}
          rowSelection="multiple"
          enableCellTextSelection={true}
          ensureDomOrder={true}
          suppressRowClickSelection={true}
        />
      </div>

      {/* Results Settings Modal */}
      <Modal
        title="Results Settings"
        open={settingsModalOpen}
        onCancel={() => setSettingsModalOpen(false)}
        footer={null}
        width={400}
      >
        <div style={{ marginBottom: 16 }}>
          <Text strong style={{ display: 'block', marginBottom: 8 }}>
            Timestamp Display Format
          </Text>
          <Select
            value={timestampFormat}
            onChange={(value) => {
              onResultsSettingsChange?.({
                ...resultsSettings,
                ...DEFAULT_RESULTS_SETTINGS,
                timestampDisplayFormat: value,
              });
            }}
            style={{ width: '100%' }}
            options={[
              { value: 'locale', label: 'Locale (browser default)' },
              { value: 'iso', label: 'ISO 8601' },
              { value: 'utc', label: 'UTC' },
              { value: 'epoch', label: 'Epoch (raw milliseconds)' },
            ]}
          />
        </div>
      </Modal>
    </div>
  );
}

// Format a timestamp value based on the user's chosen display format
function formatTimestamp(value: unknown, format: string): string {
  if (value == null || value === '') {
    return '';
  }
  const date = new Date(typeof value === 'number' ? value : String(value));
  if (isNaN(date.getTime())) {
    return String(value);
  }
  switch (format) {
    case 'iso': return date.toISOString();
    case 'utc': return date.toUTCString();
    case 'epoch': return String(date.getTime());
    case 'locale':
    default: return date.toLocaleString();
  }
}

// Format a cell value for display — JSON-stringify objects/arrays
function formatCellValue(value: unknown): string {
  if (value == null) {
    return '';
  }
  if (typeof value === 'object') {
    return JSON.stringify(value);
  }
  return String(value);
}

// Custom comparator for AG Grid date filter — parses Drill date/timestamp strings
function dateComparator(filterDate: Date, cellValue: string): number {
  if (!cellValue) {
    return -1;
  }
  const cellDate = new Date(cellValue);
  if (isNaN(cellDate.getTime())) {
    return 0;
  }
  const filterTime = filterDate.getTime();
  const cellTime = cellDate.getTime();
  if (cellTime < filterTime) {
    return -1;
  }
  if (cellTime > filterTime) {
    return 1;
  }
  return 0;
}

interface ColumnTypeConfig {
  cellDataType: string;
  filter: string;
  filterParams?: Record<string, unknown>;
  filterValueGetter?: (params: { data: Record<string, unknown>; colDef: { field?: string } }) => unknown;
}

// Determine column filter configuration from Drill metadata type
function getColumnConfig(metadataType?: string): ColumnTypeConfig {
  if (!metadataType) {
    return { cellDataType: 'text', filter: 'agTextColumnFilter' };
  }

  const type = metadataType.toUpperCase();

  // Numeric types
  if (type.includes('INT') || type.includes('FLOAT') || type.includes('DOUBLE') || type.includes('DECIMAL') || type.includes('NUMERIC')) {
    return {
      cellDataType: 'text',
      filter: 'agNumberColumnFilter',
      filterParams: {
        buttons: ['reset', 'apply'],
        filterOptions: [
          'equals', 'notEqual',
          'lessThan', 'lessThanOrEqual',
          'greaterThan', 'greaterThanOrEqual',
          'inRange', 'blank', 'notBlank',
        ],
      },
      filterValueGetter: (params: { data: Record<string, unknown>; colDef: { field?: string } }) => {
        const value = params.data?.[params.colDef.field || ''];
        if (value == null || value === '') {
          return null;
        }
        const num = Number(value);
        return isNaN(num) ? null : num;
      },
    };
  }

  // Date and Timestamp types
  if (type.includes('DATE') || type.includes('TIMESTAMP')) {
    return {
      cellDataType: 'text',
      filter: 'agDateColumnFilter',
      filterParams: {
        buttons: ['reset', 'apply'],
        filterOptions: [
          'equals', 'notEqual',
          'lessThan', 'greaterThan',
          'inRange', 'blank', 'notBlank',
        ],
        comparator: dateComparator,
      },
    };
  }

  // Time types — no AG Grid time-specific filter
  if (type.includes('TIME')) {
    return { cellDataType: 'text', filter: 'agTextColumnFilter' };
  }

  // Boolean
  if (type.includes('BOOL')) {
    return { cellDataType: 'text', filter: 'agTextColumnFilter' };
  }

  // Default: text
  return { cellDataType: 'text', filter: 'agTextColumnFilter' };
}
