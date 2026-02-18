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
import { Button, Space, Dropdown, Typography, Alert, Empty, Modal, Checkbox, Divider } from 'antd';
import {
  DownloadOutlined,
  BarChartOutlined,
  ExpandOutlined,
  RobotOutlined,
  UnorderedListOutlined,
  SortAscendingOutlined,
  ApiOutlined,
} from '@ant-design/icons';
import type { ColDef, GridReadyEvent, GridApi, SortModelItem } from 'ag-grid-community';
import type { MenuProps } from 'antd';
import type { QueryResult, QueryError } from '../../types';
import JsonCellRenderer from './JsonCellRenderer';
import CustomHeader from './CustomHeader';
import type { ColumnTransformation } from '../../utils/sqlTransformations';

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
  onTransformColumn?: (columnName: string, transformation: ColumnTransformation) => void;
  onShareApi?: () => void;
}

export default function ResultsGrid({
  results,
  error,
  isLoading,
  onCreateVisualization,
  resultsSettings,
  onFixWithProspector,
  prospectorAvailable,
  onTransformColumn,
  onShareApi,
}: ResultsGridProps) {
  const gridRef = useRef<AgGridReact>(null);
  const gridApiRef = useRef<GridApi | null>(null);
  const [columnManagerOpen, setColumnManagerOpen] = useState(false);
  const [sortManagerOpen, setSortManagerOpen] = useState(false);
  const [columnVisibility, setColumnVisibility] = useState<Record<string, boolean>>({});

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
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        headerComponent: CustomHeader as any,
        headerComponentParams: {
          onTransformColumn,
          rowData: results.rows,
          metadata: metadataType,
          columnIndex: index,
        },
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
  }, [results, timestampFormat, onTransformColumn]);

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

  const saveColumnState = useCallback(() => {
    const state = gridApiRef.current?.getColumnState();
    if (state) {
      try {
        localStorage.setItem('drill-grid-column-state', JSON.stringify(state));
      } catch {
        // Ignore storage errors
      }
    }
  }, []);

  const onGridReady = useCallback((params: GridReadyEvent) => {
    gridApiRef.current = params.api;

    // Restore saved column state
    try {
      const savedState = localStorage.getItem('drill-grid-column-state');
      if (savedState) {
        params.api.applyColumnState({ state: JSON.parse(savedState), applyOrder: true });
      }
    } catch {
      // Ignore storage errors
    }

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

  const syncColumnVisibility = useCallback(() => {
    if (gridApiRef.current && results?.columns) {
      const vis: Record<string, boolean> = {};
      results.columns.forEach((col) => {
        const column = gridApiRef.current!.getColumn(col);
        vis[col] = column ? column.isVisible() : true;
      });
      setColumnVisibility(vis);
    }
  }, [results?.columns]);

  const handleShowAllColumns = useCallback(() => {
    if (gridApiRef.current && results?.columns) {
      results.columns.forEach((col) => {
        gridApiRef.current!.setColumnsVisible([col], true);
      });
      syncColumnVisibility();
      saveColumnState();
    }
  }, [results?.columns, saveColumnState, syncColumnVisibility]);

  // Sort manager state
  const getSortModel = useCallback(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    () => (gridApiRef.current as any)?.getSortModel?.() || [],
    []
  );

  const handleClearSort = useCallback(() => {
    if (gridApiRef.current) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (gridApiRef.current as any).setSortModel?.([]);
      saveColumnState();
    }
  }, [saveColumnState]);

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
            icon={<ApiOutlined />}
            onClick={onShareApi}
            size="small"
          >
            Share as API
          </Button>
          <Button
            icon={<UnorderedListOutlined />}
            onClick={() => {
              syncColumnVisibility();
              setColumnManagerOpen(true);
            }}
            size="small"
          >
            Columns
          </Button>
          <Button
            icon={<SortAscendingOutlined />}
            onClick={() => setSortManagerOpen(true)}
            size="small"
          >
            Sort
          </Button>
          <Button
            icon={<ExpandOutlined />}
            onClick={() => gridApiRef.current?.sizeColumnsToFit()}
            size="small"
          >
            Fit Columns
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
          onColumnVisible={() => saveColumnState()}
          onColumnPinned={() => saveColumnState()}
          onSortChanged={() => saveColumnState()}
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

      {/* Column Manager Modal */}
      <Modal
        title="Manage Columns"
        open={columnManagerOpen}
        onCancel={() => setColumnManagerOpen(false)}
        onOk={() => setColumnManagerOpen(false)}
        width={400}
      >
        <Space direction="vertical" style={{ width: '100%' }}>
          <Button
            block
            onClick={handleShowAllColumns}
          >
            Show All Columns
          </Button>
          <Divider style={{ margin: '8px 0' }} />
          {results?.columns && (
            <div style={{ maxHeight: 400, overflowY: 'auto' }}>
              {results.columns.map((col) => (
                <div
                  key={col}
                  style={{ marginBottom: 8 }}
                >
                  <Checkbox
                    checked={columnVisibility[col] !== false}
                    onChange={(e) => {
                      gridApiRef.current?.setColumnsVisible([col], e.target.checked);
                      setColumnVisibility((prev) => ({ ...prev, [col]: e.target.checked }));
                      saveColumnState();
                    }}
                  >
                    {col}
                  </Checkbox>
                </div>
              ))}
            </div>
          )}
        </Space>
      </Modal>

      {/* Sort Manager Modal */}
      <Modal
        title="Manage Sorting"
        open={sortManagerOpen}
        onCancel={() => setSortManagerOpen(false)}
        onOk={() => setSortManagerOpen(false)}
        width={400}
      >
        <Space direction="vertical" style={{ width: '100%' }}>
          <Button
            block
            onClick={handleClearSort}
            danger
          >
            Clear All Sorts
          </Button>
          <Divider style={{ margin: '8px 0' }} />
          {getSortModel().length > 0 ? (
            <div>
              <Text strong>Current Sort Order:</Text>
              <div style={{ marginTop: 8 }}>
                {getSortModel().map((sort: SortModelItem, index: number) => (
                  <div
                    key={`${sort.colId}-${index}`}
                    style={{ marginBottom: 8 }}
                  >
                    <Text>
                      {index + 1}. {sort.colId} ({sort.sort?.toUpperCase()})
                    </Text>
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <Empty description="No sorts applied" />
          )}
        </Space>
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
