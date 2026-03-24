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
import { Button, Space, Dropdown, Typography, Alert, Empty, Modal, Checkbox, Divider, Grid, message } from 'antd';
import {
  DownloadOutlined,
  BarChartOutlined,
  ColumnWidthOutlined,
  ColumnHeightOutlined,
  RobotOutlined,
  UnorderedListOutlined,
  SortAscendingOutlined,
  ApiOutlined,
  EyeOutlined,
  ProfileOutlined,
  UndoOutlined,
  FundProjectionScreenOutlined,
  ClearOutlined,
  ReloadOutlined,
  FilterOutlined,
} from '@ant-design/icons';
import type { ColDef, GridReadyEvent, GridApi, SortModelItem } from 'ag-grid-community';
import type { MenuProps } from 'antd';
import type { QueryResult, QueryError } from '../../types';
import JsonCellRenderer from './JsonCellRenderer';
import CustomHeader from './CustomHeader';
import type { ColumnTransformation } from '../../utils/sqlTransformations';
import { useTheme } from '../../hooks/useTheme';
import { getDownloadUrl } from '../../api/resultCache';
import { useServerPagination } from '../../hooks/useServerPagination';
import ServerPaginationBar from './ServerPaginationBar';

import { DataProfiler } from '../data-profiler';

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
  cacheId?: string; // Backend cache ID for server-side exports
  sql?: string; // Current SQL for data profiling
  onClearResults?: () => void;
  onRerun?: () => void;
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
  cacheId,
  sql,
  onClearResults,
  onRerun,
}: ResultsGridProps) {
  const gridRef = useRef<AgGridReact>(null);
  const gridApiRef = useRef<GridApi | null>(null);
  const [columnManagerOpen, setColumnManagerOpen] = useState(false);
  const [sortManagerOpen, setSortManagerOpen] = useState(false);
  const [columnVisibility, setColumnVisibility] = useState<Record<string, boolean>>({});
  const [profilerOpen, setProfilerOpen] = useState(false);
  const [showFilters, setShowFilters] = useState(false);
  const screens = Grid.useBreakpoint();
  const isCompact = !screens.lg;
  const { isDark } = useTheme();

  const timestampFormat = resultsSettings?.timestampDisplayFormat ?? DEFAULT_RESULTS_SETTINGS.timestampDisplayFormat;

  // Server-side pagination: loads pages from backend when results are large
  const serverPagination = useServerPagination(cacheId, results);
  const displayResults = serverPagination.effectiveResults || results;

  // Generate column definitions from results with type-aware filters
  const columnDefs = useMemo<ColDef[]>(() => {
    if (!displayResults?.columns) return [];

    return displayResults.columns.map((col, index) => {
      const metadataType = displayResults.metadata?.[index];
      const config = getColumnConfig(metadataType);
      const isDateColumn = metadataType && (metadataType.toUpperCase().includes('DATE') || metadataType.toUpperCase().includes('TIMESTAMP'));

      const colDef: ColDef = {
        field: col,
        headerName: col,
        sortable: true,
        resizable: true,
        minWidth: Math.max(100, col.length * 9 + 60),
        filter: config.filter,
        filterParams: config.filterParams,
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        headerComponent: CustomHeader as any,
        headerComponentParams: {
          onTransformColumn,
          rowData: displayResults.rows,
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
  }, [displayResults, timestampFormat, onTransformColumn]);

  // Convert row data — stringify nested objects/arrays so AG Grid doesn't show [object Object]
  const rowData = useMemo(() => {
    if (!displayResults?.rows) return [];
    return displayResults.rows.map((row) => {
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
  }, [displayResults]);

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

  // Export functions — prefer backend streaming download when cacheId is available
  const exportToCsv = useCallback(() => {
    if (cacheId) {
      // Stream from backend (handles large results without browser memory issues)
      const link = document.createElement('a');
      link.href = getDownloadUrl(cacheId, 'csv');
      link.download = `drill-query-${results?.queryId || 'results'}.csv`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
    } else {
      gridApiRef.current?.exportDataAsCsv({
        fileName: `drill-query-${results?.queryId || 'results'}.csv`,
      });
    }
  }, [results?.queryId, cacheId]);

  const exportToJson = useCallback(() => {
    if (cacheId) {
      // Stream from backend
      const link = document.createElement('a');
      link.href = getDownloadUrl(cacheId, 'json');
      link.download = `drill-query-${results?.queryId || 'results'}.json`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
    } else if (results) {
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
    }
  }, [results, cacheId]);

  const syncColumnVisibility = useCallback(() => {
    if (gridApiRef.current && displayResults?.columns) {
      const vis: Record<string, boolean> = {};
      displayResults.columns.forEach((col) => {
        const column = gridApiRef.current!.getColumn(col);
        vis[col] = column ? column.isVisible() : true;
      });
      setColumnVisibility(vis);
    }
  }, [displayResults?.columns]);

  const handleShowAllColumns = useCallback(() => {
    if (gridApiRef.current && displayResults?.columns) {
      displayResults.columns.forEach((col) => {
        gridApiRef.current!.setColumnsVisible([col], true);
      });
      syncColumnVisibility();
      saveColumnState();
    }
  }, [displayResults?.columns, saveColumnState, syncColumnVisibility]);

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

  const copyAsTsv = useCallback(() => {
    if (!displayResults) return;
    const cols = displayResults.columns;
    const header = cols.join('\t');
    const body = displayResults.rows
      .map((row) => cols.map((c) => String(row[c] ?? '')).join('\t'))
      .join('\n');
    navigator.clipboard.writeText(`${header}\n${body}`).then(() => {
      message.success('Copied as TSV', 1);
    }).catch(() => {});
  }, [displayResults]);

  const copyAsMarkdown = useCallback(() => {
    if (!displayResults) return;
    const cols = displayResults.columns;
    const escape = (v: unknown) => String(v ?? '').replace(/\\/g, '\\\\').replace(/\|/g, '\\|');
    const header = `| ${cols.map(escape).join(' | ')} |`;
    const sep = `| ${cols.map(() => '---').join(' | ')} |`;
    const body = displayResults.rows
      .map((row) => `| ${cols.map((c) => escape(row[c])).join(' | ')} |`)
      .join('\n');
    navigator.clipboard.writeText(`${header}\n${sep}\n${body}`).then(() => {
      message.success('Copied as Markdown', 1);
    }).catch(() => {});
  }, [displayResults]);

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
    { type: 'divider' },
    {
      key: 'tsv',
      label: 'Copy as TSV',
      onClick: copyAsTsv,
    },
    {
      key: 'markdown',
      label: 'Copy as Markdown',
      onClick: copyAsMarkdown,
    },
  ];

  const analyzeMenuItems: MenuProps['items'] = [
    {
      key: 'chart',
      icon: <BarChartOutlined />,
      label: 'Create Chart',
      onClick: onCreateVisualization,
    },
    {
      key: 'profile',
      icon: <ProfileOutlined />,
      label: 'Profile Data',
      onClick: () => setProfilerOpen(true),
    },
    { type: 'divider' },
    {
      key: 'share',
      icon: <ApiOutlined />,
      label: 'Share as API',
      onClick: onShareApi,
    },
  ];

  const viewMenuItems: MenuProps['items'] = [
    {
      key: 'columns',
      icon: <UnorderedListOutlined />,
      label: 'Manage Columns',
      onClick: () => { syncColumnVisibility(); setColumnManagerOpen(true); },
    },
    {
      key: 'sort',
      icon: <SortAscendingOutlined />,
      label: 'Manage Sort',
      onClick: () => setSortManagerOpen(true),
    },
    { type: 'divider' },
    {
      key: 'autosize',
      icon: <ColumnWidthOutlined />,
      label: 'Auto-size to Content',
      onClick: () => gridApiRef.current?.autoSizeAllColumns(),
    },
    {
      key: 'fit',
      icon: <ColumnHeightOutlined />,
      label: 'Fit to Grid Width',
      onClick: () => gridApiRef.current?.sizeColumnsToFit(),
    },
    { type: 'divider' },
    {
      key: 'reset',
      icon: <UndoOutlined />,
      label: 'Reset Column Layout',
      onClick: () => {
        localStorage.removeItem('drill-grid-column-state');
        gridApiRef.current?.resetColumnState();
        gridApiRef.current?.sizeColumnsToFit();
      },
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
  if (!displayResults && !isLoading) {
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
          <Dropdown menu={{ items: exportMenuItems }} trigger={['click']}>
            <Button icon={<DownloadOutlined />} size="small">
              {!isCompact && 'Export'}
            </Button>
          </Dropdown>
          <Dropdown menu={{ items: analyzeMenuItems }} trigger={['click']}>
            <Button icon={<FundProjectionScreenOutlined />} size="small">
              {!isCompact && 'Analyze'}
            </Button>
          </Dropdown>
          <Dropdown menu={{ items: viewMenuItems }} trigger={['click']}>
            <Button icon={<EyeOutlined />} size="small">
              {!isCompact && 'View'}
            </Button>
          </Dropdown>
          <Button
            icon={<FilterOutlined />}
            size="small"
            type={showFilters ? 'primary' : 'text'}
            onClick={() => setShowFilters(!showFilters)}
          >
            {!isCompact && (showFilters ? 'Hide Filters' : 'Show Filters')}
          </Button>
        </Space>
        <Space>
          {onRerun && (
            <Button icon={<ReloadOutlined />} size="small" onClick={onRerun}>
              {!isCompact && 'Re-run'}
            </Button>
          )}
          {onClearResults && (
            <Button icon={<ClearOutlined />} size="small" onClick={() => {
              Modal.confirm({
                title: 'Clear Results',
                content: 'Are you sure you want to clear the current results?',
                okText: 'Clear',
                okType: 'danger',
                onOk: onClearResults,
              });
            }}>
              {!isCompact && 'Clear'}
            </Button>
          )}
        </Space>
      </div>

      {/* AG Grid */}
      <div className={isDark ? 'ag-theme-alpine-dark' : 'ag-theme-alpine'} style={{ flex: 1, width: '100%' }}>
        <AgGridReact
          ref={gridRef}
          columnDefs={columnDefs}
          rowData={rowData}
          onGridReady={onGridReady}
          onColumnVisible={() => saveColumnState()}
          onColumnPinned={() => saveColumnState()}
          onSortChanged={() => saveColumnState()}
          onCellDoubleClicked={(params) => {
            const val = params.value != null ? String(params.value) : '';
            navigator.clipboard.writeText(val).then(() => {
              message.success('Copied to clipboard', 1);
            }).catch(() => {});
          }}
          defaultColDef={{
            sortable: true,
            resizable: true,
            floatingFilter: showFilters,
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

      {/* Results footer: row count + query ID */}
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        padding: '4px 12px',
        borderTop: '1px solid var(--color-border)',
        fontSize: 12,
        color: 'var(--color-text-secondary)',
        flexShrink: 0,
      }}>
        <Text type="secondary" style={{ fontSize: 12 }}>
          {serverPagination.effectiveTotalRows.toLocaleString()} row{serverPagination.effectiveTotalRows !== 1 ? 's' : ''}
        </Text>
        {displayResults?.queryId && (
          <a
            href={`/sqllab/profiles/${displayResults.queryId}`}
            target="_blank"
            rel="noopener noreferrer"
            style={{ fontSize: 12, color: 'var(--color-text-tertiary)' }}
          >
            {displayResults.queryId}
          </a>
        )}
      </div>

      {/* Server-side pagination bar (shown when result set exceeds threshold) */}
      <ServerPaginationBar
        state={serverPagination.state}
        onGoToPage={serverPagination.goToPage}
        onNextPage={serverPagination.nextPage}
        onPrevPage={serverPagination.prevPage}
        onPageSizeChange={serverPagination.setPageSize}
      />

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
          {displayResults?.columns && (
            <div style={{ maxHeight: 400, overflowY: 'auto' }}>
              {displayResults.columns.map((col) => (
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

      {/* Data Profiler Modal */}
      <DataProfiler
        open={profilerOpen}
        onClose={() => setProfilerOpen(false)}
        cacheId={cacheId}
        columns={displayResults?.columns}
        metadata={displayResults?.metadata}
        totalRows={serverPagination.effectiveTotalRows}
        sql={sql}
      />

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
