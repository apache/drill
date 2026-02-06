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
import { useMemo, useCallback, useRef } from 'react';
import { AgGridReact } from 'ag-grid-react';
import { Button, Space, Dropdown, Typography, Alert, Empty } from 'antd';
import {
  DownloadOutlined,
  CopyOutlined,
  BarChartOutlined,
  ExpandOutlined,
} from '@ant-design/icons';
import type { ColDef, GridReadyEvent, GridApi } from 'ag-grid-community';
import type { MenuProps } from 'antd';
import type { QueryResult, QueryError } from '../../types';

import 'ag-grid-community/styles/ag-grid.css';
import 'ag-grid-community/styles/ag-theme-alpine.css';

const { Text } = Typography;

interface ResultsGridProps {
  results?: QueryResult;
  error?: QueryError;
  isLoading?: boolean;
  onCreateVisualization?: () => void;
}

export default function ResultsGrid({
  results,
  error,
  isLoading,
  onCreateVisualization,
}: ResultsGridProps) {
  const gridRef = useRef<AgGridReact>(null);
  const gridApiRef = useRef<GridApi | null>(null);

  // Generate column definitions from results
  const columnDefs = useMemo<ColDef[]>(() => {
    if (!results?.columns) return [];

    return results.columns.map((col, index) => ({
      field: col,
      headerName: col,
      sortable: true,
      filter: true,
      resizable: true,
      minWidth: 100,
      // Use metadata for type-specific formatting
      cellDataType: getColumnType(results.metadata?.[index]),
    }));
  }, [results]);

  // Convert row data
  const rowData = useMemo(() => {
    if (!results?.rows) return [];
    return results.rows;
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
      .map((row) => results.columns.map((col) => String(row[col] ?? '')).join('\t'))
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
            filter: true,
            resizable: true,
            floatingFilter: true,
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
    </div>
  );
}

// Helper to determine column type from metadata
function getColumnType(metadataType?: string): string {
  if (!metadataType) return 'text';

  const type = metadataType.toUpperCase();
  if (type.includes('INT') || type.includes('FLOAT') || type.includes('DOUBLE') || type.includes('DECIMAL')) {
    return 'number';
  }
  if (type.includes('DATE')) {
    return 'date';
  }
  if (type.includes('TIME')) {
    return 'text'; // AG Grid handles timestamps as text
  }
  if (type.includes('BOOL')) {
    return 'boolean';
  }
  return 'text';
}
