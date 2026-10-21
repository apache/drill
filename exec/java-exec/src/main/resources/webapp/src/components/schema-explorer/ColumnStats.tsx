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
import { useState, useCallback } from 'react';
import { Drawer, Table, Spin, Typography, Alert, Button } from 'antd';
import { BarChartOutlined } from '@ant-design/icons';
import { executeQuery } from '../../api/queries';

const { Text } = Typography;

interface ColumnStatsProps {
  open: boolean;
  onClose: () => void;
  schemaName: string;
  tableName: string;
}

interface StatRow {
  column_name: string;
  total_count: string;
  distinct_count: string;
  min_value: string;
  max_value: string;
}

export default function ColumnStats({ open, onClose, schemaName, tableName }: ColumnStatsProps) {
  const [loading, setLoading] = useState(false);
  const [stats, setStats] = useState<StatRow[]>([]);
  const [error, setError] = useState<string | null>(null);

  const loadStats = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      // First get the columns via DESCRIBE
      const descResult = await executeQuery({
        query: `DESCRIBE \`${schemaName}\`.\`${tableName}\``,
        queryType: 'SQL',
      });

      const columns = descResult.rows.map((r) => String(r['COLUMN_NAME'] || r['column_name'] || ''));
      const filteredColumns = columns.filter((c) => c.length > 0).slice(0, 20); // Limit to 20

      if (filteredColumns.length === 0) {
        setStats([]);
        setLoading(false);
        return;
      }

      // Build a single query to get stats for all columns
      const selectParts = filteredColumns.map(
        (col) =>
          `COUNT(\`${col}\`) AS \`cnt_${col}\`, ` +
          `COUNT(DISTINCT \`${col}\`) AS \`dcnt_${col}\`, ` +
          `MIN(\`${col}\`) AS \`min_${col}\`, ` +
          `MAX(\`${col}\`) AS \`max_${col}\``
      );

      const statsResult = await executeQuery({
        query: `SELECT ${selectParts.join(', ')} FROM \`${schemaName}\`.\`${tableName}\` LIMIT 10000`,
        queryType: 'SQL',
        autoLimitRowCount: 1,
      });

      const row = statsResult.rows[0] || {};
      const parsed: StatRow[] = filteredColumns.map((col) => ({
        column_name: col,
        total_count: String(row[`cnt_${col}`] ?? 'N/A'),
        distinct_count: String(row[`dcnt_${col}`] ?? 'N/A'),
        min_value: String(row[`min_${col}`] ?? 'N/A'),
        max_value: String(row[`max_${col}`] ?? 'N/A'),
      }));
      setStats(parsed);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load statistics');
    } finally {
      setLoading(false);
    }
  }, [schemaName, tableName]);

  // Auto-load on open
  const handleAfterOpenChange = useCallback(
    (visible: boolean) => {
      if (visible && stats.length === 0 && !loading) {
        loadStats();
      }
    },
    [stats.length, loading, loadStats],
  );

  const tableColumns = [
    { title: 'Column', dataIndex: 'column_name', key: 'column_name', width: 140 },
    { title: 'Count', dataIndex: 'total_count', key: 'total_count', width: 80 },
    { title: 'Distinct', dataIndex: 'distinct_count', key: 'distinct_count', width: 80 },
    { title: 'Min', dataIndex: 'min_value', key: 'min_value', ellipsis: true },
    { title: 'Max', dataIndex: 'max_value', key: 'max_value', ellipsis: true },
  ];

  return (
    <Drawer
      title={
        <span>
          <BarChartOutlined style={{ marginRight: 8 }} />
          Column Statistics: <Text code>{`${schemaName}.${tableName}`}</Text>
        </span>
      }
      placement="right"
      width={560}
      open={open}
      onClose={onClose}
      afterOpenChange={handleAfterOpenChange}
      extra={
        <Button size="small" onClick={loadStats} loading={loading}>
          Refresh
        </Button>
      }
    >
      {error && <Alert type="error" message={error} style={{ marginBottom: 12 }} showIcon closable />}
      {loading ? (
        <div style={{ textAlign: 'center', padding: 40 }}>
          <Spin tip="Computing statistics..." />
        </div>
      ) : (
        <Table
          dataSource={stats}
          columns={tableColumns}
          rowKey="column_name"
          size="small"
          pagination={false}
          scroll={{ y: 500 }}
        />
      )}
    </Drawer>
  );
}
