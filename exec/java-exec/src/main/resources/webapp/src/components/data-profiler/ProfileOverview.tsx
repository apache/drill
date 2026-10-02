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
import { Row, Col, Statistic, Alert, Tag, Space, Progress } from 'antd';
import {
  TableOutlined,
  ColumnWidthOutlined,
  WarningOutlined,
  DatabaseOutlined,
  FieldNumberOutlined,
} from '@ant-design/icons';
import type { DataProfileResult, ColumnDataType } from '../../types/profile';

function formatBytes(bytes: number): string {
  if (bytes === 0) { return '0 B'; }
  const units = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
}

const TYPE_COLORS: Record<ColumnDataType, string> = {
  numeric: '#1890ff',
  string: '#52c41a',
  temporal: '#722ed1',
  boolean: '#fa8c16',
  other: '#8c8c8c',
};

interface Props {
  profile: DataProfileResult;
}

export default function ProfileOverview({ profile }: Props) {
  const typeCounts: Record<ColumnDataType, number> = { numeric: 0, string: 0, temporal: 0, boolean: 0, other: 0 };
  for (const col of profile.columns) {
    typeCounts[col.dataType]++;
  }

  const highMissingCols = profile.columns.filter((c) => c.missingPct > 50);
  const constantCols = profile.columns.filter((c) => c.distinct <= 1 && c.count > 0);
  const uniqueCols = profile.columns.filter((c) => c.distinct === c.count && c.count > 0);

  return (
    <div>
      <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
        <Col xs={12} sm={8} md={4}>
          <Statistic title="Rows" value={profile.rowCount} prefix={<TableOutlined />} />
        </Col>
        <Col xs={12} sm={8} md={4}>
          <Statistic title="Columns" value={profile.columnCount} prefix={<ColumnWidthOutlined />} />
        </Col>
        <Col xs={12} sm={8} md={4}>
          <Statistic
            title="Missing Cells"
            value={profile.missingCells}
            suffix={<span style={{ fontSize: 14, color: '#8c8c8c' }}>({profile.missingCellsPct.toFixed(1)}%)</span>}
            prefix={<WarningOutlined />}
          />
        </Col>
        <Col xs={12} sm={8} md={4}>
          <Statistic title="Memory Est." value={formatBytes(profile.memorySizeEstimate)} prefix={<DatabaseOutlined />} />
        </Col>
        <Col xs={12} sm={8} md={4}>
          <Statistic title="Sample Size" value={profile.sampleSize} prefix={<FieldNumberOutlined />} />
        </Col>
      </Row>

      {/* Column type breakdown */}
      <div style={{ marginBottom: 24 }}>
        <div style={{ marginBottom: 8, fontWeight: 500 }}>Column Types</div>
        <Space wrap>
          {(Object.entries(typeCounts) as [ColumnDataType, number][])
            .filter(([, count]) => count > 0)
            .map(([type, count]) => (
              <Tag key={type} color={TYPE_COLORS[type]}>
                {type}: {count}
              </Tag>
            ))}
        </Space>
        <Progress
          percent={100}
          success={{ percent: (typeCounts.numeric / profile.columnCount) * 100 }}
          strokeColor="#52c41a"
          trailColor="#f0f0f0"
          showInfo={false}
          style={{ marginTop: 8 }}
        />
      </div>

      {/* Alerts */}
      {highMissingCols.length > 0 && (
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 8 }}
          message={`${highMissingCols.length} column(s) have >50% missing values: ${highMissingCols.map((c) => c.name).join(', ')}`}
        />
      )}
      {constantCols.length > 0 && (
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 8 }}
          message={`${constantCols.length} constant column(s): ${constantCols.map((c) => c.name).join(', ')}`}
        />
      )}
      {uniqueCols.length > 0 && (
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 8 }}
          message={`${uniqueCols.length} column(s) with all unique values (possible IDs): ${uniqueCols.map((c) => c.name).join(', ')}`}
        />
      )}
    </div>
  );
}
