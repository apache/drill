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
import { Button, Select, Space, Typography, Spin, Tooltip } from 'antd';
import {
  LeftOutlined,
  RightOutlined,
  DoubleLeftOutlined,
  DoubleRightOutlined,
  CloudServerOutlined,
} from '@ant-design/icons';
import type { ServerPaginationState } from '../../hooks/useServerPagination';
import { SERVER_PAGE_SIZE_OPTIONS } from '../../hooks/useServerPagination';

const { Text } = Typography;

interface ServerPaginationBarProps {
  state: ServerPaginationState;
  onGoToPage: (page: number) => void;
  onNextPage: () => void;
  onPrevPage: () => void;
  onPageSizeChange: (size: number) => void;
}

export default function ServerPaginationBar({
  state,
  onGoToPage,
  onNextPage,
  onPrevPage,
  onPageSizeChange,
}: ServerPaginationBarProps) {
  if (!state.enabled) {
    return null;
  }

  const { currentPage, totalPages, totalRows, pageSize, offset, isLoading } = state;
  const rangeStart = offset + 1;
  const rangeEnd = Math.min(offset + pageSize, totalRows);

  return (
    <div className="server-pagination-bar">
      <Space size="small">
        <Tooltip title="Server-side pagination active">
          <CloudServerOutlined style={{ color: '#1890ff', fontSize: 14 }} />
        </Tooltip>
        <Text type="secondary" style={{ fontSize: 12 }}>
          Rows {rangeStart.toLocaleString()}–{rangeEnd.toLocaleString()} of {totalRows.toLocaleString()}
        </Text>
      </Space>

      <Space size="small">
        {isLoading && <Spin size="small" />}

        <Select
          size="small"
          value={pageSize}
          onChange={onPageSizeChange}
          style={{ width: 110 }}
          options={SERVER_PAGE_SIZE_OPTIONS.map((s) => ({
            value: s,
            label: `${s.toLocaleString()} / page`,
          }))}
        />

        <Button
          size="small"
          icon={<DoubleLeftOutlined />}
          disabled={currentPage <= 1 || isLoading}
          onClick={() => onGoToPage(1)}
        />
        <Button
          size="small"
          icon={<LeftOutlined />}
          disabled={currentPage <= 1 || isLoading}
          onClick={onPrevPage}
        />

        <Text style={{ fontSize: 12, minWidth: 60, textAlign: 'center' }}>
          {currentPage} / {totalPages}
        </Text>

        <Button
          size="small"
          icon={<RightOutlined />}
          disabled={currentPage >= totalPages || isLoading}
          onClick={onNextPage}
        />
        <Button
          size="small"
          icon={<DoubleRightOutlined />}
          disabled={currentPage >= totalPages || isLoading}
          onClick={() => onGoToPage(totalPages)}
        />
      </Space>
    </div>
  );
}
