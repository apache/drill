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

import { Button, Popover } from 'antd';
import { MoreOutlined } from '@ant-design/icons';
import type { IHeaderParams } from 'ag-grid-community';
import ColumnMenu from './ColumnMenu';
import type { ColumnTransformation } from '../../utils/sqlTransformations';

export interface CustomHeaderProps extends IHeaderParams {
  rowData?: Record<string, unknown>[];
  onTransformColumn?: (columnName: string, transformation: ColumnTransformation) => void;
}

export default function CustomHeader(props: CustomHeaderProps) {
  const { displayName, columnApi, rowData, onTransformColumn } = props;

  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        width: '100%',
        height: '100%',
      }}
    >
      <span style={{ flex: 1 }}>{displayName}</span>
      <Popover
        content={
          <ColumnMenu
            columnName={displayName || ''}
            rowData={rowData || []}
            columnApi={columnApi || undefined}
            onTransformColumn={onTransformColumn}
          />
        }
        trigger="click"
        placement="bottomRight"
      >
        <Button
          type="text"
          size="small"
          icon={<MoreOutlined />}
          style={{ padding: '0 4px' }}
        />
      </Popover>
    </div>
  );
}
