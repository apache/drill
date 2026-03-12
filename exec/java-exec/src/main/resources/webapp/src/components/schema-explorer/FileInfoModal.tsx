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
import { Modal, Descriptions, Table, Tag } from 'antd';
import type { FileInfo, ColumnInfo } from '../../types';

interface FileInfoModalProps {
  open: boolean;
  onClose: () => void;
  fileInfo: FileInfo | null;
  qualifiedName: string;
  columns: ColumnInfo[];
}

function formatFileSize(bytes: number): string {
  if (bytes === 0) {
    return '0 B';
  }
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  const size = bytes / Math.pow(1024, i);
  return `${size.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

function getFileExtension(name: string): string {
  const dot = name.lastIndexOf('.');
  return dot >= 0 ? name.substring(dot + 1).toLowerCase() : '';
}

const columnTableCols = [
  { title: 'Column', dataIndex: 'name', key: 'name' },
  {
    title: 'Type',
    dataIndex: 'type',
    key: 'type',
    render: (type: string) => <Tag>{type}</Tag>,
  },
];

export default function FileInfoModal({ open, onClose, fileInfo, qualifiedName, columns }: FileInfoModalProps) {
  if (!fileInfo) {
    return null;
  }

  const ext = getFileExtension(fileInfo.name);

  return (
    <Modal
      title={fileInfo.name}
      open={open}
      onCancel={onClose}
      footer={null}
      width={560}
    >
      <Descriptions column={1} size="small" bordered style={{ marginBottom: columns.length > 0 ? 16 : 0 }}>
        <Descriptions.Item label="Path">{qualifiedName}</Descriptions.Item>
        <Descriptions.Item label="Size">{formatFileSize(fileInfo.length)}</Descriptions.Item>
        {ext && <Descriptions.Item label="Format">{ext.toUpperCase()}</Descriptions.Item>}
        {fileInfo.owner && <Descriptions.Item label="Owner">{fileInfo.owner}</Descriptions.Item>}
        {fileInfo.group && <Descriptions.Item label="Group">{fileInfo.group}</Descriptions.Item>}
        {fileInfo.permissions && <Descriptions.Item label="Permissions">{fileInfo.permissions}</Descriptions.Item>}
        {fileInfo.modificationTime && (
          <Descriptions.Item label="Modified">{fileInfo.modificationTime}</Descriptions.Item>
        )}
        {columns.length > 0 && (
          <Descriptions.Item label="Columns">{columns.length}</Descriptions.Item>
        )}
      </Descriptions>

      {columns.length > 0 && (
        <Table
          dataSource={columns.map((c, i) => ({ ...c, key: i }))}
          columns={columnTableCols}
          size="small"
          pagination={columns.length > 20 ? { pageSize: 20, size: 'small' } : false}
          scroll={columns.length > 10 ? { y: 300 } : undefined}
        />
      )}
    </Modal>
  );
}
