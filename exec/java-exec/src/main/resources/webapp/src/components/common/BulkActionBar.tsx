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
import { Button, Space, Typography, Popconfirm } from 'antd';
import { DeleteOutlined, FolderOutlined, CloseOutlined } from '@ant-design/icons';

const { Text } = Typography;

interface BulkActionBarProps {
  selectedCount: number;
  onAddToProject?: () => void;
  onDelete?: () => void;
  onClear: () => void;
}

export default function BulkActionBar({ selectedCount, onAddToProject, onDelete, onClear }: BulkActionBarProps) {
  if (selectedCount === 0) {
    return null;
  }

  return (
    <div style={{
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'space-between',
      padding: '8px 16px',
      background: 'var(--color-bg-elevated)',
      border: '1px solid var(--color-primary)',
      borderRadius: 8,
      marginBottom: 12,
    }}>
      <Text strong>{selectedCount} selected</Text>
      <Space>
        {onAddToProject && (
          <Button size="small" icon={<FolderOutlined />} onClick={onAddToProject}>
            Add to Project
          </Button>
        )}
        {onDelete && (
          <Popconfirm
            title={`Delete ${selectedCount} item${selectedCount > 1 ? 's' : ''}?`}
            description="This action cannot be undone."
            onConfirm={onDelete}
            okText="Delete"
            cancelText="Cancel"
            okButtonProps={{ danger: true }}
          >
            <Button size="small" danger icon={<DeleteOutlined />}>
              Delete
            </Button>
          </Popconfirm>
        )}
        <Button size="small" type="text" icon={<CloseOutlined />} onClick={onClear}>
          Clear
        </Button>
      </Space>
    </div>
  );
}
