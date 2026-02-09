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
import { useCallback } from 'react';
import { Dropdown, message } from 'antd';
import type { MenuProps } from 'antd';
import {
  CopyOutlined,
  ReloadOutlined,
  TableOutlined,
  FileSearchOutlined,
  BarChartOutlined,
  StarOutlined,
  StarFilled,
} from '@ant-design/icons';

export type NodeType = 'plugin' | 'schema' | 'table' | 'file' | 'column';

export interface ContextMenuProps {
  nodeType: NodeType;
  nodeKey: string;
  qualifiedName: string;
  children: React.ReactNode;
  onInsertText?: (text: string) => void;
  onRefreshNode?: (key: string) => void;
  onShowStats?: (schemaName: string, tableName: string) => void;
  isFavorite?: boolean;
  onToggleFavorite?: (key: string) => void;
}

function copyToClipboard(text: string) {
  navigator.clipboard.writeText(text).then(
    () => message.success('Copied to clipboard'),
    () => message.error('Failed to copy'),
  );
}

export default function ContextMenu({
  nodeType,
  nodeKey,
  qualifiedName,
  children,
  onInsertText,
  onRefreshNode,
  onShowStats,
  isFavorite,
  onToggleFavorite,
}: ContextMenuProps) {
  const handleMenuClick = useCallback(
    (info: { key: string }) => {
      switch (info.key) {
        case 'copy':
          copyToClipboard(qualifiedName);
          break;
        case 'refresh':
          onRefreshNode?.(nodeKey);
          break;
        case 'select-star': {
          const sql = `SELECT *\nFROM ${qualifiedName}\nLIMIT 100`;
          onInsertText?.(sql);
          break;
        }
        case 'describe': {
          const sql = `DESCRIBE ${qualifiedName}`;
          onInsertText?.(sql);
          break;
        }
        case 'use-schema': {
          const sql = `USE ${qualifiedName}`;
          onInsertText?.(sql);
          break;
        }
        case 'show-stats': {
          // Parse schema and table from key
          const parts = nodeKey.split(':');
          if (parts.length >= 3) {
            onShowStats?.(parts[1], parts.slice(2).join(':'));
          }
          break;
        }
        case 'toggle-favorite':
          onToggleFavorite?.(nodeKey);
          break;
      }
    },
    [qualifiedName, nodeKey, onInsertText, onRefreshNode, onShowStats, onToggleFavorite],
  );

  const items: MenuProps['items'] = [];

  // Copy name – always available
  items.push({ key: 'copy', label: 'Copy Name', icon: <CopyOutlined /> });

  // Favorite toggle for tables, schemas, files
  if (nodeType !== 'column' && onToggleFavorite) {
    items.push({
      key: 'toggle-favorite',
      label: isFavorite ? 'Remove from Favorites' : 'Add to Favorites',
      icon: isFavorite ? <StarFilled style={{ color: '#faad14' }} /> : <StarOutlined />,
    });
  }

  items.push({ type: 'divider' });

  if (nodeType === 'plugin') {
    items.push({ key: 'refresh', label: 'Refresh Schemas', icon: <ReloadOutlined /> });
  }

  if (nodeType === 'schema') {
    items.push({ key: 'use-schema', label: 'Generate USE', icon: <TableOutlined /> });
    items.push({ key: 'refresh', label: 'Refresh', icon: <ReloadOutlined /> });
  }

  if (nodeType === 'table' || nodeType === 'file') {
    items.push({ key: 'select-star', label: 'Generate SELECT *', icon: <FileSearchOutlined /> });
    items.push({ key: 'describe', label: 'Generate DESCRIBE', icon: <TableOutlined /> });
    if (nodeType === 'table') {
      items.push({ key: 'show-stats', label: 'Show Statistics', icon: <BarChartOutlined /> });
    }
    items.push({ key: 'refresh', label: 'Refresh Columns', icon: <ReloadOutlined /> });
  }

  return (
    <Dropdown menu={{ items, onClick: handleMenuClick }} trigger={['contextMenu']}>
      {children}
    </Dropdown>
  );
}
