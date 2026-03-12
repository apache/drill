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
  InfoCircleOutlined,
  SettingOutlined,
  ProfileOutlined,
} from '@ant-design/icons';
import type { FileInfo, ColumnInfo } from '../../types';

export type NodeType = 'plugin' | 'schema' | 'table' | 'file' | 'column';

export interface ContextMenuProps {
  nodeType: NodeType;
  nodeKey: string;
  qualifiedName: string;
  columnNames?: string[];
  fileInfo?: FileInfo;
  columnInfos?: ColumnInfo[];
  children: React.ReactNode;
  onInsertText?: (text: string) => void;
  onRefreshNode?: (key: string) => void;
  onShowStats?: (schemaName: string, tableName: string) => void;
  onShowFileInfo?: (fileInfo: FileInfo, qualifiedName: string, columns: ColumnInfo[]) => void;
  onEditPlugin?: (pluginName: string) => void;
  onProfileData?: (schemaName: string, tableName: string) => void;
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
  columnNames,
  fileInfo,
  columnInfos,
  children,
  onInsertText,
  onRefreshNode,
  onShowStats,
  onShowFileInfo,
  onEditPlugin,
  onProfileData,
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
          const cols = columnNames && columnNames.length > 0
            ? columnNames.map((c) => `\`${c}\``).join(',\n       ')
            : '*';
          const sql = `SELECT ${cols}\nFROM ${qualifiedName}\nLIMIT 100`;
          onInsertText?.(sql);
          break;
        }
        case 'describe': {
          if (qualifiedName.startsWith('table(')) {
            // Table function syntax — DESCRIBE doesn't support it, use SELECT * LIMIT 0 instead
            const sql = `SELECT *\nFROM ${qualifiedName}\nLIMIT 0`;
            onInsertText?.(sql);
          } else {
            const sql = `DESCRIBE ${qualifiedName}`;
            onInsertText?.(sql);
          }
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
        case 'file-info':
          if (fileInfo) {
            onShowFileInfo?.(fileInfo, qualifiedName, columnInfos || []);
          }
          break;
        case 'edit-plugin': {
          // Extract plugin name from either "plugin:dfs" or "schema:dfs.tmp"
          const raw = nodeKey.replace(/^(plugin|schema):/, '');
          const pluginName = raw.split('.')[0];
          onEditPlugin?.(pluginName);
          break;
        }
        case 'profile-data': {
          const parts = nodeKey.split(':');
          if (parts.length >= 3) {
            onProfileData?.(parts[1], parts.slice(2).join(':'));
          }
          break;
        }
      }
    },
    [qualifiedName, nodeKey, columnNames, fileInfo, columnInfos, onInsertText, onRefreshNode, onShowStats, onShowFileInfo, onEditPlugin, onProfileData, onToggleFavorite],
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
    if (onEditPlugin) {
      items.push({ key: 'edit-plugin', label: 'Edit Configuration', icon: <SettingOutlined /> });
    }
    items.push({ key: 'refresh', label: 'Refresh Schemas', icon: <ReloadOutlined /> });
  }

  if (nodeType === 'schema') {
    items.push({ key: 'use-schema', label: 'Generate USE', icon: <TableOutlined /> });
    if (onEditPlugin) {
      items.push({ key: 'edit-plugin', label: 'Edit Plugin Configuration', icon: <SettingOutlined /> });
    }
    items.push({ key: 'refresh', label: 'Refresh', icon: <ReloadOutlined /> });
  }

  if (nodeType === 'table' || nodeType === 'file') {
    items.push({ key: 'select-star', label: 'Generate SELECT *', icon: <FileSearchOutlined /> });
    items.push({ key: 'describe', label: 'Generate DESCRIBE', icon: <TableOutlined /> });
    if (nodeType === 'table') {
      items.push({ key: 'show-stats', label: 'Show Statistics', icon: <BarChartOutlined /> });
    }
    if (onProfileData) {
      items.push({ key: 'profile-data', label: 'Profile Data', icon: <ProfileOutlined /> });
    }
    if (nodeType === 'file' && fileInfo && onShowFileInfo) {
      items.push({ key: 'file-info', label: 'Get Info', icon: <InfoCircleOutlined /> });
    }
    items.push({ key: 'refresh', label: 'Refresh Columns', icon: <ReloadOutlined /> });
  }

  return (
    <Dropdown menu={{ items, onClick: handleMenuClick }} trigger={['contextMenu']}>
      {children}
    </Dropdown>
  );
}
