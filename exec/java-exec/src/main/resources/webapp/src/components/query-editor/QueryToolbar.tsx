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
import { Button, Select, InputNumber, Space, Tooltip, Dropdown, Switch, Typography, Modal, Slider } from 'antd';
import {
  PlayCircleOutlined,
  StopOutlined,
  SaveOutlined,
  FormatPainterOutlined,
  HistoryOutlined,
  DownOutlined,
  SettingOutlined,
} from '@ant-design/icons';
import type { MenuProps } from 'antd';
import type { EditorSettings } from './SqlEditor';

const { Text } = Typography;

interface QueryToolbarProps {
  onExecute: () => void;
  onCancel: () => void;
  onSave?: () => void;
  onFormat?: () => void;
  isExecuting: boolean;
  executionTime?: number;
  schemas?: Array<{ name: string }>;
  selectedSchema?: string;
  onSchemaChange?: (schema: string) => void;
  autoLimit?: number;
  onAutoLimitChange?: (limit: number | null) => void;
  editorSettings?: EditorSettings;
  onEditorSettingsChange?: (settings: EditorSettings) => void;
}

export default function QueryToolbar({
  onExecute,
  onCancel,
  onSave,
  onFormat,
  isExecuting,
  executionTime,
  schemas = [],
  selectedSchema,
  onSchemaChange,
  autoLimit = 1000,
  onAutoLimitChange,
  editorSettings,
  onEditorSettingsChange,
}: QueryToolbarProps) {
  const [autoLimitEnabled, setAutoLimitEnabled] = useState(true);
  const [settingsModalOpen, setSettingsModalOpen] = useState(false);

  const handleAutoLimitToggle = useCallback(
    (enabled: boolean) => {
      setAutoLimitEnabled(enabled);
      onAutoLimitChange?.(enabled ? autoLimit : null);
    },
    [autoLimit, onAutoLimitChange]
  );

  const handleAutoLimitChange = useCallback(
    (value: number | null) => {
      if (value !== null && autoLimitEnabled) {
        onAutoLimitChange?.(value);
      }
    },
    [autoLimitEnabled, onAutoLimitChange]
  );

  const moreMenuItems: MenuProps['items'] = [
    {
      key: 'format',
      icon: <FormatPainterOutlined />,
      label: 'Format SQL',
      onClick: onFormat,
    },
    {
      key: 'history',
      icon: <HistoryOutlined />,
      label: 'Query History',
    },
    { type: 'divider' },
    {
      key: 'settings',
      icon: <SettingOutlined />,
      label: 'Editor Settings',
      onClick: () => setSettingsModalOpen(true),
    },
  ];

  return (
    <div className="query-toolbar">
      <Space size="middle">
        {/* Execute/Cancel Button */}
        {isExecuting ? (
          <Button
            type="primary"
            danger
            icon={<StopOutlined />}
            onClick={onCancel}
          >
            Cancel
          </Button>
        ) : (
          <Tooltip title="Ctrl/Cmd + Enter">
            <Button
              type="primary"
              icon={<PlayCircleOutlined />}
              onClick={onExecute}
            >
              Run Query
            </Button>
          </Tooltip>
        )}

        {/* Save Query Button */}
        <Tooltip title="Save Query">
          <Button icon={<SaveOutlined />} onClick={onSave}>
            Save
          </Button>
        </Tooltip>

        {/* Schema Selector */}
        <Space size="small">
          <Text type="secondary">Schema:</Text>
          <Select
            placeholder="Select schema"
            style={{ width: 200 }}
            value={selectedSchema}
            onChange={onSchemaChange}
            showSearch
            allowClear
            optionFilterProp="label"
            options={schemas.map((s) => ({ value: s.name, label: s.name }))}
          />
        </Space>

        {/* Auto Limit */}
        <Space size="small">
          <Switch
            size="small"
            checked={autoLimitEnabled}
            onChange={handleAutoLimitToggle}
          />
          <Text type="secondary">Limit:</Text>
          <InputNumber
            size="small"
            min={1}
            max={100000}
            value={autoLimit}
            onChange={handleAutoLimitChange}
            disabled={!autoLimitEnabled}
            style={{ width: 80 }}
          />
        </Space>

        {/* More Options */}
        <Dropdown menu={{ items: moreMenuItems }} trigger={['click']}>
          <Button>
            More <DownOutlined />
          </Button>
        </Dropdown>
      </Space>

      {/* Execution Time */}
      <div style={{ marginLeft: 'auto' }}>
        {executionTime !== undefined && !isExecuting && (
          <Text type="secondary">
            Executed in {(executionTime / 1000).toFixed(2)}s
          </Text>
        )}
        {isExecuting && (
          <Text type="secondary">
            Executing...
          </Text>
        )}
      </div>

      {/* Editor Settings Modal */}
      {editorSettings && (
        <Modal
          title="Editor Settings"
          open={settingsModalOpen}
          onCancel={() => setSettingsModalOpen(false)}
          footer={null}
          width={400}
        >
          <Space direction="vertical" style={{ width: '100%' }} size="middle">
            <div>
              <Text strong>Theme</Text>
              <Select
                style={{ width: '100%', marginTop: 4 }}
                value={editorSettings.theme}
                onChange={(value) =>
                  onEditorSettingsChange?.({ ...editorSettings, theme: value })
                }
                options={[
                  { value: 'vs-light', label: 'Light' },
                  { value: 'vs-dark', label: 'Dark' },
                  { value: 'hc-black', label: 'High Contrast' },
                ]}
              />
            </div>
            <div>
              <Text strong>Font Size: {editorSettings.fontSize}px</Text>
              <Slider
                min={10}
                max={24}
                value={editorSettings.fontSize}
                onChange={(value) =>
                  onEditorSettingsChange?.({ ...editorSettings, fontSize: value })
                }
              />
            </div>
            <div>
              <Text strong>Tab Size</Text>
              <Select
                style={{ width: '100%', marginTop: 4 }}
                value={editorSettings.tabSize}
                onChange={(value) =>
                  onEditorSettingsChange?.({ ...editorSettings, tabSize: value })
                }
                options={[
                  { value: 2, label: '2 spaces' },
                  { value: 4, label: '4 spaces' },
                ]}
              />
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <Text strong>Word Wrap</Text>
              <Switch
                checked={editorSettings.wordWrap}
                onChange={(checked) =>
                  onEditorSettingsChange?.({ ...editorSettings, wordWrap: checked })
                }
              />
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <Text strong>Minimap</Text>
              <Switch
                checked={editorSettings.minimap}
                onChange={(checked) =>
                  onEditorSettingsChange?.({ ...editorSettings, minimap: checked })
                }
              />
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <Text strong>Line Numbers</Text>
              <Switch
                checked={editorSettings.lineNumbers}
                onChange={(checked) =>
                  onEditorSettingsChange?.({ ...editorSettings, lineNumbers: checked })
                }
              />
            </div>
          </Space>
        </Modal>
      )}
    </div>
  );
}
