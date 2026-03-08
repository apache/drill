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
import { Modal, Table, Tag, Typography } from 'antd';

const { Text } = Typography;

const isMac = typeof navigator !== 'undefined' && /Mac/.test(navigator.platform);
const mod = isMac ? '⌘' : 'Ctrl';

interface Shortcut {
  key: string;
  action: string;
  context: string;
}

const shortcuts: Shortcut[] = [
  { key: `${mod}+Enter`, action: 'Run query (or selection)', context: 'Editor' },
  { key: `${mod}+Shift+F`, action: 'Focus schema search', context: 'Global' },
  { key: `?`, action: 'Show this help', context: 'Global' },
  { key: `${mod}+S`, action: 'Save query', context: 'Editor' },
  { key: `${mod}+Shift+P`, action: 'Format SQL', context: 'Editor' },
  { key: `${mod}+/`, action: 'Toggle comment', context: 'Editor' },
  { key: `${mod}+Z`, action: 'Undo', context: 'Editor' },
  { key: `${mod}+Shift+Z`, action: 'Redo', context: 'Editor' },
  { key: `${mod}+F`, action: 'Find in editor', context: 'Editor' },
  { key: `${mod}+H`, action: 'Find & replace', context: 'Editor' },
  { key: `Alt+↑ / ↓`, action: 'Move line up / down', context: 'Editor' },
  { key: `${mod}+D`, action: 'Duplicate line', context: 'Editor' },
  { key: `Double-click cell`, action: 'Copy cell value', context: 'Results grid' },
  { key: `Arrow keys`, action: 'Navigate schema tree', context: 'Schema tree' },
  { key: `Enter`, action: 'Expand / select node', context: 'Schema tree' },
  { key: `Double-click table`, action: 'Insert SELECT * query', context: 'Schema tree' },
];

const columns = [
  {
    title: 'Shortcut',
    dataIndex: 'key',
    key: 'key',
    render: (k: string) => (
      <Text keyboard style={{ fontFamily: 'monospace', whiteSpace: 'nowrap' }}>
        {k}
      </Text>
    ),
    width: 200,
  },
  {
    title: 'Action',
    dataIndex: 'action',
    key: 'action',
  },
  {
    title: 'Context',
    dataIndex: 'context',
    key: 'context',
    render: (ctx: string) => <Tag>{ctx}</Tag>,
    width: 130,
  },
];

interface KeyboardShortcutsModalProps {
  open: boolean;
  onClose: () => void;
}

export default function KeyboardShortcutsModal({ open, onClose }: KeyboardShortcutsModalProps) {
  return (
    <Modal
      title="Keyboard Shortcuts"
      open={open}
      onCancel={onClose}
      footer={null}
      width={600}
    >
      <Table
        dataSource={shortcuts}
        columns={columns}
        rowKey="key"
        size="small"
        pagination={false}
        style={{ marginTop: 8 }}
      />
    </Modal>
  );
}
