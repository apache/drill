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
import { Modal } from 'antd';

const isMac = typeof navigator !== 'undefined' && /Mac/.test(navigator.platform);
const mod = isMac ? '⌘' : 'Ctrl';
const opt = isMac ? '⌥' : 'Alt';
const shift = '⇧';

interface Shortcut {
  keys: string[];
  action: string;
}

interface ShortcutGroup {
  title: string;
  items: Shortcut[];
}

const SHORTCUT_GROUPS: ShortcutGroup[] = [
  {
    title: 'Navigation',
    items: [
      { keys: [`${mod}K`], action: 'Open Spotlight' },
      { keys: [`${mod}0`], action: 'Toggle sidebar' },
      { keys: [`${mod}${opt}0`], action: 'Toggle inspector' },
      { keys: [`${mod},`], action: 'Open Preferences' },
      { keys: ['?'], action: 'Show this help' },
    ],
  },
  {
    title: 'SQL Lab',
    items: [
      { keys: [`${mod}↵`], action: 'Run query (or selection)' },
      { keys: [`${mod}S`], action: 'Save query' },
      { keys: [`${mod}${shift}F`], action: 'Focus schema search' },
      { keys: [`Double-click tab`], action: 'Rename tab' },
    ],
  },
  {
    title: 'Editor',
    items: [
      { keys: [`${mod}${shift}P`], action: 'Format SQL' },
      { keys: [`${mod}/`], action: 'Toggle comment' },
      { keys: [`${mod}Z`], action: 'Undo' },
      { keys: [`${mod}${shift}Z`], action: 'Redo' },
      { keys: [`${mod}F`], action: 'Find in editor' },
      { keys: [`${mod}H`], action: 'Find & replace' },
      { keys: [`${opt}↑`, `${opt}↓`], action: 'Move line up / down' },
      { keys: [`${mod}D`], action: 'Duplicate line' },
    ],
  },
  {
    title: 'Schema tree',
    items: [
      { keys: ['↑', '↓', '←', '→'], action: 'Navigate' },
      { keys: ['↵'], action: 'Expand or select node' },
      { keys: ['Double-click table'], action: 'Insert SELECT * query' },
    ],
  },
  {
    title: 'Results grid',
    items: [
      { keys: ['Double-click cell'], action: 'Copy cell value' },
    ],
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
      width={680}
      destroyOnClose
    >
      <div className="shortcuts-grid">
        {SHORTCUT_GROUPS.map((group) => (
          <section key={group.title} className="shortcuts-group">
            <h3 className="shortcuts-group-title">{group.title}</h3>
            <ul className="shortcuts-list">
              {group.items.map((item, i) => (
                <li key={i} className="shortcuts-row">
                  <span className="shortcuts-action">{item.action}</span>
                  <span className="shortcuts-keys">
                    {item.keys.map((k, j) => (
                      <kbd key={j} className="shortcuts-key">{k}</kbd>
                    ))}
                  </span>
                </li>
              ))}
            </ul>
          </section>
        ))}
      </div>
    </Modal>
  );
}
