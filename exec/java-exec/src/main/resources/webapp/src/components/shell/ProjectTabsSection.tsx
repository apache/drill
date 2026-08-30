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
import { Dropdown, Tooltip } from 'antd';
import { CodeOutlined, LockOutlined, ApiOutlined } from '@ant-design/icons';
import type { MenuProps } from 'antd';

/** The subset of a tab the tree needs. */
export interface TabTreeItem {
  id: string;
  name: string;
  /** Closed but not deleted. Still listed here — the tree is the way back. */
  hidden: boolean;
  locked?: boolean;
  lockType?: 'manual' | 'api';
}

interface ProjectTabsSectionProps {
  tabs: TabTreeItem[];
  /** Activates the tab, unhiding it first when it was closed. */
  onOpen: (id: string) => void;
  onDelete: (id: string) => void;
}

/**
 * Lists a project's query tabs in the sidebar.
 *
 * Tabs are the first item-level content in a sidebar that otherwise lists sections,
 * which is why this is its own component rather than another `ProjectSection`.
 *
 * Both open and hidden tabs appear. A hidden tab has left the tab strip, so this list
 * is the only route back to it; leaving them out would make closing a tab feel like
 * losing it. See docs/dev/TabPersistence.md.
 */
export default function ProjectTabsSection({ tabs, onOpen, onDelete }: ProjectTabsSectionProps) {
  if (tabs.length === 0) {
    return (
      <div className="shell-sidebar-tab-empty">No tabs yet</div>
    );
  }

  return (
    <>
      {tabs.map((tab) => {
        // A locked tab cannot be deleted, so the menu says why instead of offering an
        // action that the server would refuse with a 409.
        const menuItems: MenuProps['items'] = tab.locked
          ? [{
            key: 'locked',
            disabled: true,
            label: tab.lockType === 'api'
              ? 'Locked: API endpoint active'
              : 'Locked — unlock to delete',
          }]
          : [{ key: 'delete', danger: true, label: 'Delete' }];

        const onMenuClick: MenuProps['onClick'] = ({ key, domEvent }) => {
          domEvent.stopPropagation();
          if (key === 'delete') {
            onDelete(tab.id);
          }
        };

        return (
          <Dropdown
            key={tab.id}
            trigger={['contextMenu']}
            menu={{ items: menuItems, onClick: onMenuClick }}
          >
            <button
              type="button"
              className={`shell-sidebar-tab-row${tab.hidden ? ' is-hidden-tab' : ''}`}
              onClick={() => onOpen(tab.id)}
            >
              <span className="shell-sidebar-tab-icon">
                <CodeOutlined />
              </span>
              <span className="shell-sidebar-tab-label">{tab.name}</span>
              {tab.locked && (
                <Tooltip title={tab.lockType === 'api' ? 'API endpoint active' : 'Locked'}>
                  <span className="shell-sidebar-tab-lock">
                    {tab.lockType === 'api' ? <ApiOutlined /> : <LockOutlined />}
                  </span>
                </Tooltip>
              )}
            </button>
          </Dropdown>
        );
      })}
    </>
  );
}
