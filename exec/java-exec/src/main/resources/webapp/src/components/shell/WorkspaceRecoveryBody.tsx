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
import { useCallback, useMemo, useState } from 'react';
import { Button, Dropdown, Select, Tooltip, message } from 'antd';
import type { MenuProps } from 'antd';
import {
  CaretRightOutlined,
  ReloadOutlined,
  MoreOutlined,
  ArrowRightOutlined,
  DeleteOutlined,
  DownloadOutlined,
  CodeOutlined,
} from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { getProjects } from '../../api/projects';
import {
  listAllTabStates,
  moveTabState,
  deleteTabState,
  type TabStateEntry,
} from '../../utils/workspacePersistence';

const GLOBAL_VALUE = '__global__';

function formatRelative(ts: number | undefined): string {
  if (!ts) {
    return 'unknown';
  }
  const date = new Date(ts);
  const diff = Date.now() - date.getTime();
  const min = Math.floor(diff / 60000);
  if (min < 1) {
    return 'just now';
  }
  if (min < 60) {
    return `${min}m ago`;
  }
  const hr = Math.floor(min / 60);
  if (hr < 24) {
    return `${hr}h ago`;
  }
  const days = Math.floor(hr / 24);
  if (days < 7) {
    return `${days}d ago`;
  }
  return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' });
}

function singleLine(sql: string, max = 120): string {
  const t = sql.replace(/\s+/g, ' ').trim();
  return t.length > max ? t.slice(0, max) + '…' : t;
}

function entryKey(e: TabStateEntry): string {
  return e.projectId ?? GLOBAL_VALUE;
}

function downloadJson(filename: string, payload: unknown): void {
  try {
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  } catch (e) {
    message.error(`Couldn't download: ${(e as Error).message}`);
  }
}

export default function WorkspaceRecoveryBody() {
  const { data: projects } = useQuery({
    queryKey: ['projects'],
    queryFn: getProjects,
    staleTime: 60_000,
  });

  const [entries, setEntries] = useState<TabStateEntry[]>(() => listAllTabStates());
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  const [movingKey, setMovingKey] = useState<string | null>(null);
  const [moveTarget, setMoveTarget] = useState<string | null>(null);

  const refresh = useCallback(() => {
    setEntries(listAllTabStates());
    setMovingKey(null);
    setMoveTarget(null);
  }, []);

  const projectName = useCallback(
    (projectId: string | null): { label: string; isOrphaned: boolean } => {
      if (projectId === null) {
        return { label: 'Global (SQL Lab)', isOrphaned: false };
      }
      const p = projects?.find((x) => x.id === projectId);
      if (p) {
        return { label: p.name, isOrphaned: false };
      }
      return { label: `Orphaned: ${projectId.slice(0, 8)}…`, isOrphaned: true };
    },
    [projects],
  );

  // Sort: orphaned first (most likely to need attention), then by last saved
  const sortedEntries = useMemo(() => {
    return [...entries].sort((a, b) => {
      const aOrphaned = projectName(a.projectId).isOrphaned ? 0 : 1;
      const bOrphaned = projectName(b.projectId).isOrphaned ? 0 : 1;
      if (aOrphaned !== bOrphaned) {
        return aOrphaned - bOrphaned;
      }
      return (b.state.savedAt ?? 0) - (a.state.savedAt ?? 0);
    });
  }, [entries, projectName]);

  const orphanedCount = useMemo(
    () => sortedEntries.filter((e) => projectName(e.projectId).isOrphaned).length,
    [sortedEntries, projectName],
  );

  const handleDelete = useCallback((entry: TabStateEntry) => {
    deleteTabState(entry.projectId);
    message.success('Workspace deleted');
    setEntries(listAllTabStates());
  }, []);

  const handleDownload = useCallback((entry: TabStateEntry) => {
    const tag = entry.projectId ?? 'global';
    downloadJson(`drill-tabs-${tag}.json`, entry.state);
  }, []);

  const startMove = useCallback((entry: TabStateEntry) => {
    setMovingKey(entryKey(entry));
    setMoveTarget(null);
  }, []);

  const confirmMove = useCallback(
    (entry: TabStateEntry) => {
      if (moveTarget === null) {
        return;
      }
      const toProjectId = moveTarget === GLOBAL_VALUE ? null : moveTarget;
      const fromProjectId = entry.projectId;
      if (fromProjectId === toProjectId) {
        message.info('Already in that destination');
        setMovingKey(null);
        return;
      }
      const ok = moveTabState({ projectId: fromProjectId }, { projectId: toProjectId });
      if (ok) {
        message.success('Workspace moved — re-open the destination to see the tabs');
        setMovingKey(null);
        setMoveTarget(null);
        setEntries(listAllTabStates());
      } else {
        message.error('Move failed');
      }
    },
    [moveTarget],
  );

  // Build the move-target options: existing projects + Global
  const moveOptions = useMemo(
    () => [
      { value: GLOBAL_VALUE, label: 'Global (SQL Lab)' },
      ...((projects ?? [])
        .map((p) => ({ value: p.id, label: p.name }))
        .sort((a, b) => a.label.localeCompare(b.label))),
    ],
    [projects],
  );

  return (
    <div className="settings-body recovery-body">
      <div className="recovery-header">
        <div>
          <p className="recovery-explainer">
            Every persisted set of SQL Lab tabs in this browser. Tabs marked
            <span className="recovery-orphan-pill">Orphaned</span>
            were saved under project IDs that no longer exist — usually leak
            debris from earlier sessions. You can move them onto a real project
            or delete them.
          </p>
          {orphanedCount > 0 && (
            <p className="recovery-warning">
              {orphanedCount} orphaned {orphanedCount === 1 ? 'workspace' : 'workspaces'} found.
            </p>
          )}
        </div>
        <Tooltip title="Re-scan localStorage">
          <Button size="small" icon={<ReloadOutlined />} onClick={refresh} />
        </Tooltip>
      </div>

      {sortedEntries.length === 0 ? (
        <div className="recovery-empty">
          <CodeOutlined className="recovery-empty-glyph" />
          <h3>No persisted workspaces</h3>
          <p>Open SQL Lab and save a tab to see it here.</p>
        </div>
      ) : (
        <ul className="recovery-list">
          {sortedEntries.map((entry) => {
            const key = entryKey(entry);
            const { label, isOrphaned } = projectName(entry.projectId);
            const open = !!expanded[key];
            const moving = movingKey === key;

            const menuItems: MenuProps['items'] = [
              {
                key: 'move',
                icon: <ArrowRightOutlined />,
                label: 'Move to project…',
                onClick: () => startMove(entry),
              },
              {
                key: 'download',
                icon: <DownloadOutlined />,
                label: 'Download JSON',
                onClick: () => handleDownload(entry),
              },
              { type: 'divider' as const },
              {
                key: 'delete',
                icon: <DeleteOutlined />,
                label: 'Delete',
                danger: true,
                onClick: () => handleDelete(entry),
              },
            ];

            return (
              <li
                key={key}
                className={`recovery-entry${isOrphaned ? ' is-orphaned' : ''}${moving ? ' is-moving' : ''}`}
              >
                <div className="recovery-entry-row">
                  <button
                    type="button"
                    className={`recovery-entry-disclosure${open ? ' is-open' : ''}`}
                    onClick={() => setExpanded((p) => ({ ...p, [key]: !p[key] }))}
                    aria-expanded={open}
                    aria-label={open ? 'Collapse' : 'Expand'}
                  >
                    <CaretRightOutlined />
                  </button>
                  <div className="recovery-entry-info">
                    <div className="recovery-entry-name-row">
                      <span className="recovery-entry-name" title={label}>{label}</span>
                      {isOrphaned && <span className="recovery-orphan-pill">Orphaned</span>}
                    </div>
                    <div className="recovery-entry-meta">
                      {entry.state.tabs.length} {entry.state.tabs.length === 1 ? 'tab' : 'tabs'}
                      {' · '}
                      saved {formatRelative(entry.state.savedAt)}
                      {entry.projectId && <> · <code className="recovery-entry-id">{entry.projectId}</code></>}
                    </div>
                  </div>
                  <Dropdown menu={{ items: menuItems }} trigger={['click']} placement="bottomRight">
                    <button type="button" className="recovery-entry-actions" aria-label="Actions">
                      <MoreOutlined />
                    </button>
                  </Dropdown>
                </div>

                {open && (
                  <ul className="recovery-tab-list">
                    {entry.state.tabs.map((t) => (
                      <li key={t.id} className="recovery-tab">
                        <span className="recovery-tab-name">{t.name}</span>
                        <span className="recovery-tab-sql">
                          {t.sql.trim() ? singleLine(t.sql, 140) : <em>(empty)</em>}
                        </span>
                      </li>
                    ))}
                  </ul>
                )}

                {moving && (
                  <div className="recovery-move-bar">
                    <span className="recovery-move-label">Move to:</span>
                    <Select
                      size="small"
                      value={moveTarget}
                      onChange={(v) => setMoveTarget(v as string)}
                      placeholder="Choose destination"
                      style={{ flex: 1, minWidth: 180 }}
                      options={moveOptions.filter((o) =>
                        o.value === GLOBAL_VALUE
                          ? entry.projectId !== null
                          : o.value !== entry.projectId,
                      )}
                      showSearch
                      filterOption={(input, option) =>
                        (option?.label ?? '').toLowerCase().includes(input.toLowerCase())
                      }
                    />
                    <Button size="small" onClick={() => setMovingKey(null)}>Cancel</Button>
                    <Button
                      type="primary"
                      size="small"
                      disabled={moveTarget === null}
                      onClick={() => confirmMove(entry)}
                    >
                      Move
                    </Button>
                  </div>
                )}
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}
