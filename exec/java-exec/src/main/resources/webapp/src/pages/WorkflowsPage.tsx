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
import { useState, useMemo, type ReactNode } from 'react';
import {
  Tag,
  Button,
  Tooltip,
  Spin,
  Input,
  InputNumber,
  Switch,
  message,
  Dropdown,
  Modal,
} from 'antd';
import type { MenuProps } from 'antd';
import {
  SearchOutlined,
  DeleteOutlined,
  EditOutlined,
  ReloadOutlined,
  SettingOutlined,
  PlayCircleOutlined,
  PauseCircleOutlined,
  ClockCircleOutlined,
  WarningOutlined,
  MoreOutlined,
  CaretRightOutlined,
  DatabaseOutlined,
} from '@ant-design/icons';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import {
  getSchedules,
  deleteSchedule,
  renewSchedule,
  runScheduleNow,
  updateSchedule,
  getSnapshots,
  getScheduleConfig,
  updateScheduleConfig,
} from '../api/schedules';
import { getSavedQueries } from '../api/savedQueries';
import { usePageChrome } from '../contexts/AppChromeContext';
import type { QuerySchedule, QuerySnapshot } from '../types';
import ScheduleModal from '../components/query-editor/ScheduleModal';
import Markdown from 'react-markdown';
import rehypeRaw from 'rehype-raw';

type Bucket = 'now' | 'today' | 'tomorrow' | 'week' | 'later' | 'inactive' | 'expired';
type Filter = 'all' | 'active' | 'paused' | 'expiring' | 'alerts' | 'data';

const BUCKET_ORDER: Bucket[] = ['now', 'today', 'tomorrow', 'week', 'later', 'inactive', 'expired'];

const BUCKET_LABELS: Record<Bucket, string> = {
  now: 'Now',
  today: 'Today',
  tomorrow: 'Tomorrow',
  week: 'This Week',
  later: 'Later',
  inactive: 'Inactive',
  expired: 'Expired',
};

const DAY_LABELS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

function formatSchedule(s: QuerySchedule): string {
  const time = s.timeOfDay || '00:00';
  switch (s.frequency) {
    case 'hourly':
      return `Every hour at :${time.split(':')[1] || '00'}`;
    case 'daily':
      return `Daily at ${time}`;
    case 'weekly':
      return `${DAY_LABELS[s.dayOfWeek ?? 0]}s at ${time}`;
    case 'monthly':
      return `${s.dayOfMonth ?? 1}${ordinalSuffix(s.dayOfMonth ?? 1)} of month at ${time}`;
    default:
      return s.frequency;
  }
}

function ordinalSuffix(n: number): string {
  if (n >= 11 && n <= 13) {
    return 'th';
  }
  switch (n % 10) {
    case 1: return 'st';
    case 2: return 'nd';
    case 3: return 'rd';
    default: return 'th';
  }
}

function daysUntilExpiry(expiresAt?: string): number | null {
  if (!expiresAt) {
    return null;
  }
  return Math.ceil((new Date(expiresAt).getTime() - Date.now()) / 86400000);
}

function timeUntil(iso: string | undefined): string {
  if (!iso) {
    return '—';
  }
  const ms = new Date(iso).getTime() - Date.now();
  if (ms < 0) {
    return 'overdue';
  }
  if (ms < 60_000) {
    return 'in <1m';
  }
  const min = Math.floor(ms / 60_000);
  if (min < 60) {
    return `in ${min}m`;
  }
  const hr = Math.floor(min / 60);
  if (hr < 24) {
    return `in ${hr}h`;
  }
  const days = Math.floor(hr / 24);
  if (days < 7) {
    return `in ${days}d`;
  }
  if (days < 30) {
    return `in ${Math.floor(days / 7)}w`;
  }
  return new Date(iso).toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

function bucketFor(s: QuerySchedule): Bucket {
  if (s.status === 'expired') {
    return 'expired';
  }
  if (!s.enabled || s.paused) {
    return 'inactive';
  }
  if (s.isRunning) {
    return 'now';
  }
  if (!s.nextRunAt) {
    return 'later';
  }
  const next = new Date(s.nextRunAt).getTime();
  const now = Date.now();
  if (next < now) {
    return 'now';
  }
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const tomorrow = new Date(today);
  tomorrow.setDate(today.getDate() + 1);
  const dayAfter = new Date(tomorrow);
  dayAfter.setDate(tomorrow.getDate() + 1);
  const weekFromNow = new Date(today);
  weekFromNow.setDate(today.getDate() + 7);

  if (next < tomorrow.getTime()) {
    return 'today';
  }
  if (next < dayAfter.getTime()) {
    return 'tomorrow';
  }
  if (next < weekFromNow.getTime()) {
    return 'week';
  }
  return 'later';
}

function statusColor(s: QuerySchedule, expiringSoon: boolean): string {
  if (s.status === 'expired') {
    return 'var(--color-error)';
  }
  if (!s.enabled || s.paused) {
    return 'var(--color-warning)';
  }
  if (s.isRunning) {
    return 'var(--color-primary)';
  }
  if (expiringSoon) {
    return 'var(--color-warning)';
  }
  return 'var(--color-success)';
}

function RunDots({ snapshots }: { snapshots: QuerySnapshot[] }) {
  const recent = snapshots.slice(0, 5).reverse();
  if (recent.length === 0) {
    return <span className="workflow-row-runs is-empty">No runs</span>;
  }
  return (
    <span className="workflow-row-runs">
      {recent.map((snap) => (
        <Tooltip key={snap.id} title={`${new Date(snap.executedAt).toLocaleString()} — ${snap.status}`}>
          <span
            className={`workflow-run-dot is-${snap.status}`}
            aria-label={snap.status}
          />
        </Tooltip>
      ))}
    </span>
  );
}

interface WorkflowsPageProps {
  /** When set, only schedules whose savedQueryId is in this list are shown. */
  filterSavedQueryIds?: string[];
  /** Set to disable workflow-config editing on project-scoped views. */
  hideSettings?: boolean;
}

export default function WorkflowsPage({ filterSavedQueryIds, hideSettings }: WorkflowsPageProps = {}) {
  const queryClient = useQueryClient();
  const [searchText, setSearchText] = useState('');
  const [filter, setFilter] = useState<Filter>('all');
  const [editingSchedule, setEditingSchedule] = useState<{ id: string; name: string; sql?: string } | null>(null);
  const [runningIds, setRunningIds] = useState<Set<string>>(new Set());
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());
  const [expandedSnapshots, setExpandedSnapshots] = useState<Set<string>>(new Set());
  const [showConfig, setShowConfig] = useState(false);
  const [configDraft, setConfigDraft] = useState<{
    expirationEnabled: boolean;
    expirationDays: number;
    warningDaysBeforeExpiry: number;
  } | null>(null);

  const { data: serverConfig } = useQuery({
    queryKey: ['workflow-config'],
    queryFn: getScheduleConfig,
  });

  const config = useMemo(
    () => configDraft || serverConfig || {
      expirationEnabled: true,
      expirationDays: 90,
      warningDaysBeforeExpiry: 14,
    },
    [configDraft, serverConfig],
  );

  const { data: schedules, isLoading, refetch, isFetching } = useQuery({
    queryKey: ['schedules'],
    queryFn: getSchedules,
  });

  const { data: savedQueries } = useQuery({
    queryKey: ['savedQueries'],
    queryFn: getSavedQueries,
  });

  const { data: allSnapshots } = useQuery({
    queryKey: ['all-snapshots', schedules?.map((s) => s.id).join(',')],
    queryFn: async () => {
      if (!schedules) {
        return {};
      }
      const map: Record<string, QuerySnapshot[]> = {};
      for (const s of schedules) {
        map[s.id] = await getSnapshots(s.id);
      }
      return map;
    },
    enabled: !!schedules && schedules.length > 0,
  });

  const queryMap = useMemo(() => {
    const m: Record<string, { name: string; sql: string }> = {};
    for (const q of savedQueries || []) {
      m[q.id] = { name: q.name, sql: q.sql };
    }
    return m;
  }, [savedQueries]);

  const isExpiring = (s: QuerySchedule): boolean => {
    if (!config.expirationEnabled) {
      return false;
    }
    const days = daysUntilExpiry(s.expiresAt);
    return days !== null && days > 0 && days <= config.warningDaysBeforeExpiry && s.enabled;
  };

  const hasAlerts = (s: QuerySchedule): boolean => {
    const latest = (allSnapshots?.[s.id] || [])[0];
    return !!(latest?.triggeredAlerts && latest.triggeredAlerts.length > 0);
  };

  /**
   * "Visible data" means a snapshot can be inspected for content beyond bare
   * status: an AI summary, a persisted result path, or in-page preview rows.
   */
  const hasViewableData = (s: QuerySchedule): boolean => {
    const snaps = allSnapshots?.[s.id] || [];
    return snaps.some(
      (sn) =>
        !!sn.aiSummary ||
        !!sn.resultPath ||
        (Array.isArray(sn.previewRows) && sn.previewRows.length > 0),
    );
  };

  const filteredSchedules = useMemo(() => {
    if (!schedules) {
      return [];
    }
    let result = schedules;
    if (filterSavedQueryIds) {
      const allowed = new Set(filterSavedQueryIds);
      result = result.filter((s) => allowed.has(s.savedQueryId));
    }
    if (filter === 'active') {
      result = result.filter((s) => s.enabled && !s.paused && s.status !== 'expired');
    } else if (filter === 'paused') {
      result = result.filter((s) => !s.enabled || s.paused);
    } else if (filter === 'expiring') {
      result = result.filter(isExpiring);
    } else if (filter === 'alerts') {
      result = result.filter(hasAlerts);
    } else if (filter === 'data') {
      result = result.filter(hasViewableData);
    }
    if (searchText) {
      const lower = searchText.toLowerCase();
      result = result.filter((s) => {
        const qName = queryMap[s.savedQueryId]?.name || '';
        return (
          qName.toLowerCase().includes(lower) ||
          (s.description && s.description.toLowerCase().includes(lower)) ||
          s.frequency.toLowerCase().includes(lower)
        );
      });
    }
    return result;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [schedules, searchText, filter, filterSavedQueryIds, queryMap, allSnapshots, config.expirationEnabled, config.warningDaysBeforeExpiry]);

  const grouped = useMemo(() => {
    const groups = new Map<Bucket, QuerySchedule[]>();
    BUCKET_ORDER.forEach((b) => groups.set(b, []));
    for (const s of filteredSchedules) {
      const b = bucketFor(s);
      groups.get(b)!.push(s);
    }
    // Sort each bucket by the most-relevant date for that bucket's intent:
    //  - Now/Today/Tomorrow/Week/Later: by nextRunAt ascending (soonest first)
    //  - Inactive/Expired: by lastRunAt descending (most recently run first)
    const bucketKey = (s: QuerySchedule, b: Bucket): number => {
      if (b === 'inactive' || b === 'expired') {
        if (s.lastRunAt) {
          return -new Date(s.lastRunAt).getTime(); // negative for desc
        }
        return -new Date(s.createdAt).getTime();
      }
      if (s.nextRunAt) {
        return new Date(s.nextRunAt).getTime();
      }
      return Number.MAX_SAFE_INTEGER;
    };
    for (const b of BUCKET_ORDER) {
      const arr = groups.get(b)!;
      arr.sort((a, c) => bucketKey(a, b) - bucketKey(c, b));
    }
    return groups;
  }, [filteredSchedules]);

  const stats = useMemo(() => {
    const total = schedules?.length ?? 0;
    const expiring = (schedules || []).filter(isExpiring).length;
    const alertCount = (schedules || []).filter(hasAlerts).length;
    const running = (schedules || []).filter((s) => s.isRunning).length;
    return { total, expiring, alertCount, running };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [schedules, allSnapshots, config.expirationEnabled, config.warningDaysBeforeExpiry]);

  const handleDelete = async (id: string) => {
    try {
      await deleteSchedule(id);
      message.success('Workflow deleted');
      queryClient.invalidateQueries({ queryKey: ['schedules'] });
    } catch {
      message.error('Failed to delete');
    }
  };

  const handleRenew = async (id: string) => {
    try {
      await renewSchedule(id);
      message.success(`Renewed for ${config.expirationDays} days`);
      queryClient.invalidateQueries({ queryKey: ['schedules'] });
    } catch {
      message.error('Failed to renew');
    }
  };

  const handleRunNow = async (id: string) => {
    try {
      setRunningIds((prev) => new Set(prev).add(id));
      await runScheduleNow(id);
      message.success('Workflow executed');
      queryClient.invalidateQueries({ queryKey: ['schedules'] });
      queryClient.invalidateQueries({ queryKey: ['all-snapshots'] });
    } catch {
      message.error('Failed to run');
    } finally {
      setRunningIds((prev) => {
        const next = new Set(prev);
        next.delete(id);
        return next;
      });
    }
  };

  const handleTogglePause = async (record: QuerySchedule) => {
    try {
      await updateSchedule(record.id, { paused: !record.paused });
      message.success(record.paused ? 'Resumed' : 'Paused');
      queryClient.invalidateQueries({ queryKey: ['schedules'] });
    } catch {
      message.error('Failed to update');
    }
  };

  const handleSaveConfig = async () => {
    try {
      await updateScheduleConfig(config);
      message.success('Settings saved');
      setConfigDraft(null);
      setShowConfig(false);
      queryClient.invalidateQueries({ queryKey: ['workflow-config'] });
      queryClient.invalidateQueries({ queryKey: ['schedules'] });
    } catch {
      message.error('Failed to save settings (admin required)');
    }
  };

  const toggleExpand = (id: string) => {
    setExpandedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  };

  const toolbarActions = useMemo(
    () => (
      <span style={{ display: 'inline-flex', gap: 4 }}>
        <Tooltip title="Refresh">
          <Button type="text" size="small" icon={<ReloadOutlined spin={isFetching} />} onClick={() => refetch()} />
        </Tooltip>
        {!hideSettings && (
          <Tooltip title="Workflow settings">
            <Button type="text" size="small" icon={<SettingOutlined />} onClick={() => setShowConfig((v) => !v)} />
          </Tooltip>
        )}
      </span>
    ),
    [refetch, isFetching, hideSettings],
  );
  usePageChrome({ toolbarActions });

  return (
    <div className="page-workflows">
      <header className="page-workflows-header">
        <div>
          <h1 className="page-workflows-title">Workflows</h1>
          <p className="page-workflows-subtitle">
            {schedules === undefined
              ? 'Loading…'
              : (
                <>
                  {stats.running > 0 && (
                    <span className="page-workflows-stat-pill page-workflows-stat-running">
                      <span className="page-workflows-running-dot" />
                      {stats.running} running
                    </span>
                  )}
                  {stats.alertCount > 0 && (
                    <span
                      className="page-workflows-stat-pill page-workflows-stat-alert"
                      onClick={() => setFilter('alerts')}
                      role="button"
                      tabIndex={0}
                    >
                      <WarningOutlined />
                      {stats.alertCount} with alerts
                    </span>
                  )}
                  {stats.expiring > 0 && (
                    <span
                      className="page-workflows-stat-pill page-workflows-stat-expiring"
                      onClick={() => setFilter('expiring')}
                      role="button"
                      tabIndex={0}
                    >
                      <ClockCircleOutlined />
                      {stats.expiring} expiring
                    </span>
                  )}{' '}
                  {stats.total === 0
                    ? 'No scheduled queries yet'
                    : `${stats.total} total`}
                </>
              )}
          </p>
        </div>
      </header>

      {/* Settings panel — slides down when toggled */}
      {showConfig && (
        <div className="page-workflows-settings">
          <div className="page-workflows-settings-header">
            <span>Workflow Expiration</span>
            <Tag>Admin</Tag>
          </div>
          <div className="page-workflows-settings-grid">
            <label className="page-workflows-settings-row">
              <span>Auto-expire scheduled queries</span>
              <Switch
                checked={config.expirationEnabled}
                onChange={(v) => setConfigDraft({ ...config, expirationEnabled: v })}
              />
            </label>
            {config.expirationEnabled && (
              <>
                <label className="page-workflows-settings-row">
                  <span>Expiration period (days)</span>
                  <InputNumber
                    min={7}
                    max={365}
                    value={config.expirationDays}
                    onChange={(v) => setConfigDraft({ ...config, expirationDays: v || 90 })}
                    style={{ width: 100 }}
                  />
                </label>
                <label className="page-workflows-settings-row">
                  <span>Warning before expiry (days)</span>
                  <InputNumber
                    min={1}
                    max={30}
                    value={config.warningDaysBeforeExpiry}
                    onChange={(v) => setConfigDraft({ ...config, warningDaysBeforeExpiry: v || 14 })}
                    style={{ width: 100 }}
                  />
                </label>
              </>
            )}
          </div>
          <div className="page-workflows-settings-actions">
            <Button size="small" onClick={() => { setShowConfig(false); setConfigDraft(null); }}>
              Cancel
            </Button>
            <Button type="primary" size="small" onClick={handleSaveConfig}>
              Save
            </Button>
          </div>
        </div>
      )}

      <div className="page-workflows-toolbar">
        <Input
          placeholder="Search workflows…"
          prefix={<SearchOutlined style={{ color: 'var(--color-text-tertiary)' }} />}
          value={searchText}
          onChange={(e) => setSearchText(e.target.value)}
          allowClear
          className="page-workflows-search"
        />

        <div className="page-workflows-chips">
          {(['all', 'active', 'paused', 'data', 'expiring', 'alerts'] as Filter[]).map((f) => (
            <button
              key={f}
              type="button"
              className={`page-workflows-chip${filter === f ? ' is-active' : ''}`}
              onClick={() => setFilter(f)}
            >
              {f === 'all' && 'All'}
              {f === 'active' && (
                <>
                  <span className="page-workflows-chip-dot" style={{ background: 'var(--color-success)' }} />
                  Active
                </>
              )}
              {f === 'paused' && (
                <>
                  <span className="page-workflows-chip-dot" style={{ background: 'var(--color-warning)' }} />
                  Paused
                </>
              )}
              {f === 'data' && (
                <>
                  <DatabaseOutlined style={{ fontSize: 11, marginRight: 4 }} />
                  Data
                </>
              )}
              {f === 'expiring' && 'Expiring'}
              {f === 'alerts' && 'Alerts'}
            </button>
          ))}
        </div>
      </div>

      {isLoading ? (
        <div className="page-workflows-loading"><Spin size="large" /></div>
      ) : filteredSchedules.length === 0 ? (
        <div className="page-workflows-empty">
          <ClockCircleOutlined className="page-workflows-empty-glyph" />
          <h2>{searchText || filter !== 'all' ? 'No matches' : 'No workflows yet'}</h2>
          <p>
            {searchText || filter !== 'all'
              ? 'Try a different search or filter.'
              : 'Schedule a saved query to create a workflow that runs automatically.'}
          </p>
          {(searchText || filter !== 'all') && (
            <Button onClick={() => { setSearchText(''); setFilter('all'); }}>Clear filters</Button>
          )}
        </div>
      ) : (
        <div className="page-workflows-list">
          {BUCKET_ORDER.map((bucket) => {
            const items = grouped.get(bucket) ?? [];
            if (items.length === 0) {
              return null;
            }
            return (
              <section key={bucket} className={`workflow-bucket workflow-bucket-${bucket}`}>
                <h2 className="workflow-bucket-header">
                  {BUCKET_LABELS[bucket]}
                  <span className="workflow-bucket-count">{items.length}</span>
                </h2>
                <ul className="workflow-bucket-list">
                  {items.map((s) => {
                    const q = queryMap[s.savedQueryId];
                    const expanded = expandedIds.has(s.id);
                    const expiringSoon = isExpiring(s);
                    const dotColor = statusColor(s, expiringSoon);
                    const isRunning = s.isRunning || runningIds.has(s.id);
                    const days = daysUntilExpiry(s.expiresAt);
                    const needsRenewal = days !== null && days > 0 && days <= config.warningDaysBeforeExpiry;
                    const snapshots = allSnapshots?.[s.id] || [];
                    const latestSnap = snapshots[0];
                    const alertCount = latestSnap?.triggeredAlerts?.length ?? 0;

                    const moreItems: MenuProps['items'] = [
                      {
                        key: 'edit',
                        icon: <EditOutlined />,
                        label: 'Edit schedule…',
                        onClick: () => setEditingSchedule({ id: s.savedQueryId, name: q?.name ?? 'Query', sql: q?.sql }),
                      },
                      {
                        key: 'pause',
                        icon: s.paused ? <PlayCircleOutlined /> : <PauseCircleOutlined />,
                        label: s.paused ? 'Resume' : 'Pause',
                        onClick: () => handleTogglePause(s),
                      },
                      ...(needsRenewal
                        ? [{ key: 'renew', icon: <ReloadOutlined />, label: 'Renew', onClick: () => handleRenew(s.id) }]
                        : []),
                      { type: 'divider' as const },
                      {
                        key: 'delete',
                        icon: <DeleteOutlined />,
                        label: 'Delete',
                        danger: true,
                        onClick: () => {
                          Modal.confirm({
                            title: 'Delete this workflow?',
                            content: 'This action cannot be undone.',
                            okText: 'Delete',
                            okButtonProps: { danger: true },
                            onOk: () => handleDelete(s.id),
                          });
                        },
                      },
                    ];

                    return (
                      <li
                        key={s.id}
                        className={`workflow-row${expanded ? ' is-expanded' : ''}${isRunning ? ' is-running' : ''}`}
                      >
                        <button
                          type="button"
                          className="workflow-row-summary"
                          onClick={() => toggleExpand(s.id)}
                        >
                          <span className="workflow-row-disclosure" aria-hidden="true">
                            <CaretRightOutlined />
                          </span>
                          <span className={`workflow-row-dot${isRunning ? ' is-pulsing' : ''}`} style={{ background: dotColor }} />

                          <span className="workflow-row-time">{timeUntil(s.nextRunAt)}</span>

                          <span className="workflow-row-info">
                            <span className="workflow-row-name">{q?.name ?? s.savedQueryId}</span>
                            {s.description && <span className="workflow-row-desc">{s.description}</span>}
                          </span>

                          <span className="workflow-row-frequency">{formatSchedule(s)}</span>

                          <RunDots snapshots={snapshots} />

                          {hasViewableData(s) && (
                            <Tooltip title="Has viewable run data — preview rows, AI summary, or persisted results">
                              <span className="workflow-row-data">
                                <DatabaseOutlined />
                              </span>
                            </Tooltip>
                          )}

                          {alertCount > 0 && (
                            <span className="workflow-row-alerts">
                              <WarningOutlined /> {alertCount}
                            </span>
                          )}

                          {expiringSoon && (
                            <Tooltip title={`Expires in ${days} day${days !== 1 ? 's' : ''}`}>
                              <span className="workflow-row-expiry">{days}d</span>
                            </Tooltip>
                          )}
                        </button>

                        <span className="workflow-row-actions" onClick={(e) => e.stopPropagation()}>
                          {!isRunning && (
                            <Tooltip title="Run now">
                              <Button
                                size="small"
                                type="text"
                                icon={<PlayCircleOutlined />}
                                loading={runningIds.has(s.id)}
                                onClick={() => handleRunNow(s.id)}
                              />
                            </Tooltip>
                          )}
                          {needsRenewal && (
                            <Tooltip title={`Renew schedule for ${config.expirationDays} days`}>
                              <Button
                                size="small"
                                type="primary"
                                ghost
                                icon={<ReloadOutlined />}
                                onClick={() => handleRenew(s.id)}
                              >
                                Renew
                              </Button>
                            </Tooltip>
                          )}
                          <Dropdown menu={{ items: moreItems }} placement="bottomRight" trigger={['click']}>
                            <Button size="small" type="text" icon={<MoreOutlined />} />
                          </Dropdown>
                        </span>

                        {expanded && (
                          <div className="workflow-row-detail">
                            {snapshots.length === 0 ? (
                              <p className="workflow-detail-empty">No runs yet.</p>
                            ) : (
                              <RunHistory
                                snapshots={snapshots}
                                expandedSnapshots={expandedSnapshots}
                                onToggleSnapshot={(snapshotId) => {
                                  setExpandedSnapshots((prev) => {
                                    const next = new Set(prev);
                                    if (next.has(snapshotId)) {
                                      next.delete(snapshotId);
                                    } else {
                                      next.add(snapshotId);
                                    }
                                    return next;
                                  });
                                }}
                              />
                            )}
                          </div>
                        )}
                      </li>
                    );
                  })}
                </ul>
              </section>
            );
          })}
        </div>
      )}

      {editingSchedule && (
        <ScheduleModal
          open={!!editingSchedule}
          onClose={() => setEditingSchedule(null)}
          savedQueryId={editingSchedule.id}
          savedQueryName={editingSchedule.name}
          savedQuerySql={editingSchedule.sql}
          onSuccess={() => {
            queryClient.invalidateQueries({ queryKey: ['schedules'] });
            setEditingSchedule(null);
          }}
        />
      )}
    </div>
  );
}

interface RunHistoryProps {
  snapshots: QuerySnapshot[];
  expandedSnapshots: Set<string>;
  onToggleSnapshot: (id: string) => void;
}

function RunHistory({ snapshots, expandedSnapshots, onToggleSnapshot }: RunHistoryProps): ReactNode {
  // Sort snapshots most-recent first defensively (the API ordering isn't
  // guaranteed) so the user always sees the latest run at the top.
  const sortedSnapshots = useMemo(
    () =>
      [...snapshots].sort(
        (a, b) => new Date(b.executedAt).getTime() - new Date(a.executedAt).getTime(),
      ),
    [snapshots],
  );
  const latest = sortedSnapshots[0];
  const isLatestExpanded = latest ? expandedSnapshots.has(latest.id) : false;
  const hasAnyExpanded = sortedSnapshots.some((s) => expandedSnapshots.has(s.id));
  const showLatest = isLatestExpanded || !hasAnyExpanded;

  return (
    <div className="workflow-history">
      <div className="workflow-history-header">
        <span className="workflow-history-title">Run history</span>
        <span className="workflow-history-count">
          {sortedSnapshots.length} {sortedSnapshots.length === 1 ? 'run' : 'runs'}
        </span>
      </div>
      <ul className="workflow-history-list">
        {sortedSnapshots.map((snap, idx) => {
          const isExpanded =
            expandedSnapshots.has(snap.id) || (idx === 0 && showLatest);
          return (
            <li key={snap.id} className={`workflow-history-snapshot${isExpanded ? ' is-expanded' : ''}`}>
              <button
                type="button"
                className="workflow-history-snapshot-summary"
                onClick={() => onToggleSnapshot(snap.id)}
              >
                <span className="workflow-history-disclosure" aria-hidden="true">
                  <CaretRightOutlined />
                </span>
                <span className={`workflow-run-dot is-${snap.status}`} aria-label={snap.status} />
                <span className="workflow-history-time">
                  {new Date(snap.executedAt).toLocaleString(undefined, {
                    month: 'short',
                    day: 'numeric',
                    hour: '2-digit',
                    minute: '2-digit',
                  })}
                </span>
                <span className={`workflow-history-status is-${snap.status}`}>{snap.status}</span>
                {snap.rowCount != null && (
                  <span className="workflow-history-rows">
                    {snap.rowCount.toLocaleString()} {snap.rowCount === 1 ? 'row' : 'rows'}
                  </span>
                )}
                {snap.rowCountDelta != null && snap.rowCountDelta !== 0 && (
                  <span
                    className={`workflow-history-delta${snap.rowCountDelta > 0 ? ' is-up' : ' is-down'}`}
                  >
                    {snap.rowCountDelta > 0 ? `+${snap.rowCountDelta}` : snap.rowCountDelta}
                  </span>
                )}
                {snap.triggeredAlerts && snap.triggeredAlerts.length > 0 && (
                  <span className="workflow-history-alert-pill">
                    <WarningOutlined /> {snap.triggeredAlerts.length}
                  </span>
                )}
              </button>
              {isExpanded && (
                <div className="workflow-history-snapshot-detail">
                  <RunDetail snapshot={snap} />
                </div>
              )}
            </li>
          );
        })}
      </ul>
    </div>
  );
}

function RunDetail({ snapshot }: { snapshot: QuerySnapshot }): ReactNode {
  const hasAdditionalContent =
    !!snapshot.errorMessage ||
    !!snapshot.aiSummary ||
    !!snapshot.resultPath ||
    (snapshot.previewRows != null && snapshot.previewRows.length > 0) ||
    (snapshot.triggeredAlerts != null && snapshot.triggeredAlerts.length > 0) ||
    (snapshot.rowCountDelta != null && snapshot.previousRowCount != null);

  return (
    <div className="workflow-detail-grid">
      {/* Always show the run timestamp + status + row count as a baseline.
          Without this, runs whose snapshot has no preview/AI/results render
          as a visually-empty block. */}
      <div className="workflow-detail-row">
        <span className="workflow-detail-label">Run</span>
        <span>
          {new Date(snapshot.executedAt).toLocaleString()}
          {' · '}
          <span className={`workflow-detail-status is-${snapshot.status}`}>{snapshot.status}</span>
          {snapshot.rowCount != null && <> · {snapshot.rowCount.toLocaleString()} rows</>}
          {snapshot.duration != null && <> · {Math.round(snapshot.duration)} ms</>}
        </span>
      </div>

      {snapshot.errorMessage && (
        <div className="workflow-detail-row">
          <span className="workflow-detail-label">Error</span>
          <span className="workflow-detail-alert">{snapshot.errorMessage}</span>
        </div>
      )}

      {!hasAdditionalContent && (
        <div className="workflow-detail-row">
          <span className="workflow-detail-label">Output</span>
          <span className="workflow-detail-empty-hint">
            No previewed data, AI summary, or persisted results for this run.
            {snapshot.status === 'success' && ' Enable result persistence or AI summary on the schedule to see content here.'}
          </span>
        </div>
      )}

      {snapshot.rowCountDelta != null && snapshot.previousRowCount != null && (
        <div className="workflow-detail-row">
          <span className="workflow-detail-label">Compared to previous</span>
          <span>
            <span
              className={`workflow-detail-delta${
                snapshot.rowCountDelta > 0 ? ' is-up' : snapshot.rowCountDelta < 0 ? ' is-down' : ''
              }`}
            >
              {snapshot.rowCountDelta > 0 ? `+${snapshot.rowCountDelta}` : snapshot.rowCountDelta}
            </span>
            {snapshot.previousRowCount != null && (
              <span className="workflow-detail-prev"> (prev {snapshot.previousRowCount.toLocaleString()})</span>
            )}
          </span>
        </div>
      )}

      {snapshot.triggeredAlerts && snapshot.triggeredAlerts.length > 0 && (
        <div className="workflow-detail-row">
          <span className="workflow-detail-label">Alerts</span>
          <span className="workflow-detail-alerts">
            {snapshot.triggeredAlerts.map((a, i) => (
              <span key={i} className="workflow-detail-alert">{a.message}</span>
            ))}
          </span>
        </div>
      )}

      {snapshot.resultPath && (
        <div className="workflow-detail-row">
          <span className="workflow-detail-label">Results</span>
          <code className="workflow-detail-path">{`SELECT * FROM dfs.\`${snapshot.resultPath}\``}</code>
        </div>
      )}

      {snapshot.aiSummary && (
        <div className="workflow-detail-row">
          <span className="workflow-detail-label">AI Summary</span>
          <div className="workflow-detail-ai">
            <Markdown rehypePlugins={[rehypeRaw]}>{snapshot.aiSummary}</Markdown>
          </div>
        </div>
      )}

      {snapshot.previewRows && snapshot.previewColumns && snapshot.previewRows.length > 0 && (
        <div className="workflow-detail-row">
          <span className="workflow-detail-label">Preview</span>
          <div className="workflow-detail-preview">
            <table>
              <thead>
                <tr>
                  {snapshot.previewColumns.map((c) => <th key={c}>{c}</th>)}
                </tr>
              </thead>
              <tbody>
                {snapshot.previewRows.slice(0, 5).map((row, idx) => (
                  <tr key={idx}>
                    {snapshot.previewColumns!.map((c) => <td key={c}>{String(row[c] ?? '')}</td>)}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

