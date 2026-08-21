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
import { useState, useMemo, useCallback, type ReactNode } from 'react';
import { useNavigate } from 'react-router-dom';
import { Input, Button, Spin, message, Tooltip, Select, Popconfirm } from 'antd';
import {
  SearchOutlined,
  CheckOutlined,
  ReloadOutlined,
  StopOutlined,
} from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getQueryProfiles, cancelQuery } from '../api/queries';
import type { QueryProfile } from '../api/queries';
import { usePageChrome } from '../contexts/AppChromeContext';

type StatusFilter = 'all' | 'running' | 'succeeded' | 'failed' | 'cancelled';

const STATUS_DOT_COLORS: Record<string, string> = {
  Succeeded: 'var(--color-success)',
  Failed: 'var(--color-error)',
  Running: 'var(--color-primary)',
  Cancelled: 'var(--color-warning)',
  Cancellation_Requested: 'var(--color-warning)',
};

function detectQueryType(sql: string): string {
  if (!sql) {
    return 'Unknown';
  }
  const normalized = sql.trim().toUpperCase();
  let firstWord = '';
  for (const line of normalized.split('\n')) {
    const trimmed = line.trim();
    if (trimmed.startsWith('--') || trimmed.startsWith('/*')) {
      continue;
    }
    const m = trimmed.match(/^([A-Z]+)/);
    if (m) {
      firstWord = m[1];
      break;
    }
  }
  if (firstWord.startsWith('SELECT') || firstWord.startsWith('WITH')) return 'SELECT';
  if (firstWord.startsWith('INSERT')) return 'INSERT';
  if (firstWord.startsWith('UPDATE')) return 'UPDATE';
  if (firstWord.startsWith('DELETE')) return 'DELETE';
  if (firstWord.startsWith('CREATE') || firstWord.startsWith('CTAS')) return 'CREATE';
  if (firstWord.startsWith('DROP') || firstWord.startsWith('TRUNCATE')) return 'DROP';
  if (firstWord.startsWith('ALTER')) return 'ALTER';
  if (firstWord.startsWith('SHOW') || firstWord.startsWith('DESCRIBE') || firstWord.startsWith('DESC')) return 'SHOW';
  if (firstWord.startsWith('EXPLAIN')) return 'EXPLAIN';
  if (firstWord.startsWith('USE')) return 'USE';
  if (firstWord.startsWith('SET')) return 'SET';
  return 'Other';
}

function formatTime(ts: number): string {
  return new Date(ts).toLocaleTimeString(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
}

function dayLabel(ts: number): string {
  const d = new Date(ts);
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const tomorrow = new Date(today);
  tomorrow.setDate(today.getDate() + 1);
  const yesterday = new Date(today);
  yesterday.setDate(today.getDate() - 1);
  const weekAgo = new Date(today);
  weekAgo.setDate(today.getDate() - 6);

  if (d >= today && d < tomorrow) {
    return 'Today';
  }
  if (d >= yesterday && d < today) {
    return 'Yesterday';
  }
  if (d >= weekAgo && d < yesterday) {
    return d.toLocaleDateString(undefined, { weekday: 'long' });
  }
  return d.toLocaleDateString(undefined, { month: 'long', day: 'numeric', year: 'numeric' });
}

function dayKey(ts: number): string {
  const d = new Date(ts);
  return `${d.getFullYear()}-${d.getMonth()}-${d.getDate()}`;
}

function singleLine(s: string, max = 200): string {
  const trimmed = s.replace(/\s+/g, ' ').trim();
  return trimmed.length > max ? trimmed.slice(0, max) + '…' : trimmed;
}

function isRunningState(state: string): boolean {
  return state === 'Running' || state === 'Cancellation_Requested';
}

interface DayGroup {
  key: string;
  label: string;
  entries: QueryProfile[];
}

export default function ProfilesPage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [searchText, setSearchText] = useState('');
  const [statusFilter, setStatusFilter] = useState<StatusFilter>('all');
  const [userFilter, setUserFilter] = useState<string | null>(null);
  const [typeFilter, setTypeFilter] = useState<string | null>(null);
  const [selectedRunningIds, setSelectedRunningIds] = useState<Set<string>>(new Set());
  const [isCancelling, setIsCancelling] = useState(false);

  const { data: profilesData, isLoading, error, refetch, isFetching } = useQuery({
    queryKey: ['profiles'],
    queryFn: getQueryProfiles,
    refetchInterval: 5000,
  });

  const cancelMutation = useMutation({ mutationFn: cancelQuery });

  // All entries combined, sorted newest first
  const allEntries = useMemo(() => {
    const running = profilesData?.runningQueries ?? [];
    const finished = profilesData?.finishedQueries ?? [];
    return [...running, ...finished].sort((a, b) => b.startTime - a.startTime);
  }, [profilesData]);

  const allUsers = useMemo(() => {
    const set = new Set<string>();
    allEntries.forEach((q) => q.user && set.add(q.user));
    return Array.from(set).sort();
  }, [allEntries]);

  const allTypes = useMemo(() => {
    const set = new Set<string>();
    allEntries.forEach((q) => set.add(detectQueryType(q.query)));
    return Array.from(set).sort();
  }, [allEntries]);

  const filteredEntries = useMemo(() => {
    let result = allEntries;
    if (statusFilter !== 'all') {
      result = result.filter((q) => {
        if (statusFilter === 'running') {
          return isRunningState(q.state);
        }
        if (statusFilter === 'succeeded') {
          return q.state === 'Succeeded';
        }
        if (statusFilter === 'failed') {
          return q.state === 'Failed';
        }
        if (statusFilter === 'cancelled') {
          return q.state === 'Cancelled' || q.state === 'Cancellation_Requested';
        }
        return true;
      });
    }
    if (userFilter) {
      result = result.filter((q) => q.user === userFilter);
    }
    if (typeFilter) {
      result = result.filter((q) => detectQueryType(q.query) === typeFilter);
    }
    if (searchText) {
      const lower = searchText.toLowerCase();
      result = result.filter(
        (q) =>
          q.user.toLowerCase().includes(lower) ||
          q.query.toLowerCase().includes(lower) ||
          q.queryId.toLowerCase().includes(lower),
      );
    }
    return result;
  }, [allEntries, statusFilter, userFilter, typeFilter, searchText]);

  // Group by day
  const grouped = useMemo<DayGroup[]>(() => {
    const groups = new Map<string, DayGroup>();
    for (const e of filteredEntries) {
      const k = dayKey(e.startTime);
      if (!groups.has(k)) {
        groups.set(k, { key: k, label: dayLabel(e.startTime), entries: [] });
      }
      groups.get(k)!.entries.push(e);
    }
    return Array.from(groups.values());
  }, [filteredEntries]);

  const runningCount = useMemo(
    () => allEntries.filter((q) => isRunningState(q.state)).length,
    [allEntries],
  );

  const toggleRunningSelection = (id: string) => {
    setSelectedRunningIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  };

  const handleCancelSelected = useCallback(async () => {
    if (selectedRunningIds.size === 0) {
      return;
    }
    setIsCancelling(true);
    const failures: string[] = [];
    let successes = 0;
    for (const id of selectedRunningIds) {
      try {
        await cancelMutation.mutateAsync(id);
        successes++;
      } catch (e) {
        failures.push(`${id}: ${(e as Error).message}`);
      }
    }
    setIsCancelling(false);
    if (failures.length === 0) {
      message.success(`Cancelled ${successes} ${successes === 1 ? 'query' : 'queries'}`);
      setSelectedRunningIds(new Set());
      queryClient.invalidateQueries({ queryKey: ['profiles'] });
    } else if (successes === 0) {
      message.error(`Failed to cancel: ${failures.join('; ')}`);
    } else {
      message.warning(`Cancelled ${successes}, ${failures.length} failed`);
    }
  }, [selectedRunningIds, cancelMutation, queryClient]);

  // Toolbar action: refresh
  const toolbarActions = useMemo(
    () => (
      <Tooltip title="Refresh">
        <Button
          type="text"
          size="small"
          icon={<ReloadOutlined spin={isFetching} />}
          onClick={() => refetch()}
        />
      </Tooltip>
    ),
    [refetch, isFetching],
  );
  usePageChrome({ toolbarActions });

  if (error) {
    return (
      <div className="page-profiles-error">
        <h2>Couldn't load query history</h2>
        <p>{(error as Error).message}</p>
      </div>
    );
  }

  return (
    <div className="page-profiles">
      <header className="page-profiles-header">
        <div>
          <h1 className="page-profiles-title">Query History</h1>
          <p className="page-profiles-subtitle">
            {isLoading
              ? 'Loading…'
              : (
                <>
                  {runningCount > 0 && (
                    <span className="page-profiles-running-pill">
                      <span className="page-profiles-running-dot" />
                      {runningCount} running
                    </span>
                  )}
                  {' '}
                  {allEntries.length === 0
                    ? 'No queries yet'
                    : `${allEntries.length} total · auto-refreshing`}
                </>
              )}
          </p>
        </div>
      </header>

      <div className="page-profiles-toolbar">
        <Input
          placeholder="Search SQL, user, or query ID…"
          prefix={<SearchOutlined style={{ color: 'var(--color-text-tertiary)' }} />}
          value={searchText}
          onChange={(e) => setSearchText(e.target.value)}
          allowClear
          className="page-profiles-search"
        />

        <div className="page-profiles-chips">
          {(['all', 'running', 'succeeded', 'failed', 'cancelled'] as StatusFilter[]).map((s) => (
            <button
              key={s}
              type="button"
              className={`page-profiles-chip${statusFilter === s ? ' is-active' : ''}`}
              onClick={() => setStatusFilter(s)}
            >
              {s === 'all' && 'All'}
              {s === 'running' && (
                <>
                  <span className="page-profiles-chip-dot" style={{ background: STATUS_DOT_COLORS.Running }} />
                  Running
                </>
              )}
              {s === 'succeeded' && (
                <>
                  <span className="page-profiles-chip-dot" style={{ background: STATUS_DOT_COLORS.Succeeded }} />
                  Succeeded
                </>
              )}
              {s === 'failed' && (
                <>
                  <span className="page-profiles-chip-dot" style={{ background: STATUS_DOT_COLORS.Failed }} />
                  Failed
                </>
              )}
              {s === 'cancelled' && (
                <>
                  <span className="page-profiles-chip-dot" style={{ background: STATUS_DOT_COLORS.Cancelled }} />
                  Cancelled
                </>
              )}
            </button>
          ))}
        </div>

        {allUsers.length > 1 && (
          <Select
            size="small"
            placeholder="Any user"
            value={userFilter}
            onChange={setUserFilter}
            allowClear
            style={{ width: 140 }}
            options={allUsers.map((u) => ({ value: u, label: u }))}
          />
        )}

        {allTypes.length > 1 && (
          <Select
            size="small"
            placeholder="Any type"
            value={typeFilter}
            onChange={setTypeFilter}
            allowClear
            style={{ width: 130 }}
            options={allTypes.map((t) => ({ value: t, label: t }))}
          />
        )}

        {selectedRunningIds.size > 0 && (
          <div className="page-profiles-bulkbar">
            <span>{selectedRunningIds.size} selected</span>
            <Popconfirm
              title={`Cancel ${selectedRunningIds.size} ${selectedRunningIds.size === 1 ? 'query' : 'queries'}?`}
              onConfirm={handleCancelSelected}
              okText="Cancel them"
              cancelText="Keep"
              okButtonProps={{ danger: true, loading: isCancelling }}
            >
              <Button size="small" danger icon={<StopOutlined />} loading={isCancelling}>
                Cancel
              </Button>
            </Popconfirm>
            <Button size="small" type="text" onClick={() => setSelectedRunningIds(new Set())}>
              Clear
            </Button>
          </div>
        )}
      </div>

      {isLoading ? (
        <div className="page-profiles-loading"><Spin size="large" /></div>
      ) : grouped.length === 0 ? (
        <EmptyState
          searching={!!searchText || statusFilter !== 'all' || !!userFilter || !!typeFilter}
          onClear={() => {
            setSearchText('');
            setStatusFilter('all');
            setUserFilter(null);
            setTypeFilter(null);
          }}
        />
      ) : (
        <div className="page-profiles-timeline">
          {grouped.map((group) => (
            <section key={group.key} className="profiles-day-group">
              <h2 className="profiles-day-header">{group.label}</h2>
              <ul className="profiles-day-list">
                {group.entries.map((q) => {
                  const running = isRunningState(q.state);
                  const selected = selectedRunningIds.has(q.queryId);
                  const dotColor = STATUS_DOT_COLORS[q.state] || 'var(--color-text-tertiary)';
                  return (
                    <li
                      key={q.queryId}
                      className={`profiles-row${running ? ' is-running' : ''}${selected ? ' is-selected' : ''}`}
                      onClick={() => navigate(`/profiles/${q.queryId}`)}
                      role="button"
                      tabIndex={0}
                      onKeyDown={(e) => {
                        if (e.key === 'Enter' || e.key === ' ') {
                          e.preventDefault();
                          navigate(`/profiles/${q.queryId}`);
                        }
                      }}
                    >
                      {running && (
                        <button
                          type="button"
                          className={`profiles-row-check${selected ? ' is-on' : ''}`}
                          onClick={(e) => {
                            e.stopPropagation();
                            toggleRunningSelection(q.queryId);
                          }}
                          aria-label={selected ? 'Deselect' : 'Select for cancellation'}
                        >
                          {selected && <CheckOutlined />}
                        </button>
                      )}

                      <span className={`profiles-row-dot${running ? ' is-pulsing' : ''}`} style={{ background: dotColor }} />

                      <span className="profiles-row-time">{formatTime(q.startTime)}</span>

                      <span className={`profiles-row-type type-${detectQueryType(q.query).toLowerCase()}`}>
                        {detectQueryType(q.query)}
                      </span>

                      <span className="profiles-row-user">{q.user}</span>

                      <span className="profiles-row-sql" title={q.query}>
                        {singleLine(q.query, 240)}
                      </span>

                      <span className="profiles-row-duration">
                        {q.duration ?? (running ? 'running…' : '—')}
                      </span>
                    </li>
                  );
                })}
              </ul>
            </section>
          ))}
        </div>
      )}
    </div>
  );
}

function EmptyState({ searching, onClear }: { searching: boolean; onClear: () => void }): ReactNode {
  return (
    <div className="page-profiles-empty">
      <span className="page-profiles-empty-glyph" aria-hidden="true">
        <span className="page-profiles-empty-stack">
          <span /><span /><span />
        </span>
      </span>
      <h2>{searching ? 'No matches' : 'No query activity'}</h2>
      <p>
        {searching
          ? 'Try a different search or clear the filters.'
          : 'Run a query in SQL Lab and it will show up here.'}
      </p>
      {searching && <Button onClick={onClear}>Clear filters</Button>}
    </div>
  );
}

