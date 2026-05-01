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
import { useState, useEffect, useMemo, useRef } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { Modal, Input } from 'antd';
import type { InputRef } from 'antd';
import {
  SearchOutlined,
  FolderOutlined,
  CodeOutlined,
  BarChartOutlined,
  DashboardOutlined,
} from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { getProjects } from '../../api/projects';
import { getSavedQueries } from '../../api/savedQueries';
import { getVisualizations } from '../../api/visualizations';
import { getDashboards } from '../../api/dashboards';

interface CommandPaletteProps {
  externalOpen?: boolean;
  onExternalOpenChange?: (open: boolean) => void;
}

type ResultKind = 'project' | 'savedQuery' | 'visualization' | 'dashboard';

interface SpotlightItem {
  id: string;
  kind: ResultKind;
  title: string;
  subtitle?: string;
  /** When provided, navigation runs this callback instead of the default. */
  onSelect: () => void;
}

interface ResultGroup {
  kind: ResultKind;
  label: string;
  icon: React.ReactNode;
  items: SpotlightItem[];
}

const PER_CATEGORY_LIMIT = 5;

const KIND_LABEL: Record<ResultKind, string> = {
  project: 'Projects',
  savedQuery: 'Saved Queries',
  visualization: 'Visualizations',
  dashboard: 'Dashboards',
};

const KIND_ICON: Record<ResultKind, React.ReactNode> = {
  project: <FolderOutlined />,
  savedQuery: <CodeOutlined />,
  visualization: <BarChartOutlined />,
  dashboard: <DashboardOutlined />,
};

function score(text: string, query: string): number {
  if (!query) {
    return 1;
  }
  const t = text.toLowerCase();
  const q = query.toLowerCase();
  if (t === q) {
    return 100;
  }
  if (t.startsWith(q)) {
    return 50;
  }
  // Word-boundary match
  if (new RegExp(`\\b${q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`, 'i').test(text)) {
    return 25;
  }
  if (t.includes(q)) {
    return 10;
  }
  return 0;
}

export default function CommandPalette({ externalOpen, onExternalOpenChange }: CommandPaletteProps = {}) {
  const [internalOpen, setInternalOpen] = useState(false);
  const open = externalOpen !== undefined ? externalOpen : internalOpen;
  const setOpen = (next: boolean) => {
    if (onExternalOpenChange) {
      onExternalOpenChange(next);
    } else {
      setInternalOpen(next);
    }
  };
  const [search, setSearch] = useState('');
  const [activeIndex, setActiveIndex] = useState(0);
  const navigate = useNavigate();
  const location = useLocation();
  const inputRef = useRef<InputRef>(null);
  const listRef = useRef<HTMLDivElement>(null);

  const { data: projects } = useQuery({
    queryKey: ['projects'],
    queryFn: getProjects,
    enabled: open,
    staleTime: 60_000,
  });

  const { data: savedQueries } = useQuery({
    queryKey: ['savedQueries'],
    queryFn: getSavedQueries,
    enabled: open,
    staleTime: 60_000,
  });

  const { data: visualizations } = useQuery({
    queryKey: ['visualizations'],
    queryFn: getVisualizations,
    enabled: open,
    staleTime: 60_000,
  });

  const { data: dashboards } = useQuery({
    queryKey: ['dashboards'],
    queryFn: getDashboards,
    enabled: open,
    staleTime: 60_000,
  });

  // Project context affects link targets — saved queries, viz, dashboards in
  // a project route should navigate within that project.
  const currentProjectId = location.pathname.match(/\/projects\/([^/]+)/)?.[1];

  const groups = useMemo<ResultGroup[]>(() => {
    const buildGroup = <T,>(
      kind: ResultKind,
      source: T[] | undefined,
      pickName: (item: T) => string,
      pickSubtitle: (item: T) => string | undefined,
      onSelect: (item: T) => void,
      extraFields?: (item: T) => string[],
    ): ResultGroup => {
      const items: SpotlightItem[] = [];
      if (!source) {
        return { kind, label: KIND_LABEL[kind], icon: KIND_ICON[kind], items: [] };
      }
      const ranked = source
        .map((item) => {
          const fields = [pickName(item), ...(extraFields?.(item) ?? [])].filter(
            (s): s is string => typeof s === 'string' && s.length > 0,
          );
          const sub = pickSubtitle(item);
          if (sub) {
            fields.push(sub);
          }
          const fieldScore = Math.max(...fields.map((f) => score(f, search)));
          return { item, fieldScore };
        })
        .filter(({ fieldScore }) => fieldScore > 0)
        .sort((a, b) => b.fieldScore - a.fieldScore)
        .slice(0, PER_CATEGORY_LIMIT);

      for (const { item } of ranked) {
        const id = (item as { id: string }).id;
        items.push({
          id: `${kind}:${id}`,
          kind,
          title: pickName(item),
          subtitle: pickSubtitle(item),
          onSelect: () => onSelect(item),
        });
      }
      return { kind, label: KIND_LABEL[kind], icon: KIND_ICON[kind], items };
    };

    return [
      buildGroup(
        'project',
        projects,
        (p) => p.name,
        (p) => p.description,
        (p) => navigate(`/projects/${p.id}/query`),
        (p) => p.tags ?? [],
      ),
      buildGroup(
        'savedQuery',
        savedQueries,
        (q) => q.name,
        (q) => q.description ?? (q.sql ? q.sql.replace(/\s+/g, ' ').slice(0, 80) : undefined),
        (q) => {
          // Open in SQL Lab with the query loaded
          const target = currentProjectId ? `/projects/${currentProjectId}/query` : '/query';
          navigate(target, { state: { loadQuery: q } });
        },
      ),
      buildGroup(
        'visualization',
        visualizations,
        (v) => v.name,
        (v) => v.description ?? `${v.chartType} chart`,
        (v) => {
          const target = currentProjectId
            ? `/projects/${currentProjectId}/visualizations/${v.id}`
            : `/visualizations/${v.id}`;
          navigate(target);
        },
      ),
      buildGroup(
        'dashboard',
        dashboards,
        (d) => d.name,
        (d) => d.description ?? `${d.panels?.length ?? 0} panels`,
        (d) => {
          const target = currentProjectId
            ? `/projects/${currentProjectId}/dashboards/${d.id}`
            : `/dashboards/${d.id}`;
          navigate(target);
        },
      ),
    ];
  }, [projects, savedQueries, visualizations, dashboards, search, navigate, currentProjectId]);

  const flatItems = useMemo(() => groups.flatMap((g) => g.items), [groups]);
  const totalItems = flatItems.length;

  // Reset active index when results change
  useEffect(() => {
    setActiveIndex(0);
  }, [search]);

  // Focus input + reset state on open
  useEffect(() => {
    if (open) {
      setSearch('');
      setActiveIndex(0);
      requestAnimationFrame(() => inputRef.current?.focus());
    }
  }, [open]);

  // ⌘K toggle
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault();
        setOpen(!open);
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  // Auto-scroll the active item into view
  useEffect(() => {
    if (!open || !listRef.current) {
      return;
    }
    const el = listRef.current.querySelector<HTMLElement>(`[data-spotlight-idx="${activeIndex}"]`);
    el?.scrollIntoView({ block: 'nearest' });
  }, [activeIndex, open]);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setActiveIndex((prev) => (totalItems === 0 ? 0 : (prev + 1) % totalItems));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setActiveIndex((prev) => (totalItems === 0 ? 0 : (prev - 1 + totalItems) % totalItems));
    } else if (e.key === 'Enter') {
      const item = flatItems[activeIndex];
      if (item) {
        e.preventDefault();
        item.onSelect();
        setOpen(false);
      }
    }
  };

  return (
    <Modal
      open={open}
      onCancel={() => setOpen(false)}
      footer={null}
      closable={false}
      width={620}
      styles={{ body: { padding: 0 } }}
      className="spotlight-modal"
      destroyOnClose
    >
      <div className="spotlight-search">
        <Input
          ref={inputRef}
          placeholder="Search projects, queries, visualizations, dashboards…"
          prefix={<SearchOutlined style={{ fontSize: 16 }} />}
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          onKeyDown={handleKeyDown}
          variant="borderless"
          size="large"
          autoComplete="off"
        />
      </div>

      <div className="spotlight-results" ref={listRef}>
        {totalItems === 0 ? (
          <div className="spotlight-empty">
            {search ? `No matches for "${search}"` : 'Start typing to search…'}
          </div>
        ) : (
          (() => {
            let runningIdx = -1;
            return groups.map((group) => {
              if (group.items.length === 0) {
                return null;
              }
              return (
                <div key={group.kind} className="spotlight-group">
                  <div className="spotlight-group-header">
                    <span className="spotlight-group-icon">{group.icon}</span>
                    <span className="spotlight-group-label">{group.label}</span>
                  </div>
                  <ul className="spotlight-group-items" role="listbox">
                    {group.items.map((item) => {
                      runningIdx++;
                      const isActive = runningIdx === activeIndex;
                      return (
                        <li
                          key={item.id}
                          role="option"
                          aria-selected={isActive}
                          data-spotlight-idx={runningIdx}
                          className={`spotlight-item${isActive ? ' is-active' : ''}`}
                          onMouseEnter={() => setActiveIndex(runningIdx)}
                          onClick={() => {
                            item.onSelect();
                            setOpen(false);
                          }}
                        >
                          <span className="spotlight-item-icon">{KIND_ICON[item.kind]}</span>
                          <div className="spotlight-item-text">
                            <div className="spotlight-item-title">{item.title}</div>
                            {item.subtitle && (
                              <div className="spotlight-item-subtitle">{item.subtitle}</div>
                            )}
                          </div>
                        </li>
                      );
                    })}
                  </ul>
                </div>
              );
            });
          })()
        )}
      </div>

      <div className="spotlight-footer">
        <span><kbd>↑</kbd><kbd>↓</kbd> Navigate</span>
        <span><kbd>↵</kbd> Open</span>
        <span><kbd>Esc</kbd> Close</span>
      </div>
    </Modal>
  );
}
