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
import { type CSSProperties, type ReactNode, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Link, useLocation, useNavigate } from 'react-router-dom';
import { Tooltip, Input } from 'antd';
import {
  HomeOutlined,
  CodeOutlined,
  SaveOutlined,
  BarChartOutlined,
  DashboardOutlined,
  FieldTimeOutlined,
  DatabaseOutlined,
  HistoryOutlined,
  LineChartOutlined,
  SettingOutlined,
  FileTextOutlined,
  BookOutlined,
  CaretRightOutlined,
  SearchOutlined,
  HolderOutlined,
  FieldTimeOutlined as WorkflowIcon,
} from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { getProjects } from '../../api/projects';
import { getSchedules } from '../../api/schedules';
import { useAppChrome } from '../../contexts/AppChromeContext';
import { useProjectOrder } from './usePinnedRecentProjects';
import { coverFromProject } from '../../hooks/useProjectAppearance';
import { resolveTileBackground, TINT_PALETTE } from '../project/ProjectAppearanceField';
import type { Project } from '../../types';

const EXPANDED_KEY = 'drill-shell-expanded-projects';
const WIDTH_KEY = 'drill-shell-sidebar-width';
const MIN_WIDTH = 200;
const MAX_WIDTH = 380;
const DEFAULT_WIDTH = 232;

function readSidebarWidth(): number {
  try {
    const v = localStorage.getItem(WIDTH_KEY);
    if (v) {
      const n = parseInt(v, 10);
      if (!Number.isNaN(n)) {
        return Math.max(MIN_WIDTH, Math.min(MAX_WIDTH, n));
      }
    }
  } catch {
    // Ignore
  }
  return DEFAULT_WIDTH;
}

function readExpanded(): Record<string, boolean> {
  try {
    const raw = localStorage.getItem(EXPANDED_KEY);
    if (raw) {
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === 'object') {
        return parsed as Record<string, boolean>;
      }
    }
  } catch {
    // Ignore storage errors
  }
  return {};
}

function writeExpanded(next: Record<string, boolean>): void {
  try {
    localStorage.setItem(EXPANDED_KEY, JSON.stringify(next));
  } catch {
    // Ignore storage errors
  }
}

/**
 * Tile-color swatch (or image thumbnail) used as the project's sidebar icon.
 * Mirrors the project's tile cover so identity stays consistent everywhere.
 */
function ProjectGlyph({ project }: { project: Project }) {
  const cover = coverFromProject(project, TINT_PALETTE);
  const bg = resolveTileBackground(cover, project.id);
  return <span className="shell-sidebar-project-glyph" style={bg} aria-hidden="true" />;
}

interface NavItem {
  to: string;
  label: string;
  icon: ReactNode;
  matchPrefix?: string;
}

const WORKSPACE: NavItem[] = [
  { to: '/projects', label: 'Home', icon: <HomeOutlined /> },
  { to: '/query', label: 'SQL Lab', icon: <CodeOutlined />, matchPrefix: '/query' },
];

// Cross-project aggregate views — labels make scope explicit
const LIBRARY: NavItem[] = [
  { to: '/saved-queries', label: 'All Saved Queries', icon: <SaveOutlined />, matchPrefix: '/saved-queries' },
  { to: '/visualizations', label: 'All Visualizations', icon: <BarChartOutlined />, matchPrefix: '/visualizations' },
  { to: '/dashboards', label: 'All Dashboards', icon: <DashboardOutlined />, matchPrefix: '/dashboards' },
  { to: '/workflows', label: 'Workflows', icon: <FieldTimeOutlined />, matchPrefix: '/workflows' },
  { to: '/profiles', label: 'Query History', icon: <HistoryOutlined />, matchPrefix: '/profiles' },
];

const ADMIN: NavItem[] = [
  { to: '/datasources', label: 'Data Sources', icon: <DatabaseOutlined />, matchPrefix: '/datasources' },
  { to: '/metrics', label: 'Metrics', icon: <LineChartOutlined /> },
  { to: '/options', label: 'System Options', icon: <SettingOutlined /> },
  { to: '/logs', label: 'Server Logs', icon: <FileTextOutlined /> },
];

interface ProjectSection {
  key: string;
  label: string;
  icon: ReactNode;
}

const BASE_PROJECT_SECTIONS: ProjectSection[] = [
  { key: 'query', label: 'Query', icon: <CodeOutlined /> },
  { key: 'queries', label: 'Saved Queries', icon: <SaveOutlined /> },
  { key: 'visualizations', label: 'Visualizations', icon: <BarChartOutlined /> },
  { key: 'dashboards', label: 'Dashboards', icon: <DashboardOutlined /> },
  { key: 'datasources', label: 'Data Sources', icon: <DatabaseOutlined /> },
];

const WORKFLOWS_SECTION: ProjectSection = {
  key: 'workflows',
  label: 'Workflows',
  icon: <WorkflowIcon />,
};

const TRAILING_PROJECT_SECTIONS: ProjectSection[] = [
  { key: 'wiki', label: 'Wiki', icon: <BookOutlined /> },
  { key: 'settings', label: 'Settings', icon: <SettingOutlined /> },
];

const VISIBLE_PROJECTS_WHEN_IDLE = 6;

function isActive(pathname: string, item: NavItem): boolean {
  if (item.matchPrefix) {
    return pathname === item.to || pathname.startsWith(item.matchPrefix + '/');
  }
  return pathname === item.to;
}

interface ItemRowProps {
  to: string;
  label: string;
  icon: ReactNode;
  active: boolean;
  collapsed: boolean;
  indent?: number;
  trailing?: ReactNode;
  onClick?: () => void;
}

function ItemRow({ to, label, icon, active, collapsed, indent = 0, trailing, onClick }: ItemRowProps) {
  const inner = (
    <Link
      to={to}
      onClick={onClick}
      className={`shell-sidebar-item${active ? ' is-active' : ''}`}
      style={{ paddingLeft: 8 + indent * 14 }}
    >
      <span className="shell-sidebar-item-icon">{icon}</span>
      {!collapsed && <span className="shell-sidebar-item-label">{label}</span>}
      {!collapsed && trailing && <span className="shell-sidebar-item-trailing">{trailing}</span>}
    </Link>
  );
  if (collapsed) {
    return (
      <Tooltip title={label} placement="right">
        {inner}
      </Tooltip>
    );
  }
  return inner;
}

interface SectionProps {
  title: string;
  collapsed: boolean;
  collapsible?: boolean;
  defaultOpen?: boolean;
  trailing?: ReactNode;
  children: ReactNode;
}

function Section({ title, collapsed, collapsible = false, defaultOpen = true, trailing, children }: SectionProps) {
  const [open, setOpen] = useState(defaultOpen);
  if (collapsed) {
    return <div className="shell-sidebar-section is-collapsed">{children}</div>;
  }
  return (
    <div className="shell-sidebar-section">
      <div
        className={`shell-sidebar-section-header${collapsible ? ' is-collapsible' : ''}`}
        onClick={collapsible ? () => setOpen((v) => !v) : undefined}
        role={collapsible ? 'button' : undefined}
      >
        {collapsible && (
          <CaretRightOutlined
            className="shell-sidebar-caret"
            style={{ transform: open ? 'rotate(90deg)' : 'rotate(0deg)' }}
          />
        )}
        <span>{title}</span>
        {trailing && <span className="shell-sidebar-section-trailing">{trailing}</span>}
      </div>
      {open && <div className="shell-sidebar-section-body">{children}</div>}
    </div>
  );
}

type DropPosition = 'before' | 'after';

interface DragState {
  draggingId: string | null;
  hoverId: string | null;
  hoverPosition: DropPosition;
}

interface ProjectRowProps {
  project: Project;
  active: boolean;
  expanded: boolean;
  collapsed: boolean;
  drag: DragState;
  sections: ProjectSection[];
  onToggleExpand: (id: string) => void;
  onNavigateToProject: (id: string) => void;
  onDragStart: (id: string) => void;
  onDragOver: (id: string, position: DropPosition) => void;
  onDragLeave: (id: string) => void;
  onDrop: (id: string) => void;
  onDragEnd: () => void;
  activeProjectSubPath: string | null;
}

function ProjectRow({
  project,
  active,
  expanded,
  collapsed,
  drag,
  sections,
  onToggleExpand,
  onNavigateToProject,
  onDragStart,
  onDragOver,
  onDragLeave,
  onDrop,
  onDragEnd,
  activeProjectSubPath,
}: ProjectRowProps) {
  const handleChevron = (e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    onToggleExpand(project.id);
  };

  const handleRowClick = (e: React.MouseEvent) => {
    e.preventDefault();
    onNavigateToProject(project.id);
  };

  if (collapsed) {
    // Icon-only sidebar: just the glyph, no drag, no sub-nav
    return (
      <Tooltip title={project.name} placement="right">
        <Link
          to={`/projects/${project.id}/query`}
          className={`shell-sidebar-item${active && !activeProjectSubPath ? ' is-active' : ''}`}
          style={{ paddingLeft: 8 }}
        >
          <span className="shell-sidebar-item-icon">
            <ProjectGlyph project={project} />
          </span>
        </Link>
      </Tooltip>
    );
  }

  const isDragging = drag.draggingId === project.id;
  const isDropTarget = drag.hoverId === project.id && drag.draggingId !== null && drag.draggingId !== project.id;

  const handleDragStart = (e: React.DragEvent) => {
    e.dataTransfer.effectAllowed = 'move';
    e.dataTransfer.setData('text/plain', project.id);
    onDragStart(project.id);
  };

  const handleDragOver = (e: React.DragEvent) => {
    if (!drag.draggingId || drag.draggingId === project.id) {
      return;
    }
    e.preventDefault();
    e.dataTransfer.dropEffect = 'move';
    const rect = e.currentTarget.getBoundingClientRect();
    const midpoint = rect.top + rect.height / 2;
    const position: DropPosition = e.clientY < midpoint ? 'before' : 'after';
    onDragOver(project.id, position);
  };

  const handleDragLeave = () => {
    onDragLeave(project.id);
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    onDrop(project.id);
  };

  return (
    <div
      className={`shell-sidebar-project${isDragging ? ' is-dragging' : ''}${
        isDropTarget ? ` is-drop-${drag.hoverPosition}` : ''
      }`}
    >
      <a
        href={`/projects/${project.id}/query`}
        onClick={handleRowClick}
        className={`shell-sidebar-item shell-sidebar-project-row${active && !activeProjectSubPath ? ' is-active' : ''}`}
        draggable
        onDragStart={handleDragStart}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        onDrop={handleDrop}
        onDragEnd={onDragEnd}
      >
        <button
          type="button"
          className={`shell-sidebar-disclosure${expanded ? ' is-open' : ''}`}
          onClick={handleChevron}
          aria-label={expanded ? 'Collapse' : 'Expand'}
          aria-expanded={expanded}
        >
          <CaretRightOutlined />
        </button>
        <ProjectGlyph project={project} />
        <span className="shell-sidebar-item-label">{project.name}</span>
        <span className="shell-sidebar-drag-handle" aria-hidden="true">
          <HolderOutlined />
        </span>
      </a>
      {expanded && (
        <div className="shell-sidebar-subgroup">
          {sections.map((s) => (
            <ItemRow
              key={s.key}
              to={`/projects/${project.id}/${s.key}`}
              label={s.label}
              icon={s.icon}
              active={active && activeProjectSubPath === s.key}
              collapsed={false}
              indent={1}
            />
          ))}
        </div>
      )}
    </div>
  );
}

export default function Sidebar() {
  const location = useLocation();
  const navigate = useNavigate();
  const { sidebarCollapsed } = useAppChrome();
  const { order, moveProject } = useProjectOrder();

  // Drag-and-drop state for project reordering
  const [drag, setDrag] = useState<DragState>({ draggingId: null, hoverId: null, hoverPosition: 'after' });
  const handleDragStart = useCallback((id: string) => {
    setDrag({ draggingId: id, hoverId: null, hoverPosition: 'after' });
  }, []);
  const handleDragOverProject = useCallback((id: string, position: DropPosition) => {
    setDrag((prev) => {
      if (prev.hoverId === id && prev.hoverPosition === position) {
        return prev;
      }
      return { ...prev, hoverId: id, hoverPosition: position };
    });
  }, []);
  const handleDragLeaveProject = useCallback((id: string) => {
    setDrag((prev) => (prev.hoverId === id ? { ...prev, hoverId: null } : prev));
  }, []);
  const handleDropProject = useCallback(
    (targetId: string) => {
      setDrag((prev) => {
        if (prev.draggingId && prev.draggingId !== targetId) {
          moveProject(prev.draggingId, targetId, prev.hoverPosition);
        }
        return { draggingId: null, hoverId: null, hoverPosition: 'after' };
      });
    },
    [moveProject],
  );
  const handleDragEnd = useCallback(() => {
    setDrag({ draggingId: null, hoverId: null, hoverPosition: 'after' });
  }, []);

  const projectMatch = location.pathname.match(/^\/projects\/([^/]+)(?:\/(.*))?$/);
  const activeProjectId = projectMatch?.[1] ?? null;
  const activeProjectSubPath = projectMatch?.[2]?.split('/')?.[0] ?? null;

  const { data: projects } = useQuery({
    queryKey: ['projects'],
    queryFn: getProjects,
  });

  const [search, setSearch] = useState('');
  const trimmedSearch = search.trim().toLowerCase();
  const isSearching = trimmedSearch.length > 0;

  // Persistent project disclosure state
  const [expandedProjects, setExpandedProjects] = useState<Record<string, boolean>>(readExpanded);
  const setExpanded = useCallback((id: string, value: boolean) => {
    setExpandedProjects((prev) => {
      const next = { ...prev, [id]: value };
      writeExpanded(next);
      return next;
    });
  }, []);
  const toggleExpand = useCallback(
    (id: string) => setExpandedProjects((prev) => {
      const next = { ...prev, [id]: !prev[id] };
      writeExpanded(next);
      return next;
    }),
    [],
  );

  // Auto-expand when entering a project
  useEffect(() => {
    if (activeProjectId && !expandedProjects[activeProjectId]) {
      setExpanded(activeProjectId, true);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeProjectId]);

  const handleNavigateToProject = useCallback(
    (id: string) => {
      setExpanded(id, true);
      navigate(`/projects/${id}/query`);
    },
    [navigate, setExpanded],
  );

  const projectsById = useMemo(() => {
    const m = new Map<string, Project>();
    (projects ?? []).forEach((p) => m.set(p.id, p));
    return m;
  }, [projects]);

  // Schedules drive whether the Workflows item appears for a given project.
  // Only fetched when a project is active; the result is cached briefly so
  // that hopping between project routes doesn't spam the server.
  const { data: schedules } = useQuery({
    queryKey: ['schedules'],
    queryFn: getSchedules,
    enabled: !!activeProjectId,
    staleTime: 30_000,
  });

  // Set of saved-query IDs that have a schedule attached
  const scheduledQueryIds = useMemo(() => {
    const set = new Set<string>();
    (schedules ?? []).forEach((s) => set.add(s.savedQueryId));
    return set;
  }, [schedules]);

  /** Returns the project's sub-nav, inserting Workflows iff the project has any scheduled queries. */
  const sectionsFor = useCallback(
    (project: Project): ProjectSection[] => {
      const hasWorkflows = (project.savedQueryIds ?? []).some((id) => scheduledQueryIds.has(id));
      return hasWorkflows
        ? [...BASE_PROJECT_SECTIONS, WORKFLOWS_SECTION, ...TRAILING_PROJECT_SECTIONS]
        : [...BASE_PROJECT_SECTIONS, ...TRAILING_PROJECT_SECTIONS];
    },
    [scheduledQueryIds],
  );

  // Stable order: user-defined order (in 'order') → everything else alphabetic.
  // Clicking a project never reorders; only drag-and-drop changes order.
  const curatedProjects = useMemo(() => {
    if (!projects || projects.length === 0) {
      return [];
    }
    const out: Project[] = [];
    const seen = new Set<string>();
    const push = (p: Project | undefined) => {
      if (p && !seen.has(p.id)) {
        out.push(p);
        seen.add(p.id);
      }
    };

    // User-ordered first
    order.forEach((id) => push(projectsById.get(id)));
    // Backfill alpha for projects not yet in order
    const alpha = [...projects].sort((a, b) => a.name.localeCompare(b.name));
    for (const p of alpha) {
      if (out.length >= VISIBLE_PROJECTS_WHEN_IDLE) {
        break;
      }
      push(p);
    }
    // Always include the active project so its sub-nav stays reachable
    if (activeProjectId) {
      push(projectsById.get(activeProjectId));
    }
    return out;
  }, [projects, projectsById, activeProjectId, order]);

  const searchResults = useMemo(() => {
    if (!isSearching || !projects) {
      return [];
    }
    return projects
      .filter((p) =>
        p.name.toLowerCase().includes(trimmedSearch) ||
        (p.description && p.description.toLowerCase().includes(trimmedSearch)) ||
        p.tags?.some((t) => t.toLowerCase().includes(trimmedSearch)),
      )
      .slice(0, 50);
  }, [projects, isSearching, trimmedSearch]);

  const totalProjects = projects?.length ?? 0;
  const visibleSet = isSearching ? searchResults : curatedProjects;
  const hiddenCount = isSearching
    ? Math.max(0, totalProjects - searchResults.length)
    : Math.max(0, totalProjects - curatedProjects.length);

  // Resizable width — only when expanded
  const [sidebarWidth, setSidebarWidth] = useState<number>(readSidebarWidth);
  const [resizing, setResizing] = useState(false);
  const widthRef = useRef(sidebarWidth);
  widthRef.current = sidebarWidth;

  const onResizeStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    setResizing(true);
    const prevCursor = document.body.style.cursor;
    const prevSelect = document.body.style.userSelect;
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';

    const onMouseMove = (ev: MouseEvent) => {
      // Sidebar is anchored to the left edge.
      const next = Math.max(MIN_WIDTH, Math.min(MAX_WIDTH, ev.clientX));
      setSidebarWidth(next);
    };
    const onMouseUp = () => {
      setResizing(false);
      document.body.style.cursor = prevCursor;
      document.body.style.userSelect = prevSelect;
      try {
        localStorage.setItem(WIDTH_KEY, String(widthRef.current));
      } catch {
        // Ignore
      }
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
    };
    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  }, []);

  const inlineStyle: CSSProperties | undefined = sidebarCollapsed
    ? undefined
    : { width: sidebarWidth };

  return (
    <aside
      className={`shell-sidebar${sidebarCollapsed ? ' is-collapsed' : ''}`}
      style={inlineStyle}
    >
      {!sidebarCollapsed && (
        <div
          className={`shell-sidebar-resize-handle${resizing ? ' is-dragging' : ''}`}
          onMouseDown={onResizeStart}
          role="separator"
          aria-orientation="vertical"
          aria-label="Resize sidebar"
        />
      )}
      <div className="shell-sidebar-header">
        <Link to="/projects" className="shell-sidebar-brand">
          <img src="/static/img/apache-drill-logo.png" alt="Drill" />
          {!sidebarCollapsed && <span>Drill</span>}
        </Link>
      </div>

      <div className="shell-sidebar-scroll">
        <Section title="Workspace" collapsed={sidebarCollapsed}>
          {WORKSPACE.map((item) => (
            <ItemRow
              key={item.to}
              to={item.to}
              label={item.label}
              icon={item.icon}
              active={
                item.to === '/projects'
                  ? location.pathname === '/projects'
                  : isActive(location.pathname, item)
              }
              collapsed={sidebarCollapsed}
            />
          ))}
        </Section>

        <Section title="Projects" collapsed={sidebarCollapsed}>
          {!sidebarCollapsed && totalProjects > VISIBLE_PROJECTS_WHEN_IDLE && (
            <div className="shell-sidebar-search">
              <Input
                size="small"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Filter projects…"
                allowClear
                prefix={<SearchOutlined style={{ fontSize: 11, color: 'var(--color-text-tertiary)' }} />}
              />
            </div>
          )}

          {visibleSet.length === 0 && !sidebarCollapsed && (
            <div className="shell-sidebar-empty">
              {isSearching
                ? 'No matching projects'
                : projects
                  ? 'No projects yet'
                  : 'Loading…'}
            </div>
          )}

          {visibleSet.map((project) => (
            <ProjectRow
              key={project.id}
              project={project}
              active={project.id === activeProjectId}
              expanded={!!expandedProjects[project.id]}
              collapsed={sidebarCollapsed}
              drag={drag}
              sections={sectionsFor(project)}
              onToggleExpand={toggleExpand}
              onNavigateToProject={handleNavigateToProject}
              onDragStart={handleDragStart}
              onDragOver={handleDragOverProject}
              onDragLeave={handleDragLeaveProject}
              onDrop={handleDropProject}
              onDragEnd={handleDragEnd}
              activeProjectSubPath={activeProjectSubPath}
            />
          ))}

          {!sidebarCollapsed && hiddenCount > 0 && (
            <Link to="/projects" className="shell-sidebar-show-all">
              {isSearching
                ? `Showing ${searchResults.length} of ${totalProjects}`
                : `All ${totalProjects} projects →`}
            </Link>
          )}
        </Section>

        <Section title="Library" collapsed={sidebarCollapsed}>
          {LIBRARY.map((item) => (
            <ItemRow
              key={item.to}
              to={item.to}
              label={item.label}
              icon={item.icon}
              active={isActive(location.pathname, item)}
              collapsed={sidebarCollapsed}
            />
          ))}
        </Section>

        <Section title="Administration" collapsed={sidebarCollapsed} collapsible defaultOpen={false}>
          {ADMIN.map((item) => (
            <ItemRow
              key={item.to}
              to={item.to}
              label={item.label}
              icon={item.icon}
              active={isActive(location.pathname, item)}
              collapsed={sidebarCollapsed}
            />
          ))}
        </Section>
      </div>
    </aside>
  );
}
