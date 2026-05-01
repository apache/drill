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
import { Fragment, useEffect, useState } from 'react';
import { Link, useLocation } from 'react-router-dom';
import { Button, Dropdown, Space, Tooltip } from 'antd';
import type { MenuProps } from 'antd';
import {
  RightOutlined,
  SearchOutlined,
  SunOutlined,
  MoonOutlined,
  BulbOutlined,
  FileTextOutlined,
  RocketOutlined,
  SettingOutlined,
  QuestionCircleOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
} from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { useTheme } from '../../hooks/useTheme';
import { useAppChrome, type BreadcrumbSegment } from '../../contexts/AppChromeContext';
import { useAiModal } from '../../contexts/AiModalContext';
import PreferencesModal from './PreferencesModal';
import { getProjects } from '../../api/projects';

const PROJECT_SECTION_LABELS: Record<string, string> = {
  query: 'Query',
  queries: 'Saved Queries',
  visualizations: 'Visualizations',
  dashboards: 'Dashboards',
  datasources: 'Data Sources',
  wiki: 'Wiki',
  settings: 'Settings',
};

const TOP_LEVEL_LABELS: Record<string, string> = {
  '/query': 'SQL Lab',
  '/saved-queries': 'Saved Queries',
  '/visualizations': 'Visualizations',
  '/dashboards': 'Dashboards',
  '/workflows': 'Workflows',
  '/profiles': 'Query History',
  '/datasources': 'Data Sources',
  '/metrics': 'Metrics',
  '/options': 'System Options',
  '/logs': 'Server Logs',
};

interface ToolbarProps {
  /** Open the command palette (Spotlight) */
  onOpenCommand: () => void;
}

export default function Toolbar({ onOpenCommand }: ToolbarProps) {
  const location = useLocation();
  const { isDark, toggle } = useTheme();
  const { chrome, toggleSidebar, toggleInspector, inspectorOpen, sidebarCollapsed } = useAppChrome();
  const { openModal } = useAiModal();

  const [preferencesOpen, setPreferencesOpen] = useState(false);

  // ⌘, opens Preferences (Apple convention)
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === ',') {
        e.preventDefault();
        setPreferencesOpen(true);
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, []);

  // Look up project name for breadcrumb derivation in project routes
  const { data: projects } = useQuery({
    queryKey: ['projects'],
    queryFn: getProjects,
    staleTime: 60_000,
  });

  const breadcrumb = chrome.breadcrumb && chrome.breadcrumb.length > 0
    ? chrome.breadcrumb
    : deriveBreadcrumb(location.pathname, projects);

  const aiMenuItems: MenuProps['items'] = [
    { key: 'suggestions', icon: <BulbOutlined />, label: 'Query Suggestions', onClick: () => openModal('suggestions') },
    { key: 'explain', icon: <FileTextOutlined />, label: 'Explain Query', onClick: () => openModal('explain') },
    { key: 'optimize', icon: <RocketOutlined />, label: 'Optimize Query', onClick: () => openModal('optimize') },
  ];

  return (
    <header className="shell-toolbar">
      {/* Window controls / sidebar toggle */}
      <div className="shell-toolbar-left">
        <Tooltip title={sidebarCollapsed ? 'Show Sidebar (⌘0)' : 'Hide Sidebar (⌘0)'}>
          <Button
            type="text"
            size="small"
            icon={<MenuFoldOutlined />}
            onClick={toggleSidebar}
            className="shell-toolbar-icon-btn"
          />
        </Tooltip>

        <nav className="shell-breadcrumb">
          {breadcrumb.map((seg, i) => (
            <Fragment key={seg.key}>
              {i > 0 && <RightOutlined className="shell-breadcrumb-sep" />}
              {seg.to ? (
                <Link to={seg.to} className="shell-breadcrumb-segment">{seg.label}</Link>
              ) : seg.onClick ? (
                <button type="button" className="shell-breadcrumb-segment is-button" onClick={seg.onClick}>
                  {seg.label}
                </button>
              ) : (
                <span className={`shell-breadcrumb-segment${i === breadcrumb.length - 1 ? ' is-current' : ''}`}>
                  {seg.label}
                </span>
              )}
            </Fragment>
          ))}
        </nav>
      </div>

      {/* Spotlight-style search trigger */}
      <button type="button" className="shell-spotlight" onClick={onOpenCommand} aria-label="Search">
        <SearchOutlined className="shell-spotlight-icon" />
        <span className="shell-spotlight-text">Search projects, queries…</span>
        <kbd className="shell-spotlight-kbd">⌘K</kbd>
      </button>

      {/* Right-side cluster: page actions, AI, settings, theme, inspector */}
      <div className="shell-toolbar-right">
        {chrome.toolbarActions && (
          <div className="shell-toolbar-page-actions">{chrome.toolbarActions}</div>
        )}

        <Space size={2}>
          <Dropdown menu={{ items: aiMenuItems }} placement="bottomRight" trigger={['click']}>
            <Tooltip title="AI Assistant">
              <Button type="text" size="small" icon={<BulbOutlined />} className="shell-toolbar-icon-btn" />
            </Tooltip>
          </Dropdown>

          <Tooltip title={isDark ? 'Light Appearance' : 'Dark Appearance'}>
            <Button
              type="text"
              size="small"
              icon={isDark ? <SunOutlined /> : <MoonOutlined />}
              onClick={toggle}
              className="shell-toolbar-icon-btn"
            />
          </Tooltip>

          <Tooltip title="Preferences (⌘,)">
            <Button
              type="text"
              size="small"
              icon={<SettingOutlined />}
              onClick={() => setPreferencesOpen(true)}
              className="shell-toolbar-icon-btn"
            />
          </Tooltip>

          <Tooltip title="Help">
            <Button
              type="text"
              size="small"
              icon={<QuestionCircleOutlined />}
              href="https://drill.apache.org/docs/"
              target="_blank"
              className="shell-toolbar-icon-btn"
            />
          </Tooltip>

          <Tooltip title={inspectorOpen ? 'Hide Inspector (⌘⌥0)' : 'Show Inspector (⌘⌥0)'}>
            <Button
              type="text"
              size="small"
              icon={<MenuUnfoldOutlined />}
              onClick={toggleInspector}
              className={`shell-toolbar-icon-btn${inspectorOpen ? ' is-active' : ''}`}
            />
          </Tooltip>
        </Space>
      </div>

      <PreferencesModal open={preferencesOpen} onClose={() => setPreferencesOpen(false)} />
    </header>
  );
}

/**
 * Fallback breadcrumb derived from the URL when a page hasn't registered its own.
 * Pages should call usePageChrome({ breadcrumb: [...] }) for richer labels.
 */
function deriveBreadcrumb(
  pathname: string,
  projects?: { id: string; name: string }[],
): BreadcrumbSegment[] {
  if (pathname === '/' || pathname === '/projects') {
    return [{ key: 'home', label: 'Projects' }];
  }

  // Project routes: /projects/:id and /projects/:id/:section/...
  const projectMatch = pathname.match(/^\/projects\/([^/]+)(?:\/([^/]+))?/);
  if (projectMatch) {
    const projectId = projectMatch[1];
    const section = projectMatch[2];
    const project = projects?.find((p) => p.id === projectId);
    const projectName = project?.name ?? 'Project';

    const segments: BreadcrumbSegment[] = [
      { key: 'projects', label: 'Projects', to: '/projects' },
      {
        key: 'project',
        label: projectName,
        to: section ? `/projects/${projectId}/query` : undefined,
      },
    ];
    if (section && section !== 'query') {
      const sectionLabel = PROJECT_SECTION_LABELS[section] ?? section;
      segments.push({ key: 'section', label: sectionLabel });
    } else if (section === 'query') {
      segments.push({ key: 'section', label: PROJECT_SECTION_LABELS.query });
    }
    return segments;
  }

  for (const [prefix, label] of Object.entries(TOP_LEVEL_LABELS)) {
    if (pathname === prefix || pathname.startsWith(prefix + '/')) {
      return [{ key: prefix, label }];
    }
  }
  return [{ key: 'home', label: 'Drill' }];
}
