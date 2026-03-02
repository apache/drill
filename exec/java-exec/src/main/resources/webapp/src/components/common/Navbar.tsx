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
import { useState } from 'react';
import { Link, useLocation } from 'react-router-dom';
import { Layout, Menu, Button, Space, Dropdown, Tooltip } from 'antd';
import {
  FolderOutlined,
  DatabaseOutlined,
  CodeOutlined,
  SaveOutlined,
  BarChartOutlined,
  DashboardOutlined,
  HomeOutlined,
  QuestionCircleOutlined,
  RobotOutlined,
  SettingOutlined,
} from '@ant-design/icons';
import { SunOutlined, MoonOutlined } from '@ant-design/icons';
import type { MenuProps } from 'antd';
import { ProspectorSettingsModal } from '../prospector/index';
import { useTheme } from '../../hooks/useTheme';

const { Header } = Layout;

const navItems = [
  { key: '/projects', icon: <FolderOutlined />, label: 'Projects' },
  { key: '/datasources', icon: <DatabaseOutlined />, label: 'Data Sources' },
  { key: '/query', icon: <CodeOutlined />, label: 'SQL Lab' },
  { key: '/saved-queries', icon: <SaveOutlined />, label: 'Saved Queries' },
  { key: '/visualizations', icon: <BarChartOutlined />, label: 'Visualizations' },
  { key: '/dashboards', icon: <DashboardOutlined />, label: 'Dashboards' },
];

const adminMenuItems: MenuProps['items'] = [
  { key: '/profiles', label: <Link to="/profiles">Query History</Link> },
];

const legacyMenuItems: MenuProps['items'] = [
  { key: 'query', label: <a href="/query">Query (Legacy)</a> },
  { key: 'storage', label: <a href="/storage">Storage</a> },
  { key: 'options', label: <a href="/options">Options</a> },
  { key: 'metrics', label: <a href="/status/metrics">Metrics</a> },
  { key: 'logs', label: <a href="/logs">Logs</a> },
];

export default function Navbar() {
  const location = useLocation();
  const { isDark, toggle } = useTheme();
  const [prospectorSettingsOpen, setProspectorSettingsOpen] = useState(false);

  // Determine selected nav key, handling sub-routes like /projects/:id, /datasources/:name
  let selectedKey = location.pathname;
  if (location.pathname.startsWith('/projects')) {
    selectedKey = '/projects';
  } else if (location.pathname.startsWith('/datasources')) {
    selectedKey = '/datasources';
  } else if (location.pathname.startsWith('/profiles')) {
    selectedKey = '/profiles';
  }

  const navBg = isDark ? 'rgba(15, 23, 42, 0.8)' : 'rgba(255, 255, 255, 0.8)';
  const textColor = 'var(--color-text)';

  return (
    <Header
      style={{
        display: 'flex',
        alignItems: 'center',
        padding: '0 24px',
        background: navBg,
        backdropFilter: 'blur(12px)',
        WebkitBackdropFilter: 'blur(12px)',
        borderBottom: '1px solid var(--color-border)',
        boxShadow: 'var(--shadow-sm)',
        height: 52,
        zIndex: 100,
      }}
    >
      {/* Logo */}
      <Link
        to="/projects"
        style={{
          display: 'flex',
          alignItems: 'center',
          marginRight: 24,
          color: textColor,
          textDecoration: 'none',
        }}
      >
        <img
          src="/static/img/apache-drill-logo.png"
          alt="Apache Drill"
          style={{ height: 28, marginRight: 8 }}
        />
        <span style={{ fontSize: 16, fontWeight: 600 }}>SQL Lab</span>
      </Link>

      {/* Main Navigation */}
      <Menu
        theme={isDark ? 'dark' : 'light'}
        mode="horizontal"
        selectedKeys={[selectedKey]}
        items={navItems.map((item) => ({
          key: item.key,
          icon: item.icon,
          label: <Link to={item.key}>{item.label}</Link>,
        }))}
        style={{ flex: 1, minWidth: 0, background: 'transparent', borderBottom: 'none' }}
      />

      {/* Right side actions */}
      <Space>
        <Tooltip title={isDark ? 'Switch to Light Mode' : 'Switch to Dark Mode'}>
          <Button
            type="text"
            icon={isDark ? <SunOutlined /> : <MoonOutlined />}
            style={{ color: textColor }}
            onClick={toggle}
          />
        </Tooltip>

        <Dropdown menu={{ items: adminMenuItems }} placement="bottomRight">
          <Button type="text" icon={<SettingOutlined />} style={{ color: textColor }}>
            Admin
          </Button>
        </Dropdown>

        <Tooltip title="Prospector Settings">
          <Button
            type="text"
            icon={<RobotOutlined />}
            style={{ color: textColor }}
            onClick={() => setProspectorSettingsOpen(true)}
          />
        </Tooltip>

        <Dropdown menu={{ items: legacyMenuItems }} placement="bottomRight">
          <Button type="text" icon={<HomeOutlined />} style={{ color: textColor }}>
            Drill UI
          </Button>
        </Dropdown>

        <Button
          type="text"
          icon={<QuestionCircleOutlined />}
          style={{ color: textColor }}
          href="https://drill.apache.org/docs/"
          target="_blank"
        />
      </Space>

      <ProspectorSettingsModal
        open={prospectorSettingsOpen}
        onClose={() => setProspectorSettingsOpen(false)}
      />
    </Header>
  );
}
