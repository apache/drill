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
import { Layout, Menu, Button, Space, Dropdown, Tooltip, Drawer, Grid } from 'antd';
import {
  FolderOutlined,
  DatabaseOutlined,
  CodeOutlined,
  SaveOutlined,
  BarChartOutlined,
  DashboardOutlined,
  QuestionCircleOutlined,
  RobotOutlined,
  SettingOutlined,
  MenuOutlined,
  MailOutlined,
} from '@ant-design/icons';
import { SunOutlined, MoonOutlined } from '@ant-design/icons';
import type { MenuProps } from 'antd';
import { ProspectorSettingsModal } from '../prospector/index';
import SmtpSettingsModal from '../smtp/SmtpSettingsModal';
import ProfileSettingsModal from '../data-profiler/ProfileSettingsModal';
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

const staticAdminMenuItems: MenuProps['items'] = [
  { key: '/profiles', label: <Link to="/profiles">Query History</Link> },
  { key: '/metrics', label: <Link to="/metrics">Metrics</Link> },
];


export default function Navbar() {
  const location = useLocation();
  const { isDark, toggle } = useTheme();
  const [prospectorSettingsOpen, setProspectorSettingsOpen] = useState(false);
  const [smtpSettingsOpen, setSmtpSettingsOpen] = useState(false);
  const [profileSettingsOpen, setProfileSettingsOpen] = useState(false);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const screens = Grid.useBreakpoint();
  const isMobile = !screens.md;

  const adminMenuItems: MenuProps['items'] = [
    ...staticAdminMenuItems!,
    { type: 'divider' },
    { key: 'smtp', icon: <MailOutlined />, label: 'Email Settings', onClick: () => setSmtpSettingsOpen(true) },
    { key: 'profiler', icon: <BarChartOutlined />, label: 'Profiler Settings', onClick: () => setProfileSettingsOpen(true) },
    { key: 'prospector', icon: <RobotOutlined />, label: 'Prospector Settings', onClick: () => setProspectorSettingsOpen(true) },
    { type: 'divider' },
    { key: 'options', icon: <SettingOutlined />, label: <Link to="/options">System Options</Link> },
    { key: 'logs', icon: <CodeOutlined />, label: <Link to="/logs">Server Logs</Link> },
  ];

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
        padding: isMobile ? '0 12px' : '0 24px',
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

      {/* Main Navigation — hidden on mobile */}
      {!isMobile && (
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
      )}

      {/* Spacer when nav is hidden */}
      {isMobile && <div style={{ flex: 1 }} />}

      {/* Right side actions */}
      <Space size={isMobile ? 4 : 8}>
        <Tooltip title={isDark ? 'Switch to Light Mode' : 'Switch to Dark Mode'}>
          <Button
            type="text"
            icon={isDark ? <SunOutlined /> : <MoonOutlined />}
            style={{ color: textColor }}
            onClick={toggle}
          />
        </Tooltip>

        {!isMobile && (
          <Dropdown menu={{ items: adminMenuItems }} placement="bottomRight">
            <Button type="text" icon={<SettingOutlined />} style={{ color: textColor }}>
              Admin
            </Button>
          </Dropdown>
        )}

        <Button
          type="text"
          icon={<QuestionCircleOutlined />}
          style={{ color: textColor }}
          href="https://drill.apache.org/docs/"
          target="_blank"
        />

        {/* Hamburger on mobile */}
        {isMobile && (
          <Button
            type="text"
            icon={<MenuOutlined />}
            style={{ color: textColor }}
            onClick={() => setDrawerOpen(true)}
          />
        )}
      </Space>

      {/* Mobile Navigation Drawer */}
      <Drawer
        title="Menu"
        placement="right"
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
        width={260}
      >
        <Menu
          theme={isDark ? 'dark' : 'light'}
          mode="inline"
          selectedKeys={[selectedKey]}
          items={navItems.map((item) => ({
            key: item.key,
            icon: item.icon,
            label: <Link to={item.key} onClick={() => setDrawerOpen(false)}>{item.label}</Link>,
          }))}
          style={{ background: 'transparent', border: 'none' }}
        />
        <div style={{ borderTop: '1px solid var(--color-border)', marginTop: 16, paddingTop: 16 }}>
          <Dropdown menu={{ items: adminMenuItems }} placement="bottomLeft">
            <Button type="text" icon={<SettingOutlined />} style={{ color: textColor, width: '100%', textAlign: 'left' }}>
              Admin
            </Button>
          </Dropdown>
        </div>
      </Drawer>

      <ProspectorSettingsModal
        open={prospectorSettingsOpen}
        onClose={() => setProspectorSettingsOpen(false)}
      />

      <SmtpSettingsModal
        open={smtpSettingsOpen}
        onClose={() => setSmtpSettingsOpen(false)}
      />

      <ProfileSettingsModal
        open={profileSettingsOpen}
        onClose={() => setProfileSettingsOpen(false)}
      />
    </Header>
  );
}
