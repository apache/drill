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
import { Link, useLocation } from 'react-router-dom';
import { Layout, Menu, Button, Space, Dropdown } from 'antd';
import {
  CodeOutlined,
  SaveOutlined,
  BarChartOutlined,
  DashboardOutlined,
  HomeOutlined,
  QuestionCircleOutlined,
} from '@ant-design/icons';
import type { MenuProps } from 'antd';

const { Header } = Layout;

const navItems = [
  { key: '/', icon: <CodeOutlined />, label: 'SQL Lab' },
  { key: '/saved-queries', icon: <SaveOutlined />, label: 'Saved Queries' },
  { key: '/visualizations', icon: <BarChartOutlined />, label: 'Visualizations' },
  { key: '/dashboards', icon: <DashboardOutlined />, label: 'Dashboards' },
];

const legacyMenuItems: MenuProps['items'] = [
  { key: 'query', label: <a href="/query">Query (Legacy)</a> },
  { key: 'profiles', label: <a href="/profiles">Profiles</a> },
  { key: 'storage', label: <a href="/storage">Storage</a> },
  { key: 'options', label: <a href="/options">Options</a> },
  { key: 'metrics', label: <a href="/status/metrics">Metrics</a> },
  { key: 'logs', label: <a href="/logs">Logs</a> },
];

export default function Navbar() {
  const location = useLocation();

  return (
    <Header
      style={{
        display: 'flex',
        alignItems: 'center',
        padding: '0 24px',
        background: '#001529',
        height: 48,
      }}
    >
      {/* Logo */}
      <Link
        to="/"
        style={{
          display: 'flex',
          alignItems: 'center',
          marginRight: 24,
          color: '#fff',
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
        theme="dark"
        mode="horizontal"
        selectedKeys={[location.pathname]}
        items={navItems.map((item) => ({
          key: item.key,
          icon: item.icon,
          label: <Link to={item.key}>{item.label}</Link>,
        }))}
        style={{ flex: 1, minWidth: 0, background: 'transparent', borderBottom: 'none' }}
      />

      {/* Right side actions */}
      <Space>
        <Dropdown menu={{ items: legacyMenuItems }} placement="bottomRight">
          <Button type="text" icon={<HomeOutlined />} style={{ color: '#fff' }}>
            Drill UI
          </Button>
        </Dropdown>

        <Button
          type="text"
          icon={<QuestionCircleOutlined />}
          style={{ color: '#fff' }}
          href="https://drill.apache.org/docs/"
          target="_blank"
        />
      </Space>
    </Header>
  );
}
