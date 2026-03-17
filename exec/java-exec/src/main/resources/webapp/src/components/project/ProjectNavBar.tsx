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
import { useMemo } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { Menu, Button, Space, Typography, Tooltip, Dropdown } from 'antd';
import {
  CodeOutlined,
  FileTextOutlined,
  BarChartOutlined,
  DashboardOutlined,
  DatabaseOutlined,
  BookOutlined,
  SettingOutlined,
  LogoutOutlined,
  DownOutlined,
  FolderOutlined,
  CheckOutlined,
} from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { getProjects } from '../../api/projects';
import { useProjectContext } from '../../contexts/ProjectContext';

const { Text } = Typography;

const navItems = [
  { key: 'query', label: 'Query', icon: <CodeOutlined /> },
  { key: 'queries', label: 'Queries', icon: <FileTextOutlined /> },
  { key: 'visualizations', label: 'Visualizations', icon: <BarChartOutlined /> },
  { key: 'dashboards', label: 'Dashboards', icon: <DashboardOutlined /> },
  { key: 'datasources', label: 'Data Sources', icon: <DatabaseOutlined /> },
  { key: 'wiki', label: 'Wiki', icon: <BookOutlined /> },
];

export default function ProjectNavBar() {
  const { project, projectId } = useProjectContext();
  const location = useLocation();
  const navigate = useNavigate();

  const { data: allProjects } = useQuery({
    queryKey: ['projects'],
    queryFn: getProjects,
  });

  // Determine active key from current path
  const pathSegments = location.pathname.split('/');
  // /projects/:id/something -> something is at index 3
  const activeKey = pathSegments[3] || 'query';

  // Build dropdown menu items for project switcher
  const projectMenuItems = useMemo(() => {
    if (!allProjects) {
      return [];
    }
    return allProjects.map((p) => ({
      key: p.id,
      label: (
        <Space>
          {p.id === projectId ? <CheckOutlined style={{ color: 'var(--color-primary)' }} /> : <FolderOutlined />}
          <span>{p.name}</span>
        </Space>
      ),
    }));
  }, [allProjects, projectId]);

  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        height: 40,
        borderBottom: '1px solid var(--color-border-secondary)',
        background: 'var(--color-bg-elevated)',
        padding: '0 16px',
        gap: 8,
      }}
    >
      {/* Left: project name with switcher dropdown */}
      <Dropdown
        menu={{
          items: projectMenuItems,
          onClick: ({ key }) => {
            if (key !== projectId) {
              navigate(`/projects/${key}/${activeKey}`);
            }
          },
          style: { maxHeight: 320, overflow: 'auto' },
        }}
        trigger={['click']}
      >
        <Space
          style={{ cursor: 'pointer', whiteSpace: 'nowrap', flexShrink: 0 }}
        >
          <Text strong>{project?.name || 'Project'}</Text>
          <DownOutlined style={{ fontSize: 10 }} />
        </Space>
      </Dropdown>

      {/* Center: nav items */}
      <Menu
        mode="horizontal"
        selectedKeys={[activeKey]}
        onClick={({ key }) => navigate(`/projects/${projectId}/${key}`)}
        items={navItems}
        style={{
          flex: 1,
          border: 'none',
          background: 'transparent',
          minWidth: 0,
        }}
      />

      {/* Right: settings + exit */}
      <Space size={4} style={{ flexShrink: 0 }}>
        <Tooltip title="Project settings">
          <Button
            type="text"
            size="small"
            icon={<SettingOutlined />}
            onClick={() => navigate(`/projects/${projectId}/settings`)}
          />
        </Tooltip>
        <Tooltip title="Exit project">
          <Button
            type="text"
            size="small"
            icon={<LogoutOutlined />}
            onClick={() => navigate('/projects')}
          >
            Exit
          </Button>
        </Tooltip>
      </Space>
    </div>
  );
}
