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
import { Timeline, Typography, Space, Empty, Tag } from 'antd';
import {
  CodeOutlined,
  BarChartOutlined,
  DashboardOutlined,
  FileTextOutlined,
} from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { getSavedQueries } from '../../api/savedQueries';
import { getVisualizations } from '../../api/visualizations';
import { getDashboards } from '../../api/dashboards';
import type { Project } from '../../types';

const { Text } = Typography;

interface Props {
  project: Project;
}

interface ActivityItem {
  type: 'query' | 'visualization' | 'dashboard' | 'wiki' | 'dataset';
  name: string;
  timestamp: number;
  icon: React.ReactNode;
  color: string;
}

export default function ProjectActivityFeed({ project }: Props) {
  const { data: allQueries } = useQuery({ queryKey: ['savedQueries'], queryFn: getSavedQueries });
  const { data: allVizs } = useQuery({ queryKey: ['visualizations'], queryFn: getVisualizations });
  const { data: allDashboards } = useQuery({ queryKey: ['dashboards'], queryFn: getDashboards });

  const activities = useMemo(() => {
    const items: ActivityItem[] = [];
    const queryIdSet = new Set(project.savedQueryIds || []);
    const vizIdSet = new Set(project.visualizationIds || []);
    const dashIdSet = new Set(project.dashboardIds || []);

    (allQueries || []).filter(q => queryIdSet.has(q.id)).forEach(q => {
      items.push({
        type: 'query',
        name: q.name,
        timestamp: typeof q.updatedAt === 'number' ? q.updatedAt : new Date(q.updatedAt).getTime(),
        icon: <CodeOutlined />,
        color: '#52c41a',
      });
    });
    (allVizs || []).filter(v => vizIdSet.has(v.id)).forEach(v => {
      items.push({
        type: 'visualization',
        name: v.name,
        timestamp: typeof v.updatedAt === 'number' ? v.updatedAt : new Date(v.updatedAt).getTime(),
        icon: <BarChartOutlined />,
        color: '#1890ff',
      });
    });
    (allDashboards || []).filter(d => dashIdSet.has(d.id)).forEach(d => {
      items.push({
        type: 'dashboard',
        name: d.name,
        timestamp: typeof d.updatedAt === 'number' ? d.updatedAt : new Date(d.updatedAt).getTime(),
        icon: <DashboardOutlined />,
        color: '#722ed1',
      });
    });
    (project.wikiPages || []).forEach(w => {
      items.push({
        type: 'wiki',
        name: w.title,
        timestamp: w.updatedAt,
        icon: <FileTextOutlined />,
        color: '#faad14',
      });
    });

    return items.sort((a, b) => b.timestamp - a.timestamp).slice(0, 15);
  }, [project, allQueries, allVizs, allDashboards]);

  if (activities.length === 0) {
    return <Empty description="No recent activity" />;
  }

  const formatRelativeTime = (ts: number) => {
    const diff = Date.now() - ts;
    const mins = Math.floor(diff / 60000);
    if (mins < 1) {
      return 'just now';
    }
    if (mins < 60) {
      return `${mins}m ago`;
    }
    const hours = Math.floor(mins / 60);
    if (hours < 24) {
      return `${hours}h ago`;
    }
    const days = Math.floor(hours / 24);
    if (days < 30) {
      return `${days}d ago`;
    }
    return new Date(ts).toLocaleDateString();
  };

  return (
    <Timeline
      items={activities.map((a, i) => ({
        key: i,
        color: a.color,
        dot: a.icon,
        children: (
          <Space>
            <Text strong>{a.name}</Text>
            <Tag>{a.type}</Tag>
            <Text type="secondary" style={{ fontSize: 12 }}>{formatRelativeTime(a.timestamp)}</Text>
          </Space>
        ),
      }))}
    />
  );
}
