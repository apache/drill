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
import { useParams, useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { Spin, Empty, Button, Typography } from 'antd';
import { ArrowLeftOutlined, SettingOutlined } from '@ant-design/icons';
import { getProject } from '../api/projects';
import SqlLabPage from './SqlLabPage';

const { Title } = Typography;

export default function ProjectQueryPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();

  const { data: project, isLoading } = useQuery({
    queryKey: ['project', id],
    queryFn: () => getProject(id!),
    enabled: !!id,
  });

  if (isLoading) {
    return (
      <div style={{ padding: 24, textAlign: 'center', flex: 1 }}>
        <Spin size="large" />
      </div>
    );
  }

  if (!project) {
    return (
      <div style={{ padding: 24, flex: 1 }}>
        <Empty description="Project not found">
          <Button onClick={() => navigate('/projects')}>Back to Projects</Button>
        </Empty>
      </div>
    );
  }

  const datasetFilter = project.datasets && project.datasets.length > 0
    ? { datasets: project.datasets }
    : { datasets: [] };

  const header = (
    <div style={{
      display: 'flex',
      alignItems: 'center',
      gap: 8,
      padding: '6px 16px',
      borderBottom: '1px solid #f0f0f0',
      background: '#fafafa',
    }}>
      <Button
        type="text"
        size="small"
        icon={<ArrowLeftOutlined />}
        onClick={() => navigate('/projects')}
      />
      <Title level={5} style={{ margin: 0, flex: 1 }}>
        {project.name}
      </Title>
      <Button
        type="text"
        size="small"
        icon={<SettingOutlined />}
        onClick={() => navigate(`/projects/${id}`)}
      />
    </div>
  );

  return <SqlLabPage datasetFilter={datasetFilter} headerContent={header} projectId={id} />;
}
