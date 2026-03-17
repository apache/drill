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
import { useParams, Outlet } from 'react-router-dom';
import { Spin, Empty, Button } from 'antd';
import { useNavigate } from 'react-router-dom';
import { ProjectContextProvider, useProjectContext } from '../../contexts/ProjectContext';
import ProjectNavBar from './ProjectNavBar';

function ProjectLayoutInner() {
  const { isLoading, error, project } = useProjectContext();
  const navigate = useNavigate();

  if (isLoading) {
    return (
      <div style={{ padding: 24, textAlign: 'center', flex: 1 }}>
        <Spin size="large" />
      </div>
    );
  }

  if (error || !project) {
    return (
      <div style={{ padding: 24, flex: 1 }}>
        <Empty description={error ? `Failed to load project: ${error.message}` : 'Project not found'}>
          <Button onClick={() => navigate('/projects')}>Back to Projects</Button>
        </Empty>
      </div>
    );
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', flex: 1, overflow: 'hidden' }}>
      <ProjectNavBar />
      <div style={{ flex: 1, overflow: 'auto', display: 'flex', flexDirection: 'column' }}>
        <Outlet />
      </div>
    </div>
  );
}

export default function ProjectLayout() {
  const { id } = useParams<{ id: string }>();

  if (!id) {
    return null;
  }

  return (
    <ProjectContextProvider projectId={id}>
      <ProjectLayoutInner />
    </ProjectContextProvider>
  );
}
