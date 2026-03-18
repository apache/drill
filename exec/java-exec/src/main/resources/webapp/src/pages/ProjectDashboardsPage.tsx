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
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { getDashboards } from '../api/dashboards';
import { useProjectContext } from '../contexts/ProjectContext';
import { AddItemModal } from '../components/project';
import DashboardsPage from './DashboardsPage';

export default function ProjectDashboardsPage() {
  const { project, projectId } = useProjectContext();
  const queryClient = useQueryClient();
  const [addModalOpen, setAddModalOpen] = useState(false);

  const { data: allDashboards } = useQuery({
    queryKey: ['dashboards'],
    queryFn: getDashboards,
  });

  return (
    <>
      <DashboardsPage
        filterIds={project?.dashboardIds}
        projectId={projectId}
        projectName={project?.name}
        onAdd={() => setAddModalOpen(true)}
      />
      <AddItemModal
        open={addModalOpen}
        type="dashboard"
        projectId={projectId!}
        existingIds={project?.dashboardIds || []}
        items={(allDashboards || []).map((d) => ({ id: d.id, name: d.name, description: d.description }))}
        onClose={() => setAddModalOpen(false)}
        onSuccess={() => {
          queryClient.invalidateQueries({ queryKey: ['project', projectId] });
          setAddModalOpen(false);
        }}
      />
    </>
  );
}
