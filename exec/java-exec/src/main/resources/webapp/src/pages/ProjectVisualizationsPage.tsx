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
import { getVisualizations } from '../api/visualizations';
import { useProjectContext } from '../contexts/ProjectContext';
import { AddItemModal } from '../components/project';
import VisualizationsPage from './VisualizationsPage';

export default function ProjectVisualizationsPage() {
  const { project, projectId } = useProjectContext();
  const queryClient = useQueryClient();
  const [addModalOpen, setAddModalOpen] = useState(false);

  const { data: allVisualizations } = useQuery({
    queryKey: ['visualizations'],
    queryFn: getVisualizations,
  });

  return (
    <>
      <VisualizationsPage
        filterIds={project?.visualizationIds}
        projectId={projectId}
        projectOwner={project?.owner}
        onAdd={() => setAddModalOpen(true)}
      />
      <AddItemModal
        open={addModalOpen}
        type="visualization"
        projectId={projectId!}
        existingIds={project?.visualizationIds || []}
        items={(allVisualizations || []).map((v) => ({ id: v.id, name: v.name, description: v.description }))}
        onClose={() => setAddModalOpen(false)}
        onSuccess={() => {
          queryClient.invalidateQueries({ queryKey: ['project', projectId] });
          setAddModalOpen(false);
        }}
      />
    </>
  );
}
