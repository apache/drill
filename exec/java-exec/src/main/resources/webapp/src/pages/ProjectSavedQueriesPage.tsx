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
import { getSavedQueries } from '../api/savedQueries';
import { useProjectContext } from '../contexts/ProjectContext';
import { AddItemModal } from '../components/project';
import SavedQueriesPage from './SavedQueriesPage';

export default function ProjectSavedQueriesPage() {
  const { project, projectId } = useProjectContext();
  const queryClient = useQueryClient();
  const [addModalOpen, setAddModalOpen] = useState(false);

  const { data: allQueries } = useQuery({
    queryKey: ['savedQueries'],
    queryFn: getSavedQueries,
  });

  return (
    <>
      <SavedQueriesPage
        filterIds={project?.savedQueryIds}
        projectId={projectId}
        projectOwner={project?.owner}
        onAdd={() => setAddModalOpen(true)}
      />
      <AddItemModal
        open={addModalOpen}
        type="savedQuery"
        projectId={projectId!}
        existingIds={project?.savedQueryIds || []}
        items={(allQueries || []).map((q) => ({ id: q.id, name: q.name, description: q.description }))}
        onClose={() => setAddModalOpen(false)}
        onSuccess={() => {
          queryClient.invalidateQueries({ queryKey: ['project', projectId] });
          setAddModalOpen(false);
        }}
      />
    </>
  );
}
