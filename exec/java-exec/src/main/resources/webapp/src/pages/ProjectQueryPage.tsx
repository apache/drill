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
import { useSelector, useDispatch } from 'react-redux';
import { useProjectContext } from '../contexts/ProjectContext';
import { QuerySuggestions } from '../components/project';
import type { RootState } from '../store';
import { setSql } from '../store/querySlice';
import SqlLabPage from './SqlLabPage';

export default function ProjectQueryPage() {
  const { project, projectId } = useProjectContext();
  const dispatch = useDispatch();
  const activeTabId = useSelector((state: RootState) => state.query.activeTabId);

  const datasets = project?.datasets || [];
  const datasetFilter = { datasets };

  const handleSelectSql = (sql: string) => {
    dispatch(setSql({ tabId: activeTabId, sql }));
  };

  const suggestionPanel = project?.datasets && project.datasets.length > 0 ? (
    <QuerySuggestions
      projectId={projectId!}
      datasets={datasets}
      savedQueryCount={project?.savedQueryIds?.length || 0}
      onSelectSql={handleSelectSql}
    />
  ) : null;

  return <SqlLabPage datasetFilter={datasetFilter} projectId={projectId} savedQueryIds={project?.savedQueryIds} headerContent={suggestionPanel} />;
}
