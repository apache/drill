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
import { useRef, useEffect } from 'react';
import { useSelector, useDispatch } from 'react-redux';
import { useProjectContext } from '../contexts/ProjectContext';
import AiAssistantModal from '../components/ai/AiAssistantModal';
import type { RootState } from '../store';
import { setSql, addTab } from '../store/querySlice';
import SqlLabPage from './SqlLabPage';

export default function ProjectQueryPage() {
  const { project, projectId } = useProjectContext();
  const dispatch = useDispatch();
  const activeTabId = useSelector((state: RootState) => state.query.activeTabId);
  const currentTab = useSelector((state: RootState) =>
    state.query.tabs.find(tab => tab.id === activeTabId)
  );
  const currentSql = currentTab?.sql || '';
  const pendingSqlRef = useRef<string | null>(null);
  const lastActiveTabRef = useRef<string>(activeTabId);

  const datasets = project?.datasets || [];
  const datasetFilter = { datasets };

  // When a new tab is created and becomes active, set the pending SQL
  useEffect(() => {
    if (pendingSqlRef.current && activeTabId !== lastActiveTabRef.current) {
      dispatch(setSql({ tabId: activeTabId, sql: pendingSqlRef.current }));
      pendingSqlRef.current = null;
    }
    lastActiveTabRef.current = activeTabId;
  }, [activeTabId, dispatch]);

  const handleSelectSql = (sql: string, title?: string) => {
    // Store the SQL and create a new tab with optional title
    pendingSqlRef.current = sql;
    dispatch(addTab(title));
  };

  return (
    <>
      <SqlLabPage datasetFilter={datasetFilter} projectId={projectId} savedQueryIds={project?.savedQueryIds} />
      {project?.datasets && project.datasets.length > 0 && (
        <AiAssistantModal
          projectId={projectId!}
          datasets={datasets}
          savedQueryCount={project?.savedQueryIds?.length || 0}
          project={project}
          currentSql={currentSql}
          onSelectSql={handleSelectSql}
        />
      )}
    </>
  );
}
