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
import { createContext, useContext, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getProject } from '../api/projects';
import type { Project } from '../types';

interface ProjectContextValue {
  project: Project | undefined;
  isLoading: boolean;
  error: Error | null;
  projectId: string | undefined;
  savedQueryIdSet: Set<string>;
  visualizationIdSet: Set<string>;
  dashboardIdSet: Set<string>;
}

const ProjectContext = createContext<ProjectContextValue | null>(null);

export function ProjectContextProvider({
  projectId,
  children,
}: {
  projectId: string;
  children: React.ReactNode;
}) {
  const { data: project, isLoading, error } = useQuery({
    queryKey: ['project', projectId],
    queryFn: () => getProject(projectId),
    enabled: !!projectId,
    retry: (failureCount, error: unknown) => {
      // Don't retry 404s (project not found) immediately
      // Only retry on network errors or server errors (5xx)
      if (error && typeof error === 'object' && 'response' in error) {
        const httpError = error as { response?: { status?: number } };
        if (httpError.response?.status === 404) {
          // Don't retry 404 errors
          return false;
        }
      }
      // Retry up to 3 times for other errors
      return failureCount < 3;
    },
    retryDelay: (attemptIndex) => {
      // Exponential backoff: 100ms, 300ms, 700ms
      return Math.min(1000 * 2 ** attemptIndex, 30000);
    },
  });

  const savedQueryIdSet = useMemo(
    () => new Set(project?.savedQueryIds || []),
    [project?.savedQueryIds]
  );

  const visualizationIdSet = useMemo(
    () => new Set(project?.visualizationIds || []),
    [project?.visualizationIds]
  );

  const dashboardIdSet = useMemo(
    () => new Set(project?.dashboardIds || []),
    [project?.dashboardIds]
  );

  const value = useMemo(
    () => ({
      project,
      isLoading,
      error: error as Error | null,
      projectId,
      savedQueryIdSet,
      visualizationIdSet,
      dashboardIdSet,
    }),
    [project, isLoading, error, projectId, savedQueryIdSet, visualizationIdSet, dashboardIdSet]
  );

  return (
    <ProjectContext.Provider value={value}>
      {children}
    </ProjectContext.Provider>
  );
}

export function useProjectContext(): ProjectContextValue {
  const ctx = useContext(ProjectContext);
  if (!ctx) {
    throw new Error('useProjectContext must be used within a ProjectContextProvider');
  }
  return ctx;
}
