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
import apiClient from './client';
import type { Project, ProjectCreate, ProjectUpdate, DatasetRef, WikiPage } from '../types';

const PROJECTS_BASE = '/api/v1/projects';

export interface ProjectsResponse {
  projects: Project[];
}

/**
 * Fetch all projects accessible by the current user
 */
export async function getProjects(): Promise<Project[]> {
  const response = await apiClient.get<ProjectsResponse>(PROJECTS_BASE);
  return response.data.projects;
}

/**
 * Get a project by ID
 */
export async function getProject(id: string): Promise<Project> {
  const response = await apiClient.get<Project>(`${PROJECTS_BASE}/${encodeURIComponent(id)}`);
  return response.data;
}

/**
 * Create a new project
 */
export async function createProject(project: ProjectCreate): Promise<Project> {
  const response = await apiClient.post<Project>(PROJECTS_BASE, project);
  return response.data;
}

/**
 * Update an existing project
 */
export async function updateProject(id: string, project: ProjectUpdate): Promise<Project> {
  const response = await apiClient.put<Project>(
    `${PROJECTS_BASE}/${encodeURIComponent(id)}`,
    project
  );
  return response.data;
}

/**
 * Delete a project (soft-delete; recoverable from Trash)
 */
export async function deleteProject(id: string): Promise<void> {
  await apiClient.delete(`${PROJECTS_BASE}/${encodeURIComponent(id)}`);
}

/**
 * List soft-deleted projects owned by the current user
 */
export async function getTrashedProjects(): Promise<Project[]> {
  const response = await apiClient.get<ProjectsResponse>(`${PROJECTS_BASE}/trash`);
  return response.data.projects;
}

/**
 * Restore a soft-deleted project
 */
export async function restoreProject(id: string): Promise<Project> {
  const response = await apiClient.post<Project>(
    `${PROJECTS_BASE}/${encodeURIComponent(id)}/restore`
  );
  return response.data;
}

/**
 * Permanently delete a project from trash
 */
export async function purgeProject(id: string): Promise<void> {
  await apiClient.delete(`${PROJECTS_BASE}/${encodeURIComponent(id)}/purge`);
}

/**
 * Add a dataset reference to a project
 */
export async function addDataset(projectId: string, dataset: DatasetRef): Promise<Project> {
  const response = await apiClient.post<Project>(
    `${PROJECTS_BASE}/${encodeURIComponent(projectId)}/datasets`,
    dataset
  );
  return response.data;
}

/**
 * Remove a dataset reference from a project
 */
export async function removeDataset(projectId: string, datasetId: string): Promise<Project> {
  const response = await apiClient.delete<Project>(
    `${PROJECTS_BASE}/${encodeURIComponent(projectId)}/datasets/${encodeURIComponent(datasetId)}`
  );
  return response.data;
}

/**
 * Remove all dataset references for a deleted plugin from all projects.
 */
export async function cleanupPluginDatasets(pluginName: string): Promise<void> {
  await apiClient.delete(
    `${PROJECTS_BASE}/cleanup/plugin/${encodeURIComponent(pluginName)}`
  );
}

/**
 * Add a saved query to a project (removes from other projects)
 */
export async function addSavedQuery(projectId: string, queryId: string): Promise<Project> {
  const response = await apiClient.post<Project>(
    `${PROJECTS_BASE}/${encodeURIComponent(projectId)}/saved-queries/${encodeURIComponent(queryId)}`
  );
  return response.data;
}

/**
 * Remove a saved query from a project
 */
export async function removeSavedQuery(projectId: string, queryId: string): Promise<Project> {
  const response = await apiClient.delete<Project>(
    `${PROJECTS_BASE}/${encodeURIComponent(projectId)}/saved-queries/${encodeURIComponent(queryId)}`
  );
  return response.data;
}

/**
 * Add a visualization to a project (removes from other projects)
 */
export async function addVisualization(projectId: string, vizId: string): Promise<Project> {
  const response = await apiClient.post<Project>(
    `${PROJECTS_BASE}/${encodeURIComponent(projectId)}/visualizations/${encodeURIComponent(vizId)}`
  );
  return response.data;
}

/**
 * Remove a visualization from a project
 */
export async function removeVisualization(projectId: string, vizId: string): Promise<Project> {
  const response = await apiClient.delete<Project>(
    `${PROJECTS_BASE}/${encodeURIComponent(projectId)}/visualizations/${encodeURIComponent(vizId)}`
  );
  return response.data;
}

/**
 * Add a dashboard to a project (removes from other projects)
 */
export async function addDashboard(projectId: string, dashId: string): Promise<Project> {
  const response = await apiClient.post<Project>(
    `${PROJECTS_BASE}/${encodeURIComponent(projectId)}/dashboards/${encodeURIComponent(dashId)}`
  );
  return response.data;
}

/**
 * Remove a dashboard from a project
 */
export async function removeDashboard(projectId: string, dashId: string): Promise<Project> {
  const response = await apiClient.delete<Project>(
    `${PROJECTS_BASE}/${encodeURIComponent(projectId)}/dashboards/${encodeURIComponent(dashId)}`
  );
  return response.data;
}

/**
 * Create a wiki page in a project
 */
export async function createWikiPage(
  projectId: string,
  page: { title: string; content?: string; order?: number }
): Promise<WikiPage> {
  const response = await apiClient.post<WikiPage>(
    `${PROJECTS_BASE}/${encodeURIComponent(projectId)}/wiki`,
    page
  );
  return response.data;
}

/**
 * Update a wiki page in a project
 */
export async function updateWikiPage(
  projectId: string,
  pageId: string,
  page: { title?: string; content?: string; order?: number }
): Promise<WikiPage> {
  const response = await apiClient.put<WikiPage>(
    `${PROJECTS_BASE}/${encodeURIComponent(projectId)}/wiki/${encodeURIComponent(pageId)}`,
    page
  );
  return response.data;
}

/**
 * Delete a wiki page from a project
 */
export async function deleteWikiPage(projectId: string, pageId: string): Promise<void> {
  await apiClient.delete(
    `${PROJECTS_BASE}/${encodeURIComponent(projectId)}/wiki/${encodeURIComponent(pageId)}`
  );
}

/**
 * Get the current user's favorited project IDs
 */
export async function getFavorites(): Promise<string[]> {
  const response = await apiClient.get<{ projectIds: string[] }>(`${PROJECTS_BASE}/favorites`);
  return response.data.projectIds;
}

/**
 * Toggle a project as favorite for the current user
 */
export async function toggleFavorite(id: string): Promise<{ favorited: boolean }> {
  const response = await apiClient.post<{ favorited: boolean }>(
    `${PROJECTS_BASE}/${encodeURIComponent(id)}/favorite`
  );
  return response.data;
}
