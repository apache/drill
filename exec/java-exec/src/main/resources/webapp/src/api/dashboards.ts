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
import type { Dashboard, DashboardCreate } from '../types';

const DASHBOARDS_BASE = '/api/v1/dashboards';

export interface DashboardsResponse {
  dashboards: Dashboard[];
}

/**
 * Fetch all dashboards accessible by the current user
 */
export async function getDashboards(): Promise<Dashboard[]> {
  const response = await apiClient.get<DashboardsResponse>(DASHBOARDS_BASE);
  return response.data.dashboards;
}

/**
 * Get a dashboard by ID
 */
export async function getDashboard(id: string): Promise<Dashboard> {
  const response = await apiClient.get<Dashboard>(`${DASHBOARDS_BASE}/${encodeURIComponent(id)}`);
  return response.data;
}

/**
 * Create a new dashboard
 */
export async function createDashboard(dashboard: DashboardCreate): Promise<Dashboard> {
  const response = await apiClient.post<Dashboard>(DASHBOARDS_BASE, dashboard);
  return response.data;
}

/**
 * Update an existing dashboard
 */
export async function updateDashboard(id: string, dashboard: Partial<DashboardCreate>): Promise<Dashboard> {
  const response = await apiClient.put<Dashboard>(`${DASHBOARDS_BASE}/${encodeURIComponent(id)}`, dashboard);
  return response.data;
}

/**
 * Delete a dashboard
 */
export async function deleteDashboard(id: string): Promise<void> {
  await apiClient.delete(`${DASHBOARDS_BASE}/${encodeURIComponent(id)}`);
}

/**
 * Get the current user's favorited dashboard IDs
 */
export async function getFavorites(): Promise<string[]> {
  const response = await apiClient.get<{ dashboardIds: string[] }>(`${DASHBOARDS_BASE}/favorites`);
  return response.data.dashboardIds;
}

/**
 * Toggle a dashboard as favorite for the current user
 */
export async function toggleFavorite(id: string): Promise<{ favorited: boolean }> {
  const response = await apiClient.post<{ favorited: boolean }>(
    `${DASHBOARDS_BASE}/${encodeURIComponent(id)}/favorite`
  );
  return response.data;
}

/**
 * Upload an image file for use in dashboard image panels
 */
export async function uploadImage(file: File): Promise<{ url: string; filename: string }> {
  const formData = new FormData();
  formData.append('file', file);
  const response = await apiClient.post<{ url: string; filename: string }>(
    `${DASHBOARDS_BASE}/upload-image`, formData,
    { headers: { 'Content-Type': 'multipart/form-data' } }
  );
  return response.data;
}
