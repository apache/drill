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
import type { Visualization, VisualizationCreate } from '../types';

const VISUALIZATIONS_BASE = '/api/v1/visualizations';

export interface VisualizationsResponse {
  visualizations: Visualization[];
}

/**
 * Fetch all visualizations accessible by the current user
 */
export async function getVisualizations(): Promise<Visualization[]> {
  const response = await apiClient.get<VisualizationsResponse>(VISUALIZATIONS_BASE);
  return response.data.visualizations;
}

/**
 * Get a visualization by ID
 */
export async function getVisualization(id: string): Promise<Visualization> {
  const response = await apiClient.get<Visualization>(`${VISUALIZATIONS_BASE}/${encodeURIComponent(id)}`);
  return response.data;
}

/**
 * Create a new visualization
 */
export async function createVisualization(viz: VisualizationCreate): Promise<Visualization> {
  const response = await apiClient.post<Visualization>(VISUALIZATIONS_BASE, viz);
  return response.data;
}

/**
 * Update an existing visualization
 */
export async function updateVisualization(id: string, viz: Partial<VisualizationCreate>): Promise<Visualization> {
  const response = await apiClient.put<Visualization>(`${VISUALIZATIONS_BASE}/${encodeURIComponent(id)}`, viz);
  return response.data;
}

/**
 * Delete a visualization
 */
export async function deleteVisualization(id: string): Promise<void> {
  await apiClient.delete(`${VISUALIZATIONS_BASE}/${encodeURIComponent(id)}`);
}
