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
import type { SavedQuery, SavedQueryCreate } from '../types';

const SAVED_QUERIES_BASE = '/api/v1/saved-queries';

export interface SavedQueriesResponse {
  queries: SavedQuery[];
}

/**
 * Fetch all saved queries accessible by the current user
 */
export async function getSavedQueries(): Promise<SavedQuery[]> {
  const response = await apiClient.get<SavedQueriesResponse>(SAVED_QUERIES_BASE);
  return response.data.queries;
}

/**
 * Get a saved query by ID
 */
export async function getSavedQuery(id: string): Promise<SavedQuery> {
  const response = await apiClient.get<SavedQuery>(`${SAVED_QUERIES_BASE}/${encodeURIComponent(id)}`);
  return response.data;
}

/**
 * Create a new saved query
 */
export async function createSavedQuery(query: SavedQueryCreate): Promise<SavedQuery> {
  const response = await apiClient.post<SavedQuery>(SAVED_QUERIES_BASE, query);
  return response.data;
}

/**
 * Update an existing saved query
 */
export async function updateSavedQuery(id: string, query: Partial<SavedQueryCreate>): Promise<SavedQuery> {
  const response = await apiClient.put<SavedQuery>(`${SAVED_QUERIES_BASE}/${encodeURIComponent(id)}`, query);
  return response.data;
}

/**
 * Delete a saved query
 */
export async function deleteSavedQuery(id: string): Promise<void> {
  await apiClient.delete(`${SAVED_QUERIES_BASE}/${encodeURIComponent(id)}`);
}
