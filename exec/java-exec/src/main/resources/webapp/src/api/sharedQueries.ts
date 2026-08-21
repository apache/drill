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
import type { SharedQueryApi, SharedQueryApiCreate } from '../types';

const BASE = '/api/v1/shared-queries';

export interface SharedQueryApisResponse {
  queries: SharedQueryApi[];
}

export async function getSharedQueryApis(): Promise<SharedQueryApi[]> {
  const response = await apiClient.get<SharedQueryApisResponse>(BASE);
  return response.data.queries;
}

export async function getSharedQueryApi(id: string): Promise<SharedQueryApi> {
  const response = await apiClient.get<SharedQueryApi>(`${BASE}/${encodeURIComponent(id)}`);
  return response.data;
}

export async function createSharedQueryApi(data: SharedQueryApiCreate): Promise<SharedQueryApi> {
  const response = await apiClient.post<SharedQueryApi>(BASE, data);
  return response.data;
}

export async function updateSharedQueryApi(id: string, data: Partial<SharedQueryApiCreate>): Promise<SharedQueryApi> {
  const response = await apiClient.put<SharedQueryApi>(`${BASE}/${encodeURIComponent(id)}`, data);
  return response.data;
}

export async function deleteSharedQueryApi(id: string): Promise<void> {
  await apiClient.delete(`${BASE}/${encodeURIComponent(id)}`);
}
