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
import type { ProfileConfig } from '../types/profile';

const DEFAULT_CONFIG: ProfileConfig = {
  correlationMaxColumns: 20,
  profileMaxRows: 50000,
  profileSampleSize: 10000,
  histogramBins: 20,
  topKValues: 10,
};

export async function getProfileConfig(): Promise<ProfileConfig> {
  try {
    const response = await apiClient.get<ProfileConfig>('/api/v1/profile/config');
    return response.data;
  } catch {
    return DEFAULT_CONFIG;
  }
}

export async function updateProfileConfig(config: Partial<ProfileConfig>): Promise<ProfileConfig> {
  const response = await apiClient.put<ProfileConfig>('/api/v1/profile/config', config);
  return response.data;
}
