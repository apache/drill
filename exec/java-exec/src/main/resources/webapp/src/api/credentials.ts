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

/**
 * Per-plugin entry returned by {@code GET /credentials.json}. Only plugins
 * with {@code USER_TRANSLATION} auth mode are listed here — i.e. plugins
 * where each end user supplies their own credentials.
 */
export interface CredentialPlugin {
  name: string;
  /** Whether the plugin is currently enabled in the storage registry. */
  enabled: boolean;
  /** True if this plugin uses OAuth (clientID present in credentials). */
  isOauth: boolean;
  /** Pre-built authorize URL with client_id / redirect_uri / scope; empty for non-OAuth plugins. */
  authorizationUrl: string;
  /** Raw plugin config — includes credentialsProvider, oAuthConfig, etc. */
  config: Record<string, unknown>;
}

export async function getCredentialPlugins(): Promise<CredentialPlugin[]> {
  const response = await apiClient.get<CredentialPlugin[]>('/credentials.json');
  return response.data;
}

/**
 * Update the current user's credentials for a USER_TRANSLATION plugin. The
 * plugin name is the storage config name. Returns the server's status string.
 */
export async function updateCredentials(
  pluginName: string,
  username: string,
  password: string,
): Promise<string> {
  const response = await apiClient.post<string>(
    `/credentials/${encodeURIComponent(pluginName)}/update_credentials.json`,
    { username, password },
  );
  return response.data;
}
