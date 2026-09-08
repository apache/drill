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
import axios from 'axios';

export interface AuthConfig {
  authEnabled: boolean;
  formEnabled: boolean;
  spnegoEnabled: boolean;
}

/**
 * Fetched by the login pages before render so they know which auth
 * mechanisms to surface. Uses raw axios (not the apiClient interceptor) so a
 * pre-auth 401 redirect loop is impossible — the response is read no matter
 * what status comes back.
 */
export async function getAuthConfig(): Promise<AuthConfig> {
  const response = await axios.get<AuthConfig>('/api/v1/auth-config', {
    withCredentials: true,
  });
  return response.data;
}
