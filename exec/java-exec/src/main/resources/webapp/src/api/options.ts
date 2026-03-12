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

export interface DrillOption {
  name: string;
  value: unknown;
  defaultValue: string;
  accessibleScopes: string;
  kind: string;       // BOOLEAN | LONG | STRING | DOUBLE
  optionScope: string; // BOOT | SYSTEM | SESSION | QUERY
}

export async function getOptions(): Promise<DrillOption[]> {
  const response = await apiClient.get<DrillOption[]>('/options.json');
  return response.data;
}

export async function getInternalOptions(): Promise<DrillOption[]> {
  const response = await apiClient.get<DrillOption[]>('/internal_options.json');
  return response.data;
}

export async function updateOption(name: string, value: string, kind: string): Promise<void> {
  await apiClient.post(
    `/option/${encodeURIComponent(name)}`,
    new URLSearchParams({ name, value, kind }),
    { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } },
  );
}

/**
 * Fetch option descriptions from the generated JS file.
 * Returns a map of option name → description string.
 */
export async function getOptionDescriptions(): Promise<Record<string, string>> {
  try {
    const response = await apiClient.get<string>('/dynamic/options.describe.js', {
      responseType: 'text',
    });
    const js = response.data;
    // Parse the JS map: var optsDescripMap = { "name" : "desc", ... };
    const descriptions: Record<string, string> = {};
    const regex = /"([^"]+)"\s*:\s*"((?:[^"\\]|\\.)*)"/g;
    let match;
    while ((match = regex.exec(js)) !== null) {
      descriptions[match[1]] = match[2]
        .replace(/\\'/g, "'")
        .replace(/\\"/g, '"')
        .replace(/\\\\/g, '\\');
    }
    return descriptions;
  } catch {
    return {};
  }
}
