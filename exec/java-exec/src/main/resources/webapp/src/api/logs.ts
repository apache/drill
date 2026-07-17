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

export interface LogFile {
  name: string;
  size: string;
  lastModified: string;
}

export interface LogContent {
  name: string;
  lines: string[];
  maxLines: number;
}

export async function getLogFiles(): Promise<LogFile[]> {
  const { data } = await axios.get<LogFile[]>('/logs.json');
  return data;
}

export async function getLogContent(name: string): Promise<LogContent> {
  const { data } = await axios.get<LogContent>(`/log/${encodeURIComponent(name)}/content.json`);
  return data;
}

export function getLogDownloadUrl(name: string): string {
  return `/log/${encodeURIComponent(name)}/download`;
}

export interface LogSqlStatus {
  logDirConfigured: boolean;
  logDir: string | null;
  workspaceExists: boolean;
  formatExists: boolean;
  ready: boolean;
}

export interface LogSetupResponse {
  success: boolean;
  message: string;
}

export async function getLogSqlStatus(): Promise<LogSqlStatus> {
  const { data } = await axios.get<LogSqlStatus>('/api/v1/logs/sql-status');
  return data;
}

export async function setupLogSql(): Promise<LogSetupResponse> {
  const { data } = await axios.post<LogSetupResponse>('/api/v1/logs/sql-setup');
  return data;
}
