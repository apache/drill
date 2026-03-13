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

const SMTP_CONFIG_BASE = '/api/v1/smtp/config';

export interface SmtpConfig {
  host: string;
  port: number;
  username: string;
  passwordSet: boolean;
  fromAddress: string;
  fromName: string;
  encryption: string;
  enabled: boolean;
}

export interface SmtpConfigUpdate {
  host?: string;
  port?: number;
  username?: string;
  password?: string;
  fromAddress?: string;
  fromName?: string;
  encryption?: string;
  enabled?: boolean;
}

export interface SmtpTestResult {
  success: boolean;
  message: string;
}

export async function getSmtpConfig(): Promise<SmtpConfig> {
  const response = await apiClient.get<SmtpConfig>(SMTP_CONFIG_BASE);
  return response.data;
}

export async function updateSmtpConfig(config: SmtpConfigUpdate): Promise<SmtpConfig> {
  const response = await apiClient.put<SmtpConfig>(SMTP_CONFIG_BASE, config);
  return response.data;
}

export async function testSmtpConfig(recipientEmail?: string): Promise<SmtpTestResult> {
  const response = await apiClient.post<SmtpTestResult>(`${SMTP_CONFIG_BASE}/test`, {
    recipientEmail,
  });
  return response.data;
}
