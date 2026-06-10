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

export interface ThreadEntry {
  id: number;
  name: string;
  state: string;
  daemon: boolean;
  priority: number;
  blockedCount: number;
  blockedTime: number;
  waitedCount: number;
  waitedTime: number;
  lockName: string | null;
  lockOwnerId: number;
  lockOwnerName: string | null;
  stackTrace: string[];
}

export interface ThreadDump {
  count: number;
  peakCount: number;
  daemonCount: number;
  totalStartedCount: number;
  /** Thread IDs participating in a deadlock; empty when none detected. */
  deadlockedThreadIds: number[];
  threads: ThreadEntry[];
}

export async function getThreadDump(): Promise<ThreadDump> {
  const response = await apiClient.get<ThreadDump>('/api/v1/threads');
  return response.data;
}
