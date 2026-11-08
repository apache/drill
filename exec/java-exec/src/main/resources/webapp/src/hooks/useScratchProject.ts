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
import { useQuery } from '@tanstack/react-query';
import { getScratchProjectId } from '../api/conversations';

export interface ScratchProject {
  /** Undefined until the lookup succeeds; never a placeholder id. */
  projectId: string | undefined;
  isLoading: boolean;
  isError: boolean;
}

/**
 * The caller's personal Scratch project, home for Prospector conversations started outside
 * any project. The server creates it on first lookup, so the id is stable for the session.
 * Pass false to skip the lookup (and the creation it can trigger) when a page already
 * has a project to talk in.
 */
export function useScratchProject(enabled = true): ScratchProject {
  const { data, isLoading, isError } = useQuery({
    queryKey: ['scratch-project'],
    queryFn: getScratchProjectId,
    staleTime: Infinity,
    enabled,
  });
  return { projectId: data, isLoading, isError };
}
