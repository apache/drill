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
import { useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { useScratchProject } from './useScratchProject';
import type { ConversationSeed } from '../types/ai';

export interface AskProspector {
  /**
   * Opens Prospector's Conversations page. With a seed it starts a new conversation and
   * sends the seed as its first turn. Undefined until a target project is known, so
   * there is never a link to a bad id.
   */
  ask: ((seed?: ConversationSeed) => void) | undefined;
  isLoading: boolean;
  isError: boolean;
}

/**
 * The way into Prospector from pages that are not the Conversations page. Targets the given
 * project, or the user's Scratch project when there is none.
 */
export function useAskProspector(projectId?: string): AskProspector {
  const navigate = useNavigate();
  const scratch = useScratchProject(!projectId);
  const targetId = projectId ?? scratch.projectId;
  const ask = useCallback(
    (seed?: ConversationSeed) => {
      navigate(`/projects/${targetId}/chat`, seed ? { state: { seed } } : undefined);
    },
    [navigate, targetId],
  );
  return {
    ask: targetId ? ask : undefined,
    isLoading: !projectId && scratch.isLoading,
    isError: !projectId && scratch.isError,
  };
}
