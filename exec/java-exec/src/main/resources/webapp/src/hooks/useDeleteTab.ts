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
import { useDispatch } from 'react-redux';
import { useQueryClient } from '@tanstack/react-query';
import type { AppDispatch } from '../store';
import { deleteTab as deleteTabAction } from '../store/querySlice';
import { deleteTab as deleteServerTab } from '../api/tabs';
import { prospectorChatKey } from './useProspector';

/**
 * Deletes a tab everywhere it exists.
 *
 * A tab has four homes — Redux, the server, its Prospector conversation in
 * localStorage, and the sidebar's cached tab list — and a delete that misses any of
 * them looks broken. Deleting only the server record is the worst case: the tab stays
 * open in the strip, and the next sync tick promotes it straight back, so the row
 * reappears in the tree a second later.
 *
 * This exists because the sidebar and the editor each grew their own delete and drifted
 * apart. One implementation, two call sites.
 */
export function useDeleteTab(projectId?: string) {
  const dispatch = useDispatch<AppDispatch>();
  const queryClient = useQueryClient();

  return useCallback(async (tabId: string) => {
    // Redux first, so the tab leaves the UI immediately and the sync loop stops
    // seeing it as something to promote. Deleting a tab belonging to another project
    // is a no-op here, which is correct: Redux only holds the open project's tabs.
    dispatch(deleteTabAction(tabId));

    const chatKey = prospectorChatKey(projectId, tabId);
    if (chatKey) {
      try {
        localStorage.removeItem(chatKey);
      } catch {
        // Storage unavailable; an orphaned conversation is harmless.
      }
    }

    try {
      await deleteServerTab(tabId);
    } catch {
      // A 409 means the tab is locked, which the menus already prevent offering.
      // Anything else is transient; refreshing the list below shows the real state.
    }

    queryClient.invalidateQueries({ queryKey: ['project-tabs', projectId] });
  }, [dispatch, queryClient, projectId]);
}
