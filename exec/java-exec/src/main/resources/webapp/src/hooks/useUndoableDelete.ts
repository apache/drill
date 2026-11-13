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
import { Button, notification } from 'antd';
import { UndoOutlined } from '@ant-design/icons';
import React from 'react';

export interface UndoableDeleteOptions<T> {
  /** Capture whatever data is needed to restore the entity. Called BEFORE the delete fires. */
  capture: () => T | Promise<T>;
  /** Run the destructive action. Returns when the server confirms. */
  remove: () => Promise<unknown>;
  /** Recreate the entity from the captured snapshot. */
  restore: (snapshot: T) => Promise<unknown>;
  /** Friendly label for the toast — e.g. `Deleted "Q1 Revenue"`. */
  label: string;
  /** Toast button caption (default `Undo`). */
  undoLabel?: string;
  /** Auto-dismiss after this many seconds. Default 8. */
  durationSec?: number;
  /** Optional callback fired after the delete + before the restore window starts. */
  onDeleted?: () => void;
  /** Optional callback fired after a successful restore. */
  onRestored?: () => void;
}

/**
 * Run a destructive action with an Apple-style "Deleted X. Undo." toast.
 *
 * Pairs with the backend soft-delete model: `remove` moves the entity to trash,
 * `restore` clears its `deletedAt` so cross-references stay intact. Even after
 * the toast expires, items remain recoverable from the Trash view in Preferences.
 */
export function useUndoableDelete() {
  const [api, contextHolder] = notification.useNotification();

  const run = useCallback(
    async <T,>(opts: UndoableDeleteOptions<T>) => {
      const { capture, remove, restore, label, undoLabel = 'Undo', durationSec = 8, onDeleted, onRestored } = opts;
      let snapshot: T;
      try {
        snapshot = await capture();
      } catch {
        // If we can't capture for some reason, fall back to a plain delete.
        await remove();
        notification.success({ message: label, duration: 3 });
        return;
      }

      try {
        await remove();
      } catch (err) {
        notification.error({
          message: 'Delete failed',
          description: err instanceof Error ? err.message : String(err),
        });
        return;
      }

      onDeleted?.();

      const key = `undo-${Date.now()}-${Math.random()}`;
      api.open({
        key,
        message: label,
        description: 'You can undo this within a few seconds.',
        duration: durationSec,
        placement: 'bottomLeft',
        btn: React.createElement(
          Button,
          {
            type: 'primary',
            size: 'small',
            icon: React.createElement(UndoOutlined),
            onClick: async () => {
              api.destroy(key);
              try {
                await restore(snapshot);
                notification.success({
                  message: 'Restored',
                  duration: 2,
                  placement: 'bottomLeft',
                });
                onRestored?.();
              } catch (err) {
                notification.error({
                  message: 'Could not restore',
                  description: err instanceof Error ? err.message : String(err),
                });
              }
            },
          },
          undoLabel,
        ),
      });
    },
    [api],
  );

  return { run, contextHolder };
}
