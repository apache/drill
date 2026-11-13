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
import { useCallback, useEffect, useState } from 'react';

const ORDER_KEY = 'drill-shell-project-order';
const LEGACY_PINNED_KEY = 'drill-shell-pinned-projects';

function readOrder(): string[] {
  try {
    const raw = localStorage.getItem(ORDER_KEY);
    if (raw) {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        return parsed.filter((x) => typeof x === 'string');
      }
    }
    // One-time migration from the legacy pinned-only list
    const legacy = localStorage.getItem(LEGACY_PINNED_KEY);
    if (legacy) {
      const parsed = JSON.parse(legacy);
      if (Array.isArray(parsed)) {
        const ids = parsed.filter((x) => typeof x === 'string');
        if (ids.length > 0) {
          localStorage.setItem(ORDER_KEY, JSON.stringify(ids));
        }
        return ids;
      }
    }
  } catch {
    // Ignore storage errors / malformed JSON
  }
  return [];
}

function writeOrder(value: string[]): void {
  try {
    localStorage.setItem(ORDER_KEY, JSON.stringify(value));
  } catch {
    // Ignore storage errors
  }
}

export interface ProjectOrderApi {
  /** User-defined order (subset of project IDs). Projects not present fall back to alpha. */
  order: string[];
  /** Move `draggedId` to before/after `targetId`. Adds either if not already in the list. */
  moveProject: (draggedId: string, targetId: string, position: 'before' | 'after') => void;
  /** Move `draggedId` to the very top (used for explicit "pin" gestures if added later). */
  moveToTop: (id: string) => void;
  /** Drop a project ID from the order (e.g. on delete) — falls back to alpha. */
  removeFromOrder: (id: string) => void;
}

export function useProjectOrder(): ProjectOrderApi {
  const [order, setOrder] = useState<string[]>(readOrder);

  useEffect(() => {
    const onStorage = (e: StorageEvent) => {
      if (e.key === ORDER_KEY) {
        setOrder(readOrder());
      }
    };
    window.addEventListener('storage', onStorage);
    return () => window.removeEventListener('storage', onStorage);
  }, []);

  const moveProject = useCallback(
    (draggedId: string, targetId: string, position: 'before' | 'after') => {
      if (draggedId === targetId) {
        return;
      }
      setOrder((prev) => {
        let next = prev.filter((id) => id !== draggedId);
        let targetIndex = next.indexOf(targetId);
        if (targetIndex === -1) {
          // Target not yet ordered — append it so we have an anchor.
          next = [...next, targetId];
          targetIndex = next.length - 1;
        }
        const insertAt = targetIndex + (position === 'after' ? 1 : 0);
        next.splice(insertAt, 0, draggedId);
        writeOrder(next);
        return next;
      });
    },
    [],
  );

  const moveToTop = useCallback((id: string) => {
    setOrder((prev) => {
      const next = [id, ...prev.filter((x) => x !== id)];
      writeOrder(next);
      return next;
    });
  }, []);

  const removeFromOrder = useCallback((id: string) => {
    setOrder((prev) => {
      if (!prev.includes(id)) {
        return prev;
      }
      const next = prev.filter((x) => x !== id);
      writeOrder(next);
      return next;
    });
  }, []);

  return { order, moveProject, moveToTop, removeFromOrder };
}
