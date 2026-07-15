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
import { useEffect, useRef, useState, type MutableRefObject } from 'react';
import { getAiStatus } from '../api/ai';

export interface SendDataToAiState {
  /** Current value of the flag; re-renders the caller when it changes. */
  sendDataToAi: boolean;
  /**
   * Same value, mirrored into a ref for callers (e.g. useCallback bodies invoked
   * from async tool loops) that must read the latest value without adding it to a
   * dependency array — refs are stable across renders, so including this ref in a
   * dependency array is unnecessary and reading `.current` always sees the latest fetch.
   */
  sendDataToAiRef: MutableRefObject<boolean>;
}

/**
 * Reads the `sendDataToAi` privacy flag once on mount from `/api/v1/ai/status` and
 * keeps it up to date.
 *
 * This is deliberately sourced from `/status`, not the admin-only `/config` endpoint:
 * a non-admin's fetch of `/config` 403s, and falling back to a permissive default in
 * that case is exactly the bug this hook exists to prevent — sample data would be sent
 * to an LLM for every non-admin user regardless of what the deployment configured.
 * `/status` is readable by every authenticated user and mirrors the same setting.
 *
 * The default, and the value used while the fetch is in flight or if it fails, is
 * `false` (withhold). A privacy flag must fail closed, not open.
 */
export function useSendDataToAi(): SendDataToAiState {
  const [sendDataToAi, setSendDataToAi] = useState(false);
  const sendDataToAiRef = useRef(false);

  useEffect(() => {
    let cancelled = false;
    getAiStatus()
      .then((status) => {
        if (!cancelled) {
          setSendDataToAi(status.sendDataToAi);
          sendDataToAiRef.current = status.sendDataToAi;
        }
      })
      .catch(() => {
        if (!cancelled) {
          setSendDataToAi(false);
          sendDataToAiRef.current = false;
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  return { sendDataToAi, sendDataToAiRef };
}
