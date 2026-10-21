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
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { renderHook, waitFor } from '@testing-library/react';

import { useSendDataToAi } from './useSendDataToAi';
import { getAiStatus } from '../api/ai';

vi.mock('../api/ai', () => ({ getAiStatus: vi.fn() }));

/**
 * sendDataToAi is a privacy flag: it controls whether sample rows from the user's
 * data are sent to an LLM. It must default to withholding, and must keep withholding
 * whenever the true value can't be determined (still loading, or the fetch failed).
 * It was previously sourced from the admin-only /config endpoint with a permissive
 * default, which silently leaked sample data for every non-admin user. This hook is
 * the single place both SqlLabPage and useProspector now read the flag from — if
 * either the default or the fail-closed behaviour regresses here, both callers break
 * the same way that bug already happened once.
 */
describe('useSendDataToAi', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('defaults to withholding before the status fetch resolves', () => {
    // Never resolves during this test — simulates "still loading".
    vi.mocked(getAiStatus).mockReturnValue(new Promise(() => {}));
    const { result } = renderHook(() => useSendDataToAi());

    expect(result.current.sendDataToAi).toBe(false);
    expect(result.current.sendDataToAiRef.current).toBe(false);
  });

  it('withholds when the server reports sendDataToAi: false', async () => {
    vi.mocked(getAiStatus).mockResolvedValue(
      { enabled: true, configured: true, sendDataToAi: false } as never);
    const { result } = renderHook(() => useSendDataToAi());

    await waitFor(() => expect(getAiStatus).toHaveBeenCalled());
    await waitFor(() => expect(result.current.sendDataToAiRef.current).toBe(false));
    expect(result.current.sendDataToAi).toBe(false);
  });

  it('allows sending sample data when the server reports sendDataToAi: true', async () => {
    vi.mocked(getAiStatus).mockResolvedValue(
      { enabled: true, configured: true, sendDataToAi: true } as never);
    const { result } = renderHook(() => useSendDataToAi());

    await waitFor(() => expect(result.current.sendDataToAi).toBe(true));
    expect(result.current.sendDataToAiRef.current).toBe(true);
  });

  it('withholds when the status fetch rejects, e.g. against an older server', async () => {
    vi.mocked(getAiStatus).mockRejectedValue(new Error('404'));
    const { result } = renderHook(() => useSendDataToAi());

    await waitFor(() => expect(getAiStatus).toHaveBeenCalled());
    // Give the rejection handler a tick to run.
    await waitFor(() => expect(result.current.sendDataToAiRef.current).toBe(false));
    expect(result.current.sendDataToAi).toBe(false);
  });
});
