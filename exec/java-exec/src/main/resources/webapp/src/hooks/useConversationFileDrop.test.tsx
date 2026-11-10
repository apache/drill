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
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useConversationFileDrop } from './useConversationFileDrop';
import { useProjectFileUpload } from './useProjectFileUpload';

vi.mock('./useProjectFileUpload', () => ({ useProjectFileUpload: vi.fn() }));

const file = new File(['a,b'], 'sales.csv', { type: 'text/csv' });

describe('useConversationFileDrop', () => {
  const upload = vi.fn();
  const sendMessage = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(useProjectFileUpload).mockReturnValue(upload);
  });

  it('sends a message naming the file and its table path', async () => {
    upload.mockResolvedValue({ name: 'sales.csv', path: 'proj.sales.csv', schema: 'dfs.uploads.`proj`.`sales.csv`' });
    const { result } = renderHook(() => useConversationFileDrop('proj', sendMessage));

    await act(() => result.current(file));

    expect(upload).toHaveBeenCalledWith(file);
    expect(sendMessage).toHaveBeenCalledTimes(1);
    const text = sendMessage.mock.calls[0][0] as string;
    expect(text).toContain('sales.csv');
    expect(text).toContain('dfs.uploads.`proj`.`sales.csv`');
  });

  it('sends nothing when the upload did not complete', async () => {
    upload.mockResolvedValue(undefined);
    const { result } = renderHook(() => useConversationFileDrop('proj', sendMessage));

    await act(() => result.current(file));

    expect(sendMessage).not.toHaveBeenCalled();
  });

  it('sends one message for several files dropped together', async () => {
    upload.mockImplementation(async (f: File) => ({ name: f.name, path: `p.${f.name}`, schema: `dfs.\`${f.name}\`` }));
    const { result } = renderHook(() => useConversationFileDrop('proj', sendMessage));
    const other = new File(['x'], 'costs.csv', { type: 'text/csv' });

    await act(async () => {
      await Promise.all([result.current(file), result.current(other)]);
    });

    expect(sendMessage).toHaveBeenCalledTimes(1);
    const text = sendMessage.mock.calls[0][0] as string;
    expect(text).toContain('sales.csv');
    expect(text).toContain('costs.csv');
    expect(text).toContain('dfs.`costs.csv`');
  });
});
