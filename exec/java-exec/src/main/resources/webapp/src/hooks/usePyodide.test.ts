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
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';

// Mock apache-arrow module
vi.mock('apache-arrow', () => ({
  tableFromJSON: vi.fn(() => ({ schema: { fields: [] } })),
  tableToIPC: vi.fn(() => new Uint8Array([1, 2, 3])),
}));

import { usePyodide } from './usePyodide';
import type { WorkspaceInfo, WorkspacesResult } from './usePyodide';

describe('usePyodide', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('initial state', () => {
    it('returns correct initial state', () => {
      const { result } = renderHook(() => usePyodide());

      expect(result.current.loading).toBe(false);
      expect(result.current.error).toBeNull();
      expect(result.current.statusMessage).toBeDefined();
      expect(typeof result.current.initialize).toBe('function');
      expect(typeof result.current.runPython).toBe('function');
      expect(typeof result.current.injectDataFrame).toBe('function');
      expect(typeof result.current.listDataFrames).toBe('function');
      expect(typeof result.current.installPackage).toBe('function');
      expect(typeof result.current.uninstallPackage).toBe('function');
      expect(typeof result.current.listInstalledPackages).toBe('function');
      expect(typeof result.current.listAllPackages).toBe('function');
      expect(typeof result.current.listVariables).toBe('function');
      expect(typeof result.current.resetNamespace).toBe('function');
      expect(typeof result.current.saveToDrill).toBe('function');
      expect(typeof result.current.listWritableWorkspaces).toBe('function');
    });
  });

  describe('runPython', () => {
    it('returns error when runtime is not initialized', async () => {
      const { result } = renderHook(() => usePyodide());

      let output;
      await act(async () => {
        output = await result.current.runPython('print("hello")');
      });

      expect(output).toEqual({
        stdout: '',
        stderr: 'Python runtime not initialized',
        result: null,
        htmlResult: null,
        imageData: null,
        error: true,
      });
    });
  });

  describe('injectDataFrame', () => {
    it('throws when runtime is not initialized', async () => {
      const { result } = renderHook(() => usePyodide());

      await expect(
        act(async () => {
          await result.current.injectDataFrame('df', ['a', 'b'], [{ a: 1, b: 2 }]);
        })
      ).rejects.toThrow('Python runtime not initialized');
    });
  });

  describe('listDataFrames', () => {
    it('returns empty array when runtime is not initialized', async () => {
      const { result } = renderHook(() => usePyodide());

      let dfs;
      await act(async () => {
        dfs = await result.current.listDataFrames();
      });

      expect(dfs).toEqual([]);
    });
  });

  describe('installPackage', () => {
    it('returns error when runtime is not initialized', async () => {
      const { result } = renderHook(() => usePyodide());

      let output;
      await act(async () => {
        output = await result.current.installPackage('seaborn');
      });

      expect(output).toEqual({
        stdout: '',
        stderr: 'Python runtime not initialized',
        result: null,
        htmlResult: null,
        imageData: null,
        error: true,
      });
    });
  });

  describe('uninstallPackage', () => {
    it('returns error when runtime is not initialized', async () => {
      const { result } = renderHook(() => usePyodide());

      let output;
      await act(async () => {
        output = await result.current.uninstallPackage('seaborn');
      });

      expect(output).toEqual({
        stdout: '',
        stderr: 'Python runtime not initialized',
        result: null,
        htmlResult: null,
        imageData: null,
        error: true,
      });
    });
  });

  describe('listInstalledPackages', () => {
    it('returns empty array when runtime is not initialized', async () => {
      const { result } = renderHook(() => usePyodide());

      let packages;
      await act(async () => {
        packages = await result.current.listInstalledPackages();
      });

      expect(packages).toEqual([]);
    });
  });

  describe('listAllPackages', () => {
    it('returns empty array when runtime is not initialized', async () => {
      const { result } = renderHook(() => usePyodide());

      let packages;
      await act(async () => {
        packages = await result.current.listAllPackages();
      });

      expect(packages).toEqual([]);
    });
  });

  describe('listVariables', () => {
    it('returns empty array when runtime is not initialized', async () => {
      const { result } = renderHook(() => usePyodide());

      let vars;
      await act(async () => {
        vars = await result.current.listVariables();
      });

      expect(vars).toEqual([]);
    });
  });

  describe('resetNamespace', () => {
    it('does nothing when runtime is not initialized', async () => {
      const { result } = renderHook(() => usePyodide());

      // Should not throw
      await act(async () => {
        await result.current.resetNamespace();
      });
    });
  });

  describe('listWritableWorkspaces', () => {
    afterEach(() => {
      vi.restoreAllMocks();
    });

    it('returns workspaces and writePolicy on success', async () => {
      const mockWorkspaces: WorkspaceInfo[] = [
        { plugin: 'dfs', workspace: 'tmp', location: '/tmp', writable: true },
        { plugin: 'dfs', workspace: 'data', location: '/data', writable: true },
      ];

      const mockFetch = vi.fn().mockResolvedValue({
        ok: true,
        json: () => Promise.resolve({ workspaces: mockWorkspaces, writePolicy: 'filesystem' }),
      });
      vi.stubGlobal('fetch', mockFetch);

      const { result } = renderHook(() => usePyodide());

      let wsResult: WorkspacesResult | undefined;
      await act(async () => {
        wsResult = await result.current.listWritableWorkspaces();
      });

      expect(wsResult!.workspaces).toEqual(mockWorkspaces);
      expect(wsResult!.writePolicy).toBe('filesystem');
      expect(mockFetch).toHaveBeenCalledWith('/api/v1/notebook/workspaces', expect.objectContaining({ credentials: 'include' }));
    });

    it('returns empty result on fetch error', async () => {
      const mockFetch = vi.fn().mockRejectedValue(new Error('Network error'));
      vi.stubGlobal('fetch', mockFetch);

      const { result } = renderHook(() => usePyodide());

      let wsResult: WorkspacesResult | undefined;
      await act(async () => {
        wsResult = await result.current.listWritableWorkspaces();
      });

      expect(wsResult!.workspaces).toEqual([]);
      expect(wsResult!.writePolicy).toBe('filesystem');
    });

    it('returns disabled policy on non-ok response', async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        ok: false,
        status: 500,
      });
      vi.stubGlobal('fetch', mockFetch);

      const { result } = renderHook(() => usePyodide());

      let wsResult: WorkspacesResult | undefined;
      await act(async () => {
        wsResult = await result.current.listWritableWorkspaces();
      });

      expect(wsResult!.workspaces).toEqual([]);
      expect(wsResult!.writePolicy).toBe('disabled');
    });

    it('returns empty workspaces when response has no workspaces', async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        ok: true,
        json: () => Promise.resolve({}),
      });
      vi.stubGlobal('fetch', mockFetch);

      const { result } = renderHook(() => usePyodide());

      let wsResult: WorkspacesResult | undefined;
      await act(async () => {
        wsResult = await result.current.listWritableWorkspaces();
      });

      expect(wsResult!.workspaces).toEqual([]);
      expect(wsResult!.writePolicy).toBe('filesystem');
    });

    it('includes CSRF token in request headers when available', async () => {
      // Add a CSRF meta tag
      const meta = document.createElement('meta');
      meta.setAttribute('name', 'csrf-token');
      meta.setAttribute('content', 'test-csrf-token');
      document.head.appendChild(meta);

      const mockFetch = vi.fn().mockResolvedValue({
        ok: true,
        json: () => Promise.resolve({ workspaces: [], writePolicy: 'filesystem' }),
      });
      vi.stubGlobal('fetch', mockFetch);

      const { result } = renderHook(() => usePyodide());

      await act(async () => {
        await result.current.listWritableWorkspaces();
      });

      expect(mockFetch).toHaveBeenCalledWith('/api/v1/notebook/workspaces', {
        credentials: 'include',
        headers: { 'X-CSRF-Token': 'test-csrf-token' },
      });

      // Cleanup
      document.head.removeChild(meta);
    });
  });

  describe('saveToDrill', () => {
    it('returns error when runtime is not initialized', async () => {
      const { result } = renderHook(() => usePyodide());

      let output;
      await act(async () => {
        output = await result.current.saveToDrill('df', 'dfs', 'tmp', 'test_table', 'json');
      });

      // saveToDrill calls runPython, which returns error when not initialized
      expect(output).toEqual({
        stdout: '',
        stderr: 'Python runtime not initialized',
        result: null,
        htmlResult: null,
        imageData: null,
        error: true,
      });
    });
  });
});
