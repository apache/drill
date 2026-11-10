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
import { describe, it, expect, vi } from 'vitest';
import { render, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import SchemaExplorer from './SchemaExplorer';

const captured = vi.hoisted(() => ({ props: [] as Array<Record<string, unknown>> }));

vi.mock('./ContextMenu', () => ({
  default: (props: Record<string, unknown> & { children: React.ReactNode }) => {
    captured.props.push(props);
    return <>{props.children}</>;
  },
}));
vi.mock('../../api/uploads', () => ({
  getUploadStatus: vi.fn().mockResolvedValue({
    configured: true, plugin: 'dfs', workspace: 'uploads', maxSizeBytes: 1,
  }),
}));
vi.mock('./hooks', () => {
  // A stable reference: the component syncs state from it in an effect.
  const result = { data: [{ name: 'dfs', type: 'file', enabled: true }], isLoading: false };
  return { usePlugins: () => result };
});
vi.mock('../../api/workflows', () => ({ listWorkflows: vi.fn().mockResolvedValue([]) }));
vi.mock('../../api/visualizations', () => ({ getVisualizations: vi.fn().mockResolvedValue([]) }));
vi.mock('../../api/projects', () => ({ getProject: vi.fn().mockResolvedValue(null) }));
vi.mock('../../hooks/useSchemaUsage', () => ({ useSchemaUsage: () => ({}) }));

describe('SchemaExplorer upload wiring', () => {
  it('invalidates the project schema cache when an uploaded file is deleted', async () => {
    const queryClient = new QueryClient();
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');
    render(
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>
          <SchemaExplorer projectId="proj-a" />
        </MemoryRouter>
      </QueryClientProvider>,
    );

    // Once the upload status arrives, the menu is handed the target schema and a delete hook.
    await waitFor(() => {
      const last = captured.props[captured.props.length - 1];
      expect(last?.uploadSchema).toBe('dfs.uploads');
    });
    const last = captured.props[captured.props.length - 1];
    expect(last.projectId).toBe('proj-a');

    (last.onFileDeleted as () => void)();
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ['schemaCache', 'proj-a'] });
  });
});
