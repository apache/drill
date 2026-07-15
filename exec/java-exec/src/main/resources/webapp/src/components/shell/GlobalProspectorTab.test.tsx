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
import { render } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import type { ChatContext } from '../../types/ai';
import GlobalProspectorTab from './GlobalProspectorTab';

const capturedContexts: ChatContext[] = [];

vi.mock('../../hooks/useProspector', () => ({
  useProspector: vi.fn(() => ({ mocked: true })),
}));

vi.mock('../prospector', () => ({
  ProspectorPanel: (props: { context: ChatContext }) => {
    capturedContexts.push(props.context);
    return null;
  },
}));

function renderAt(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="/projects/:id/*" element={<GlobalProspectorTab />} />
        <Route path="*" element={<GlobalProspectorTab />} />
      </Routes>
    </MemoryRouter>
  );
}

describe('GlobalProspectorTab', () => {
  beforeEach(() => {
    capturedContexts.length = 0;
  });

  it('passes the projectId from a nested project route (/projects/:id/sql)', () => {
    renderAt('/projects/proj-42/sql');

    expect(capturedContexts).toHaveLength(1);
    expect(capturedContexts[0]).toEqual({
      feature: 'global_chat',
      projectId: 'proj-42',
    });
  });

  it('passes the projectId from the bare project route (/projects/:id)', () => {
    renderAt('/projects/proj-42');

    expect(capturedContexts).toHaveLength(1);
    expect(capturedContexts[0]).toEqual({
      feature: 'global_chat',
      projectId: 'proj-42',
    });
  });

  it('leaves projectId undefined outside of any project route', () => {
    renderAt('/');

    expect(capturedContexts).toHaveLength(1);
    expect(capturedContexts[0]).toEqual({
      feature: 'global_chat',
      projectId: undefined,
    });
  });
});
