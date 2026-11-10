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
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import ProjectFileDropZone from './ProjectFileDropZone';
import {
  uploadProjectFile,
  getUploadStatus,
  UploadCollisionError,
  UploadNotConfiguredError,
} from '../../api/uploads';

vi.mock('../../contexts/ProjectContext', () => ({
  useProjectContext: () => ({ project: { id: 'p1', name: 'Project One' }, projectId: 'p1' }),
}));
vi.mock('../../api/uploads', async (orig) => ({
  ...(await orig<typeof import('../../api/uploads')>()),
  uploadProjectFile: vi.fn(),
  getUploadStatus: vi.fn(),
}));

const file = new File(['a,b'], 'sales.csv', { type: 'text/csv' });
const fileDrag = { dataTransfer: { types: ['Files'], files: [file] } };

function setup() {
  const client = new QueryClient();
  const invalidate = vi.spyOn(client, 'invalidateQueries');
  render(
    <QueryClientProvider client={client}>
      <ProjectFileDropZone projectId="p1">
        <div data-testid="child">content</div>
      </ProjectFileDropZone>
    </QueryClientProvider>,
  );
  return { invalidate, zone: screen.getByTestId('child').parentElement as HTMLElement };
}

describe('ProjectFileDropZone', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(getUploadStatus).mockResolvedValue({ configured: true, maxSizeBytes: 1000 });
  });

  it('shows no overlay until a file is dragged over it', () => {
    const { zone } = setup();
    expect(screen.queryByText('Drop to add to Project One')).toBeNull();
    // A drag inside the app (e.g. a schema tree node) carries no files.
    fireEvent.dragEnter(zone, { dataTransfer: { types: ['text/plain'] } });
    expect(screen.queryByText('Drop to add to Project One')).toBeNull();
  });

  it('shows the project name in the overlay while dragging, without flicker across children', () => {
    const { zone } = setup();
    const child = screen.getByTestId('child');
    fireEvent.dragEnter(zone, fileDrag);
    expect(screen.getByText('Drop to add to Project One')).toBeTruthy();
    fireEvent.dragEnter(child, fileDrag);
    fireEvent.dragLeave(zone, fileDrag);
    expect(screen.getByText('Drop to add to Project One')).toBeTruthy();
    fireEvent.dragLeave(child, fileDrag);
    expect(screen.queryByText('Drop to add to Project One')).toBeNull();
  });

  it('uploads the dropped file and reports the new table path', async () => {
    vi.mocked(uploadProjectFile).mockResolvedValue({ name: 'sales.csv', path: 'dfs.up.sales' });
    const { zone, invalidate } = setup();
    fireEvent.dragEnter(zone, fileDrag);
    fireEvent.drop(zone, fileDrag);
    expect(await screen.findByText(/dfs\.up\.sales/)).toBeTruthy();
    expect(uploadProjectFile).toHaveBeenCalledWith('p1', file, undefined);
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ['schemaCache', 'p1'] });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ['project', 'p1'] });
    expect(screen.queryByText('Drop to add to Project One')).toBeNull();
  });

  it('says to ask an admin when uploads are not configured', async () => {
    vi.mocked(getUploadStatus).mockResolvedValue({ configured: false, maxSizeBytes: 1000 });
    const { zone } = setup();
    fireEvent.drop(zone, fileDrag);
    expect(await screen.findByText(/ask an administrator/i)).toBeTruthy();
    expect(uploadProjectFile).not.toHaveBeenCalled();
  });

  it('says to ask an admin when the server reports no upload target', async () => {
    vi.mocked(uploadProjectFile).mockRejectedValue(new UploadNotConfiguredError('nope'));
    const { zone } = setup();
    fireEvent.drop(zone, fileDrag);
    expect(await screen.findByText(/ask an administrator/i)).toBeTruthy();
    expect(uploadProjectFile).toHaveBeenCalledTimes(1);
  });

  it('asks before overwriting when the name is taken', async () => {
    vi.mocked(uploadProjectFile)
      .mockRejectedValueOnce(new UploadCollisionError('exists'))
      .mockResolvedValueOnce({ name: 'sales.csv', path: 'dfs.up.sales' });
    const { zone } = setup();
    fireEvent.drop(zone, fileDrag);
    expect(await screen.findByText(/sales\.csv/, { selector: '.ant-modal-confirm-content *, .ant-modal-confirm-content' })).toBeTruthy();
    expect(uploadProjectFile).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByRole('button', { name: /replace/i }));
    await waitFor(() => expect(uploadProjectFile).toHaveBeenCalledTimes(2));
    expect(uploadProjectFile).toHaveBeenLastCalledWith('p1', file, { overwrite: true });
  });
});
