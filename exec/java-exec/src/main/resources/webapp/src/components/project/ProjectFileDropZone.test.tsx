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
import ChatInput from '../prospector/ChatInput';
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

/**
 * The 201 body the server actually sends, for project `p1` with the cluster target at
 * `dfs.uploads`: `path` is the bare file name, `schema` the project's folder with the id
 * unquoted, and `tablePath` the only field that is valid Drill SQL.
 */
const uploaded = {
  name: 'sales.csv',
  path: 'sales.csv',
  schema: 'dfs.uploads.p1',
  tablePath: 'dfs.`uploads`.`p1/sales.csv`',
};

/** An axios rejection: the message is boilerplate and the reason is in the body. */
function axiosFailure(status: number, serverMessage: string) {
  return Object.assign(new Error(`Request failed with status code ${status}`), {
    isAxiosError: true,
    response: { status, data: { message: serverMessage } },
  });
}

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
    vi.mocked(uploadProjectFile).mockResolvedValue(uploaded);
    const { zone, invalidate } = setup();
    fireEvent.dragEnter(zone, fileDrag);
    fireEvent.drop(zone, fileDrag);
    // The quoted, queryable path, not the bare file name the old toast showed.
    expect(await screen.findByText('Added sales.csv as dfs.`uploads`.`p1/sales.csv`'))
      .toBeTruthy();
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
      .mockResolvedValueOnce(uploaded);
    const { zone } = setup();
    fireEvent.drop(zone, fileDrag);
    expect(await screen.findByText(/sales\.csv/, { selector: '.ant-modal-confirm-content *, .ant-modal-confirm-content' })).toBeTruthy();
    expect(uploadProjectFile).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByRole('button', { name: /replace/i }));
    await waitFor(() => expect(uploadProjectFile).toHaveBeenCalledTimes(2));
    expect(uploadProjectFile).toHaveBeenLastCalledWith('p1', file, { overwrite: true });
  });

  it('shows the server reason when the upload fails for another reason', async () => {
    vi.mocked(uploadProjectFile).mockRejectedValue(new Error('File too large'));
    const { zone } = setup();
    fireEvent.drop(zone, fileDrag);
    expect(await screen.findByText(/Could not upload sales\.csv: File too large/)).toBeTruthy();
    expect(uploadProjectFile).toHaveBeenCalledTimes(1);
  });

  it('shows the server reason, not the axios boilerplate, when a write is refused', async () => {
    vi.mocked(uploadProjectFile).mockRejectedValue(
      axiosFailure(403, 'You do not have write access to this project'),
    );
    const { zone } = setup();
    fireEvent.drop(zone, fileDrag);
    expect(await screen.findByText(
      'Could not upload sales.csv: You do not have write access to this project',
    )).toBeTruthy();
  });

  it('shows the size the server refused the file against', async () => {
    vi.mocked(uploadProjectFile).mockRejectedValue(
      axiosFailure(413, 'File exceeds the maximum upload size of 10 bytes'),
    );
    const { zone } = setup();
    fireEvent.drop(zone, fileDrag);
    expect(await screen.findByText(/maximum upload size of 10 bytes/)).toBeTruthy();
  });

  it('uploads once and clears the overlay when a file is dropped on a composer inside the zone', async () => {
    vi.mocked(uploadProjectFile).mockResolvedValue(uploaded);
    const onFileDropped = vi.fn();
    render(
      <QueryClientProvider client={new QueryClient()}>
        <ProjectFileDropZone projectId="p1">
          <ChatInput onSend={vi.fn()} onStop={vi.fn()} isStreaming={false} onFileDropped={onFileDropped} />
        </ProjectFileDropZone>
      </QueryClientProvider>,
    );
    const composer = screen.getByRole('textbox').closest('.prospector-chat-input') as HTMLElement;

    fireEvent.dragEnter(composer, fileDrag);
    expect(screen.getByText('Drop to add to Project One')).toBeTruthy();
    fireEvent.drop(composer, fileDrag);

    expect(screen.queryByText('Drop to add to Project One')).toBeNull();
    expect(onFileDropped).toHaveBeenCalledTimes(1);
    await new Promise((r) => setTimeout(r, 0));
    expect(uploadProjectFile).not.toHaveBeenCalled();

    // A later drag must start from a clean count, so one enter shows and one leave hides.
    fireEvent.dragEnter(composer, fileDrag);
    expect(screen.getByText('Drop to add to Project One')).toBeTruthy();
    fireEvent.dragLeave(composer, fileDrag);
    expect(screen.queryByText('Drop to add to Project One')).toBeNull();
  });
});
