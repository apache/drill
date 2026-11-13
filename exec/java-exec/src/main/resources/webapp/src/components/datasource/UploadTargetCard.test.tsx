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
import UploadTargetCard from './UploadTargetCard';
import * as storage from '../../api/storage';
import * as uploads from '../../api/uploads';

vi.mock('../../api/storage');
vi.mock('../../api/uploads');

const plugins = [
  {
    name: 'dfs',
    config: {
      type: 'file',
      enabled: true,
      workspaces: {
        tmp: { location: '/tmp', writable: true },
        root: { location: '/', writable: false },
      },
    },
  },
  {
    name: 'mongo',
    config: { type: 'mongo', enabled: true, workspaces: { nope: { location: '/x', writable: true } } },
  },
];

function renderCard() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <UploadTargetCard />
    </QueryClientProvider>
  );
}

describe('UploadTargetCard', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    vi.mocked(storage.getPlugins).mockResolvedValue(plugins);
    vi.mocked(uploads.getUploadStatus).mockResolvedValue({ configured: false, maxSizeBytes: 100 });
    vi.mocked(uploads.getUploadConfig).mockResolvedValue({});
  });

  it('shows Not configured with a prompt to pick a workspace', async () => {
    renderCard();
    expect(await screen.findByText('Not configured')).toBeInTheDocument();
    expect(screen.getByText(/pick a workspace/i)).toBeInTheDocument();
  });

  async function openOptions() {
    const combo = await screen.findByRole('combobox');
    // The list fills in once the plugin query resolves.
    fireEvent.mouseDown(combo);
    await waitFor(() => {
      expect(document.querySelector('.ant-select-item-option')).not.toBeNull();
    });
    return combo;
  }

  it('offers only writable workspaces of filesystem plugins', async () => {
    renderCard();
    await openOptions();
    expect(screen.getAllByText('dfs.tmp').length).toBeGreaterThan(0);
    expect(screen.queryByText('dfs.root')).toBeNull();
    expect(screen.queryByText('mongo.nope')).toBeNull();
  });

  it('shows the warning returned after saving', async () => {
    vi.mocked(uploads.setUploadConfig).mockResolvedValue({
      config: { plugin: 'dfs', workspace: 'tmp' },
      warning: 'Local file: target on a multi-node cluster',
    });
    renderCard();
    await openOptions();
    fireEvent.click(document.querySelector('.ant-select-item-option') as Element);
    fireEvent.click(screen.getByRole('button', { name: /save/i }));
    expect(await screen.findByText(/multi-node cluster/)).toBeInTheDocument();
    expect(uploads.setUploadConfig).toHaveBeenCalledWith(
      expect.objectContaining({ plugin: 'dfs', workspace: 'tmp' })
    );
  });
});
