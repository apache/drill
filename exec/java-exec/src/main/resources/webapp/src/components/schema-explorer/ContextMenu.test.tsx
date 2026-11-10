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
import { Modal } from 'antd';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import ContextMenu from './ContextMenu';
import { deleteProjectFile } from '../../api/uploads';

vi.mock('../../api/uploads', () => ({
  deleteProjectFile: vi.fn(),
  downloadProjectFileUrl: (p: string, n: string) => `/dl/${p}/${n}`,
}));

function openMenu(props: Partial<React.ComponentProps<typeof ContextMenu>>) {
  render(
    <ContextMenu nodeType="file" nodeKey="file:dfs.tmp:caps/traffic.pcapng"
      qualifiedName="dfs.`tmp`.`caps/traffic.pcapng`" {...props}>
      <span>node</span>
    </ContextMenu>,
  );
  fireEvent.contextMenu(screen.getByText('node'));
}

describe('ContextMenu sessionize item', () => {
  it('opens a sessionized query in a new tab for a packet capture', () => {
    const onOpenInNewTab = vi.fn();
    openMenu({ dataFormat: 'pcapng', onOpenInNewTab });
    fireEvent.click(screen.getByText('Sessionize TCP Streams'));
    expect(onOpenInNewTab).toHaveBeenCalledWith(
      "SELECT *\nFROM table(dfs.`tmp`.`caps/traffic.pcapng` (type => 'pcap', sessionizeTCPStreams => true))\nLIMIT 100",
      'traffic.pcapng sessions',
    );
  });

  it('is offered for a folder whose files are all captures', () => {
    openMenu({ nodeKey: 'dir:dfs:captures', dataFormat: 'pcap', onOpenInNewTab: vi.fn() });
    expect(screen.getByText('Sessionize TCP Streams')).toBeTruthy();
  });

  it('is not offered for other formats', () => {
    openMenu({ nodeKey: 'file:dfs.tmp:data.csv', dataFormat: 'csv', onOpenInNewTab: vi.fn() });
    expect(screen.getByText('Generate SELECT *')).toBeTruthy();
    expect(screen.queryByText('Sessionize TCP Streams')).toBeNull();
  });
});

describe('ContextMenu uploaded-file items', () => {
  const upload = {
    projectId: 'proj-a',
    uploadSchema: 'dfs.uploads',
    nodeKey: 'file:dfs.uploads:proj-a/data.csv',
  };

  beforeEach(() => {
    vi.mocked(deleteProjectFile).mockReset();
    vi.mocked(deleteProjectFile).mockResolvedValue(undefined);
  });

  afterEach(() => Modal.destroyAll());

  it('offers Delete and Download on a file in the uploads folder', () => {
    openMenu(upload);
    expect(screen.getByText('Delete')).toBeTruthy();
    expect(screen.getByText('Download')).toBeTruthy();
  });

  it('offers neither on a file outside it', () => {
    for (const nodeKey of [
      'file:dfs.uploads:proj-a-backup/data.csv', // sibling folder sharing a prefix
      'file:dfs.uploads:proj-b/data.csv',
      'file:dfs.uploads:proj-a', // the folder name itself, not a file inside
      'file:dfs.tmp:proj-a/data.csv', // other schema
    ]) {
      const { unmount } = render(
        <ContextMenu nodeType="file" nodeKey={nodeKey} qualifiedName="q"
          projectId={upload.projectId} uploadSchema={upload.uploadSchema}>
          <span>n</span>
        </ContextMenu>,
      );
      fireEvent.contextMenu(screen.getByText('n'));
      expect(screen.queryByText('Delete'), nodeKey).toBeNull();
      expect(screen.queryByText('Download'), nodeKey).toBeNull();
      unmount();
    }
  });

  it('confirms before deleting, naming the file', async () => {
    openMenu(upload);
    fireEvent.click(screen.getByText('Delete'));
    expect(await screen.findByText(/data\.csv/)).toBeTruthy();
    expect(deleteProjectFile).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: /^Delete$/ }));
    await waitFor(() => expect(deleteProjectFile).toHaveBeenCalledWith('proj-a', 'data.csv'));
  });

  it('refreshes the tree after a delete', async () => {
    const onRefreshNode = vi.fn();
    const onFileDeleted = vi.fn();
    openMenu({ ...upload, onRefreshNode, onFileDeleted });
    fireEvent.click(screen.getByRole('menuitem', { name: /Delete/ }));
    fireEvent.click(await screen.findByRole('button', { name: /^Delete$/ }));
    await waitFor(() => expect(onRefreshNode).toHaveBeenCalledWith('schema:dfs.uploads'));
    expect(onFileDeleted).toHaveBeenCalled();
  });
});
