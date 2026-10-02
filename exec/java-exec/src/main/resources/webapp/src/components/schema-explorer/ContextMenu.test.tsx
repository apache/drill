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
import { render, screen, fireEvent } from '@testing-library/react';
import ContextMenu from './ContextMenu';

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
