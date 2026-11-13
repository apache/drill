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
import ProjectTabsSection from './ProjectTabsSection';
import type { TabTreeItem } from './ProjectTabsSection';

const tabs: TabTreeItem[] = [
  { id: 'a', name: 'Open one', hidden: false },
  { id: 'b', name: 'Hidden one', hidden: true },
];

describe('ProjectTabsSection', () => {
  /**
   * The tree is the only route back to a closed tab, so it must list hidden ones
   * alongside open ones.
   */
  it('lists both open and hidden tabs', () => {
    render(<ProjectTabsSection tabs={tabs} onOpen={vi.fn()} onDelete={vi.fn()} />);
    expect(screen.getByText('Open one')).toBeInTheDocument();
    expect(screen.getByText('Hidden one')).toBeInTheDocument();
  });

  it('opens a hidden tab when clicked', () => {
    const onOpen = vi.fn();
    render(<ProjectTabsSection tabs={tabs} onOpen={onOpen} onDelete={vi.fn()} />);
    fireEvent.click(screen.getByText('Hidden one'));
    expect(onOpen).toHaveBeenCalledWith('b');
  });

  it('activates an already-open tab when clicked', () => {
    const onOpen = vi.fn();
    render(<ProjectTabsSection tabs={tabs} onOpen={onOpen} onDelete={vi.fn()} />);
    fireEvent.click(screen.getByText('Open one'));
    expect(onOpen).toHaveBeenCalledWith('a');
  });

  it('marks hidden tabs so they read as closed', () => {
    render(<ProjectTabsSection tabs={tabs} onOpen={vi.fn()} onDelete={vi.fn()} />);
    const hidden = screen.getByText('Hidden one').closest('button');
    expect(hidden?.className).toMatch(/hidden/);
  });

  it('offers Delete on right-click', () => {
    render(<ProjectTabsSection tabs={tabs} onOpen={vi.fn()} onDelete={vi.fn()} />);
    fireEvent.contextMenu(screen.getByText('Open one'));
    expect(screen.getByText(/delete/i)).toBeInTheDocument();
  });

  it('calls onDelete with the tab id', () => {
    const onDelete = vi.fn();
    render(<ProjectTabsSection tabs={tabs} onOpen={vi.fn()} onDelete={onDelete} />);
    fireEvent.contextMenu(screen.getByText('Open one'));
    fireEvent.click(screen.getByText(/delete/i));
    expect(onDelete).toHaveBeenCalledWith('a');
  });

  /** The last remaining tab cannot be deleted, so the menu says so instead. */
  it('does not offer Delete when only one tab remains', () => {
    render(<ProjectTabsSection
      tabs={[{ id: 'a', name: 'Only one', hidden: false }]}
      onOpen={vi.fn()} onDelete={vi.fn()} />);
    fireEvent.contextMenu(screen.getByText('Only one'));
    expect(screen.queryByText(/^delete$/i)).not.toBeInTheDocument();
    expect(screen.getByText(/only tab/i)).toBeInTheDocument();
  });

  it('offers Delete again once a second tab exists', () => {
    render(<ProjectTabsSection tabs={tabs} onOpen={vi.fn()} onDelete={vi.fn()} />);
    fireEvent.contextMenu(screen.getByText('Open one'));
    expect(screen.getByText(/^delete$/i)).toBeInTheDocument();
  });

  /** Locked tabs cannot be deleted, so the menu must not offer it. */
  it('does not offer Delete for a locked tab', () => {
    render(<ProjectTabsSection
      tabs={[{ id: 'c', name: 'Locked one', hidden: false, locked: true }]}
      onOpen={vi.fn()} onDelete={vi.fn()} />);
    fireEvent.contextMenu(screen.getByText('Locked one'));
    expect(screen.queryByText(/^delete$/i)).not.toBeInTheDocument();
  });

  it('explains why a locked tab cannot be deleted', () => {
    render(<ProjectTabsSection
      tabs={[{ id: 'c', name: 'Locked one', hidden: false, locked: true }]}
      onOpen={vi.fn()} onDelete={vi.fn()} />);
    fireEvent.contextMenu(screen.getByText('Locked one'));
    expect(screen.getByText(/unlock to delete/i)).toBeInTheDocument();
  });

  it('shows how many tabs the project has', () => {
    render(<ProjectTabsSection tabs={tabs} onOpen={vi.fn()} onDelete={vi.fn()} />);
    expect(screen.getByText('2')).toBeInTheDocument();
  });

  /** The group stays visible at zero, so the tree does not appear to lose a section. */
  it('renders the group with a zero count when there are no tabs', () => {
    render(<ProjectTabsSection tabs={[]} onOpen={vi.fn()} onDelete={vi.fn()} />);
    expect(screen.getByText('Tabs')).toBeInTheDocument();
    expect(screen.getByText('0')).toBeInTheDocument();
    expect(screen.queryByText(/no tabs/i)).not.toBeInTheDocument();
  });

  it('collapses and expands the list', () => {
    render(<ProjectTabsSection tabs={tabs} onOpen={vi.fn()} onDelete={vi.fn()} />);
    expect(screen.getByText('Open one')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: /tabs/i }));
    expect(screen.queryByText('Open one')).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: /tabs/i }));
    expect(screen.getByText('Open one')).toBeInTheDocument();
  });

  it('marks the group expanded state for assistive tech', () => {
    render(<ProjectTabsSection tabs={tabs} onOpen={vi.fn()} onDelete={vi.fn()} />);
    const header = screen.getByRole('button', { name: /tabs/i });
    expect(header).toHaveAttribute('aria-expanded', 'true');
    fireEvent.click(header);
    expect(header).toHaveAttribute('aria-expanded', 'false');
  });
});
