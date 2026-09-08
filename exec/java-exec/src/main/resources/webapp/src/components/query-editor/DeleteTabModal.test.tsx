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
import DeleteTabModal from './DeleteTabModal';

describe('DeleteTabModal', () => {
  it('names the tab being deleted', () => {
    render(<DeleteTabModal open tab={{ id: 'a', name: 'Sales' }}
      vizNames={{}} publishedApis={[]} onConfirm={vi.fn()} onCancel={vi.fn()} />);
    expect(screen.getByText(/Sales/)).toBeInTheDocument();
  });

  it('names the visualizations that will break', () => {
    render(<DeleteTabModal open tab={{ id: 'a', name: 'Sales', vizIds: ['v1', 'v2'] }}
      vizNames={{ v1: 'Revenue chart', v2: 'Trend' }}
      publishedApis={[]} onConfirm={vi.fn()} onCancel={vi.fn()} />);
    expect(screen.getByText(/Revenue chart/)).toBeInTheDocument();
    expect(screen.getByText(/Trend/)).toBeInTheDocument();
  });

  it('warns that the Prospector conversation goes too', () => {
    render(<DeleteTabModal open tab={{ id: 'a', name: 'Sales', conversationLength: 12 }}
      vizNames={{}} publishedApis={[]} onConfirm={vi.fn()} onCancel={vi.fn()} />);
    expect(screen.getByText(/conversation/i)).toBeInTheDocument();
  });

  it('says nothing about a conversation when there is none', () => {
    render(<DeleteTabModal open tab={{ id: 'a', name: 'Sales', conversationLength: 0 }}
      vizNames={{}} publishedApis={[]} onConfirm={vi.fn()} onCancel={vi.fn()} />);
    expect(screen.queryByText(/conversation/i)).not.toBeInTheDocument();
  });

  /**
   * A published API holds its own copy of the SQL and keeps serving after the tab is
   * gone, so this line must not read as a breakage warning. The user can be wrong in
   * either direction: thinking they broke production, or thinking they revoked an
   * endpoint they meant to take down.
   */
  it('says a published API stays live rather than breaking', () => {
    render(<DeleteTabModal open tab={{ id: 'a', name: 'Sales' }} vizNames={{}}
      publishedApis={[{ id: 'api1', name: 'Sales feed' }]}
      onConfirm={vi.fn()} onCancel={vi.fn()} />);
    expect(screen.getByText(/Sales feed/)).toBeInTheDocument();
    expect(screen.getByText(/keeps serving|stays live|remain live/i)).toBeInTheDocument();
  });

  it('does not describe a published API as breaking', () => {
    render(<DeleteTabModal open tab={{ id: 'a', name: 'Sales' }} vizNames={{}}
      publishedApis={[{ id: 'api1', name: 'Sales feed' }]}
      onConfirm={vi.fn()} onCancel={vi.fn()} />);
    const apiLine = screen.getByText(/Sales feed/).closest('li');
    expect(apiLine?.textContent).not.toMatch(/break/i);
  });

  it('still warns that visualizations break alongside a live API', () => {
    render(<DeleteTabModal open tab={{ id: 'a', name: 'Sales', vizIds: ['v1'] }}
      vizNames={{ v1: 'Revenue chart' }}
      publishedApis={[{ id: 'api1', name: 'Sales feed' }]}
      onConfirm={vi.fn()} onCancel={vi.fn()} />);
    expect(screen.getByText(/Revenue chart/)).toBeInTheDocument();
    expect(screen.getByText(/Sales feed/)).toBeInTheDocument();
  });

  it('still allows deletion despite dependants', () => {
    const onConfirm = vi.fn();
    render(<DeleteTabModal open tab={{ id: 'a', name: 'Sales', vizIds: ['v1'] }}
      vizNames={{ v1: 'Revenue chart' }} publishedApis={[]}
      onConfirm={onConfirm} onCancel={vi.fn()} />);
    fireEvent.click(screen.getByRole('button', { name: /delete/i }));
    expect(onConfirm).toHaveBeenCalled();
  });

  it('says nothing about dependants when there are none', () => {
    render(<DeleteTabModal open tab={{ id: 'a', name: 'Scratch' }}
      vizNames={{}} publishedApis={[]} onConfirm={vi.fn()} onCancel={vi.fn()} />);
    expect(screen.queryByText(/will break/i)).not.toBeInTheDocument();
    expect(screen.queryByRole('list')).not.toBeInTheDocument();
  });

  it('cancels without deleting', () => {
    const onConfirm = vi.fn();
    const onCancel = vi.fn();
    render(<DeleteTabModal open tab={{ id: 'a', name: 'Sales' }}
      vizNames={{}} publishedApis={[]} onConfirm={onConfirm} onCancel={onCancel} />);
    fireEvent.click(screen.getByRole('button', { name: /cancel/i }));
    expect(onCancel).toHaveBeenCalled();
    expect(onConfirm).not.toHaveBeenCalled();
  });

  /** A visualization whose name has not loaded should still be counted, not dropped. */
  it('falls back to the id when a visualization name is unknown', () => {
    render(<DeleteTabModal open tab={{ id: 'a', name: 'Sales', vizIds: ['v9'] }}
      vizNames={{}} publishedApis={[]} onConfirm={vi.fn()} onCancel={vi.fn()} />);
    expect(screen.getByText(/v9/)).toBeInTheDocument();
  });
});
