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
import { render, screen, fireEvent } from '@testing-library/react';
import BulkActionBar from './BulkActionBar';

describe('BulkActionBar', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders nothing when selectedCount is 0', () => {
    const { container } = render(
      <BulkActionBar selectedCount={0} onClear={vi.fn()} />
    );
    expect(container.innerHTML).toBe('');
  });

  it('shows selected count', () => {
    render(
      <BulkActionBar selectedCount={5} onClear={vi.fn()} />
    );
    expect(screen.getByText('5 selected')).toBeInTheDocument();
  });

  it('shows Add to Project button when onAddToProject provided', () => {
    render(
      <BulkActionBar
        selectedCount={3}
        onAddToProject={vi.fn()}
        onClear={vi.fn()}
      />
    );
    expect(screen.getByText('Add to Project')).toBeInTheDocument();
  });

  it('hides Add to Project button when onAddToProject not provided', () => {
    render(
      <BulkActionBar selectedCount={3} onClear={vi.fn()} />
    );
    expect(screen.queryByText('Add to Project')).not.toBeInTheDocument();
  });

  it('shows Delete button when onDelete provided', () => {
    render(
      <BulkActionBar
        selectedCount={2}
        onDelete={vi.fn()}
        onClear={vi.fn()}
      />
    );
    expect(screen.getByText('Delete')).toBeInTheDocument();
  });

  it('calls onClear when Clear is clicked', () => {
    const onClear = vi.fn();
    render(
      <BulkActionBar selectedCount={1} onClear={onClear} />
    );
    fireEvent.click(screen.getByText('Clear'));
    expect(onClear).toHaveBeenCalledTimes(1);
  });
});
