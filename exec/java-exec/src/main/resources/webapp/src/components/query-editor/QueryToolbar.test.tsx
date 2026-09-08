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
import QueryToolbar from './QueryToolbar';

function renderToolbar(props: Partial<React.ComponentProps<typeof QueryToolbar>> = {}) {
  const onExecute = vi.fn();
  render(
    <QueryToolbar
      onExecute={onExecute}
      onCancel={vi.fn()}
      isExecuting={false}
      {...props}
    />
  );
  return { onExecute };
}

describe('QueryToolbar Run button', () => {
  it('is disabled when the editor is empty', () => {
    renderToolbar({ hasSql: false });
    expect(screen.getByRole('button', { name: /run/i })).toBeDisabled();
  });

  it('is enabled once the editor has SQL', () => {
    renderToolbar({ hasSql: true });
    expect(screen.getByRole('button', { name: /run/i })).toBeEnabled();
  });

  it('does not execute when clicked while empty', () => {
    const { onExecute } = renderToolbar({ hasSql: false });
    fireEvent.click(screen.getByRole('button', { name: /run/i }));
    expect(onExecute).not.toHaveBeenCalled();
  });

  it('executes when clicked with SQL present', () => {
    const { onExecute } = renderToolbar({ hasSql: true });
    fireEvent.click(screen.getByRole('button', { name: /run/i }));
    expect(onExecute).toHaveBeenCalledTimes(1);
  });

  /**
   * While a query is running the button becomes Cancel, which must stay clickable
   * regardless of what the editor holds.
   */
  it('shows an enabled Cancel button while executing, even with an empty editor', () => {
    const onCancel = vi.fn();
    render(
      <QueryToolbar onExecute={vi.fn()} onCancel={onCancel} isExecuting hasSql={false} />
    );
    const cancel = screen.getByRole('button', { name: /cancel/i });
    expect(cancel).toBeEnabled();
    fireEvent.click(cancel);
    expect(onCancel).toHaveBeenCalledTimes(1);
  });
});
