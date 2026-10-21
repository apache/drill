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
import { useState } from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';

const parse = vi.hoisted(() => vi.fn());
vi.mock('../../api/workflows', () => ({ parseCondition: parse }));

import ConditionEditor from './ConditionEditor';
import type { Condition } from '../../types/workflow';

describe('ConditionEditor', () => {
  it('shows a positioned parse error in expression mode', async () => {
    parse.mockResolvedValue({ error: 'Expected a value at position 10', position: 10 });
    const onChange = vi.fn();
    render(<ConditionEditor value={{ expr: 'rowCount >' }} onChange={onChange} refs={['rowCount']} />);
    await waitFor(() => expect(screen.getByText(/position 10/)).toBeTruthy());
  });

  it('builder edits emit a tree', () => {
    const onChange = vi.fn();
    render(<ConditionEditor value={{ tree: { all: [{ field: 'rowCount', op: '>', value: 5 }] } }}
      onChange={onChange} refs={['rowCount']} />);
    fireEvent.change(screen.getByDisplayValue('5'), { target: { value: '9' } });
    expect(onChange).toHaveBeenCalledWith({ tree: { all: [{ field: 'rowCount', op: '>', value: 9 }] } });
  });

  it('keeps typed decimals intact in the builder', () => {
    const onChange = vi.fn();
    function Harness() {
      const [v, setV] = useState<Condition>({ tree: { all: [{ field: 'rowCount', op: '>', value: 5 }] } });
      return <ConditionEditor value={v} onChange={(c) => { onChange(c); setV(c); }} refs={['rowCount']} />;
    }
    render(<Harness />);
    const input = screen.getByLabelText('Condition value') as HTMLInputElement;
    ['0', '0.', '0.5'].forEach((t) => fireEvent.change(input, { target: { value: t } }));
    expect(input.value).toBe('0.5');
    expect(onChange).toHaveBeenLastCalledWith({ tree: { all: [{ field: 'rowCount', op: '>', value: 0.5 }] } });
  });
});
