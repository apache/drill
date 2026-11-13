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
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import ToolCallDisplay from './ToolCallDisplay';
import type { ChatMessage, ToolCall } from '../../types/ai';

const call: ToolCall = {
  id: 't1',
  name: 'execute_sql',
  arguments: JSON.stringify({ sql: 'SELECT a\nFROM t', title: 'Product Cost Summary' }),
} as ToolCall;

function result(content: string): ChatMessage {
  return { role: 'tool', toolCallId: 't1', content } as unknown as ChatMessage;
}

async function open(results: ChatMessage[]) {
  render(
    <MemoryRouter>
      <ToolCallDisplay toolCalls={[call]} toolResults={results} />
    </MemoryRouter>
  );
  await userEvent.click(screen.getByText('Execute SQL'));
}

describe('ToolCallDisplay', () => {
  it('shows the SQL as code and the other arguments as fields', async () => {
    await open([]);
    // The query reads as it was written, not as an escaped JSON string.
    expect(screen.getByText(/SELECT a\s+FROM t/)).toBeInTheDocument();
    expect(screen.queryByText(/\\n/)).not.toBeInTheDocument();
    expect(screen.getByText('title')).toBeInTheDocument();
    expect(screen.getByText('Product Cost Summary')).toBeInTheDocument();
  });

  it('shows query results as a table', async () => {
    await open([result(JSON.stringify({
      columns: ['total_products', 'min_cost'],
      rowCount: 1,
      rows: [{ total_products: 1124, min_cost: 5.62 }],
    }))]);
    expect(screen.getByRole('columnheader', { name: 'total_products' })).toBeInTheDocument();
    expect(screen.getByRole('cell', { name: '1124' })).toBeInTheDocument();
    expect(screen.getByRole('cell', { name: '5.62' })).toBeInTheDocument();
    // rowCount is a field beside the table, not a column of it.
    expect(screen.getByText('rowCount')).toBeInTheDocument();
  });

  it('falls back to JSON for a result it cannot lay out', async () => {
    await open([result('not json at all')]);
    expect(screen.getByText('not json at all')).toBeInTheDocument();
  });
});
