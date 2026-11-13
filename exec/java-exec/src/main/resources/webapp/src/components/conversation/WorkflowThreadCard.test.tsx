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
import { render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router-dom';
import ChatMessageBubble from '../prospector/ChatMessageBubble';
import { getWorkflow, setWorkflowEnabled } from '../../api/workflows';
import type { ChatMessage } from '../../types/ai';

vi.mock('../../api/workflows', () => ({ getWorkflow: vi.fn(), setWorkflowEnabled: vi.fn() }));

const assistant: ChatMessage = {
  role: 'assistant',
  content: 'Set up.',
  toolCalls: [{ id: 'w', name: 'create_workflow', arguments: '{}' }],
};
const toolResult = (body: unknown): ChatMessage => (
  { role: 'tool', content: JSON.stringify(body), toolCallId: 'w' }
);

function renderBubble(result: ChatMessage) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter>
        <ChatMessageBubble message={assistant} toolResults={[result]} onOpenArtifact={vi.fn()} />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe('a created workflow in the thread', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(getWorkflow).mockResolvedValue({
      id: 'wf-1', projectId: 'p1', name: 'Nightly check', savedQueryId: 'q1', retentionCount: 30,
      status: 'paused', actions: [],
      schedule: { frequency: 'daily', timeOfDay: '08:00', timeoutSeconds: 300, enabled: false },
    });
  });

  it('shows one artifact card with the Activate control attached, and activates nothing', async () => {
    renderBubble(toolResult({ id: 'wf-1', name: 'Nightly check', status: 'paused' }));
    expect(screen.getByRole('button', { name: /^Workflow\s*Nightly check/ })).toBeInTheDocument();
    expect(await screen.findByRole('button', { name: 'Activate' })).toBeInTheDocument();
    expect(setWorkflowEnabled).not.toHaveBeenCalled();
  });

  it('shows neither when the workflow was not created', () => {
    renderBubble(toolResult({ error: 'Workflow not created. name: Name is required' }));
    expect(screen.queryByRole('button', { name: /^Workflow\s*\S/ })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Activate' })).not.toBeInTheDocument();
    expect(getWorkflow).not.toHaveBeenCalled();
  });
});
