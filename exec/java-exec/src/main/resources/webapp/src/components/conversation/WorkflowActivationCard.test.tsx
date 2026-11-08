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
import { render, screen, fireEvent, waitFor, within, act } from '@testing-library/react';
import { Modal } from 'antd';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import WorkflowActivationCard from './WorkflowActivationCard';
import { getWorkflow, setWorkflowEnabled } from '../../api/workflows';
import type { Workflow } from '../../types/workflow';

vi.mock('../../api/workflows', () => ({ getWorkflow: vi.fn(), setWorkflowEnabled: vi.fn() }));

const workflow = (over: Partial<Workflow> = {}): Workflow => ({
  id: 'wf-1',
  projectId: 'p1',
  name: 'Nightly error check',
  savedQueryId: 'q1',
  retentionCount: 30,
  status: 'paused',
  schedule: { frequency: 'daily', timeOfDay: '06:30', timeoutSeconds: 300, enabled: false },
  actions: [
    { id: 'notify', type: 'email', condition: { expr: 'rowCount > 0' },
      config: { to: ['oncall@example.com', 'sre@example.com'], subject: 'Errors', body: 'See run' } },
    { id: 'keep', type: 'save_to_table', condition: { always: true }, config: { location: 'dfs.tmp' } },
  ],
  ...over,
});

function renderCard() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <WorkflowActivationCard workflowId="wf-1" name="Nightly error check" />
    </QueryClientProvider>,
  );
}

const activate = async () => fireEvent.click(await screen.findByRole('button', { name: 'Activate' }));
const dialog = () => screen.findByRole('dialog');

describe('WorkflowActivationCard', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(getWorkflow).mockResolvedValue(workflow());
    vi.mocked(setWorkflowEnabled).mockResolvedValue(
      workflow({ status: 'active', schedule: { ...workflow().schedule, enabled: true } }));
  });

  // Modal.confirm mounts outside the tree under test, so it outlives the render.
  // The close animation never finishes under jsdom, so the closed dialog is removed by hand.
  afterEach(() => {
    Modal.destroyAll();
    document.querySelectorAll('.ant-modal-root').forEach((n) => n.remove());
  });

  it('says the workflow is off its schedule until activated and offers Activate', async () => {
    renderCard();
    // Run now has no enabled check, so the owner can still execute a paused workflow.
    expect(await screen.findByText(
      'Created paused, so it will not run on a schedule until you activate it.')).toBeInTheDocument();
    expect(screen.queryByText(/not running/i)).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Activate' })).toBeInTheDocument();
    expect(setWorkflowEnabled).not.toHaveBeenCalled();
  });

  it('asks first, and the question names the workflow, its schedule and what each action does', async () => {
    renderCard();
    await activate();
    const d = await dialog();
    expect(d).toHaveTextContent('Activate "Nightly error check"?');
    expect(d).toHaveTextContent('Daily at 06:30');
    // Who gets emailed is the consequence the user needs to see before saying yes.
    expect(d).toHaveTextContent('Send an email to oncall@example.com, sre@example.com');
    expect(d).toHaveTextContent('Run when: rowCount > 0');
    expect(d).toHaveTextContent('in dfs.tmp');
    expect(setWorkflowEnabled).not.toHaveBeenCalled();
  });

  it('enables the workflow only after the user confirms', async () => {
    renderCard();
    await activate();
    expect(setWorkflowEnabled).not.toHaveBeenCalled();
    fireEvent.click(within(await dialog()).getByRole('button', { name: 'Activate workflow' }));
    await waitFor(() => expect(setWorkflowEnabled).toHaveBeenCalledWith('wf-1', true));
    expect(setWorkflowEnabled).toHaveBeenCalledOnce();
    expect(await screen.findByText(/^Active/)).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Activate' })).not.toBeInTheDocument();
  });

  it('leaves the workflow paused when the user cancels', async () => {
    renderCard();
    await activate();
    fireEvent.click(within(await dialog()).getByRole('button', { name: 'Cancel' }));
    // Give a wrongly wired cancel handler time to fire before asserting it did not.
    await act(async () => { await new Promise((r) => setTimeout(r, 50)); });
    expect(setWorkflowEnabled).not.toHaveBeenCalled();
    expect(screen.getByRole('button', { name: 'Activate' })).toBeInTheDocument();
  });

  it('shows the reason when the server refuses, and stays paused', async () => {
    vi.mocked(setWorkflowEnabled).mockRejectedValue({
      isAxiosError: true,
      response: { status: 409, data: { error: 'This workflow has expired. Renew it first.' } },
    });
    renderCard();
    await activate();
    fireEvent.click(within(await dialog()).getByRole('button', { name: 'Activate workflow' }));
    expect(await screen.findByText(/expired\. Renew it first/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Activate' })).toBeInTheDocument();
  });

  it('offers no Activate for a workflow that is already active', async () => {
    vi.mocked(getWorkflow).mockResolvedValue(
      workflow({ status: 'active', schedule: { ...workflow().schedule, enabled: true } }));
    renderCard();
    expect(await screen.findByText(/^Active/)).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Activate' })).not.toBeInTheDocument();
  });

  it.each(['expired', 'owner_lost_access'] as const)('offers no Activate for a %s workflow', async (status) => {
    vi.mocked(getWorkflow).mockResolvedValue(workflow({ status }));
    renderCard();
    await waitFor(() => expect(getWorkflow).toHaveBeenCalled());
    expect(await screen.findByText(status === 'expired' ? /expired/i : /lost access/i)).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Activate' })).not.toBeInTheDocument();
  });

  // Held or auto-repeating Enter must not be able to switch on an emailing workflow.
  it('puts focus on Cancel, not on the button that activates', async () => {
    renderCard();
    await activate();
    const d = await dialog();
    await waitFor(() => expect(document.activeElement).toBe(within(d).getByRole('button', { name: 'Cancel' })));
  });

  it('shows where an action of an unrecognised type sends, so it is never confirmed blind', async () => {
    vi.mocked(getWorkflow).mockResolvedValue(workflow({
      actions: [
        { id: 'hook', type: 'webhook', condition: { always: true },
          config: { url: 'https://hooks.example.com/alerts', method: 'POST' } },
        { id: 'page', type: 'pager', condition: { always: true }, config: { to: ['a@x.com', 'b@x.com'] } },
        { id: 'misc', type: 'mystery', condition: { always: true }, config: { note: 'x' } },
      ],
    }));
    renderCard();
    await activate();
    const d = await dialog();
    expect(d).toHaveTextContent('webhook');
    expect(d).toHaveTextContent('url: https://hooks.example.com/alerts');
    expect(d).toHaveTextContent('to: a@x.com, b@x.com');
    // Config with no recognisable destination is not dumped wholesale.
    expect(d).not.toHaveTextContent('note');
  });

  it('says it could not load, and offers Retry, for a failure that is not a 404', async () => {
    vi.mocked(getWorkflow).mockRejectedValueOnce({ isAxiosError: true, response: { status: 500 } });
    renderCard();
    expect(await screen.findByText(/could not load/i)).toBeInTheDocument();
    expect(screen.queryByText(/no longer exists/i)).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Activate' })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
    expect(await screen.findByRole('button', { name: 'Activate' })).toBeInTheDocument();
  });

  it('offers no Activate for a workflow that has been deleted', async () => {
    vi.mocked(getWorkflow).mockRejectedValue({ isAxiosError: true, response: { status: 404 } });
    renderCard();
    expect(await screen.findByText(/no longer exists/i)).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Activate' })).not.toBeInTheDocument();
  });
});
