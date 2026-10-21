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
import { describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import RunsTab from './RunsTab';
import HealthTab from './HealthTab';

vi.mock('../../api/workflows', () => ({
  listRuns: vi.fn().mockRejectedValue({ response: { status: 403 } }),
  getWorkflowHealth: vi.fn().mockRejectedValue({ response: { status: 403 } }),
}));

function wrap(ui: React.ReactElement) {
  return <QueryClientProvider client={new QueryClient()}>{ui}</QueryClientProvider>;
}

describe('run history for non-members', () => {
  it('RunsTab shows the members-only message on 403', async () => {
    render(wrap(<RunsTab workflowId="w1" />));
    expect(await screen.findByText('Run history is visible to project members')).toBeTruthy();
  });
  it('HealthTab shows the members-only message on 403', async () => {
    render(wrap(<HealthTab workflowId="w1" />));
    expect(await screen.findByText('Run history is visible to project members')).toBeTruthy();
  });
});
