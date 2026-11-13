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
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { MemoryRouter, Routes, Route, useLocation } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import AskProspectorButton from './AskProspectorButton';
import { getScratchProjectId } from '../../api/conversations';

vi.mock('../../api/conversations', () => ({
  getScratchProjectId: vi.fn(),
}));

function Where() {
  return <div data-testid="where">{useLocation().pathname}</div>;
}

function renderButton(props: { projectId?: string } = {}) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={['/query']}>
        <Routes>
          <Route path="*" element={<><AskProspectorButton {...props} /><Where /></>} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>
  );
}

describe('AskProspectorButton', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('navigates to the scratch project chat once the lookup resolves', async () => {
    vi.mocked(getScratchProjectId).mockResolvedValue('scratch_alice');
    renderButton();
    const button = screen.getByRole('button', { name: /ask prospector/i });
    await waitFor(() => expect(button).toBeEnabled());
    fireEvent.click(button);
    expect(screen.getByTestId('where')).toHaveTextContent('/projects/scratch_alice/chat');
  });

  it('is disabled while the lookup is in flight, so it cannot navigate to a bad id', () => {
    vi.mocked(getScratchProjectId).mockReturnValue(new Promise(() => {}));
    renderButton();
    const button = screen.getByRole('button', { name: /ask prospector/i });
    expect(button).toBeDisabled();
    fireEvent.click(button);
    expect(screen.getByTestId('where')).toHaveTextContent('/query');
  });

  it('is disabled with a message when the lookup fails, and does not navigate', async () => {
    vi.mocked(getScratchProjectId).mockRejectedValue(new Error('boom'));
    renderButton();
    expect(await screen.findByText(/prospector is unavailable/i)).toBeInTheDocument();
    const button = screen.getByRole('button', { name: /ask prospector/i });
    expect(button).toBeDisabled();
    fireEvent.click(button);
    expect(screen.getByTestId('where')).toHaveTextContent('/query');
  });

  it('goes to the given project instead of Scratch, without looking Scratch up', () => {
    renderButton({ projectId: 'proj_1' });
    const button = screen.getByRole('button', { name: /ask prospector/i });
    expect(button).toBeEnabled();
    fireEvent.click(button);
    expect(screen.getByTestId('where')).toHaveTextContent('/projects/proj_1/chat');
    expect(getScratchProjectId).not.toHaveBeenCalled();
  });
});
