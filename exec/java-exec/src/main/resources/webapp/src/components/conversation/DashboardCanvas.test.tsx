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
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { ReactNode } from 'react';
import DashboardCanvas, { INITIAL_PANELS } from './DashboardCanvas';
import { getVisualization } from '../../api/visualizations';
import type { Dashboard, DashboardPanel, Visualization } from '../../types';

vi.mock('../../api/visualizations', () => ({ getVisualization: vi.fn() }));
vi.mock('../../api/queries', () => ({
  executeQuery: vi.fn().mockResolvedValue({ columns: ['a'], rows: [{ a: 1 }], rowCount: 1 }),
}));
// The thumbnail itself is ChartPreview's mini mode, covered with the chart.
vi.mock('../visualization/ChartPreview', () => ({
  default: ({ mini }: { mini?: boolean }) => <div data-testid="thumb" data-mini={String(!!mini)} />,
}));

function panel(over: Partial<DashboardPanel> = {}): DashboardPanel {
  return {
    id: 'p1', type: 'visualization', visualizationId: 'v1',
    x: 0, y: 0, width: 6, height: 4, ...over,
  } as DashboardPanel;
}

function dashboard(panels: DashboardPanel[]): Dashboard {
  return {
    id: 'd1', name: 'Sales', panels, owner: 'me', createdAt: '', updatedAt: '', isPublic: false,
  } as unknown as Dashboard;
}

function show(d: Dashboard, onOpenChart?: (id: string) => void): ReactNode {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <DashboardCanvas dashboard={d} onOpenChart={onOpenChart} />
    </QueryClientProvider>
  );
}

describe('DashboardCanvas', () => {
  beforeEach(() => {
    vi.mocked(getVisualization).mockImplementation(async (id: string) => ({
      id, name: `Chart ${id}`, chartType: 'bar', config: {}, sql: 'SELECT 1',
    } as unknown as Visualization));
  });

  it('shows each chart as a named thumbnail, not as a full panel', async () => {
    show(dashboard([panel(), panel({ id: 'p2', visualizationId: 'v2' })]));
    expect(await screen.findByText('Chart v1')).toBeInTheDocument();
    expect(await screen.findByText('Chart v2')).toBeInTheDocument();
    await waitFor(() => expect(screen.getAllByTestId('thumb')).toHaveLength(2));
    // Mini mode: a thumbnail with the axes and legend dropped, not a shrunken chart.
    screen.getAllByTestId('thumb').forEach((t) => expect(t).toHaveAttribute('data-mini', 'true'));
  });

  it('opens a chart on the canvas when its tile is clicked', async () => {
    const onOpenChart = vi.fn();
    show(dashboard([panel()]), onOpenChart);
    await userEvent.click(await screen.findByText('Chart v1'));
    expect(onOpenChart).toHaveBeenCalledWith('v1');
  });

  it('leaves the tiles inert when there is nowhere to open a chart', async () => {
    show(dashboard([panel()]));
    await screen.findByText('Chart v1');
    expect(screen.queryByRole('button', { name: /Chart v1/ })).not.toBeInTheDocument();
  });

  it('says what a panel is when it is not a chart', async () => {
    show(dashboard([panel({ id: 'p9', type: 'executiveSummary', visualizationId: undefined })]));
    expect(await screen.findByText('AI summary')).toBeInTheDocument();
  });

  it('holds back the rest until asked, so one dashboard does not fire every query at once', async () => {
    const many = Array.from({ length: INITIAL_PANELS + 3 }, (_, i) =>
      panel({ id: `p${i}`, visualizationId: `v${i}`, y: i }));
    show(dashboard(many));
    await waitFor(() => expect(screen.getAllByTestId('thumb')).toHaveLength(INITIAL_PANELS));

    await userEvent.click(screen.getByRole('button', { name: `Show all ${many.length} panels` }));
    await waitFor(() => expect(screen.getAllByTestId('thumb')).toHaveLength(many.length));
  });
});
