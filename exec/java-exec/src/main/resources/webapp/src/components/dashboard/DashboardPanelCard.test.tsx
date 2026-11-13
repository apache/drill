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
import type { ReactNode } from 'react';
import DashboardPanelCard from './DashboardPanelCard';
import { getVisualization } from '../../api/visualizations';
import type { DashboardPanel, Visualization } from '../../types';

vi.mock('../../api/visualizations', () => ({ getVisualization: vi.fn() }));
vi.mock('../../api/queries', () => ({ executeQuery: vi.fn().mockResolvedValue({ columns: [], rows: [], rowCount: 0 }) }));
// The chart itself is covered in ChartPreview's own tests and needs a canvas.
vi.mock('../visualization/ChartPreview', () => ({ default: () => <div data-testid="chart" /> }));

const viz = {
  id: 'v1',
  name: 'Revenue by Category',
  description: 'Share of revenue per category; one category carries most of it.',
  chartType: 'pie',
  config: { xAxis: 'category', metrics: ['revenue'] },
  sql: 'SELECT 1',
} as unknown as Visualization;

const panel = { id: 'p1', type: 'visualization', visualizationId: 'v1', x: 0, y: 0, width: 6, height: 4 } as DashboardPanel;

function wrap(children: ReactNode) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}

describe('DashboardPanelCard', () => {
  beforeEach(() => {
    vi.mocked(getVisualization).mockResolvedValue(viz);
  });

  it('shows the chart description under its title', async () => {
    render(wrap(<DashboardPanelCard panel={panel} editMode={false} />));
    expect(await screen.findByText(viz.description!)).toBeInTheDocument();
  });

  it('offers a way to configure the chart while editing, without leaving the dashboard', async () => {
    render(wrap(<DashboardPanelCard panel={panel} editMode />));
    const link = await screen.findByLabelText('Edit chart settings');
    expect(link).toHaveAttribute('href', '/visualizations/v1');
    // The dashboard's unsaved layout has to survive the trip.
    expect(link).toHaveAttribute('target', '_blank');
  });

  it('does not offer it when the dashboard is not being edited', async () => {
    render(wrap(<DashboardPanelCard panel={panel} editMode={false} />));
    await screen.findByText(viz.name);
    expect(screen.queryByLabelText('Edit chart settings')).not.toBeInTheDocument();
  });
});
