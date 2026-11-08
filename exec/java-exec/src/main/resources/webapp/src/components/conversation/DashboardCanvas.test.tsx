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
import { render, screen } from '@testing-library/react';
import DashboardCanvas from './DashboardCanvas';
import type { Dashboard, DashboardPanel } from '../../types';

// The card is the dashboard page's own and covered there. Here it reports the size it was
// given, which is what the canvas decides.
vi.mock('../dashboard', () => ({
  DashboardPanelCard: ({ panel, chartHeight }: { panel: DashboardPanel; chartHeight?: number }) => (
    <div data-testid="panel" data-id={panel.id} data-chart-height={chartHeight} />
  ),
}));

function panel(over: Partial<DashboardPanel> = {}): DashboardPanel {
  return { id: 'p1', type: 'visualization', x: 0, y: 0, width: 6, height: 4, ...over } as DashboardPanel;
}

function dashboard(panels: DashboardPanel[]): Dashboard {
  return {
    id: 'd1', name: 'Sales', panels, owner: 'me', createdAt: '', updatedAt: '', isPublic: false,
  } as unknown as Dashboard;
}

describe('DashboardCanvas', () => {
  it('sizes each chart to fit inside its slot, so nothing is clipped off the bottom', () => {
    render(<DashboardCanvas dashboard={dashboard([panel()])} />);
    const card = screen.getByTestId('panel');
    const slot = card.parentElement as HTMLElement;
    const slotHeight = Number.parseInt(slot.style.height, 10);
    const chartHeight = Number(card.getAttribute('data-chart-height'));
    expect(chartHeight).toBeGreaterThan(0);
    expect(chartHeight).toBeLessThan(slotHeight);
  });

  it('keeps short panels tall enough for a readable chart', () => {
    render(<DashboardCanvas dashboard={dashboard([panel({ height: 1 })])} />);
    expect(Number(screen.getByTestId('panel').getAttribute('data-chart-height'))).toBeGreaterThanOrEqual(150);
  });
});
