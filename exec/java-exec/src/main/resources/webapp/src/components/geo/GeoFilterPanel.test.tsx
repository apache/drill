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

// The map is mocked: this panel's job is gating, column resolution and producing the
// right SQL, none of which needs OpenLayers. The mock exposes a button that fires a
// drawn polygon so the Apply path can be exercised.
const drawnGeometry = {
  kind: 'polygon' as const,
  ring: [
    [-71.19, 42.28], [-70.99, 42.28], [-70.99, 42.40], [-71.19, 42.40], [-71.19, 42.28],
  ] as [number, number][],
};

vi.mock('./GeoMapCanvas', () => ({
  default: ({ onGeometryChange }: { onGeometryChange: (g: unknown) => void }) => (
    <div data-testid="map">
      <button type="button" onClick={() => onGeometryChange(drawnGeometry)}>
        simulate draw
      </button>
    </div>
  ),
}));

vi.mock('../../api/mapConfig', () => ({
  getMapConfig: vi.fn(() => Promise.resolve({ tileUrl: '', attribution: '' })),
}));

import GeoFilterPanel from './GeoFilterPanel';

const GEO_RESULTS = {
  columns: ['id', 'lat', 'lon'],
  metadata: ['VARCHAR', 'DOUBLE', 'DOUBLE'],
};

describe('GeoFilterPanel', () => {
  beforeEach(() => vi.clearAllMocks());

  /** The builder filters a query, so there has to be one that has run. */
  it('explains itself when no query has run yet', () => {
    render(<GeoFilterPanel results={null} sql="" onApplyFilter={vi.fn()} />);
    expect(screen.getByText(/run a query/i)).toBeInTheDocument();
    expect(screen.queryByTestId('map')).not.toBeInTheDocument();
  });

  it('renders the map when coordinates are detected', async () => {
    render(<GeoFilterPanel results={GEO_RESULTS} sql="SELECT * FROM t"
      onApplyFilter={vi.fn()} />);
    expect(await screen.findByTestId('map')).toBeInTheDocument();
  });

  /** An empty map with no explanation is the worst version of this. */
  it('explains itself when no coordinate columns are found', () => {
    render(<GeoFilterPanel
      results={{ columns: ['id', 'total'], metadata: ['VARCHAR', 'DOUBLE'] }}
      sql="SELECT * FROM t" onApplyFilter={vi.fn()} />);
    expect(screen.getByText(/no coordinate columns found/i)).toBeInTheDocument();
  });

  it('lets the user pick columns when detection found nothing', () => {
    render(<GeoFilterPanel
      results={{ columns: ['id', 'a', 'b'], metadata: ['VARCHAR', 'DOUBLE', 'DOUBLE'] }}
      sql="SELECT * FROM t" onApplyFilter={vi.fn()} />);
    expect(screen.getByLabelText(/latitude column/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/longitude column/i)).toBeInTheDocument();
  });

  /** Apply before drawing would produce invalid SQL. */
  it('disables Apply until geometry exists', async () => {
    render(<GeoFilterPanel results={GEO_RESULTS} sql="SELECT * FROM t"
      onApplyFilter={vi.fn()} />);
    await screen.findByTestId('map');
    expect(screen.getByRole('button', { name: /apply filter/i })).toBeDisabled();
  });

  it('builds and applies the filter from the drawn geometry', async () => {
    const onApplyFilter = vi.fn();
    render(<GeoFilterPanel results={GEO_RESULTS} sql="SELECT * FROM t"
      onApplyFilter={onApplyFilter} />);
    await screen.findByTestId('map');

    fireEvent.click(screen.getByText('simulate draw'));
    await waitFor(() =>
      expect(screen.getByRole('button', { name: /apply filter/i })).toBeEnabled());
    fireEvent.click(screen.getByRole('button', { name: /apply filter/i }));

    expect(onApplyFilter).toHaveBeenCalledTimes(1);
    const sql = onApplyFilter.mock.calls[0][0] as string;
    expect(sql).toContain('ST_Within');
    expect(sql).toContain('ST_Point(t.lon, t.lat)');
    expect(sql).toContain('FROM (SELECT * FROM t) t');
  });

  /** Distance only means something for geometry that has no interior. */
  it('shows a distance control for path and point only', async () => {
    render(<GeoFilterPanel results={GEO_RESULTS} sql="SELECT * FROM t"
      onApplyFilter={vi.fn()} />);
    await screen.findByTestId('map');

    fireEvent.click(screen.getByRole('button', { name: /polygon/i }));
    expect(screen.queryByLabelText(/distance/i)).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: /path/i }));
    expect(screen.getByLabelText(/distance/i)).toBeInTheDocument();
  });

  it('offers a preview of the generated SQL', async () => {
    render(<GeoFilterPanel results={GEO_RESULTS} sql="SELECT * FROM t"
      onApplyFilter={vi.fn()} />);
    await screen.findByTestId('map');
    fireEvent.click(screen.getByText('simulate draw'));

    await waitFor(() => expect(screen.getByText(/ST_Within/)).toBeInTheDocument());
  });

  it('clears the drawn geometry', async () => {
    render(<GeoFilterPanel results={GEO_RESULTS} sql="SELECT * FROM t"
      onApplyFilter={vi.fn()} />);
    await screen.findByTestId('map');
    fireEvent.click(screen.getByText('simulate draw'));
    await waitFor(() =>
      expect(screen.getByRole('button', { name: /apply filter/i })).toBeEnabled());

    fireEvent.click(screen.getByRole('button', { name: /clear/i }));
    expect(screen.getByRole('button', { name: /apply filter/i })).toBeDisabled();
  });
});
