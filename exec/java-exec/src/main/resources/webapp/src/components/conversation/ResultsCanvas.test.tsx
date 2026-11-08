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
import { render, screen, fireEvent } from '@testing-library/react';
import ResultsCanvas from './ResultsCanvas';
import ResultsGrid from '../results/ResultsGrid';

/**
 * The one place the real grid is mounted outside the SQL Lab page: proof that it needs
 * nothing from that page (no router, no query execution state) to show a result.
 */
const RESULT = {
  columns: ['region'],
  metadata: ['VARCHAR'],
  rows: [{ region: 'West' }],
  queryId: 'q1',
};

// jsdom has no breakpoint, so the grid renders its compact, icon-only toolbar.
const openAnalyze = async () => {
  fireEvent.click(await screen.findByLabelText('fund-projection-screen'));
  return screen.findByText('Profile Data');
};

describe('ResultsCanvas', () => {
  it('mounts the real results grid on a bare result', async () => {
    render(<ResultsCanvas tab="results" sql="SELECT region FROM sales" result={RESULT} />);
    expect(await screen.findByText('region')).toBeInTheDocument();
    expect(await screen.findByText('West')).toBeInTheDocument();
  });

  // The canvas has no SQL Lab tab to chart into or to publish as an API. An Analyze item
  // that closes the menu and does nothing is worse than no item.
  it('offers only the Analyze actions that work on a canvas', async () => {
    render(<ResultsCanvas tab="results" sql="SELECT region FROM sales" result={RESULT} />);
    await openAnalyze();
    expect(screen.queryByText('Create Chart')).not.toBeInTheDocument();
    expect(screen.queryByText('Share as API')).not.toBeInTheDocument();
  });

  it('still offers Create Chart and Share as API where the page wires them', async () => {
    render(
      <ResultsGrid
        results={RESULT}
        sql="SELECT region FROM sales"
        onCreateVisualization={() => undefined}
        onShareApi={() => undefined}
      />,
    );
    await openAnalyze();
    expect(screen.getByText('Create Chart')).toBeInTheDocument();
    expect(screen.getByText('Share as API')).toBeInTheDocument();
  });
});
