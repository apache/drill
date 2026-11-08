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
import ResultsCanvas from './ResultsCanvas';

/**
 * The one place the real grid is mounted outside the SQL Lab page: proof that it needs
 * nothing from that page (no router, no query execution state) to show a result.
 */
describe('ResultsCanvas', () => {
  it('mounts the real results grid on a bare result', async () => {
    render(
      <ResultsCanvas
        tab="results"
        sql="SELECT region FROM sales"
        result={{
          columns: ['region'],
          metadata: ['VARCHAR'],
          rows: [{ region: 'West' }],
          queryId: 'q1',
        }}
      />,
    );
    expect(await screen.findByText('region')).toBeInTheDocument();
    expect(await screen.findByText('West')).toBeInTheDocument();
  });
});
