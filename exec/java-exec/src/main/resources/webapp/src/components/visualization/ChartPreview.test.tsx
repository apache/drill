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
import ChartPreview from './ChartPreview';
import type { EChartsOption } from 'echarts';
import type { QueryResult } from '../../types';

// ECharts draws to a canvas jsdom does not have. The option it is handed is the whole of
// what this component decides, so the stub reports that instead of rendering.
vi.mock('echarts-for-react', () => ({
  default: ({ option }: { option: EChartsOption }) => (
    <div data-testid="chart" data-option={JSON.stringify(option)} />
  ),
}));

const data = {
  columns: ['category', 'total_revenue'],
  rows: [
    { category: 'ABC', total_revenue: 10 },
    { category: 'DEF', total_revenue: 30 },
  ],
  rowCount: 2,
} as unknown as QueryResult;

function seriesData() {
  const option = JSON.parse(screen.getByTestId('chart').getAttribute('data-option')!);
  return option.series[0].data;
}

describe('ChartPreview slice labels', () => {
  it('labels a pie from the x axis when no dimension is given', () => {
    // A chart built from a description names the category column as the x axis; a pie has
    // no axes, and before this it drew nothing at all.
    render(
      <ChartPreview
        chartType="pie"
        config={{ xAxis: 'category', metrics: ['total_revenue'] }}
        data={data}
        height={300}
      />
    );
    expect(screen.queryByText(/Configure chart settings/)).not.toBeInTheDocument();
    expect(seriesData().map((d: { name: string }) => d.name)).toEqual(['ABC', 'DEF']);
  });

  it('still labels a pie from its dimension when that repeats the x axis', () => {
    render(
      <ChartPreview
        chartType="pie"
        config={{ xAxis: 'category', dimensions: ['category'], metrics: ['total_revenue'] }}
        data={data}
        height={300}
      />
    );
    expect(seriesData().map((d: { name: string }) => d.name)).toEqual(['ABC', 'DEF']);
  });

  it('drops a bar grouping that only repeats the x axis', () => {
    render(
      <ChartPreview
        chartType="bar"
        config={{ xAxis: 'category', dimensions: ['category'], metrics: ['total_revenue'] }}
        data={data}
        height={300}
      />
    );
    const option = JSON.parse(screen.getByTestId('chart').getAttribute('data-option')!);
    // One series over both categories, not one hairline series per category.
    expect(option.series).toHaveLength(1);
  });
});
