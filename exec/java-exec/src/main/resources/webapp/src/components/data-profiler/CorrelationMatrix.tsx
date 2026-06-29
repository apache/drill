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
import { Empty } from 'antd';
import ReactECharts from 'echarts-for-react';
import type { CorrelationMatrix as CorrelationMatrixData } from '../../types/profile';

interface Props {
  matrix: CorrelationMatrixData | undefined;
}

export default function CorrelationMatrix({ matrix }: Props) {
  if (!matrix || matrix.columns.length < 2) {
    return <Empty description="Correlation requires at least 2 numeric columns" />;
  }

  const { columns, values } = matrix;

  // Build heatmap data: [colIdx, rowIdx, value]
  const data: [number, number, number][] = [];
  for (let i = 0; i < columns.length; i++) {
    for (let j = 0; j < columns.length; j++) {
      data.push([j, i, Math.round(values[i][j] * 1000) / 1000]);
    }
  }

  const option = {
    tooltip: {
      position: 'top' as const,
      formatter: (params: { value: [number, number, number] }) => {
        const [x, y, v] = params.value;
        return `${columns[y]} vs ${columns[x]}: <strong>${v.toFixed(3)}</strong>`;
      },
    },
    grid: {
      top: 10,
      right: 80,
      bottom: 80,
      left: 120,
    },
    xAxis: {
      type: 'category' as const,
      data: columns,
      axisLabel: { rotate: 45, fontSize: 11 },
      splitArea: { show: true },
    },
    yAxis: {
      type: 'category' as const,
      data: columns,
      axisLabel: { fontSize: 11 },
      splitArea: { show: true },
    },
    visualMap: {
      min: -1,
      max: 1,
      calculable: true,
      orient: 'vertical' as const,
      right: 10,
      top: 'center' as const,
      inRange: {
        color: ['#2166ac', '#67a9cf', '#d1e5f0', '#f7f7f7', '#fddbc7', '#ef8a62', '#b2182b'],
      },
      text: ['+1', '-1'],
      textStyle: { fontSize: 11 },
    },
    series: [{
      type: 'heatmap' as const,
      data,
      label: {
        show: columns.length <= 10,
        formatter: (params: { value: [number, number, number] }) => params.value[2].toFixed(2),
        fontSize: 10,
      },
      emphasis: {
        itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0, 0, 0, 0.3)' },
      },
    }],
  };

  const height = Math.max(400, columns.length * 40 + 120);

  return (
    <ReactECharts
      option={option}
      style={{ height, width: '100%' }}
    />
  );
}
