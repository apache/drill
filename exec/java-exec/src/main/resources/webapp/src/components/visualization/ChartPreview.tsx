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
import { useMemo } from 'react';
import ReactECharts from 'echarts-for-react';
import { Empty, Spin, Table } from 'antd';
import type { EChartsOption } from 'echarts';
import type { ChartType, VisualizationConfig, QueryResult } from '../../types';

interface ChartPreviewProps {
  chartType: ChartType;
  config: VisualizationConfig;
  data: QueryResult | null;
  loading?: boolean;
  height?: number;
  mini?: boolean;
}

// Color schemes
const colorSchemes: Record<string, string[]> = {
  default: ['#5470c6', '#91cc75', '#fac858', '#ee6666', '#73c0de', '#3ba272', '#fc8452', '#9a60b4', '#ea7ccc'],
  warm: ['#ff7f50', '#ff6347', '#ff4500', '#ffa500', '#ffd700', '#ffb6c1', '#ff69b4', '#ff1493'],
  cool: ['#00bfff', '#1e90ff', '#4169e1', '#0000ff', '#8a2be2', '#9400d3', '#9932cc', '#ba55d3'],
  earth: ['#8b4513', '#a0522d', '#cd853f', '#deb887', '#d2691e', '#bc8f8f', '#f4a460', '#c4a484'],
};

export default function ChartPreview({
  chartType,
  config,
  data,
  loading = false,
  height = 400,
  mini = false,
}: ChartPreviewProps) {
  const colors = colorSchemes[config.colorScheme || 'default'] || colorSchemes.default;

  const chartOption: EChartsOption | null = useMemo(() => {
    if (!data || !data.rows || data.rows.length === 0) {
      return null;
    }

    const { xAxis, yAxis, metrics, dimensions } = config;

    switch (chartType) {
      case 'bar': {
        if (!xAxis || !metrics || metrics.length === 0) {
          return null;
        }
        const categories = data.rows.map((row) => String(row[xAxis] ?? ''));
        const series = metrics.map((metric, idx) => ({
          name: metric,
          type: 'bar' as const,
          data: data.rows.map((row) => Number(row[metric]) || 0),
          itemStyle: { color: colors[idx % colors.length] },
        }));
        return {
          tooltip: { trigger: 'axis' },
          legend: { data: metrics, bottom: 0 },
          xAxis: { type: 'category', data: categories },
          yAxis: { type: 'value' },
          series,
          grid: { left: '3%', right: '4%', bottom: '15%', containLabel: true },
        };
      }

      case 'line': {
        if (!xAxis || !metrics || metrics.length === 0) {
          return null;
        }
        const categories = data.rows.map((row) => String(row[xAxis] ?? ''));
        const series = metrics.map((metric, idx) => ({
          name: metric,
          type: 'line' as const,
          data: data.rows.map((row) => Number(row[metric]) || 0),
          smooth: true,
          itemStyle: { color: colors[idx % colors.length] },
        }));
        return {
          tooltip: { trigger: 'axis' },
          legend: { data: metrics, bottom: 0 },
          xAxis: { type: 'category', data: categories },
          yAxis: { type: 'value' },
          series,
          grid: { left: '3%', right: '4%', bottom: '15%', containLabel: true },
        };
      }

      case 'pie': {
        if (!dimensions || dimensions.length === 0 || !metrics || metrics.length === 0) {
          return null;
        }
        const labelField = dimensions[0];
        const valueField = metrics[0];
        const pieData = data.rows.map((row, idx) => ({
          name: String(row[labelField] ?? ''),
          value: Number(row[valueField]) || 0,
          itemStyle: { color: colors[idx % colors.length] },
        }));
        return {
          tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
          legend: { orient: 'vertical', left: 'left' },
          series: [{
            type: 'pie',
            radius: ['40%', '70%'],
            data: pieData,
            emphasis: {
              itemStyle: {
                shadowBlur: 10,
                shadowOffsetX: 0,
                shadowColor: 'rgba(0, 0, 0, 0.5)',
              },
            },
          }],
        };
      }

      case 'scatter': {
        if (!xAxis || !yAxis) {
          return null;
        }
        const groupField = dimensions?.[0];
        let series;

        if (groupField) {
          const groups = [...new Set(data.rows.map((row) => String(row[groupField] ?? '')))];
          series = groups.map((group, idx) => ({
            name: group,
            type: 'scatter' as const,
            data: data.rows
              .filter((row) => String(row[groupField] ?? '') === group)
              .map((row) => [Number(row[xAxis]) || 0, Number(row[yAxis]) || 0]),
            itemStyle: { color: colors[idx % colors.length] },
          }));
        } else {
          series = [{
            type: 'scatter' as const,
            data: data.rows.map((row) => [Number(row[xAxis]) || 0, Number(row[yAxis]) || 0]),
            itemStyle: { color: colors[0] },
          }];
        }

        return {
          tooltip: { trigger: 'item' },
          legend: groupField ? { data: series.map((s) => 'name' in s ? s.name : ''), bottom: 0 } : undefined,
          xAxis: { type: 'value', name: xAxis },
          yAxis: { type: 'value', name: yAxis },
          series,
          grid: { left: '3%', right: '4%', bottom: '15%', containLabel: true },
        };
      }

      case 'heatmap': {
        if (!xAxis || !yAxis || !metrics || metrics.length === 0) {
          return null;
        }
        const valueField = metrics[0];
        const xCategories = [...new Set(data.rows.map((row) => String(row[xAxis] ?? '')))];
        const yCategories = [...new Set(data.rows.map((row) => String(row[yAxis] ?? '')))];
        const heatmapData = data.rows.map((row) => [
          xCategories.indexOf(String(row[xAxis] ?? '')),
          yCategories.indexOf(String(row[yAxis] ?? '')),
          Number(row[valueField]) || 0,
        ]);
        const maxValue = Math.max(...heatmapData.map((d) => d[2] as number));

        return {
          tooltip: { position: 'top' },
          xAxis: { type: 'category', data: xCategories, splitArea: { show: true } },
          yAxis: { type: 'category', data: yCategories, splitArea: { show: true } },
          visualMap: {
            min: 0,
            max: maxValue,
            calculable: true,
            orient: 'horizontal',
            left: 'center',
            bottom: '0%',
          },
          series: [{
            type: 'heatmap',
            data: heatmapData,
            label: { show: true },
            emphasis: {
              itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0, 0, 0, 0.5)' },
            },
          }],
          grid: { left: '3%', right: '4%', bottom: '15%', containLabel: true },
        };
      }

      case 'gauge': {
        if (!metrics || metrics.length === 0) {
          return null;
        }
        const valueField = metrics[0];
        const value = Number(data.rows[0]?.[valueField]) || 0;
        return {
          tooltip: { formatter: '{b}: {c}' },
          series: [{
            type: 'gauge',
            detail: { formatter: '{value}' },
            data: [{ value, name: valueField }],
            axisLine: {
              lineStyle: {
                color: [[0.3, '#67e0e3'], [0.7, '#37a2da'], [1, '#fd666d']],
              },
            },
          }],
        };
      }

      case 'funnel': {
        if (!dimensions || dimensions.length === 0 || !metrics || metrics.length === 0) {
          return null;
        }
        const labelField = dimensions[0];
        const valueField = metrics[0];
        const funnelData = data.rows.map((row, idx) => ({
          name: String(row[labelField] ?? ''),
          value: Number(row[valueField]) || 0,
          itemStyle: { color: colors[idx % colors.length] },
        }));
        return {
          tooltip: { trigger: 'item', formatter: '{b}: {c}' },
          legend: { data: funnelData.map((d) => d.name), bottom: 0 },
          series: [{
            type: 'funnel',
            left: '10%',
            width: '80%',
            label: { formatter: '{b}' },
            data: funnelData.sort((a, b) => b.value - a.value),
          }],
        };
      }

      case 'treemap': {
        if (!dimensions || dimensions.length === 0 || !metrics || metrics.length === 0) {
          return null;
        }
        const labelField = dimensions[0];
        const valueField = metrics[0];
        const treemapData = data.rows.map((row, idx) => ({
          name: String(row[labelField] ?? ''),
          value: Number(row[valueField]) || 0,
          itemStyle: { color: colors[idx % colors.length] },
        }));
        return {
          tooltip: { trigger: 'item', formatter: '{b}: {c}' },
          series: [{
            type: 'treemap',
            data: treemapData,
            label: { show: true, formatter: '{b}' },
            breadcrumb: { show: false },
          }],
        };
      }

      default:
        return null;
    }
  }, [chartType, config, data, colors]);

  // Strip down chart chrome for mini mode
  const finalOption = useMemo(() => {
    if (!chartOption || !mini) {
      return chartOption;
    }
    return {
      ...chartOption,
      tooltip: undefined,
      legend: undefined,
      visualMap: undefined,
      grid: { left: 4, right: 4, top: 4, bottom: 4, containLabel: false },
      xAxis: chartOption.xAxis ? { ...chartOption.xAxis as Record<string, unknown>, show: false } : undefined,
      yAxis: chartOption.yAxis ? { ...chartOption.yAxis as Record<string, unknown>, show: false } : undefined,
      animation: false,
    };
  }, [chartOption, mini]);

  if (loading) {
    return (
      <div style={{ height, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <Spin tip={mini ? undefined : "Loading data..."} size={mini ? 'small' : 'default'} />
      </div>
    );
  }

  if (!data || !data.rows || data.rows.length === 0) {
    if (mini) {
      return null;
    }
    return (
      <div style={{ height, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <Empty description="No data available. Run a query first." />
      </div>
    );
  }

  // For table type, use Ant Design Table (skip in mini mode)
  if (chartType === 'table' && !mini) {
    const columnsToShow = config.dimensions && config.dimensions.length > 0
      ? config.dimensions
      : data.columns;

    const tableColumns = columnsToShow.map((col) => ({
      title: col,
      dataIndex: col,
      key: col,
      sorter: (a: Record<string, unknown>, b: Record<string, unknown>) => {
        const aVal = a[col];
        const bVal = b[col];
        if (typeof aVal === 'number' && typeof bVal === 'number') {
          return aVal - bVal;
        }
        return String(aVal ?? '').localeCompare(String(bVal ?? ''));
      },
    }));

    return (
      <Table
        dataSource={data.rows.map((row, idx) => ({ ...row, key: idx }))}
        columns={tableColumns}
        scroll={{ y: height - 100 }}
        size="small"
        pagination={{ pageSize: 50, showSizeChanger: true }}
      />
    );
  }

  if (!finalOption) {
    if (mini) {
      return null;
    }
    return (
      <div style={{ height, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <Empty description="Configure chart settings to see preview" />
      </div>
    );
  }

  return (
    <ReactECharts
      option={finalOption}
      style={{ height, width: '100%' }}
      opts={{ renderer: 'canvas' }}
    />
  );
}
