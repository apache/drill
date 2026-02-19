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
import { useMemo, useCallback } from 'react';
import ReactECharts from 'echarts-for-react';
import { Empty, Spin, Table, Typography } from 'antd';
import { CaretUpOutlined, CaretDownOutlined, MinusOutlined, TableOutlined, InfoCircleOutlined } from '@ant-design/icons';
import type { EChartsOption } from 'echarts';
import { graphic } from 'echarts';
import { useTheme } from '../../hooks/useTheme';
import type { ChartType, VisualizationConfig, QueryResult } from '../../types';

interface ChartPreviewProps {
  chartType: ChartType;
  config: VisualizationConfig;
  data: QueryResult | null;
  loading?: boolean;
  height?: number | string;
  mini?: boolean;
  /** Override dark mode detection (e.g. from dashboard theme). */
  darkMode?: boolean;
  /** Called when user clicks a chart element. Receives the column name, value, and type flags. */
  onChartClick?: (column: string, value: string, isTemporal?: boolean, isNumeric?: boolean) => void;
}

// Color schemes
const colorSchemes: Record<string, string[]> = {
  default: ['#5470c6', '#91cc75', '#fac858', '#ee6666', '#73c0de', '#3ba272', '#fc8452', '#9a60b4', '#ea7ccc'],
  warm: ['#ff7f50', '#ff6347', '#ff4500', '#ffa500', '#ffd700', '#ffb6c1', '#ff69b4', '#ff1493'],
  cool: ['#00bfff', '#1e90ff', '#4169e1', '#0000ff', '#8a2be2', '#9400d3', '#9932cc', '#ba55d3'],
  earth: ['#8b4513', '#a0522d', '#cd853f', '#deb887', '#d2691e', '#bc8f8f', '#f4a460', '#c4a484'],
};

export type DateFormat = 'auto' | 'YYYY-MM-DD' | 'MM/DD/YYYY' | 'DD/MM/YYYY'
  | 'YYYY-MM-DD HH:mm' | 'MM/DD/YYYY HH:mm' | 'MMM DD, YYYY' | 'MMMM DD, YYYY';

const MONTH_SHORT = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
const MONTH_LONG = [
  'January','February','March','April','May','June',
  'July','August','September','October','November','December',
];

function pad2(n: number): string {
  return n < 10 ? `0${n}` : String(n);
}

/**
 * Format a raw date value (epoch number, ISO string, or date string)
 * according to the chosen format. Returns the original string if
 * the value cannot be parsed as a date.
 */
function formatDateValue(raw: unknown, format: DateFormat): string {
  if (raw == null || raw === '') {
    return '';
  }
  const str = String(raw);

  // Try to parse as a date
  let date: Date;
  if (typeof raw === 'number') {
    date = new Date(raw);
  } else {
    date = new Date(str);
  }

  if (isNaN(date.getTime())) {
    return str;
  }

  const y = date.getFullYear();
  const m = date.getMonth();
  const d = date.getDate();
  const hh = pad2(date.getHours());
  const mm = pad2(date.getMinutes());
  const md = pad2(m + 1);
  const dd = pad2(d);

  switch (format) {
    case 'YYYY-MM-DD':
      return `${y}-${md}-${dd}`;
    case 'MM/DD/YYYY':
      return `${md}/${dd}/${y}`;
    case 'DD/MM/YYYY':
      return `${dd}/${md}/${y}`;
    case 'YYYY-MM-DD HH:mm':
      return `${y}-${md}-${dd} ${hh}:${mm}`;
    case 'MM/DD/YYYY HH:mm':
      return `${md}/${dd}/${y} ${hh}:${mm}`;
    case 'MMM DD, YYYY':
      return `${MONTH_SHORT[m]} ${dd}, ${y}`;
    case 'MMMM DD, YYYY':
      return `${MONTH_LONG[m]} ${dd}, ${y}`;
    case 'auto':
    default:
      // Auto: if time component is midnight, show date only, otherwise show date+time
      if (date.getHours() === 0 && date.getMinutes() === 0 && date.getSeconds() === 0) {
        return `${y}-${md}-${dd}`;
      }
      return `${y}-${md}-${dd} ${hh}:${mm}`;
  }
}

/**
 * Check if an xAxis column is temporal based on the query metadata.
 */
function isXAxisTemporal(data: QueryResult, xAxis: string): boolean {
  const colIdx = data.columns.indexOf(xAxis);
  if (colIdx < 0 || !data.metadata?.[colIdx]) {
    return false;
  }
  const t = data.metadata[colIdx].toUpperCase();
  return t.includes('DATE') || t.includes('TIMESTAMP') || t.includes('TIME');
}

/**
 * Check if a column is numeric based on the query metadata.
 */
function isColumnNumeric(data: QueryResult, col: string): boolean {
  const colIdx = data.columns.indexOf(col);
  if (colIdx < 0 || !data.metadata?.[colIdx]) {
    return false;
  }
  const t = data.metadata[colIdx].toUpperCase();
  return t.includes('INT') || t.includes('BIGINT') || t.includes('FLOAT')
    || t.includes('DOUBLE') || t.includes('DECIMAL') || t.includes('NUMERIC');
}

export default function ChartPreview({
  chartType,
  config,
  data,
  loading = false,
  height = 400,
  mini = false,
  darkMode,
  onChartClick,
}: ChartPreviewProps) {
  const { isDark: globalDark } = useTheme();
  const isDark = darkMode ?? globalDark;
  const colors = colorSchemes[config.colorScheme || 'default'] || colorSchemes.default;

  // Map ECharts click event params → (column, value, isTemporal, isNumeric) based on chart type + config
  const handleChartClick = useCallback((params: Record<string, unknown>) => {
    if (!onChartClick || !config || !data) {
      return;
    }

    const { xAxis: xCol, dimensions } = config;
    const groupField = dimensions?.[0];

    const emitClick = (col: string, value: string) => {
      const temporal = isXAxisTemporal(data, col);
      const numeric = !temporal && isColumnNumeric(data, col);
      onChartClick(col, value, temporal, numeric);
    };

    switch (chartType) {
      case 'bar':
      case 'line':
      case 'area': {
        if (groupField && params.seriesName) {
          emitClick(groupField, String(params.seriesName));
        } else if (xCol && params.name != null) {
          const dataIndex = params.dataIndex as number | undefined;
          if (dataIndex != null && data.rows[dataIndex]) {
            const rawValue = data.rows[dataIndex][xCol];
            emitClick(xCol, rawValue == null ? 'null' : String(rawValue));
          } else {
            emitClick(xCol, String(params.name));
          }
        }
        break;
      }
      case 'pie':
      case 'treemap':
      case 'funnel': {
        if (groupField || dimensions?.[0]) {
          const dimCol = groupField || dimensions![0];
          if (params.name != null) {
            emitClick(dimCol, String(params.name));
          }
        }
        break;
      }
      case 'scatter': {
        if (xCol) {
          const val = params.value;
          if (Array.isArray(val) && val.length > 0) {
            emitClick(xCol, String(val[0]));
          }
        }
        break;
      }
      case 'heatmap': {
        if (xCol && params.name != null) {
          emitClick(xCol, String(params.name));
        }
        break;
      }
      default:
        break;
    }
  }, [onChartClick, chartType, config, data]);

  const onEvents = useMemo(() => {
    if (!onChartClick) {
      return undefined;
    }
    return { click: handleChartClick };
  }, [onChartClick, handleChartClick]);

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
        const barDateFmt = (config.chartOptions?.dateFormat as DateFormat) || 'auto';
        const barIsTemporal = isXAxisTemporal(data, xAxis);
        const barGroupField = dimensions?.[0];

        if (barGroupField) {
          // Grouped mode: one bar series per unique dimension value
          const uniqueCategories = [...new Set(data.rows.map((row) =>
            barIsTemporal ? formatDateValue(row[xAxis], barDateFmt) : String(row[xAxis] ?? '')
          ))];
          const groups = [...new Set(data.rows.map((row) => String(row[barGroupField] ?? '')))];
          const metric = metrics[0];
          const series = groups.map((group, idx) => {
            const valueMap: Record<string, number> = {};
            data.rows.forEach((row) => {
              if (String(row[barGroupField] ?? '') === group) {
                const cat = barIsTemporal ? formatDateValue(row[xAxis], barDateFmt) : String(row[xAxis] ?? '');
                valueMap[cat] = Number(row[metric]) || 0;
              }
            });
            return {
              name: group,
              type: 'bar' as const,
              data: uniqueCategories.map((cat) => valueMap[cat] ?? 0),
              itemStyle: { color: colors[idx % colors.length] },
            };
          });
          return {
            tooltip: { trigger: 'axis' },
            legend: { data: groups, bottom: 0 },
            xAxis: { type: 'category', data: uniqueCategories },
            yAxis: { type: 'value' },
            series,
            grid: { left: '3%', right: '4%', bottom: '15%', containLabel: true },
          };
        }

        // Standard mode: one bar series per metric
        const categories = data.rows.map((row) =>
          barIsTemporal ? formatDateValue(row[xAxis], barDateFmt) : String(row[xAxis] ?? '')
        );
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
        const smoothLine = config.chartOptions?.smoothLine !== false;
        const showDataLabels = config.chartOptions?.showDataLabels === true;
        const lineDateFmt = (config.chartOptions?.dateFormat as DateFormat) || 'auto';
        const lineIsTemporal = isXAxisTemporal(data, xAxis);
        const markerShape = (config.chartOptions?.markerShape as string) || 'circle';
        const markerSize = Number(config.chartOptions?.markerSize) || 4;
        const hideMarkers = markerShape === 'none';
        const lineGroupField = dimensions?.[0];

        if (lineGroupField) {
          // Grouped mode: one line per unique dimension value
          const uniqueCategories = [...new Set(data.rows.map((row) =>
            lineIsTemporal ? formatDateValue(row[xAxis], lineDateFmt) : String(row[xAxis] ?? '')
          ))];
          const groups = [...new Set(data.rows.map((row) => String(row[lineGroupField] ?? '')))];
          const metric = metrics[0];
          const series = groups.map((group, idx) => {
            // Build a map of category -> value for this group
            const valueMap: Record<string, number> = {};
            data.rows.forEach((row) => {
              if (String(row[lineGroupField] ?? '') === group) {
                const cat = lineIsTemporal ? formatDateValue(row[xAxis], lineDateFmt) : String(row[xAxis] ?? '');
                valueMap[cat] = Number(row[metric]) || 0;
              }
            });
            return {
              name: group,
              type: 'line' as const,
              data: uniqueCategories.map((cat) => valueMap[cat] ?? null),
              smooth: smoothLine,
              showSymbol: !hideMarkers,
              symbol: hideMarkers ? 'none' : markerShape,
              symbolSize: markerSize,
              itemStyle: { color: colors[idx % colors.length] },
              label: showDataLabels ? { show: true, position: 'top' as const } : undefined,
              connectNulls: false,
            };
          });
          return {
            tooltip: { trigger: 'axis' },
            legend: { data: groups, bottom: 0 },
            xAxis: { type: 'category', data: uniqueCategories },
            yAxis: { type: 'value' },
            series,
            grid: { left: '3%', right: '4%', bottom: '15%', containLabel: true },
          };
        }

        // Standard mode: one line per metric
        const categories = data.rows.map((row) =>
          lineIsTemporal ? formatDateValue(row[xAxis], lineDateFmt) : String(row[xAxis] ?? '')
        );
        const series = metrics.map((metric, idx) => ({
          name: metric,
          type: 'line' as const,
          data: data.rows.map((row) => Number(row[metric]) || 0),
          smooth: smoothLine,
          showSymbol: !hideMarkers,
          symbol: hideMarkers ? 'none' : markerShape,
          symbolSize: markerSize,
          itemStyle: { color: colors[idx % colors.length] },
          label: showDataLabels ? { show: true, position: 'top' as const } : undefined,
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

      case 'area': {
        if (!xAxis || !metrics || metrics.length === 0) {
          return null;
        }
        const smoothArea = config.chartOptions?.smoothLine !== false;
        const showAreaLabels = config.chartOptions?.showDataLabels === true;
        const useGradient = config.chartOptions?.gradientArea === true;
        const areaDateFmt = (config.chartOptions?.dateFormat as DateFormat) || 'auto';
        const areaIsTemporal = isXAxisTemporal(data, xAxis);
        const areaGroupField = dimensions?.[0];

        const buildAreaStyle = (color: string) => useGradient
          ? {
              color: new graphic.LinearGradient(0, 0, 0, 1, [
                { offset: 0, color },
                { offset: 1, color: 'rgba(255,255,255,0)' },
              ]),
            }
          : { opacity: 0.3 };

        if (areaGroupField) {
          // Grouped mode: one area per unique dimension value
          const uniqueCategories = [...new Set(data.rows.map((row) =>
            areaIsTemporal ? formatDateValue(row[xAxis], areaDateFmt) : String(row[xAxis] ?? '')
          ))];
          const groups = [...new Set(data.rows.map((row) => String(row[areaGroupField] ?? '')))];
          const metric = metrics[0];
          const series = groups.map((group, idx) => {
            const color = colors[idx % colors.length];
            const valueMap: Record<string, number> = {};
            data.rows.forEach((row) => {
              if (String(row[areaGroupField] ?? '') === group) {
                const cat = areaIsTemporal ? formatDateValue(row[xAxis], areaDateFmt) : String(row[xAxis] ?? '');
                valueMap[cat] = Number(row[metric]) || 0;
              }
            });
            return {
              name: group,
              type: 'line' as const,
              data: uniqueCategories.map((cat) => valueMap[cat] ?? null),
              smooth: smoothArea,
              areaStyle: buildAreaStyle(color),
              itemStyle: { color },
              label: showAreaLabels ? { show: true, position: 'top' as const } : undefined,
              connectNulls: false,
            };
          });
          return {
            tooltip: { trigger: 'axis' },
            legend: { data: groups, bottom: 0 },
            xAxis: { type: 'category', data: uniqueCategories },
            yAxis: { type: 'value' },
            series,
            grid: { left: '3%', right: '4%', bottom: '15%', containLabel: true },
          };
        }

        // Standard mode: one area per metric
        const categories = data.rows.map((row) =>
          areaIsTemporal ? formatDateValue(row[xAxis], areaDateFmt) : String(row[xAxis] ?? '')
        );
        const series = metrics.map((metric, idx) => {
          const color = colors[idx % colors.length];
          return {
            name: metric,
            type: 'line' as const,
            data: data.rows.map((row) => Number(row[metric]) || 0),
            smooth: smoothArea,
            areaStyle: buildAreaStyle(color),
            itemStyle: { color },
            label: showAreaLabels ? { show: true, position: 'top' as const } : undefined,
          };
        });
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
        const pieStyle = (config.chartOptions?.pieStyle as string) || 'donut';
        const showLabels = config.chartOptions?.showPieLabels !== false;
        const legendPos = (config.chartOptions?.legendPosition as string) || 'right';
        const isNested = metrics.length > 1;

        const emphasis = {
          itemStyle: {
            shadowBlur: 10,
            shadowOffsetX: 0,
            shadowColor: 'rgba(0, 0, 0, 0.5)',
          },
        };

        // Compute radius based on style
        const computeRadius = (ringIndex: number, totalRings: number): [string, string] => {
          if (totalRings === 1) {
            if (pieStyle === 'pie' || pieStyle === 'nightingale') {
              return ['0%', '70%'];
            }
            // donut
            return ['40%', '70%'];
          }
          // Nested: divide radial space into concentric rings
          const gap = 3; // percent gap between rings
          const maxOuter = 75;
          const minInner = pieStyle === 'donut' || pieStyle === 'nightingale' ? 15 : 0;
          const totalSpace = maxOuter - minInner - (totalRings - 1) * gap;
          const ringWidth = totalSpace / totalRings;
          const inner = minInner + ringIndex * (ringWidth + gap);
          const outer = inner + ringWidth;
          return [`${Math.round(inner)}%`, `${Math.round(outer)}%`];
        };

        const series = metrics.map((metric, ringIdx) => {
          const pieData = data.rows.map((row, idx) => ({
            name: String(row[labelField] ?? ''),
            value: Number(row[metric]) || 0,
            itemStyle: { color: colors[idx % colors.length] },
          }));
          const radius = computeRadius(ringIdx, metrics.length);
          return {
            name: metric,
            type: 'pie' as const,
            radius,
            roseType: pieStyle === 'nightingale' ? ('area' as const) : undefined,
            data: pieData,
            emphasis,
            label: showLabels
              ? { formatter: isNested ? '{b}: {d}%' : '{b}: {c} ({d}%)' }
              : { show: false },
            labelLine: showLabels ? {} : { show: false },
          };
        });

        // Build legend config based on position
        const legendConfig = (() => {
          if (legendPos === 'hidden') {
            return { show: false };
          }
          if (legendPos === 'left') {
            return { orient: 'vertical' as const, left: 'left', top: 'middle' };
          }
          if (legendPos === 'right') {
            return { orient: 'vertical' as const, right: 0, top: 'middle' };
          }
          if (legendPos === 'bottom') {
            return { orient: 'horizontal' as const, bottom: 0, left: 'center' };
          }
          // top
          return { orient: 'horizontal' as const, top: 0, left: 'center' };
        })();

        // Offset pie center so it doesn't overlap the legend
        const pieCenter = (() => {
          if (legendPos === 'hidden') {
            return ['50%', '50%'];
          }
          if (legendPos === 'left') {
            return ['60%', '50%'];
          }
          if (legendPos === 'right') {
            return ['40%', '50%'];
          }
          if (legendPos === 'top') {
            return ['50%', '55%'];
          }
          // bottom
          return ['50%', '45%'];
        })();

        // Apply center to each series
        const centeredSeries = series.map(s => ({ ...s, center: pieCenter }));

        return {
          tooltip: { trigger: 'item', formatter: '{a}<br/>{b}: {c} ({d}%)' },
          legend: legendConfig,
          series: centeredSeries,
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
  const miniOption = useMemo(() => {
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

  // Apply dark mode colors to ECharts (canvas doesn't respond to CSS variables)
  const finalOption = useMemo(() => {
    const opt = miniOption;
    if (!opt || !isDark) {
      return opt;
    }
    const darkText = '#e0e0e0';
    const darkLine = '#424242';
    const darkSplit = '#303030';
    const xAxisRaw = opt.xAxis as Record<string, unknown> | undefined;
    const yAxisRaw = opt.yAxis as Record<string, unknown> | undefined;
    return {
      ...opt,
      textStyle: { ...(opt.textStyle as Record<string, unknown> || {}), color: darkText },
      legend: opt.legend ? {
        ...(opt.legend as Record<string, unknown>),
        textStyle: { color: darkText },
      } : undefined,
      tooltip: opt.tooltip ? {
        ...(opt.tooltip as Record<string, unknown>),
        backgroundColor: '#1f1f1f',
        borderColor: darkLine,
        textStyle: { color: darkText },
      } : undefined,
      xAxis: xAxisRaw ? {
        ...xAxisRaw,
        axisLabel: { ...(xAxisRaw.axisLabel as Record<string, unknown> || {}), color: darkText },
        axisLine: { lineStyle: { color: darkLine } },
        splitLine: { lineStyle: { color: darkSplit } },
        splitArea: xAxisRaw.splitArea ? { ...(xAxisRaw.splitArea as Record<string, unknown>), areaStyle: { color: ['transparent', 'rgba(255,255,255,0.02)'] } } : undefined,
      } : undefined,
      yAxis: yAxisRaw ? {
        ...yAxisRaw,
        axisLabel: { ...(yAxisRaw.axisLabel as Record<string, unknown> || {}), color: darkText },
        axisLine: { lineStyle: { color: darkLine } },
        splitLine: { lineStyle: { color: darkSplit } },
        splitArea: yAxisRaw.splitArea ? { ...(yAxisRaw.splitArea as Record<string, unknown>), areaStyle: { color: ['transparent', 'rgba(255,255,255,0.02)'] } } : undefined,
      } : undefined,
      visualMap: opt.visualMap ? {
        ...(opt.visualMap as Record<string, unknown>),
        textStyle: { color: darkText },
      } : undefined,
    };
  }, [miniOption, isDark]);

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

  // For bigNumber type, render custom JSX
  if (chartType === 'bigNumber') {
    const metricField = config.metrics?.[0];
    if (!metricField) {
      if (mini) {
        return null;
      }
      return (
        <div style={{ height, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <Empty description="Select a metric to display" />
        </div>
      );
    }

    const rows = data.rows;
    const lastValue = Number(rows[rows.length - 1]?.[metricField]) || 0;
    const firstValue = Number(rows[0]?.[metricField]) || 0;
    const hasMultipleRows = rows.length >= 2;

    const showSparkline = config.chartOptions?.showSparkline !== false;
    const showTrend = config.chartOptions?.showTrend !== false;

    // Trend calculation
    let trendPercent = 0;
    let trendDirection: 'up' | 'down' | 'flat' = 'flat';
    if (hasMultipleRows && firstValue !== 0) {
      trendPercent = ((lastValue - firstValue) / Math.abs(firstValue)) * 100;
      if (trendPercent > 0) {
        trendDirection = 'up';
      } else if (trendPercent < 0) {
        trendDirection = 'down';
      }
    }

    // Mini mode: just the big number
    if (mini) {
      return (
        <div style={{
          height,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          flexDirection: 'column',
        }}>
          <div style={{ fontSize: 28, fontWeight: 700, color: 'var(--color-text)' }}>
            {lastValue.toLocaleString()}
          </div>
        </div>
      );
    }

    // Sparkline data
    const sparklineValues = rows.map((row) => Number(row[metricField]) || 0);

    const sparklineOption: EChartsOption = {
      xAxis: { type: 'category', show: false, boundaryGap: false },
      yAxis: { type: 'value', show: false },
      grid: { left: 0, right: 0, top: 0, bottom: 0 },
      series: [{
        type: 'line',
        data: sparklineValues,
        smooth: true,
        showSymbol: false,
        lineStyle: { width: 2, color: colors[0] },
        areaStyle: { color: colors[0], opacity: 0.15 },
      }],
      tooltip: undefined,
      animation: false,
    };

    return (
      <div style={{
        height,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        flexDirection: 'column',
        gap: 8,
      }}>
        <div style={{ fontSize: 48, fontWeight: 700, color: 'var(--color-text)', lineHeight: 1.2 }}>
          {lastValue.toLocaleString()}
        </div>
        <div style={{ fontSize: 14, color: 'var(--color-text-secondary)' }}>
          {metricField}
        </div>
        {showTrend && hasMultipleRows && (
          <div style={{
            fontSize: 16,
            color: trendDirection === 'up' ? '#52c41a' : trendDirection === 'down' ? '#ff4d4f' : 'var(--color-text-secondary)',
            display: 'flex',
            alignItems: 'center',
            gap: 4,
          }}>
            {trendDirection === 'up' && <CaretUpOutlined />}
            {trendDirection === 'down' && <CaretDownOutlined />}
            {trendDirection === 'flat' && <MinusOutlined />}
            {Math.abs(trendPercent).toFixed(1)}%
          </div>
        )}
        {showSparkline && hasMultipleRows && (
          <div style={{ width: '60%', maxWidth: 300 }}>
            <ReactECharts
              option={sparklineOption}
              style={{ height: 80, width: '100%' }}
              opts={{ renderer: 'canvas' }}
            />
          </div>
        )}
      </div>
    );
  }

  // For table type in mini mode, render a stylized placeholder
  if (chartType === 'table' && mini) {
    const colCount = Math.min(data.columns.length, 4);
    const rowCount = Math.min(data.rows.length, 5);
    return (
      <div style={{
        height,
        display: 'flex',
        flexDirection: 'column',
        padding: 8,
        gap: 2,
        overflow: 'hidden',
      }}>
        {/* Header row */}
        <div style={{ display: 'flex', gap: 2, flexShrink: 0 }}>
          {Array.from({ length: colCount }).map((_, i) => (
            <div key={`h-${i}`} style={{
              flex: 1,
              height: 14,
              borderRadius: 2,
              backgroundColor: colors[0],
              opacity: 0.8,
            }} />
          ))}
        </div>
        {/* Data rows */}
        {Array.from({ length: rowCount }).map((_, r) => (
          <div key={`r-${r}`} style={{ display: 'flex', gap: 2, flexShrink: 0 }}>
            {Array.from({ length: colCount }).map((_, c) => (
              <div key={`c-${c}`} style={{
                flex: 1,
                height: 12,
                borderRadius: 2,
                backgroundColor: 'var(--color-text)',
                opacity: r % 2 === 0 ? 0.08 : 0.04,
              }} />
            ))}
          </div>
        ))}
        {/* Table icon label */}
        <div style={{
          flex: 1,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          color: colors[0],
          fontSize: 11,
          opacity: 0.6,
          gap: 4,
        }}>
          <TableOutlined />
          {data.columns.length} cols &middot; {data.rows.length} rows
        </div>
      </div>
    );
  }

  // For table type, use Ant Design Table
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
      onCell: (record: Record<string, unknown>) => ({
        onClick: () => {
          if (onChartClick && record[col] != null) {
            onChartClick(col, String(record[col]));
          }
        },
        style: onChartClick ? { cursor: 'pointer' } : undefined,
      }),
    }));

    return (
      <Table
        dataSource={data.rows.map((row, idx) => ({ ...row, key: idx }))}
        columns={tableColumns}
        scroll={{ y: typeof height === 'number' ? height - 100 : 'calc(100% - 100px)' }}
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

  // Pie chart readability warning
  const PIE_SLICE_WARN = 8;
  const PIE_SLICE_SEVERE = 15;
  const pieSliceCount = chartType === 'pie' && data ? data.rows.length : 0;
  const pieWarning = !mini && chartType === 'pie' && pieSliceCount > PIE_SLICE_WARN;

  if (pieWarning) {
    const severe = pieSliceCount > PIE_SLICE_SEVERE;
    return (
      <div style={{ height, display: 'flex', flexDirection: 'column' }}>
        <div style={{
          padding: '4px 12px',
          fontSize: 12,
          color: severe ? '#faad14' : 'var(--color-text-tertiary)',
          display: 'flex',
          alignItems: 'center',
          gap: 6,
          flexShrink: 0,
        }}>
          <InfoCircleOutlined />
          <Typography.Text type={severe ? 'warning' : 'secondary'} style={{ fontSize: 12 }}>
            {pieSliceCount} slices may be hard to read.
            {severe
              ? ' Consider a bar chart, treemap, or filtering to the top values.'
              : ' A treemap or bar chart may work better for many categories.'}
          </Typography.Text>
        </div>
        <div style={{ flex: 1, minHeight: 0 }}>
          <ReactECharts
            option={finalOption}
            notMerge
            style={{ height: '100%', width: '100%', cursor: onChartClick ? 'pointer' : undefined }}
            opts={{ renderer: 'canvas' }}
            onEvents={onEvents}
          />
        </div>
      </div>
    );
  }

  return (
    <ReactECharts
      option={finalOption}
      notMerge
      style={{ height, width: '100%', cursor: onChartClick ? 'pointer' : undefined }}
      opts={{ renderer: 'canvas' }}
      onEvents={onEvents}
    />
  );
}
