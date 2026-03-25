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
import { useMemo, useCallback, useState, useEffect, useRef } from 'react';
import ReactECharts from 'echarts-for-react';
import { Alert, Button, Empty, Spin, Table, Tooltip, Typography, message } from 'antd';
import { CaretUpOutlined, CaretDownOutlined, CopyOutlined, MinusOutlined, TableOutlined, InfoCircleOutlined } from '@ant-design/icons';
import type { EChartsOption } from 'echarts';
import * as echarts from 'echarts/core';
import { graphic } from 'echarts';
import { useTheme } from '../../hooks/useTheme';
import type { ChartType, VisualizationConfig, QueryResult, PredictiveAnalyticsConfig, PredictionMethod } from '../../types';
import { generatePredictions, generateFutureLabels, generateTrendLine } from '../../utils/predictions';
import type { DataPoint } from '../../utils/predictions';
import { fetchGeoJson } from '../../api/geojson';
import { getMapDef, getActualMapId } from '../../utils/geoMapRegistry';

interface ChartPreviewProps {
  chartType: ChartType;
  config: VisualizationConfig;
  data: QueryResult | null;
  loading?: boolean;
  height?: number | string;
  mini?: boolean;
  /** The original SQL query — used by pivot table to generate a copyable Drill PIVOT statement. */
  sql?: string;
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

// Module-level promise cache for lazy-loading maps
const _mapPromiseCache = new Map<string, Promise<void>>();

/**
 * Ensure a GeoJSON map is registered with ECharts.
 * Uses module-level cache to avoid refetching the same map.
 */
function ensureMapRegistered(mapId: string): Promise<void> {
  // Check if already registered
  if (echarts.getMap(mapId)) {
    console.debug(`[ECharts] Map ${mapId} already registered`);
    return Promise.resolve();
  }

  // Check if fetch is already in progress
  if (_mapPromiseCache.has(mapId)) {
    console.debug(`[ECharts] Map ${mapId} fetch in progress, waiting...`);
    return _mapPromiseCache.get(mapId)!;
  }

  // Fetch and register the map
  console.debug(`[ECharts] Fetching map ${mapId}...`);
  const promise = fetchGeoJson(mapId)
    .then((geoJson) => {
      const features = (geoJson as Record<string, unknown>).features as unknown[];
      console.debug(`[ECharts] Registering map ${mapId} with ${features?.length ?? 0} features`);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      echarts.registerMap(mapId, geoJson as any);
    })
    .catch((error) => {
      console.error(`Failed to load map ${mapId}:`, error);
      throw error;
    });

  _mapPromiseCache.set(mapId, promise);
  return promise;
}

// Choropleth color palettes for different scales
const CHOROPLETH_PALETTES: Record<string, string[]> = {
  blue: ['#e0f3f8', '#abd9e9', '#74add1', '#4575b4', '#313695'],
  green: ['#edf8e9', '#bae4b3', '#74c476', '#31a354', '#006d2c'],
  red: ['#fff5eb', '#fdd0a2', '#fdae6b', '#e6550d', '#a63603'],
  heat: ['#ffffcc', '#fed976', '#fd8d3c', '#e31a1c', '#800026'],
  diverging: ['#313695', '#abd9e9', '#ffffbf', '#fdae61', '#a50026'],
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

/**
 * Compute [min, Q1, median, Q3, max] for a sorted array of numbers.
 * Used by the box plot renderer.
 */
function computeBoxStats(values: number[]): [number, number, number, number, number] {
  if (values.length === 0) {
    return [0, 0, 0, 0, 0];
  }
  const sorted = [...values].sort((a, b) => a - b);
  const n = sorted.length;
  const q = (p: number) => {
    const idx = (n - 1) * p;
    const lo = Math.floor(idx);
    const hi = Math.ceil(idx);
    return sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
  };
  return [sorted[0], q(0.25), q(0.5), q(0.75), sorted[n - 1]];
}

/**
 * Build ECharts series for forecast data: dashed forecast line + confidence band.
 * Returns an array of series objects (empty if predictions not enabled / insufficient data).
 */
function buildForecastSeries(
  categories: string[],
  seriesData: number[],
  seriesName: string,
  color: string,
  paConfig: PredictiveAnalyticsConfig | undefined,
  isTemporal: boolean,
  isAreaChart: boolean,
): { series: Record<string, unknown>[]; futureLabels: string[]; bandNames: string[] } {
  const empty = { series: [], futureLabels: [], bandNames: [] };
  if (!paConfig?.enabled || seriesData.length < 2) {
    return empty;
  }

  const dataPoints: DataPoint[] = seriesData.map((y, i) => ({
    x: i,
    y,
    xLabel: categories[i] ?? String(i),
  }));

  const result = generatePredictions(dataPoints, paConfig);
  if (!result || result.forecastPoints.length === 0) {
    return empty;
  }

  const futureLabels = generateFutureLabels(categories, paConfig.periods, isTemporal);

  // Build the forecast data array: nulls for historical points, then forecast values.
  // The last actual point is included to connect the forecast line seamlessly.
  const nHistorical = seriesData.length;
  const forecastLineData: (number | null)[] = new Array(nHistorical).fill(null);
  // Connect from last actual point
  forecastLineData[nHistorical - 1] = seriesData[nHistorical - 1];
  result.forecastPoints.forEach((pt) => forecastLineData.push(pt.y));

  const upperBandData: (number | null)[] = new Array(nHistorical).fill(null);
  upperBandData[nHistorical - 1] = seriesData[nHistorical - 1];
  const lowerBandData: (number | null)[] = new Array(nHistorical).fill(null);
  lowerBandData[nHistorical - 1] = seriesData[nHistorical - 1];

  result.confidenceBands.forEach((band) => {
    upperBandData.push(band.upper);
    lowerBandData.push(band.lower);
  });

  const forecastName = `${seriesName} (Forecast)`;
  const upperName = `${seriesName} (CI Upper)`;
  const lowerName = `${seriesName} (CI Lower)`;
  const stackGroup = `ci-${seriesName}`;

  const series: Record<string, unknown>[] = [
    // Forecast line — dashed, same color
    {
      name: forecastName,
      type: 'line',
      data: forecastLineData,
      smooth: false,
      showSymbol: false,
      lineStyle: { type: 'dashed', width: 2, color },
      itemStyle: { color },
      areaStyle: isAreaChart ? undefined : undefined,
      connectNulls: false,
      z: 5,
    },
    // Lower band — invisible base line for the stacked shading
    {
      name: lowerName,
      type: 'line',
      data: lowerBandData,
      smooth: false,
      showSymbol: false,
      lineStyle: { opacity: 0 },
      itemStyle: { color },
      stack: stackGroup,
      areaStyle: { opacity: 0 },
      connectNulls: false,
      z: 3,
    },
    // Upper band — shaded area between lower and upper
    {
      name: upperName,
      type: 'line',
      data: upperBandData.map((val, i) => {
        if (val == null || lowerBandData[i] == null) {
          return null;
        }
        return val - (lowerBandData[i] as number);
      }),
      smooth: false,
      showSymbol: false,
      lineStyle: { opacity: 0 },
      itemStyle: { color },
      stack: stackGroup,
      areaStyle: { opacity: 0.15, color },
      connectNulls: false,
      z: 3,
    },
  ];

  return { series, futureLabels, bandNames: [forecastName, upperName, lowerName] };
}

/**
 * Build an ECharts series for a trend line over historical data.
 * Independent of forecasting — uses generateTrendLine from predictions.ts.
 */
function buildTrendLineSeries(
  categories: string[],
  seriesData: number[],
  seriesName: string,
  color: string,
  method: PredictionMethod,
  options?: { polynomialOrder?: number; movingAverageWindow?: number },
  nFuturePadding?: number,
): { series: Record<string, unknown>; name: string } | null {
  if (seriesData.length < 2) {
    return null;
  }

  const dataPoints: DataPoint[] = seriesData.map((y, i) => ({
    x: i,
    y,
    xLabel: categories[i] ?? String(i),
  }));

  const fitted = generateTrendLine(dataPoints, method, options);
  if (!fitted) {
    return null;
  }

  const trendData: (number | null)[] = [...fitted];
  // Pad with nulls if the x-axis has been extended for forecast labels
  for (let i = 0; i < (nFuturePadding ?? 0); i++) {
    trendData.push(null);
  }

  const name = `${seriesName} (Trend)`;
  return {
    series: {
      name,
      type: 'line',
      data: trendData,
      smooth: false,
      showSymbol: false,
      lineStyle: { type: 'dotted', width: 2, color },
      itemStyle: { color },
      connectNulls: false,
      z: 4,
    },
    name,
  };
}

// Detect cycles in a directed graph using DFS. Returns true if a cycle exists.
function hasSankeyCycle(links: { source: string; target: string }[]): boolean {
  const adj = new Map<string, string[]>();
  for (const link of links) {
    if (!adj.has(link.source)) {
      adj.set(link.source, []);
    }
    adj.get(link.source)!.push(link.target);
  }
  const visited = new Set<string>();
  const inStack = new Set<string>();
  function dfs(node: string): boolean {
    visited.add(node);
    inStack.add(node);
    for (const neighbor of adj.get(node) ?? []) {
      if (inStack.has(neighbor)) {
        return true;
      }
      if (!visited.has(neighbor) && dfs(neighbor)) {
        return true;
      }
    }
    inStack.delete(node);
    return false;
  }
  for (const node of adj.keys()) {
    if (!visited.has(node) && dfs(node)) {
      return true;
    }
  }
  return false;
}

export default function ChartPreview({
  chartType,
  config,
  data,
  loading = false,
  height = 400,
  mini = false,
  sql,
  darkMode,
  onChartClick,
}: ChartPreviewProps) {
  const { isDark: globalDark } = useTheme();
  const isDark = darkMode ?? globalDark;
  const colors = colorSchemes[config.colorScheme || 'default'] || colorSchemes.default;
  const [mapReady, setMapReady] = useState(chartType !== 'choropleth');
  const mapReadyRef = useRef<string | null>(null);

  // Trigger async map loading when chart type or mapScope changes
  useEffect(() => {
    if (chartType !== 'choropleth') {
      setMapReady(true);
      mapReadyRef.current = null;
      return;
    }

    // Check if multi-select scopes are used
    const mapScopes = (config.chartOptions?.mapScopes as string[]) || [];
    let scopeKey = (config.chartOptions?.mapScope as string) || 'world';

    // Smart state-based loading: use individual state files instead of all-states merged
    // This dramatically reduces memory usage (1-3 MB per state vs 100+ MB for all)
    if (mapScopes.length > 0) {
      // Multi-select or single state: use first selected state's file
      // (User can filter data to their states, so no need for all-states)
      scopeKey = mapScopes[0];
    } else if (scopeKey.includes('-zipcodes')) {
      // Single state ZIP code selected
      scopeKey = scopeKey;  // Use the specific state file
    }

    const mapDef = getMapDef(scopeKey);
    if (!mapDef) {
      setMapReady(false);
      return;
    }

    // For world regions, we use the 'world' map
    const mapId = getActualMapId(scopeKey);

    // Avoid re-fetching if this exact map is already loaded
    if (mapReadyRef.current === mapId) {
      setMapReady(true);
      return;
    }

    setMapReady(false);
    mapReadyRef.current = null;

    ensureMapRegistered(mapId)
      .then(() => {
        setMapReady(true);
        mapReadyRef.current = mapId;
      })
      .catch((error) => {
        console.error(`Failed to load map ${mapId}:`, error);
        setMapReady(false);
        mapReadyRef.current = null;
      });
  }, [chartType, config.chartOptions?.mapScope, config.chartOptions?.mapScopes]);

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
      case 'map': {
        const dimCol = dimensions?.[0];
        if (dimCol && params.name != null) {
          emitClick(dimCol, String(params.name));
        }
        break;
      }
      case 'choropleth': {
        const dimCol = dimensions?.[0];
        if (dimCol && params.name != null) {
          emitClick(dimCol, String(params.name));
        }
        break;
      }
      case 'sankey': {
        const { xAxis: srcCol } = config;
        if (srcCol && params.name != null) {
          emitClick(srcCol, String(params.name));
        }
        break;
      }
      case 'radar': {
        const dimCol = groupField;
        if (dimCol && params.name != null) {
          emitClick(dimCol, String(params.name));
        }
        break;
      }
      case 'boxplot':
      case 'waterfall':
      case 'candlestick': {
        if (xCol && params.name != null) {
          emitClick(xCol, String(params.name));
        }
        break;
      }
      case 'sunburst': {
        const sbDim = config.dimensions?.[0];
        if (sbDim && params.name != null) {
          emitClick(sbDim, String(params.name));
        }
        break;
      }
      case 'calendar': {
        if (xCol && params.value != null) {
          const val = params.value as [string, number] | undefined;
          if (val) {
            emitClick(xCol, val[0]);
          }
        }
        break;
      }
      case 'bubble': {
        if (xCol) {
          const val = params.value;
          if (Array.isArray(val) && val.length > 0) {
            emitClick(xCol, String(val[0]));
          }
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

  // Pre-compute cycle detection for Sankey so the render can show a proper React error component
  const sankeyHasCycle = useMemo(() => {
    if (chartType !== 'sankey') {
      return false;
    }
    const { xAxis, yAxis, metrics } = config;
    if (!data?.rows || !xAxis || !yAxis || !metrics?.length) {
      return false;
    }
    const valueField = metrics[0];
    const links = data.rows
      .filter((row) => {
        const src = String(row[xAxis] ?? '').trim();
        const tgt = String(row[yAxis] ?? '').trim();
        return src && tgt && src !== tgt && Number(row[valueField]) > 0;
      })
      .map((row) => ({
        source: String(row[xAxis]).trim(),
        target: String(row[yAxis]).trim(),
      }));
    return hasSankeyCycle(links);
  }, [chartType, config, data]);

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

          // Forecast series for grouped line chart
          const allForecastSeries: Record<string, unknown>[] = [];
          const allBandNames: string[] = [];
          let lineFutureLabelsGrouped: string[] = [];
          series.forEach((s, idx) => {
            const sData = (s.data as (number | null)[]).map((v) => v ?? 0);
            const fc = buildForecastSeries(
              uniqueCategories, sData, s.name, colors[idx % colors.length],
              config.predictiveAnalytics, lineIsTemporal, false,
            );
            allForecastSeries.push(...fc.series);
            allBandNames.push(...fc.bandNames);
            if (fc.futureLabels.length > lineFutureLabelsGrouped.length) {
              lineFutureLabelsGrouped = fc.futureLabels;
            }
          });
          const extCatsGrouped = [...uniqueCategories, ...lineFutureLabelsGrouped];

          // Trend line series (independent of forecasting)
          const showTrend = config.chartOptions?.showTrendLine === true;
          const trendMethod = (config.chartOptions?.trendLineMethod as PredictionMethod) || 'linear';
          const trendOpts = {
            polynomialOrder: Number(config.chartOptions?.trendLinePolynomialOrder) || 2,
            movingAverageWindow: Number(config.chartOptions?.trendLineWindow) || 3,
          };
          const allTrendSeries: Record<string, unknown>[] = [];
          if (showTrend) {
            series.forEach((s, idx) => {
              const sData = (s.data as (number | null)[]).map((v) => v ?? 0);
              const tl = buildTrendLineSeries(
                uniqueCategories, sData, s.name, colors[idx % colors.length],
                trendMethod, trendOpts, lineFutureLabelsGrouped.length,
              );
              if (tl) {
                allTrendSeries.push(tl.series);
                allBandNames.push(tl.name);
              }
            });
          }

          return {
            tooltip: { trigger: 'axis' },
            legend: {
              data: groups.filter((g) => !allBandNames.includes(g)),
              bottom: 0,
            },
            xAxis: { type: 'category', data: extCatsGrouped },
            yAxis: { type: 'value' },
            series: [...series, ...allForecastSeries, ...allTrendSeries],
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

        // Forecast series for standard line chart
        const lineForecasts: Record<string, unknown>[] = [];
        const lineBandNames: string[] = [];
        let lineFutureLabels: string[] = [];
        series.forEach((s, idx) => {
          const sData = s.data as number[];
          const fc = buildForecastSeries(
            categories, sData, s.name, colors[idx % colors.length],
            config.predictiveAnalytics, lineIsTemporal, false,
          );
          lineForecasts.push(...fc.series);
          lineBandNames.push(...fc.bandNames);
          if (fc.futureLabels.length > lineFutureLabels.length) {
            lineFutureLabels = fc.futureLabels;
          }
        });
        const extCats = [...categories, ...lineFutureLabels];

        // Trend line series (independent of forecasting)
        const showLineTrend = config.chartOptions?.showTrendLine === true;
        const lineTrendMethod = (config.chartOptions?.trendLineMethod as PredictionMethod) || 'linear';
        const lineTrendOpts = {
          polynomialOrder: Number(config.chartOptions?.trendLinePolynomialOrder) || 2,
          movingAverageWindow: Number(config.chartOptions?.trendLineWindow) || 3,
        };
        const lineTrendSeries: Record<string, unknown>[] = [];
        if (showLineTrend) {
          series.forEach((s, idx) => {
            const sData = s.data as number[];
            const tl = buildTrendLineSeries(
              categories, sData, s.name, colors[idx % colors.length],
              lineTrendMethod, lineTrendOpts, lineFutureLabels.length,
            );
            if (tl) {
              lineTrendSeries.push(tl.series);
              lineBandNames.push(tl.name);
            }
          });
        }

        const legendNames = metrics.filter((m) => !lineBandNames.includes(m));

        return {
          tooltip: { trigger: 'axis' },
          legend: { data: legendNames, bottom: 0 },
          xAxis: { type: 'category', data: extCats },
          yAxis: { type: 'value' },
          series: [...series, ...lineForecasts, ...lineTrendSeries],
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

          // Forecast series for grouped area chart
          const areaForecastGrouped: Record<string, unknown>[] = [];
          const areaBandNamesGrouped: string[] = [];
          let areaFutureLabelsGrouped: string[] = [];
          series.forEach((s, idx) => {
            const sData = (s.data as (number | null)[]).map((v) => v ?? 0);
            const fc = buildForecastSeries(
              uniqueCategories, sData, s.name, colors[idx % colors.length],
              config.predictiveAnalytics, areaIsTemporal, true,
            );
            areaForecastGrouped.push(...fc.series);
            areaBandNamesGrouped.push(...fc.bandNames);
            if (fc.futureLabels.length > areaFutureLabelsGrouped.length) {
              areaFutureLabelsGrouped = fc.futureLabels;
            }
          });
          const extAreaCatsGrouped = [...uniqueCategories, ...areaFutureLabelsGrouped];

          // Trend line series (independent of forecasting)
          const showAreaGroupTrend = config.chartOptions?.showTrendLine === true;
          const areaGroupTrendMethod = (config.chartOptions?.trendLineMethod as PredictionMethod) || 'linear';
          const areaGroupTrendOpts = {
            polynomialOrder: Number(config.chartOptions?.trendLinePolynomialOrder) || 2,
            movingAverageWindow: Number(config.chartOptions?.trendLineWindow) || 3,
          };
          const areaGroupTrendSeries: Record<string, unknown>[] = [];
          if (showAreaGroupTrend) {
            series.forEach((s, idx) => {
              const sData = (s.data as (number | null)[]).map((v) => v ?? 0);
              const tl = buildTrendLineSeries(
                uniqueCategories, sData, s.name, colors[idx % colors.length],
                areaGroupTrendMethod, areaGroupTrendOpts, areaFutureLabelsGrouped.length,
              );
              if (tl) {
                areaGroupTrendSeries.push(tl.series);
                areaBandNamesGrouped.push(tl.name);
              }
            });
          }

          return {
            tooltip: { trigger: 'axis' },
            legend: {
              data: groups.filter((g) => !areaBandNamesGrouped.includes(g)),
              bottom: 0,
            },
            xAxis: { type: 'category', data: extAreaCatsGrouped },
            yAxis: { type: 'value' },
            series: [...series, ...areaForecastGrouped, ...areaGroupTrendSeries],
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

        // Forecast series for standard area chart
        const areaForecasts: Record<string, unknown>[] = [];
        const areaBandNames: string[] = [];
        let areaFutureLabels: string[] = [];
        series.forEach((s, idx) => {
          const sData = s.data as number[];
          const fc = buildForecastSeries(
            categories, sData, s.name, colors[idx % colors.length],
            config.predictiveAnalytics, areaIsTemporal, true,
          );
          areaForecasts.push(...fc.series);
          areaBandNames.push(...fc.bandNames);
          if (fc.futureLabels.length > areaFutureLabels.length) {
            areaFutureLabels = fc.futureLabels;
          }
        });
        const extAreaCats = [...categories, ...areaFutureLabels];

        // Trend line series (independent of forecasting)
        const showAreaTrend = config.chartOptions?.showTrendLine === true;
        const areaTrendMethod = (config.chartOptions?.trendLineMethod as PredictionMethod) || 'linear';
        const areaTrendOpts = {
          polynomialOrder: Number(config.chartOptions?.trendLinePolynomialOrder) || 2,
          movingAverageWindow: Number(config.chartOptions?.trendLineWindow) || 3,
        };
        const areaTrendSeries: Record<string, unknown>[] = [];
        if (showAreaTrend) {
          series.forEach((s, idx) => {
            const sData = s.data as number[];
            const tl = buildTrendLineSeries(
              categories, sData, s.name, colors[idx % colors.length],
              areaTrendMethod, areaTrendOpts, areaFutureLabels.length,
            );
            if (tl) {
              areaTrendSeries.push(tl.series);
              areaBandNames.push(tl.name);
            }
          });
        }

        const areaLegendNames = metrics.filter((m) => !areaBandNames.includes(m));

        return {
          tooltip: { trigger: 'axis' },
          legend: { data: areaLegendNames, bottom: 0 },
          xAxis: { type: 'category', data: extAreaCats },
          yAxis: { type: 'value' },
          series: [...series, ...areaForecasts, ...areaTrendSeries],
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

      case 'radar': {
        if (!metrics || metrics.length < 2) {
          return null;
        }
        const radarGroupField = dimensions?.[0];
        const doFill = config.chartOptions?.radarFill === true;
        const maxValues = metrics.map((m) =>
          Math.max(...data.rows.map((r) => Number(r[m]) || 0)) * 1.2 || 1
        );
        const indicator = metrics.map((m, i) => ({ name: m, max: maxValues[i] }));

        let radarData;
        if (radarGroupField) {
          radarData = data.rows.map((row, idx) => ({
            name: String(row[radarGroupField] ?? `Row ${idx + 1}`),
            value: metrics.map((m) => Number(row[m]) || 0),
            itemStyle: { color: colors[idx % colors.length] },
            lineStyle: { color: colors[idx % colors.length] },
            areaStyle: doFill ? { opacity: 0.15, color: colors[idx % colors.length] } : undefined,
          }));
        } else {
          const maxRows = Math.min(data.rows.length, 8);
          radarData = data.rows.slice(0, maxRows).map((row, idx) => ({
            name: `Row ${idx + 1}`,
            value: metrics.map((m) => Number(row[m]) || 0),
            itemStyle: { color: colors[idx % colors.length] },
            lineStyle: { color: colors[idx % colors.length] },
            areaStyle: doFill ? { opacity: 0.15, color: colors[idx % colors.length] } : undefined,
          }));
        }

        return {
          tooltip: { trigger: 'item' },
          legend: { data: radarData.map((d) => d.name), bottom: 0 },
          radar: { indicator },
          series: [{ type: 'radar', data: radarData }],
        };
      }

      case 'boxplot': {
        if (!xAxis || !metrics || metrics.length === 0) {
          return null;
        }
        const bpValueField = metrics[0];
        const bpGroups = new Map<string, number[]>();
        data.rows.forEach((row) => {
          const cat = String(row[xAxis] ?? '');
          const val = Number(row[bpValueField]) || 0;
          if (!bpGroups.has(cat)) {
            bpGroups.set(cat, []);
          }
          bpGroups.get(cat)!.push(val);
        });
        const bpCategories = Array.from(bpGroups.keys());
        const bpData = bpCategories.map((cat) => computeBoxStats(bpGroups.get(cat)!));
        return {
          tooltip: { trigger: 'item' },
          xAxis: { type: 'category', data: bpCategories },
          yAxis: { type: 'value', name: bpValueField },
          series: [{
            type: 'boxplot',
            data: bpData,
            itemStyle: { color: colors[0], borderColor: colors[0] },
          }],
          grid: { left: '3%', right: '4%', bottom: '10%', containLabel: true },
        };
      }

      case 'waterfall': {
        if (!xAxis || !metrics || metrics.length === 0) {
          return null;
        }
        const wfValueField = metrics[0];
        const wfCategories = data.rows.map((r) => String(r[xAxis] ?? ''));
        const wfValues = data.rows.map((r) => Number(r[wfValueField]) || 0);
        const showConnectors = config.chartOptions?.waterfallConnectors !== false;

        const wfBases: number[] = [];
        const wfDeltas: { value: number; itemStyle: { color: string } }[] = [];
        let wfRunning = 0;
        wfValues.forEach((v) => {
          wfBases.push(v >= 0 ? wfRunning : wfRunning + v);
          wfDeltas.push({ value: Math.abs(v), itemStyle: { color: v >= 0 ? '#52c41a' : '#ff4d4f' } });
          wfRunning += v;
        });

        const wfSeries: unknown[] = [
          {
            type: 'bar',
            stack: 'wf',
            silent: true,
            itemStyle: { borderColor: 'transparent', color: 'transparent' },
            emphasis: { itemStyle: { borderColor: 'transparent', color: 'transparent' } },
            data: wfBases,
          },
          {
            name: wfValueField,
            type: 'bar',
            stack: 'wf',
            data: wfDeltas,
            label: {
              show: true,
              position: 'top',
              formatter: (p: Record<string, unknown>) => {
                const i = p.dataIndex as number;
                return wfValues[i] >= 0 ? `+${wfValues[i]}` : String(wfValues[i]);
              },
            },
          },
        ];

        if (showConnectors) {
          const wfTotals: number[] = [];
          let rt = 0;
          wfValues.forEach((v) => { rt += v; wfTotals.push(rt); });
          wfSeries.push({
            type: 'line',
            data: wfTotals,
            symbol: 'circle',
            symbolSize: 5,
            lineStyle: { type: 'dashed', color: '#888', width: 1 },
            itemStyle: { color: '#888' },
            z: 10,
          });
        }

        return {
          tooltip: { trigger: 'axis' },
          xAxis: { type: 'category', data: wfCategories },
          yAxis: { type: 'value' },
          series: wfSeries,
          grid: { left: '3%', right: '4%', bottom: '10%', containLabel: true },
        } as unknown as EChartsOption;
      }

      case 'sunburst': {
        if (!dimensions || dimensions.length === 0 || !metrics || metrics.length === 0) {
          return null;
        }
        const sbValueField = metrics[0];
        const sbLevels = dimensions;
        const showSbLabels = config.chartOptions?.sunburstLabels !== false;

        const buildSbTree = (
          rows: typeof data.rows,
          levelIdx: number
        ): { name: string; value: number; itemStyle?: { color: string }; children?: unknown[] }[] => {
          if (levelIdx >= sbLevels.length) {
            return [];
          }
          const level = sbLevels[levelIdx];
          const groupMap = new Map<string, typeof data.rows>();
          rows.forEach((row) => {
            const key = String(row[level] ?? 'Unknown');
            if (!groupMap.has(key)) {
              groupMap.set(key, []);
            }
            groupMap.get(key)!.push(row);
          });
          return Array.from(groupMap.entries()).map(([name, groupRows], idx) => {
            const value = groupRows.reduce((acc, r) => acc + (Number(r[sbValueField]) || 0), 0);
            const children = buildSbTree(groupRows, levelIdx + 1);
            return {
              name,
              value,
              itemStyle: levelIdx === 0 ? { color: colors[idx % colors.length] } : undefined,
              children: children.length > 0 ? children : undefined,
            };
          });
        };

        return {
          tooltip: { trigger: 'item', formatter: '{b}: {c}' },
          series: [{
            type: 'sunburst',
            data: buildSbTree(data.rows, 0),
            radius: ['15%', '90%'],
            label: { show: showSbLabels, rotate: 'radial' },
            emphasis: { focus: 'ancestor' },
          }],
        } as unknown as EChartsOption;
      }

      case 'candlestick': {
        if (!xAxis || !metrics || metrics.length < 4) {
          return null;
        }
        const [csOpen, csClose, csLow, csHigh] = metrics;
        const csDates = data.rows.map((r) => String(r[xAxis] ?? ''));
        const csData = data.rows.map((r) => [
          Number(r[csOpen]) || 0,
          Number(r[csClose]) || 0,
          Number(r[csLow]) || 0,
          Number(r[csHigh]) || 0,
        ]);
        return {
          tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
          xAxis: { type: 'category', data: csDates, boundaryGap: true },
          yAxis: { type: 'value', scale: true },
          series: [{
            type: 'candlestick',
            data: csData,
            itemStyle: { color: '#52c41a', color0: '#ff4d4f', borderColor: '#52c41a', borderColor0: '#ff4d4f' },
          }],
          grid: { left: '3%', right: '4%', bottom: '10%', containLabel: true },
        };
      }

      case 'calendar': {
        if (!xAxis || !metrics || metrics.length === 0) {
          return null;
        }
        const calValueField = metrics[0];
        const calData = data.rows
          .filter((r) => r[xAxis] != null)
          .map((r) => [String(r[xAxis]), Number(r[calValueField]) || 0] as [string, number]);

        if (calData.length === 0) {
          return null;
        }

        const calValues = calData.map((d) => d[1]);
        const allDates = calData.map((d) => d[0]).sort();
        const calRange: string | [string, string] = allDates.length > 1
          ? [allDates[0], allDates[allDates.length - 1]]
          : allDates[0];

        return {
          tooltip: {
            formatter: (params: unknown) => {
              const p = params as Record<string, unknown>;
              const val = p.value as [string, number];
              return `${val[0]}: ${val[1].toLocaleString()}`;
            },
          },
          visualMap: {
            min: Math.min(...calValues),
            max: Math.max(...calValues),
            calculable: true,
            orient: 'horizontal',
            left: 'center',
            bottom: 0,
          },
          calendar: {
            range: calRange,
            cellSize: ['auto', 13],
            top: 50,
            itemStyle: { borderWidth: 0.5 },
          },
          series: [{ type: 'heatmap', coordinateSystem: 'calendar', data: calData }],
        } as unknown as EChartsOption;
      }

      case 'bubble': {
        if (!xAxis || !yAxis) {
          return null;
        }
        const bubbleSizeField = metrics?.[0];
        const bubbleGroupField = dimensions?.[0];
        const maxBubble = Number(config.chartOptions?.maxBubbleSize ?? 60);
        const sizeValues = bubbleSizeField ? data.rows.map((r) => Number(r[bubbleSizeField]) || 0) : [];
        const maxSizeVal = sizeValues.length > 0 ? Math.max(...sizeValues) || 1 : 1;

        const makePoint = (r: Record<string, unknown>) => {
          const sz = bubbleSizeField ? (Number(r[bubbleSizeField]) || 0) : maxBubble / 2;
          const normSz = bubbleSizeField ? Math.max(4, (sz / maxSizeVal) * maxBubble) : maxBubble / 2;
          return { value: [Number(r[xAxis]) || 0, Number(r[yAxis]) || 0, sz], symbolSize: normSz };
        };

        let bubbleSeries;
        if (bubbleGroupField) {
          const groups = [...new Set(data.rows.map((r) => String(r[bubbleGroupField] ?? '')))];
          bubbleSeries = groups.map((group, idx) => ({
            name: group,
            type: 'scatter' as const,
            data: data.rows.filter((r) => String(r[bubbleGroupField] ?? '') === group).map(makePoint),
            itemStyle: { color: colors[idx % colors.length], opacity: 0.7 },
          }));
        } else {
          bubbleSeries = [{
            type: 'scatter' as const,
            data: data.rows.map(makePoint),
            itemStyle: { color: colors[0], opacity: 0.7 },
          }];
        }

        return {
          tooltip: {
            trigger: 'item',
            formatter: (params: unknown) => {
              const p = params as Record<string, unknown>;
              const val = p.value as number[];
              let text = `${xAxis}: ${val[0]}<br/>${yAxis}: ${val[1]}`;
              if (bubbleSizeField) {
                text += `<br/>${bubbleSizeField}: ${val[2].toLocaleString()}`;
              }
              return text;
            },
          },
          legend: bubbleGroupField ? { data: bubbleSeries.map((s) => 'name' in s ? String(s.name) : ''), bottom: 0 } : undefined,
          xAxis: { type: 'value', name: xAxis },
          yAxis: { type: 'value', name: yAxis },
          series: bubbleSeries,
          grid: { left: '3%', right: '4%', bottom: bubbleGroupField ? '15%' : '5%', containLabel: true },
        };
      }

      case 'parallel': {
        if (!metrics || metrics.length < 2) {
          return null;
        }
        const parallelGroupField = dimensions?.[0];
        const lineOpacity = Number(config.chartOptions?.parallelOpacity ?? 0.4);
        const parallelAxis = metrics.map((m, i) => ({ dim: i, name: m }));

        let parallelSeries;
        if (parallelGroupField) {
          const groups = [...new Set(data.rows.map((r) => String(r[parallelGroupField] ?? '')))];
          parallelSeries = groups.map((group, idx) => ({
            type: 'parallel' as const,
            name: group,
            data: data.rows
              .filter((r) => String(r[parallelGroupField] ?? '') === group)
              .map((r) => metrics.map((m) => Number(r[m]) || 0)),
            lineStyle: { color: colors[idx % colors.length], opacity: lineOpacity, width: 1 },
          }));
        } else {
          parallelSeries = [{
            type: 'parallel' as const,
            data: data.rows.map((r) => metrics.map((m) => Number(r[m]) || 0)),
            lineStyle: { color: colors[0], opacity: lineOpacity, width: 1 },
          }];
        }

        return {
          tooltip: { trigger: 'item' },
          legend: parallelGroupField ? { data: parallelSeries.map((s) => 'name' in s ? String(s.name) : ''), bottom: 0 } : undefined,
          parallelAxis,
          parallel: { left: '5%', right: '5%', bottom: parallelGroupField ? '15%' : '10%', top: '10%' },
          series: parallelSeries,
        } as unknown as EChartsOption;
      }

      case 'sankey': {
        if (!xAxis || !yAxis || !metrics || metrics.length === 0) {
          return null;
        }
        const valueField = metrics[0];
        const orient = (config.chartOptions?.sankeyOrient as string) || 'horizontal';
        const showLabels = config.chartOptions?.sankeyShowLabels !== false;
        const curveness = Number(config.chartOptions?.sankeyCurveness ?? 0.5);
        const nodeWidth = Number(config.chartOptions?.sankeyNodeWidth ?? 20);

        // Collect unique node names from source and target columns
        const nodeNames = new Set<string>();
        data.rows.forEach((row) => {
          const src = String(row[xAxis] ?? '').trim();
          const tgt = String(row[yAxis] ?? '').trim();
          if (src) {
            nodeNames.add(src);
          }
          if (tgt) {
            nodeNames.add(tgt);
          }
        });

        const nodes = Array.from(nodeNames).map((name, idx) => ({
          name,
          itemStyle: { color: colors[idx % colors.length] },
        }));

        const links = data.rows
          .filter((row) => {
            const src = String(row[xAxis] ?? '').trim();
            const tgt = String(row[yAxis] ?? '').trim();
            // Exclude missing nodes, self-loops, and zero-value flows
            return src && tgt && src !== tgt && Number(row[valueField]) > 0;
          })
          .map((row) => ({
            source: String(row[xAxis]).trim(),
            target: String(row[yAxis]).trim(),
            value: Number(row[valueField]),
          }));

        if (nodes.length === 0 || links.length === 0) {
          return null;
        }

        if (hasSankeyCycle(links)) {
          return null;
        }

        return {
          tooltip: {
            trigger: 'item',
            formatter: (params: unknown) => {
              const p = params as Record<string, unknown>;
              if (p.dataType === 'node') {
                return String(p.name);
              }
              const d = p.data as Record<string, unknown>;
              return `${d.source} → ${d.target}: ${(d.value as number).toLocaleString()}`;
            },
          },
          series: [{
            type: 'sankey',
            orient,
            nodeWidth,
            data: nodes,
            links,
            emphasis: { focus: 'adjacency' },
            label: {
              show: showLabels,
              position: orient === 'vertical' ? 'top' : 'right',
            },
            lineStyle: {
              color: 'gradient',
              curveness,
            },
          }],
        } as unknown as EChartsOption;
      }

      case 'map': {
        if (!xAxis || !yAxis) {
          return null;
        }
        const pointSize = Number(config.chartOptions?.pointSize) || 8;
        const scaleByValue = config.chartOptions?.scaleByValue === true;
        const showBorders = config.chartOptions?.showBorders !== false;
        const enableRoam = config.chartOptions?.enableRoam !== false;
        const metricField = metrics?.[0];
        const labelField = dimensions?.[0];

        // Build scatter data: [lon, lat, value, label]
        const scatterData = data.rows.map((row) => {
          const lon = Number(row[xAxis]) || 0;
          const lat = Number(row[yAxis]) || 0;
          const val = metricField ? (Number(row[metricField]) || 0) : 0;
          const label = labelField ? String(row[labelField] ?? '') : '';
          return { value: [lon, lat, val], name: label };
        });

        // Compute symbol size function for scaling by value
        let symbolSizeFn: number | ((val: number[]) => number) = pointSize;
        if (scaleByValue && metricField) {
          const values = scatterData.map((d) => d.value[2]);
          const minVal = Math.min(...values);
          const maxVal = Math.max(...values);
          const range = maxVal - minVal || 1;
          symbolSizeFn = (val: number[]) => {
            const normalized = (val[2] - minVal) / range;
            return Math.max(4, pointSize * 0.5 + normalized * pointSize);
          };
        }

        return {
          tooltip: {
            trigger: 'item',
            formatter: (params: Record<string, unknown>) => {
              const val = params.value as number[];
              const name = params.name as string;
              const parts: string[] = [];
              if (name) {
                parts.push(`<strong>${name}</strong>`);
              }
              parts.push(`Lon: ${val[0].toFixed(4)}, Lat: ${val[1].toFixed(4)}`);
              if (metricField) {
                parts.push(`${metricField}: ${val[2].toLocaleString()}`);
              }
              return parts.join('<br/>');
            },
          },
          geo: {
            map: 'world',
            roam: enableRoam,
            itemStyle: {
              areaColor: '#e0e0e0',
              borderColor: showBorders ? '#aaa' : 'transparent',
              borderWidth: showBorders ? 0.5 : 0,
            },
            emphasis: {
              itemStyle: { areaColor: '#ccc' },
            },
            silent: true,
          },
          series: [{
            type: 'scatter',
            coordinateSystem: 'geo',
            data: scatterData,
            symbolSize: symbolSizeFn,
            itemStyle: { color: colors[0] },
            emphasis: { scale: true },
          }],
        } as unknown as EChartsOption;
      }

      case 'choropleth': {
        const dimCol = dimensions?.[0];
        const valueCol = metrics?.[0];
        if (!dimCol || !valueCol) {
          return null;
        }

        // Handle both single and multi-select scopes
        const mapScopes = (config.chartOptions?.mapScopes as string[]) || [];
        let scopeKey = (config.chartOptions?.mapScope as string) || 'world';

        // Smart state-based loading: use specific state files instead of merged all-states
        // This keeps memory usage low (1-3 MB per state vs 100+ MB for all)
        if (mapScopes.length > 0) {
          // Multi-select: use first selected state's file
          // User's data will naturally be filtered to their selected states
          scopeKey = mapScopes[0];
        } else if (scopeKey.includes('-zipcodes')) {
          // Single state ZIP code already selected
          scopeKey = scopeKey;
        }

        const mapDef = getMapDef(scopeKey);
        if (!mapDef) {
          return null;
        }

        // Get resolver function from map definition
        const resolverFn = mapDef.resolve;

        // Build choropleth data: array of { name: resolvedName, value: numericValue }
        const choroData = data.rows.map((row) => ({
          name: resolverFn(String(row[dimCol] ?? '')),
          value: Number(row[valueCol]) || 0,
        }));

        // Debug logging for choropleth rendering
        if (choroData.length > 0) {
          const displayScope = mapScopes.length > 1 ? `${mapScopes.length} states (showing ${mapScopes[0]})` : scopeKey;
          console.debug(`[Choropleth] scope=${displayScope}, mapId=${getActualMapId(scopeKey)}, dataRows=${choroData.length}`);
          console.debug(`[Choropleth] First 5 data points:`, choroData.slice(0, 5));
        }

        // Compute min/max for visualMap
        const values = choroData.map((d) => d.value).filter((v) => isFinite(v));
        const minVal = values.length ? Math.min(...values) : 0;
        const maxVal = values.length ? Math.max(...values) : 1;

        // Get color palette from config
        const paletteKey = (config.chartOptions?.choroplethColorScale as string) || 'blue';
        const palette = CHOROPLETH_PALETTES[paletteKey] ?? CHOROPLETH_PALETTES.blue;
        const enableRoam = config.chartOptions?.enableRoam !== false;

        return {
          tooltip: {
            trigger: 'item',
            formatter: (params: Record<string, unknown>) =>
              `${params.name}: ${params.value != null ? Number(params.value).toLocaleString() : 'N/A'}`,
          },
          visualMap: {
            min: minVal,
            max: maxVal,
            left: 'right',
            orient: 'vertical',
            calculable: true,
            inRange: { color: palette },
            text: [maxVal.toLocaleString(), minVal.toLocaleString()],
            textStyle: { color: isDark ? '#ccc' : '#333' },
          },
          series: [{
            type: 'map',
            map: getActualMapId(scopeKey),
            roam: enableRoam,
            center: mapDef.center,
            zoom: mapDef.zoom,
            data: choroData,
            nameProperty: 'name',
            itemStyle: {
              borderColor: isDark ? '#555' : '#ccc',
              borderWidth: 0.5,
              areaColor: isDark ? '#444' : '#f0f0f0',
            },
            emphasis: {
              label: { show: true, color: isDark ? '#fff' : '#333' },
              itemStyle: { areaColor: '#fac858' },
            },
            select: {
              label: { show: true },
              itemStyle: { areaColor: '#f4a732' },
            },
          }],
        } as unknown as EChartsOption;
      }

      default:
        return null;
    }
  }, [chartType, config, data, colors, isDark]);

  // Strip down chart chrome for mini mode
  const miniOption = useMemo(() => {
    if (!chartOption || !mini) {
      return chartOption;
    }

    // Calendar mini mode: drop chrome, keep the heatmap series
    if (chartType === 'calendar') {
      return {
        ...chartOption,
        tooltip: undefined,
        visualMap: undefined,
        calendar: {
          ...(chartOption as Record<string, unknown>).calendar as Record<string, unknown>,
          cellSize: 4,
          top: '5%',
          bottom: '5%',
          left: '2%',
          right: '2%',
        },
        animation: false,
      };
    }

    // Parallel mini mode: strip chrome only (no xAxis/yAxis to handle)
    if (chartType === 'parallel') {
      return { ...chartOption, tooltip: undefined, legend: undefined, animation: false };
    }

    // Map mini mode: simplified geo with smaller points and no roam
    if (chartType === 'map') {
      const geoRaw = (chartOption as Record<string, unknown>).geo as Record<string, unknown> | undefined;
      const seriesRaw = (chartOption as Record<string, unknown>).series as Record<string, unknown>[] | undefined;
      return {
        ...chartOption,
        tooltip: undefined,
        geo: geoRaw ? {
          ...geoRaw,
          roam: false,
        } : undefined,
        series: seriesRaw ? seriesRaw.map((s) => ({
          ...s,
          symbolSize: 4,
        })) : undefined,
        animation: false,
      };
    }

    // Choropleth mini mode: no roam, no tooltip
    if (chartType === 'choropleth') {
      const seriesRaw = (chartOption as Record<string, unknown>).series as Record<string, unknown>[] | undefined;
      return {
        ...chartOption,
        tooltip: undefined,
        visualMap: undefined,
        series: seriesRaw ? seriesRaw.map((s) => ({
          ...s,
          roam: false,
        })) : undefined,
        animation: false,
      };
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
  }, [chartOption, mini, chartType]);

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
      geo: (opt as Record<string, unknown>).geo ? {
        ...((opt as Record<string, unknown>).geo as Record<string, unknown>),
        itemStyle: { areaColor: '#1a1a2e', borderColor: '#333' },
        emphasis: { itemStyle: { areaColor: '#2a2a3e' } },
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

  // Pivot table mini-mode placeholder
  if (chartType === 'pivot' && mini) {
    return (
      <div style={{
        height,
        display: 'flex',
        flexDirection: 'column',
        padding: 6,
        gap: 2,
        overflow: 'hidden',
      }}>
        {[0, 1, 2, 3].map((r) => (
          <div key={r} style={{ display: 'flex', gap: 2, flex: 1 }}>
            {[0, 1, 2, 3].map((c) => (
              <div key={c} style={{
                flex: c === 0 ? 1.5 : 1,
                borderRadius: 2,
                backgroundColor: r === 0 || c === 0 ? colors[0] : 'var(--color-text)',
                opacity: r === 0 || c === 0 ? 0.4 : r % 2 === 0 ? 0.08 : 0.04,
              }} />
            ))}
          </div>
        ))}
      </div>
    );
  }

  // Pivot table full render (client-side cross-tabulation)
  if (chartType === 'pivot' && !mini) {
    const rowField = config.xAxis;
    const colField = config.yAxis;
    const valueField = config.metrics?.[0];

    if (!rowField || !colField || !valueField) {
      return (
        <div style={{ height, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <Empty description="Select Row Dimension, Column Pivot, and Value fields" />
        </div>
      );
    }

    const pivotAgg = (config.chartOptions?.pivotAggregation as string) || 'SUM';
    const showRowTotals = config.chartOptions?.showRowTotals !== false;
    const showColumnTotals = config.chartOptions?.showColumnTotals === true;
    const MAX_PIVOT_COLS = 50;

    // Collect unique row and column keys in insertion order
    const rowKeySet = new Set<string>();
    const colKeySet = new Set<string>();
    data.rows.forEach((row) => {
      rowKeySet.add(String(row[rowField] ?? '(null)'));
      colKeySet.add(String(row[colField] ?? '(null)'));
    });

    const rowKeys = Array.from(rowKeySet);
    const allColKeys = Array.from(colKeySet);
    const colKeys = allColKeys.slice(0, MAX_PIVOT_COLS);
    const truncated = allColKeys.length > MAX_PIVOT_COLS;

    // Build cell accumulator: rowKey → colKey → number[]
    const cellMap = new Map<string, Map<string, number[]>>();
    for (const rk of rowKeys) {
      cellMap.set(rk, new Map());
    }
    data.rows.forEach((row) => {
      const rk = String(row[rowField] ?? '(null)');
      const ck = String(row[colField] ?? '(null)');
      if (!colKeySet.has(ck) || colKeys.indexOf(ck) < 0) {
        return;
      }
      const val = Number(row[valueField]);
      if (isNaN(val)) {
        return;
      }
      const colMap = cellMap.get(rk);
      if (!colMap) {
        return;
      }
      const existing = colMap.get(ck) ?? [];
      existing.push(val);
      colMap.set(ck, existing);
    });

    const aggregate = (values: number[]): number | null => {
      if (values.length === 0) {
        return null;
      }
      switch (pivotAgg) {
        case 'AVG': return values.reduce((a, b) => a + b, 0) / values.length;
        case 'COUNT': return values.length;
        case 'MIN': return Math.min(...values);
        case 'MAX': return Math.max(...values);
        default: return values.reduce((a, b) => a + b, 0); // SUM
      }
    };

    const formatVal = (v: number | null): string => {
      if (v == null) {
        return '–';
      }
      return v.toLocaleString(undefined, { maximumFractionDigits: 2 });
    };

    // Build table rows
    const tableRows: Record<string, unknown>[] = rowKeys.map((rk) => {
      const colMap = cellMap.get(rk)!;
      const rowData: Record<string, unknown> = { _rowKey: rk };
      let rowTotal = 0;
      for (const ck of colKeys) {
        const agg = aggregate(colMap.get(ck) ?? []);
        rowData[`__col__${ck}`] = agg;
        if (agg != null) {
          rowTotal += agg;
        }
      }
      rowData._rowTotal = rowTotal;
      return rowData;
    });

    // Column totals row (appended last if enabled)
    const colTotals: Record<string, number> = {};
    let grandTotal = 0;
    for (const ck of colKeys) {
      let colTotal = 0;
      for (const rk of rowKeys) {
        const agg = aggregate(cellMap.get(rk)?.get(ck) ?? []);
        if (agg != null) {
          colTotal += agg;
        }
      }
      colTotals[ck] = colTotal;
      grandTotal += colTotal;
    }
    if (showColumnTotals) {
      const totalRow: Record<string, unknown> = {
        _rowKey: 'Total',
        _rowTotal: grandTotal,
        _isTotal: true,
      };
      for (const ck of colKeys) {
        totalRow[`__col__${ck}`] = colTotals[ck];
      }
      tableRows.push(totalRow);
    }

    const pivotColumns = [
      {
        title: rowField,
        dataIndex: '_rowKey',
        key: '_rowKey',
        fixed: 'left' as const,
        width: 160,
        render: (v: unknown, record: Record<string, unknown>) =>
          record._isTotal
            ? <strong>{String(v)}</strong>
            : String(v),
        sorter: (a: Record<string, unknown>, b: Record<string, unknown>) =>
          String(a._rowKey ?? '').localeCompare(String(b._rowKey ?? '')),
      },
      ...colKeys.map((ck) => ({
        title: ck,
        dataIndex: `__col__${ck}`,
        key: `__col__${ck}`,
        align: 'right' as const,
        render: (v: number | null, record: Record<string, unknown>) =>
          record._isTotal
            ? <strong>{formatVal(v)}</strong>
            : formatVal(v),
        sorter: (a: Record<string, unknown>, b: Record<string, unknown>) => {
          const av = (a[`__col__${ck}`] as number) ?? -Infinity;
          const bv = (b[`__col__${ck}`] as number) ?? -Infinity;
          return av - bv;
        },
      })),
      ...(showRowTotals ? [{
        title: 'Total',
        dataIndex: '_rowTotal',
        key: '_rowTotal',
        align: 'right' as const,
        fixed: 'right' as const,
        width: 100,
        render: (v: number) => <strong>{formatVal(v)}</strong>,
        sorter: (a: Record<string, unknown>, b: Record<string, unknown>) =>
          (a._rowTotal as number ?? 0) - (b._rowTotal as number ?? 0),
      }] : []),
    ];

    // Generate equivalent Drill SQL PIVOT query for the "Copy SQL" button
    const drillPivotSql = (() => {
      if (!data.rows.length) {
        return null;
      }
      const quotedCol = (c: string) => /[^a-zA-Z0-9_]/.test(c) ? `\`${c}\`` : c;
      const quotedVal = (v: string) => `'${v.replace(/'/g, "''")}'`;
      const inList = colKeys.map((ck) => quotedVal(ck)).join(', ');
      const cleanSql = (sql ?? 'SELECT * FROM your_table').replace(/;\s*$/, '').trim();
      return `SELECT *\nFROM (\n  ${cleanSql}\n) AS _t\nPIVOT (${pivotAgg}(${quotedCol(valueField)}) FOR ${quotedCol(colField)} IN (${inList}))`;
    })();

    const handleCopyPivotSql = () => {
      if (!drillPivotSql) {
        return;
      }
      navigator.clipboard.writeText(drillPivotSql).then(() => {
        message.success('Drill PIVOT SQL copied to clipboard');
      });
    };

    const toolbarHeight = 36;
    const scrollY = typeof height === 'number'
      ? height - toolbarHeight - (truncated ? 80 : 40)
      : 'calc(100% - 76px)';

    return (
      <div style={{ height, display: 'flex', flexDirection: 'column' }}>
        <div style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          padding: '4px 8px',
          flexShrink: 0,
          borderBottom: '1px solid var(--color-border)',
          height: toolbarHeight,
        }}>
          <Typography.Text type="secondary" style={{ fontSize: 12 }}>
            {rowKeys.length} rows &times; {colKeys.length} columns
            {truncated ? ` (truncated from ${allColKeys.length})` : ''}
          </Typography.Text>
          {drillPivotSql && (
            <Tooltip title="Copy the equivalent Drill SQL PIVOT statement">
              <Button size="small" icon={<CopyOutlined />} onClick={handleCopyPivotSql}>
                Copy as SQL PIVOT
              </Button>
            </Tooltip>
          )}
        </div>
        {truncated && (
          <Alert
            type="warning"
            banner
            message={`Showing first ${MAX_PIVOT_COLS} of ${allColKeys.length} pivot columns`}
            style={{ flexShrink: 0 }}
          />
        )}
        <Table
          dataSource={tableRows.map((row, idx) => ({ ...row, key: idx }))}
          columns={pivotColumns}
          scroll={{ x: 'max-content', y: scrollY }}
          size="small"
          pagination={false}
          bordered
          rowClassName={(record: Record<string, unknown>) =>
            record._isTotal ? 'pivot-total-row' : ''
          }
        />
      </div>
    );
  }

  if (sankeyHasCycle) {
    if (mini) {
      return null;
    }
    return (
      <div style={{ height, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 24 }}>
        <Alert
          type="warning"
          showIcon
          message="Sankey cycle detected"
          description="Sankey diagrams require a directed acyclic graph (DAG). Your data contains bidirectional flows (e.g., A → B and B → A). Check the source and target columns for cycles."
        />
      </div>
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

  // Gate choropleth rendering on map being ready
  if (!mapReady && chartType === 'choropleth') {
    return (
      <div style={{ height, width: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <Spin tip="Loading map..." />
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
