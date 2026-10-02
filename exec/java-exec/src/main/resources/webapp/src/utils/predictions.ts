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

import regression from 'regression';
import dayjs from 'dayjs';
import type { PredictiveAnalyticsConfig, PredictionMethod } from '../types';

export interface DataPoint {
  x: number;
  y: number;
  xLabel: string;
}

export interface PredictionResult {
  forecastPoints: { x: number; y: number }[];
  confidenceBands: { x: number; lower: number; upper: number }[];
  r2?: number;
}

/**
 * Look up the z critical value for a given confidence level.
 */
function getCriticalValue(confidenceLevel: number): number {
  if (confidenceLevel <= 0.80) {
    return 1.282;
  }
  if (confidenceLevel <= 0.90) {
    return 1.645;
  }
  return 1.960;
}

/**
 * Compute standard error of the residuals.
 */
function computeStandardError(actual: number[], predicted: number[]): number {
  const n = actual.length;
  if (n <= 2) {
    return 0;
  }
  const sumSquaredResiduals = actual.reduce((sum, yi, i) => {
    const diff = yi - predicted[i];
    return sum + diff * diff;
  }, 0);
  return Math.sqrt(sumSquaredResiduals / (n - 2));
}

/**
 * Calculate confidence bands that widen as forecast extends further from data.
 */
function calculateConfidenceBands(
  forecastPoints: { x: number; y: number }[],
  standardError: number,
  confidenceLevel: number,
  n: number,
  meanX: number,
  sumSquaredDevX: number,
): { x: number; lower: number; upper: number }[] {
  const z = getCriticalValue(confidenceLevel);
  return forecastPoints.map((pt) => {
    const leverage = 1 + 1 / n + (sumSquaredDevX > 0
      ? ((pt.x - meanX) ** 2) / sumSquaredDevX
      : 0);
    const margin = z * standardError * Math.sqrt(leverage);
    return {
      x: pt.x,
      lower: pt.y - margin,
      upper: pt.y + margin,
    };
  });
}

/**
 * Linear regression prediction.
 */
function predictLinear(
  dataPoints: DataPoint[],
  periods: number,
  confidenceLevel: number,
): PredictionResult | null {
  if (dataPoints.length < 2) {
    return null;
  }

  const points: [number, number][] = dataPoints.map((dp) => [dp.x, dp.y]);
  const result = regression.linear(points);

  const n = dataPoints.length;
  const lastX = dataPoints[n - 1].x;
  const step = n > 1 ? (lastX - dataPoints[0].x) / (n - 1) : 1;

  const forecastPoints: { x: number; y: number }[] = [];
  for (let i = 1; i <= periods; i++) {
    const futureX = lastX + i * step;
    const predicted = result.predict(futureX);
    forecastPoints.push({ x: futureX, y: predicted[1] });
  }

  // Compute confidence bands
  const predicted = dataPoints.map((dp) => result.predict(dp.x)[1]);
  const actual = dataPoints.map((dp) => dp.y);
  const se = computeStandardError(actual, predicted);
  const meanX = dataPoints.reduce((s, dp) => s + dp.x, 0) / n;
  const sumSqDevX = dataPoints.reduce((s, dp) => s + (dp.x - meanX) ** 2, 0);

  const confidenceBands = calculateConfidenceBands(
    forecastPoints, se, confidenceLevel, n, meanX, sumSqDevX,
  );

  return { forecastPoints, confidenceBands, r2: result.r2 };
}

/**
 * Polynomial regression prediction.
 */
function predictPolynomial(
  dataPoints: DataPoint[],
  periods: number,
  order: number,
  confidenceLevel: number,
): PredictionResult | null {
  if (dataPoints.length < order + 1) {
    return null;
  }

  const points: [number, number][] = dataPoints.map((dp) => [dp.x, dp.y]);
  const result = regression.polynomial(points, { order });

  const n = dataPoints.length;
  const lastX = dataPoints[n - 1].x;
  const step = n > 1 ? (lastX - dataPoints[0].x) / (n - 1) : 1;

  const forecastPoints: { x: number; y: number }[] = [];
  for (let i = 1; i <= periods; i++) {
    const futureX = lastX + i * step;
    const predicted = result.predict(futureX);
    forecastPoints.push({ x: futureX, y: predicted[1] });
  }

  const predicted = dataPoints.map((dp) => result.predict(dp.x)[1]);
  const actual = dataPoints.map((dp) => dp.y);
  const se = computeStandardError(actual, predicted);
  const meanX = dataPoints.reduce((s, dp) => s + dp.x, 0) / n;
  const sumSqDevX = dataPoints.reduce((s, dp) => s + (dp.x - meanX) ** 2, 0);

  const confidenceBands = calculateConfidenceBands(
    forecastPoints, se, confidenceLevel, n, meanX, sumSqDevX,
  );

  return { forecastPoints, confidenceBands, r2: result.r2 };
}

/**
 * Moving average prediction.
 */
function predictMovingAverage(
  dataPoints: DataPoint[],
  periods: number,
  windowSize: number,
  confidenceLevel: number,
): PredictionResult | null {
  const n = dataPoints.length;
  if (n < windowSize) {
    return null;
  }

  const lastX = dataPoints[n - 1].x;
  const step = n > 1 ? (lastX - dataPoints[0].x) / (n - 1) : 1;

  // Calculate the moving average value from the last windowSize points
  const windowValues = dataPoints.slice(-windowSize).map((dp) => dp.y);
  const maValue = windowValues.reduce((s, v) => s + v, 0) / windowSize;

  // Compute variance over the sliding window for confidence
  const variance = windowValues.reduce((s, v) => s + (v - maValue) ** 2, 0) / windowSize;
  const se = Math.sqrt(variance);
  const z = getCriticalValue(confidenceLevel);

  const forecastPoints: { x: number; y: number }[] = [];
  const confidenceBands: { x: number; lower: number; upper: number }[] = [];

  for (let i = 1; i <= periods; i++) {
    const futureX = lastX + i * step;
    forecastPoints.push({ x: futureX, y: maValue });
    // Bands widen slightly with distance
    const margin = z * se * Math.sqrt(1 + i / n);
    confidenceBands.push({
      x: futureX,
      lower: maValue - margin,
      upper: maValue + margin,
    });
  }

  return { forecastPoints, confidenceBands };
}

/**
 * Main entry point: generate predictions based on the configured method.
 */
export function generatePredictions(
  dataPoints: DataPoint[],
  config: PredictiveAnalyticsConfig,
): PredictionResult | null {
  if (!config.enabled || dataPoints.length < 2) {
    return null;
  }

  const confidenceLevel = config.confidenceLevel ?? 0.95;

  switch (config.method) {
    case 'linear':
      return predictLinear(dataPoints, config.periods, confidenceLevel);
    case 'polynomial':
      return predictPolynomial(
        dataPoints,
        config.periods,
        config.polynomialOrder ?? 2,
        confidenceLevel,
      );
    case 'movingAverage':
      return predictMovingAverage(
        dataPoints,
        config.periods,
        config.movingAverageWindow ?? 3,
        confidenceLevel,
      );
    default:
      return null;
  }
}

/**
 * Detect the temporal interval between date labels.
 * Returns a dayjs unit string or null if not detected.
 */
function detectTemporalInterval(labels: string[]): { amount: number; unit: dayjs.ManipulateType } | null {
  if (labels.length < 2) {
    return null;
  }

  const d1 = dayjs(labels[0]);
  const d2 = dayjs(labels[1]);
  if (!d1.isValid() || !d2.isValid()) {
    return null;
  }

  const diffDays = d2.diff(d1, 'day');
  const diffMonths = d2.diff(d1, 'month');
  const diffYears = d2.diff(d1, 'year');

  if (diffYears >= 1) {
    return { amount: diffYears, unit: 'year' };
  }
  if (diffMonths >= 1) {
    return { amount: diffMonths, unit: 'month' };
  }
  if (diffDays >= 7 && diffDays % 7 === 0) {
    return { amount: diffDays / 7, unit: 'week' };
  }
  if (diffDays >= 1) {
    return { amount: diffDays, unit: 'day' };
  }

  const diffHours = d2.diff(d1, 'hour');
  if (diffHours >= 1) {
    return { amount: diffHours, unit: 'hour' };
  }

  return null;
}

/**
 * Generate labels for the forecast region on the x-axis.
 */
export function generateFutureLabels(
  existingLabels: string[],
  periods: number,
  isTemporal: boolean,
): string[] {
  if (periods <= 0 || existingLabels.length === 0) {
    return [];
  }

  // Temporal label generation
  if (isTemporal) {
    const interval = detectTemporalInterval(existingLabels);
    if (interval) {
      const lastDate = dayjs(existingLabels[existingLabels.length - 1]);
      if (lastDate.isValid()) {
        const labels: string[] = [];
        for (let i = 1; i <= periods; i++) {
          const futureDate = lastDate.add(i * interval.amount, interval.unit);
          // Match the format of existing labels (heuristic)
          const sample = existingLabels[existingLabels.length - 1];
          if (sample.includes('T') || sample.includes(' ')) {
            labels.push(futureDate.format('YYYY-MM-DD HH:mm'));
          } else {
            labels.push(futureDate.format('YYYY-MM-DD'));
          }
        }
        return labels;
      }
    }
  }

  // Numeric step detection
  if (existingLabels.length >= 2) {
    const lastNum = Number(existingLabels[existingLabels.length - 1]);
    const prevNum = Number(existingLabels[existingLabels.length - 2]);
    if (!isNaN(lastNum) && !isNaN(prevNum)) {
      const step = lastNum - prevNum;
      const labels: string[] = [];
      for (let i = 1; i <= periods; i++) {
        labels.push(String(lastNum + i * step));
      }
      return labels;
    }
  }

  // Fallback
  const labels: string[] = [];
  for (let i = 1; i <= periods; i++) {
    labels.push(`Forecast ${i}`);
  }
  return labels;
}

/**
 * Generate a trend line (best-fit curve) over historical data.
 * Works independently of forecasting.
 */
export function generateTrendLine(
  dataPoints: DataPoint[],
  method: PredictionMethod,
  options?: { polynomialOrder?: number; movingAverageWindow?: number },
): number[] | null {
  if (dataPoints.length < 2) {
    return null;
  }

  const points: [number, number][] = dataPoints.map((dp) => [dp.x, dp.y]);

  switch (method) {
    case 'linear': {
      const result = regression.linear(points);
      return dataPoints.map((dp) => result.predict(dp.x)[1]);
    }
    case 'polynomial': {
      const order = options?.polynomialOrder ?? 2;
      if (dataPoints.length < order + 1) {
        return null;
      }
      const result = regression.polynomial(points, { order });
      return dataPoints.map((dp) => result.predict(dp.x)[1]);
    }
    case 'movingAverage': {
      const windowSize = options?.movingAverageWindow ?? 3;
      if (dataPoints.length < windowSize) {
        return null;
      }
      return dataPoints.map((_, i) => {
        const start = Math.max(0, i - windowSize + 1);
        const window = dataPoints.slice(start, i + 1);
        return window.reduce((s, dp) => s + dp.y, 0) / window.length;
      });
    }
    default:
      return null;
  }
}
