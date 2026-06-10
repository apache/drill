# Metrics

**File:** `src/pages/MetricsPage.tsx`
**Route:** `/metrics`

## Purpose

Server performance dashboard. Memory, CPU, query throughput, Drillbit status, GC pause distribution, network I/O, planner cost metrics. Trend indicators (up/down/stable) computed from the previous snapshot.

## Data sources

| API | Module |
|---|---|
| `GET /api/v1/metrics` (`getMetrics`) | `api/metrics.ts` |

Polled via `useQuery` with `refetchInterval` (default a few seconds — adjust at the call site).

## Child components

- AntD `Statistic` cards (query count, CPU %, memory %)
- AntD `Progress` bars (memory thresholds)
- ECharts gauges and line charts (throughput, GC pauses, network)
- AntD `Table` for the Drillbit list

## Key state

- Parsed metric response → gauges (memory, cpu, queueLength), counters (queryCount, rowsRead), histograms (gcPauseMs, gcCount)
- `getTrend(current, previous)` for arrow indicators

## Behavior

- **Threshold colors.** Green < 70%, yellow 70–85%, red > 85%.
- **Trend arrows.** Up / down / stable with a delta value.
- **Drillbit table.** Address, heap MB, direct MB, query count, uptime.
- **GC histogram.** Pause durations in ms.
- **Network throughput.** Bytes in / out.

## Chrome

Breadcrumb: `Administration > Metrics`. No toolbar actions, no inspector tabs, no left rail.

## Quirks

- Histograms can be empty on a fresh Drillbit; the components gracefully render placeholders.
- The previous snapshot for trend computation is held in a ref — it survives across refetches but resets on full page reload.
