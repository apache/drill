# AI Analytics

**File:** `src/pages/AiAnalyticsPage.tsx`
**Route:** `/ai-analytics`

## Purpose

Admin-only dashboard for AI / LLM feature usage and cost. Per-feature event log, per-provider/model pricing config, date-range summary. Use this to track Prospector / Transpiler / SQL formatting cost over time.

## Data sources

| API | Module |
|---|---|
| `GET /api/v1/ai-analytics/status` (`getAnalyticsStatus`) | `api/aiAnalytics.ts` |
| `POST /api/v1/ai-analytics/setup` (`setupAnalytics`) | `api/aiAnalytics.ts` |
| `GET /api/v1/ai-analytics/summary` (`getAnalyticsSummary`) | `api/aiAnalytics.ts` |
| `GET /api/v1/ai-analytics/events` (`getAnalyticsEvents`) | `api/aiAnalytics.ts` |
| `GET /api/v1/ai-analytics/pricing` (`listPricing`) | `api/aiAnalytics.ts` |
| `PUT /api/v1/ai-analytics/pricing` (`upsertPricing`) | `api/aiAnalytics.ts` |
| `DELETE /api/v1/ai-analytics/pricing/{id}` (`deletePricing`) | `api/aiAnalytics.ts` |
| Current user (`useCurrentUser`) — admin gate | `hooks/useCurrentUser.ts` |

## Child components

- AntD `Statistic` cards (total cost, request count, unique features used)
- AntD `RangePicker` for date scope
- AntD `Table` for the pricing list (provider / model / feature / cost-per-input-token / cost-per-output-token)
- ECharts line / bar chart for events over time
- AntD `Collapse` for per-feature drill-down

## Key state

- `dateRange` — Dayjs `[start, end]` from RangePicker
- `pricingList` — from `listPricing`
- `summary` — aggregated stats for the range
- `events` — detailed event entries
- Admin gate from `useCurrentUser`

## Behavior

- **Setup.** If analytics isn't initialized (`getAnalyticsStatus` returns disabled), the page shows a one-click setup. Once set up, events are written to a JSONL log and queryable via Drill itself — backend details in [`../AI_FEATURES.md`](../AI_FEATURES.md) and the project memory notes.
- **Server-side only.** Every event is recorded by the server, one per LLM call, with `source` always `"server"`. There is no client-side event log or endpoint for the browser to report its own AI usage — the server is the sole source of truth.
- **Not configured vs. no usage.** `/summary` and `/events` return `notConfigured: true` (rather than empty arrays that look like zero usage) whenever the data source isn't readable: `DRILL_LOG_DIR` unset, the `dfs.ai_logs` workspace/format unregistered, or no `ai-events*.log` written yet. The first two are genuine misconfiguration and get their own alerts on the page, driven by `/status`. Once `/status` reports `ready`, `notConfigured` can only mean the third case — set up but idle — so the page says "No AI events captured yet" and still renders the dashboard and events table rather than claiming the deployment is unconfigured.
- **Pricing rows** are editable inline. Each save triggers `upsertPricing` and an invalidate.
- **Feature labels** identify which UI surface originated the call — see [`PROSPECTOR.md`](../../PROSPECTOR.md#chat-endpoint) for the `context.feature` contract. The label map lives in [`src/constants/aiFeatures.ts`](../../../../exec/java-exec/src/main/resources/webapp/src/constants/aiFeatures.ts) and drives both the feature column and the events feature filter, so a slug missing from it is unselectable in the UI. Enforcement is type-level, not a scanner: `FEATURE_LABEL`'s keys form the `AiFeature` union, and `ChatContext.feature` is typed as `AiFeature` (required, no default), so `tsc` rejects an unlabelled or misspelled slug at the call site that emits it. Server-emitted slugs (`config_test`, `transpile`, `prospector_chat`) have no TypeScript literal to check against and stay manually maintained. As implemented, the values are:

  | Feature | Origin |
  |---|---|
  | `sql_lab_chat` | SQL Lab chat panel |
  | `sql_lab_optimize` | SQL Lab "Optimize query" |
  | `log_analysis` | Logs page chat |
  | `wiki_generation` | Project wiki generation |
  | `profile_analysis` | Query profile detail page |
  | `global_chat` | Global Prospector tab (shell) |
  | `query_suggestions` | Query suggestions panel |
  | `explain_query` | Explain Query action |
  | `optimize_query` | Optimize Query action |
  | `dashboard_qna` | Dashboard Q&A panel |
  | `executive_summary` | Dashboard executive summary panel |
  | `nl_filter` | Dashboard natural-language filter panel |
  | `ai_alerts` | Dashboard AI alerts panel |
  | `filesystem_form` | Filesystem storage-plugin form assistant |
  | `config_test` | Server-side: "Test Connection" in AI config |
  | `transpile` | Server-side: SQL dialect transpiler |
  | `prospector_chat` | Default when a caller sends no `feature` (wire compatibility with older clients) |

- **Cost formatting.** `fmtMoney` (USD by default), `fmtNum` for counts.
- **Call outcomes.** Each event is exactly one of success, failure, or cancelled; `successCount + failureCount + cancelledCount == totalCalls` for any summary window.
- **Empty state.** Non-admins see an empty state explaining they need admin/owner privileges.

## Chrome

Breadcrumb: `Administration > AI Analytics`. Toolbar: range picker, "Export CSV" (events).

## Quirks

- Pricing is per (provider, model, feature) — a model used for two features can have two different prices.
- Costs are computed client-side from event token counts × pricing rows; if a pricing row is missing for a (provider, model) combination, that event shows as `—` instead of $0.
- Events accumulate forever in the JSONL log; the page does not paginate beyond the date range filter, so very long ranges over busy clusters can be slow.
