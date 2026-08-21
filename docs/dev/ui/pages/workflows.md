# Workflows

**File:** `src/pages/WorkflowsPage.tsx`
**Routes:** `/workflows` (cross-project), `/projects/:id/workflows` (project-scoped via `ProjectWorkflowsPage`)

## Purpose

Manage scheduled queries. Shows schedules bucketed by proximity (Now, Today, Tomorrow, This Week, Later, Inactive, Expired) with frequency, next run, run count, alert configuration, and AI-generated data quality snapshots.

## Data sources

| API | Module | Used for |
|---|---|---|
| `GET /api/v1/schedules` (`getSchedules`) | `api/schedules.ts` | Schedule list |
| `DELETE /api/v1/schedules/:id` (`deleteSchedule`) | `api/schedules.ts` | Remove |
| `POST /api/v1/schedules/:id/renew` (`renewSchedule`) | `api/schedules.ts` | Extend expiring/expired |
| `POST /api/v1/schedules/:id/run` (`runScheduleNow`) | `api/schedules.ts` | Manual trigger |
| `PUT /api/v1/schedules/:id` (`updateSchedule`) | `api/schedules.ts` | Edit |
| `GET /api/v1/schedules/:id/snapshots` (`getSnapshots`) | `api/schedules.ts` | Data quality history |
| `GET /api/v1/schedules/config` (`getScheduleConfig`) | `api/schedules.ts` | Global settings |
| `PUT /api/v1/schedules/config` (`updateScheduleConfig`) | `api/schedules.ts` | Global settings |
| `GET /api/v1/savedQueries/:id` (`getSavedQuery`) | `api/savedQueries.ts` | Resolve schedule → saved query name + SQL |

## Child components

- Schedule cards (one per active schedule) grouped under bucket headers.
- `ScheduleModal` — create / edit a schedule (cron, alerts, snapshots, retention).
- Snapshot list under each card (collapsible) showing past runs and AI-generated summaries.
- Alert-config panel per schedule.
- Refresh-options dropdown (auto-refresh interval for the list view).

## Key state

- `activeSchedules` — derived from `getSchedules` and bucketed
- `searchText`
- `filter` — `'all' | 'active' | 'paused' | 'expiring' | 'alerts' | 'data'`
- Per-schedule local state: `isRunning`, `isExpiring`, `alerts`, `snapshots`
- `refreshIntervalMs` for the list auto-refetch

## Behavior

- **Bucketing.** Schedules are grouped by their next-run time relative to now. Expired and inactive get their own buckets at the bottom.
- **Schedule formatting.** Human readable: "Daily at 14:30", "Mondays at 10:00", "1st of month at 09:00" derived from the cron expression.
- **Run now.** Triggers `runScheduleNow` and polls until completion; the card shows a running indicator.
- **Snapshots.** When data-quality snapshots are configured, runs produce JSON snapshots with optional AI summaries; collapsible per schedule.
- **Renewal.** Expired schedules can be renewed in place; the renew cost is shown as an AntD `Statistic`.

## Chrome

Breadcrumb: `Library > Workflows`. Toolbar: filter chips, refresh interval picker, settings cog (cross-project only — `hideSettings` prop hides it for project scope).

## Quirks

- The page accepts optional props `filterSavedQueryIds?: string[]` and `hideSettings?: boolean`. `ProjectWorkflowsPage` passes both: filter by project's saved queries, hide the global settings button.
- "Alerts" filter shows schedules with at least one configured alert; "Data" shows schedules with snapshots configured. These are stored on the schedule itself, not a separate resource.

---

## ProjectWorkflowsPage

**File:** `src/pages/ProjectWorkflowsPage.tsx`
**Route:** `/projects/:id/workflows`

Thin wrapper. Passes `filterSavedQueryIds={project?.savedQueryIds}` and `hideSettings` to `WorkflowsPage`. No additional UI.
