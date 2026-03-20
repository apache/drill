# SQL Lab Query Scheduler

The SQL Lab Query Scheduler allows users to schedule saved queries to run automatically on a recurring basis. Results are tracked as snapshots with status, row counts, and execution duration.

## Architecture

The scheduler has three components:

1. **ScheduleResources** (`/api/v1/schedules`) — REST API for creating, updating, and managing schedules and viewing run history (snapshots)
2. **ScheduleManager** — A singleton background service that checks every 60 seconds for queries that are due to run, executes them, and records results
3. **WorkflowConfigResources** (`/api/v1/workflows/config`) — Admin-only REST API for configuring global scheduler settings like expiration policy

## Startup and Shutdown

The ScheduleManager starts automatically when Drill boots. In `Drillbit.java`:

```
webServer.start();
ScheduleManager.getOrCreate(manager, storeProvider).start();
```

It runs on a daemon thread named `drill-schedule-manager` and checks for due queries every 60 seconds (with a 30-second initial delay after startup).

On shutdown, the ScheduleManager is closed before the web server:

```
AutoCloseables.close(
    ScheduleManager.getInstance(),
    webServer,
    ...
);
```

The shutdown waits up to 10 seconds for any in-progress query to complete before force-stopping.

## Storage

All schedule and snapshot data is persisted server-side using Drill's `PersistentStore`:

| Store Name | Contents |
|------------|----------|
| `drill.sqllab.schedules` | Schedule definitions (frequency, time, enabled, expiration) |
| `drill.sqllab.snapshots` | Run history (status, row count, duration, errors) |
| `drill.sqllab.workflow_config` | Global config (expiration enabled, days, warning period) |

Data survives Drill restarts. On restart, the ScheduleManager reads persisted schedules and resumes checking for due queries.

## Query Execution

When a query is due, the ScheduleManager:

1. Looks up the saved query's SQL from `drill.sqllab.saved_queries`
2. Wraps it as `SELECT COUNT(*) AS cnt FROM (<original SQL>)` to avoid materializing large result sets
3. Executes it using an anonymous `WebUserConnection` via `RestQueryRunner`
4. Records a snapshot with status (success/error), row count, duration, and any error message
5. Updates the schedule's `lastRunAt` and computes the next `nextRunAt`
6. Enforces snapshot retention — deletes oldest snapshots beyond the schedule's `retentionCount`

## Schedule Expiration

By default, schedules expire after 90 days and must be renewed. This prevents forgotten schedules from running indefinitely.

### Configuration

Admins can configure expiration via the Workflows page settings or the REST API:

```
PUT /api/v1/workflows/config
{
  "expirationEnabled": true,
  "expirationDays": 90,
  "warningDaysBeforeExpiry": 14
}
```

| Setting | Default | Description |
|---------|---------|-------------|
| `expirationEnabled` | `true` | Whether schedules auto-expire |
| `expirationDays` | `90` | Days until a schedule expires |
| `warningDaysBeforeExpiry` | `14` | Days before expiry to show warnings |

### Behavior

- When a schedule is created, `expiresAt` is set to `now + expirationDays`
- The Workflows page shows warnings for schedules expiring within the warning period
- Users can renew a schedule with one click (`POST /api/v1/schedules/{id}/renew`), which resets `expiresAt`
- Expired schedules are automatically disabled by the ScheduleManager on its next check cycle
- Admins can disable expiration entirely by setting `expirationEnabled: false`

## REST API Reference

### Schedules

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/schedules` | List all schedules |
| `POST` | `/api/v1/schedules` | Create a schedule |
| `GET` | `/api/v1/schedules/{id}` | Get a schedule |
| `PUT` | `/api/v1/schedules/{id}` | Update a schedule |
| `DELETE` | `/api/v1/schedules/{id}` | Delete a schedule and its snapshots |
| `GET` | `/api/v1/schedules/query/{savedQueryId}` | Find schedule for a saved query |
| `POST` | `/api/v1/schedules/{id}/renew` | Renew an expiring schedule |
| `GET` | `/api/v1/schedules/{id}/snapshots` | List run history |

### Schedule Create/Update Body

```json
{
  "savedQueryId": "query-uuid",
  "description": "Daily sales report",
  "frequency": "daily",
  "enabled": false,
  "timeOfDay": "08:00",
  "dayOfWeek": 1,
  "dayOfMonth": 15,
  "notifyOnSuccess": false,
  "notifyOnFailure": true,
  "notifyEmails": ["team@example.com"],
  "retentionCount": 30
}
```

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `savedQueryId` | Yes | — | ID of the saved query to schedule |
| `description` | No | — | What this scheduled query does |
| `frequency` | Yes | — | `hourly`, `daily`, `weekly`, or `monthly` |
| `enabled` | No | `false` | Whether the schedule is active |
| `timeOfDay` | No | `08:00` | Time to run (HH:mm). For hourly, only minutes matter |
| `dayOfWeek` | No | — | 0=Sunday through 6=Saturday (weekly only) |
| `dayOfMonth` | No | — | 1-28 (monthly only) |
| `notifyOnSuccess` | No | `false` | Email on successful run |
| `notifyOnFailure` | No | `true` | Email on failed run |
| `notifyEmails` | No | `[]` | Recipient email addresses |
| `retentionCount` | No | `30` | Number of snapshots to keep |

## Multi-Node Clusters

In a multi-Drillbit deployment, each Drillbit runs its own ScheduleManager instance reading from the same PersistentStore. This can result in duplicate query executions. For production multi-node clusters, consider:

- Running schedules on only one designated Drillbit
- Implementing distributed locking via ZooKeeper (future enhancement)
- Accepting duplicate runs if queries are idempotent

## Monitoring

The ScheduleManager logs key events at INFO level:

- `ScheduleManager started for SQL Lab scheduled queries` — on Drillbit boot
- `Executing scheduled query: {queryName} (schedule: {scheduleId})` — when a query runs
- `Schedule {id} expired and has been disabled` — when auto-expiration fires
- `ScheduleManager shut down` — on Drillbit shutdown

Errors are logged at WARN level and never crash the scheduler thread.
