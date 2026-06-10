# Threads

**File:** `src/pages/ThreadsPage.tsx`
**Route:** `/threads`

## Purpose

Admin-only JVM thread dump for this Drillbit. Replaces the legacy `threads/threads.ftl` page, which polled a `/status/threads` endpoint that was removed during the codahale-metrics → Jakarta migration (the legacy page has been broken in `master` for a while). Shows live thread state, blocked/waiting locks, stack traces, deadlock detection, and a copy-to-clipboard button for pasting into bug reports.

## Data sources

| API | Module |
|---|---|
| `GET /api/v1/threads` (`getThreadDump`) | `api/threads.ts` |

The backend (`ThreadsResources.dumpThreads`) calls `ManagementFactory.getThreadMXBean().dumpAllThreads(...)` and returns a JSON snapshot with:

- `count`, `peakCount`, `daemonCount`, `totalStartedCount`
- `deadlockedThreadIds: number[]`
- `threads[]` — id, name, state, daemon flag, priority, blocked/waited counters, lock info, and a string array of stack frames

`@RolesAllowed(ADMIN_ROLE)` on the resource — stack traces leak in-flight query SQL and other sensitive context.

## Layout

1. Header + (in the toolbar) Auto-refresh switch, manual Refresh, Copy dump.
2. Deadlock banner (only when `deadlockedThreadIds` is non-empty).
3. Four summary cards: Live threads / Daemon / Peak / Total started.
4. Filter row — search by thread name or stack frame, state chips with per-state counts.
5. AntD `Collapse` of thread rows. Each row shows state tag, name, id, daemon/deadlock flags. Expanded body shows lock info (if any) and the stack frames in a monospace pre.

## Key state

- `search` — substring filter applied to thread name and every stack frame line.
- `stateFilter: string | null` — clicking a state chip filters to that state; clicking the active one clears.
- `autoRefresh` — drives react-query's `refetchInterval` (3 seconds when on, `false` when off).

## Chrome

Breadcrumb: `Administration > Threads`. Toolbar: Auto-refresh switch, Refresh, Copy dump.

## Admin gating

The page checks `useCurrentUser()` and renders a "Admin access required" alert for non-admin users so they see a clear message instead of a `401` from the network. The server enforces the same restriction via `@RolesAllowed(ADMIN_ROLE)`, so even if the client check is bypassed the data won't load.

## Quirks

- The `Copy dump` button serializes the threads in a JDK-style stack-trace text format (one thread per block, `\tat` prefixes) so the output is interchangeable with `jstack`/`jcmd` output. That's the format ops teams expect when triaging incidents.
- The state-chip colors mirror common JVM conventions: green RUNNABLE, red BLOCKED, orange WAITING, gold TIMED_WAITING.
- The dump can be tens of thousands of lines on busy Drillbits — virtualisation isn't implemented; rely on the search filter to narrow down.
- The `lockName` formatting (`waiting on <lock>` / `held by "<owner>" #<id>`) only renders when the JVM reports lock-monitor usage; some JVMs and configurations omit those details.
