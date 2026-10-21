# Cluster

**File:** `src/pages/ClusterPage.tsx`
**Route:** `/cluster`

## Purpose

Drillbit fleet view and admin actions. Replaces the legacy `index.ftl` page that used to render at `/`. Shows every Drillbit in the cluster, their state and ports, the cluster's version coherence, encryption status, distributed-queue summary, admin/process user (admin-only), and shutdown controls (admin-only).

## Data sources

| API | Module | Used for |
|---|---|---|
| `GET /cluster.json` (`getClusterInfo`) | `api/cluster.ts` | Drillbit list, encryption, queue, version, admin info |
| `GET /gracePeriod` (`getGracePeriod`) | `api/cluster.ts` | Grace-period statistic |
| `POST /gracefulShutdown` (`gracefulShutdown`) | `api/cluster.ts` | Local graceful shutdown |
| `POST /gracefulShutdown/{hostname}` (`gracefulShutdownHost`) | `api/cluster.ts` | Per-row graceful shutdown |
| `POST /quiescent` (`quiescentMode`) | `api/cluster.ts` | Local quiescent mode |
| `POST /shutdown` (`forcefulShutdown`) | `api/cluster.ts` | Local forceful shutdown |

`useCurrentUser` (`hooks/useCurrentUser.ts`) gates the admin controls.

The cluster snapshot auto-refetches every 15 seconds via react-query's `refetchInterval`. The grace-period query has a 60-second `staleTime`; it rarely changes.

## Layout

1. Title + (in the toolbar) Refresh + admin shutdown buttons.
2. Version-mismatch banner (only when `cluster.mismatchedVersions` is non-empty).
3. Four summary cards: Drillbits / Cluster version / Authentication / Grace period.
4. Drillbits table — address, state, version, ports, per-row "Graceful shutdown" button (admin-only).
5. Encryption card — user-connections and bit-to-bit toggles.
6. Distributed query queue card — Descriptions list when enabled, "not enabled" hint otherwise.
7. Administration card (admin-only) — process user, admin users, group membership.
8. Shutdown confirmation modal.

## Key state

- `pendingShutdown: PendingShutdown | null` — drives the confirmation modal; `kind` is `'graceful' | 'quiescent' | 'forceful' | 'host'`; `hostLabel` and `drillbit` carry the target when kind is `'host'`.
- `shutdownMutation` — single mutation handling all four shutdown kinds; the kind is the request payload.

## Chrome

Breadcrumb: `Administration > Cluster`. Toolbar: Refresh, Quiescent mode, Graceful shutdown, Forceful shutdown (last three rendered only when the user is admin).

## Admin gating

The page combines two signals before exposing destructive actions:

- **Client-side:** `useCurrentUser()` → `!user.authEnabled || user.isAdmin`. Anonymous (auth-disabled) mode treats everyone as admin.
- **Server-side:** `cluster.shouldShowAdminInfo` from the loaded snapshot.

Both must agree before the admin controls show or fire. This avoids showing buttons that the server will then reject.

## Quirks

- The "current Drillbit" row (the one serving the React app's request) is highlighted in the address column with a crown icon and bold text. This matches the legacy UI's "current" indicator.
- Forceful shutdown is rendered with `danger` styling and an extra "not reversible" warning inside the modal.
- The per-row graceful-shutdown button calls `/gracefulShutdown/{hostname}`, which the local Drillbit forwards to the named Drillbit over HTTP. If the cluster has only one Drillbit, this is functionally the same as the toolbar's "Graceful shutdown" button.
- The page does not poll `/queriesCount` today. If it becomes useful to surface "remaining queries" during a quiescent transition, wire it up to `getRemainingQueries()` from `api/cluster.ts` and gate on `state ∈ {QUIESCENT, GRACE}`.
- `useQueryClient().invalidateQueries(['cluster'])` runs on shutdown success so the table reflects the new state quickly. The 15-second auto-refetch will catch the change anyway.
