# Workflows

**File:** `src/pages/WorkflowsPage.tsx`
**Routes:** `/workflows` (all workflows, admin only), `/projects/:id/workflows` (project scoped, through [`ProjectWorkflowsPage`](project-workflows.md))
**Props:** `projectId?: string`

Backend and data model: [`../../WORKFLOWS.md`](../../WORKFLOWS.md). The editor is documented in [workflow-editor.md](workflow-editor.md).

## Purpose

List workflows (scheduled queries with action pipelines) with their schedule, state, last run, 30-day success rate and next run. The page runs in two modes chosen by `projectId`:

* **Project view** (`projectId` set): lists that project's workflows and shows a **New workflow** button.
* **Admin view** (no `projectId`): lists every workflow across projects. Non-admins who reach `/workflows` see an "Admin access required" alert, and the admin-only queries stay disabled for them.

## Data sources

| API | Module | Used for |
|---|---|---|
| `GET /api/v1/workflows?projectId=` (`listWorkflows`) | `api/workflows.ts` | The table |
| `GET /api/v1/workflows/observability/summary` (`getObservabilitySummary`) | `api/workflows.ts` | Success rate column (30 days) |
| `GET /api/v1/workflows/config` (`getWorkflowConfig`) | `api/workflows.ts` | Expiry warning window |
| `POST /api/v1/workflows/:id/renew` (`renewWorkflow`) | `api/workflows.ts` | Renew button on the expiring tag |
| `GET /api/v1/projects` (`getProjects`) | `api/projects.ts` | Project names, orphan assignment (admin view) |
| `GET /api/v1/workflows/observability/running` (`getRunningWorkflows`) | `api/workflows.ts` | "Running now" banner, polled every 15 s (admin view) |
| `GET /api/v1/workflows/observability/status`, `POST .../setup` | `api/workflows.ts` | Run history setup banner (admin view) |
| `POST /api/v1/workflows/:id/assign-project` (`assignProject`) | `api/workflows.ts` | Orphan assignment (admin view) |
| `PUT /api/v1/workflows/config` (`updateWorkflowConfig`) | `api/workflows.ts` | Settings modal (admin view) |

## Layout

* Four statistic cards: Workflows, Active, Failing (two or more consecutive failures), Expiring soon (within `warningDaysBeforeExpiry`).
* Table columns: Name (links to the editor), Project and Owner (admin view only), Schedule (`describeSchedule`, for example "Daily 08:00"), State, Last run, Success (30 d), Next run, Actions (one tag per action type). Failing workflows sort first, then expiring ones, then by name.
* State shows `on` or the status (`paused`, `expired`, `owner lost access`), a failure count tag, and an expiry tag with a **Renew** link.

## Admin extras (`/workflows` only)

* **Set up banner.** When `observability/status` reports not ready, offers a **Set up** button that creates the `dfs.workflow_logs` workspace (needs `DRILL_LOG_DIR`). Until then success rates and the Health tab are empty.
* **Running now.** A banner listing workflows with `isRunning` set.
* **Workflows without a project.** A card for migrated orphans, each with a project picker limited to projects that already contain the workflow's saved query. Choosing one calls `assign-project`. A failed renew or assignment shows the server's `{error}` message in a toast.
* **Settings** button opens a modal for `expirationEnabled`, `expirationDays`, `warningDaysBeforeExpiry` and `actionTimeoutSeconds`.

## Quirks

* The table only shows workflows with a project; orphans appear in their own card.
* Run statistics come from the run log, so the success column is blank for project members until an admin has run the setup. Public-project viewers see the list but the summary request is refused for them, so the column stays blank.
* The page has no schedule modal. Creating and editing happen in the [editor](workflow-editor.md).
