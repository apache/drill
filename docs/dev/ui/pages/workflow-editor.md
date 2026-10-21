# Workflow Editor

**File:** `src/pages/WorkflowEditorPage.tsx`
**Routes:** `/projects/:id/workflows/new` and `/projects/:id/workflows/:wid`
**Components:** `src/components/workflow/` (`ActionCard`, `ConditionEditor`, `SchemaForm`, `TemplateInput`, `OutcomeList`, `RunsTab`, `HealthTab`). Pure helpers in `src/utils/conditionTree.ts`.

Backend and semantics: [`../../WORKFLOWS.md`](../../WORKFLOWS.md). The list page is [workflows.md](workflows.md).

## Purpose

Create, edit, test and monitor one workflow. `/new` accepts `?query=<savedQueryId>` to preselect the saved query.

`WorkflowEditorPage` renders `WorkflowEditor` keyed by the `wid` route param, so moving between workflows or to `/new` resets all editor state (draft, errors, dry run result).

## Data sources

| API | Used for |
|---|---|
| `GET /workflows/:id` (`getWorkflow`) | Load; a failure shows an error alert instead of the form |
| `POST /workflows` / `PUT /workflows/:id` | Save |
| `POST /workflows/validate` (`validateWorkflow`) | Live validation, debounced 600 ms after each edit |
| `POST /workflows/parse-condition` (`parseCondition`) | Live expression errors and mode switching |
| `GET /workflows/action-types` (`getActionTypes`) | Add-action menu and config forms |
| `GET /api/v1/savedQueries` | Saved query picker, limited to the project's queries |
| `POST /workflows/dry-run` (`dryRunWorkflow`) | Dry run |
| `POST /workflows/:id/run`, `POST /workflows/:id/enabled` | Run now and the Enabled switch |
| `GET /workflows/:id/runs`, `GET /workflows/:id/health` | Runs and Health tabs |

## Layout

Header: title, **Enabled** switch (saved workflows), **Dry run**, **Run now** (saved workflows), **Save**. Save is disabled while the live validation reports errors. An expired workflow gets a banner with a **Renew** button. A workflow whose owner is not a member of the project (status `owner_lost_access`, or an owner missing from the project's owner and `sharedWith`, checked client-side so the fix shows before a run fails) gets a banner that says who it runs as and how to fix it; for admins it has a **Take ownership** button. Both actions leave the workflow paused.

Tabs:

* **Definition.** Name, description, saved query, schedule (frequency, time, weekday or day of month, timeout), then the action list. Switching frequency to weekly or monthly fills in the required day (Monday, or day 1) when none is set (`frequencyPatch`), so the value the input shows is the value that is saved. Each action is an `ActionCard` with its id, up and down buttons for order, a delete button, a **Run when** condition editor and a config form generated from the action type's schema. Actions with side effects carry a "side effects" tag. **Add action** lists the registered types.
* **Runs** (saved workflows). Run history table (when, trigger, status, rows, duration, an **Alerts** column with the message of each fired `record_alert`, and outcome counts). Expanding a run shows its error, preview rows and per-action outcomes. Shows "Run history is visible to project members" on a 403.
* **Health** (saved workflows). Daily success rate and average duration chart plus a per-action outcome table for the last 30 days, from the run log. Shows a setup hint when an admin has not set up run history.

## Condition editor

`ConditionEditor` has three modes in a segmented control:

| Mode | Behavior |
|---|---|
| Always | `{always: true}` |
| Builder | One level: match **All (AND)** or **Any (OR)** over rows of field, operator, value. Field autocomplete offers base fields, earlier actions' outputs and `rows[*].`. Typed values are coerced (numbers, booleans, `null`). |
| Expression | A text input in monospace with live parse errors from `parse-condition`. |

Switching Builder to Expression writes the tree as text. Switching Expression to Builder parses first and only switches if the tree fits the builder shape; otherwise it warns that the expression is too complex. A tree the builder cannot show says "Too complex for the builder; switch to Expression to edit". The action field hints come from `availableRefs`, which lists only earlier actions' declared outputs.

## Template inputs

String config fields marked `x-template` use `TemplateInput`, which offers the same references for `{{...}}` placeholders. Referencing an action's output makes the action depend on it at run time.

## Validation flow

1. Every edit schedules `POST /validate` 600 ms later; stale responses are ignored.
2. Errors come back as `{path, message}` and are matched to inputs by path (`name`, `savedQueryId`, `schedule.timeOfDay`, `schedule.dayOfWeek`, `schedule.dayOfMonth`, `schedule.timeoutSeconds`, `actions[2].id`, `actions[2].condition`, `actions[2].config.<field>`). Errors whose path no input shows (for example `actions` or `schedule.frequency`) are listed in an error alert above the tabs (`unplacedErrors` in `utils/conditionTree.ts`), so a disabled Save always has a visible reason. The saved query picker shows the server's ownership error: a workflow's saved query must be owned by the user it runs as.
3. Save sends the document. A 422 puts the server's errors on the form; other failures show a toast. A first save navigates to `/projects/:id/workflows/:newId`.

## Dry run

**Dry run** always sends the current draft (with `id` removed) to `POST /workflows/dry-run`, so it tests unsaved edits and runs as the person in the editor. Side-effect actions return a preview of what they would send or write; nothing is sent or written and no run is recorded. Results open in a drawer with the query status, row count, duration, the first 10 rows and an outcome list (fired, skipped with reason, errored with message).

## Run now and Enabled

* **Run now** runs the saved workflow immediately and refreshes the Runs and Health tabs. It fails with a message when the workflow is already running, expired, or its owner has left the project.
* **Enabled** calls the `enabled` endpoint. The server refuses with a message for `owner_lost_access`, expiry, or a missing project.
* Editing is limited to the workflow owner and admins; other users get a 403 toast on save.
