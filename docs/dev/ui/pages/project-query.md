# Project Query

**File:** `src/pages/ProjectQueryPage.tsx`
**Route:** `/projects/:id/query`

## Purpose

SQL IDE scoped to a single project. Wraps [`SqlLabPage`](sql-lab.md) and filters its schema browser / completion to the project's datasets. Adds an optional AI assistant that uses project context for query suggestions.

## Data sources

Inherits everything from `SqlLabPage`. Additionally reads from `useProjectContext()`:

- `project.datasets` — passed to `SqlLabPage` as `datasetFilter` and to the AI assistant
- `project.savedQueryIds` — passed to the AI assistant for "show me my saved queries" prompts

No direct API calls of its own.

## Child components

- `SqlLabPage` — the full SQL IDE
- `AiAssistantModal` — rendered only when the project has ≥1 dataset; provides project-aware suggestions

## Key state

- Redux `querySlice` — tabs (shared with the cross-project SQL Lab — tabs persist across both)
- `pendingSqlRef` — staging slot for SQL that needs to land in a newly-created tab

## Behavior

- **Dataset filter.** SqlLabPage's schema browser and Monaco completion only see the project's datasets.
- **AI assistant gating.** Hidden when the project has no datasets — without datasets there's nothing useful for the assistant to suggest.
- **Suggested SQL flow.** When the assistant emits a suggested query, the page creates a new tab and queues the SQL via `pendingSqlRef`; on the next tab-change effect it applies the SQL to the newly active tab.

## Chrome

Inherits from SqlLabPage. Breadcrumb is overridden by `usePageChrome` to show `Projects > <Project Name> > Query`.

## Quirks

- SQL Lab tabs are global Redux state, so opening a project-scoped SQL Lab does NOT give you a fresh tab set — you see your existing tabs. This is intentional: users often switch between project and cross-project contexts while editing the same query.
- The dataset filter affects discovery (schema browser, completion), not execution. Users can still type a fully-qualified path to anything they have permission to read.
