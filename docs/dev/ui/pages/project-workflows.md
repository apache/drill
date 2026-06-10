# Project Workflows

**File:** `src/pages/ProjectWorkflowsPage.tsx`
**Route:** `/projects/:id/workflows`

## Purpose

Thin wrapper around [`WorkflowsPage`](workflows.md). Shows only schedules for queries that are part of the current project, and hides the global settings affordance.

## Implementation

```tsx
<WorkflowsPage
  filterSavedQueryIds={project?.savedQueryIds}
  hideSettings
/>
```

That's the entire component (modulo loading / error states from `useProjectContext()`).

## Behavior

- `filterSavedQueryIds` is the project's `savedQueryIds`. `WorkflowsPage` filters its full schedule list down to schedules referencing those queries.
- `hideSettings` removes the gear icon — the global "Schedules config" panel is an admin concern, not a project concern.

## Visibility

This tab is rendered in the project nav and sidebar only when the project has at least one scheduled query. The visibility check (in `Sidebar.tsx` and `ProjectNavBar`) cross-references `project.savedQueryIds` against `/api/v1/schedules`. See [`../sidebar-navigation.md`](../sidebar-navigation.md).

## Chrome

Inherits from `WorkflowsPage`. Breadcrumb is overridden to show `Projects > <Project Name> > Workflows`.
