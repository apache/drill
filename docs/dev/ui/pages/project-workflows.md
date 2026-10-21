# Project Workflows

**File:** `src/pages/ProjectWorkflowsPage.tsx`
**Route:** `/projects/:id/workflows` (create and edit routes: `/projects/:id/workflows/new`, `/projects/:id/workflows/:wid`, see [workflow-editor.md](workflow-editor.md))

## Purpose

Thin wrapper around [`WorkflowsPage`](workflows.md) for one project.

## Implementation

```tsx
const { projectId } = useProjectContext();
return projectId ? <WorkflowsPage projectId={projectId} /> : null;
```

That is the whole component. `WorkflowsPage` fetches `GET /api/v1/workflows?projectId=...`, so the list is the project's workflows and the admin extras (settings, orphans, running banner) are hidden.

## Visibility

The Workflows entry is always shown in the project sidebar (`sectionsFor` in `Sidebar.tsx`), whether or not the project has workflows yet. `ProjectNavBar` has no Workflows tab. The server decides what each person sees: public-project viewers see definitions but not run records. See [`../sidebar-navigation.md`](../sidebar-navigation.md).

## Chrome

Inherits from `WorkflowsPage`. Breadcrumb is `Projects > <Project Name> > Workflows`.
