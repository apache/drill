# Project Wiki

**File:** `src/pages/ProjectWikiPage.tsx`
**Routes:** `/projects/:id/wiki`, `/projects/:id/wiki/:pageId`

## Purpose

Project documentation in Markdown. Two-pane layout: page list on the left, editor / viewer on the right. AI generation available when Prospector is configured: writes a "Project Overview" page seeded with the project's datasets, saved queries, visualizations, and dashboards.

## Data sources

| API | Module |
|---|---|
| `project.wikiPages` via `ProjectContext` | indirect |
| `createWikiPage()` | `api/projects.ts` |
| `deleteWikiPage()` | `api/projects.ts` |
| `POST /api/v1/ai/chat` (`streamChat`) | `api/ai.ts` |
| `GET /api/v1/ai/status` (`getAiStatus`) | `api/ai.ts` |
| `getSavedQueries`, `getVisualizations`, `getDashboards` | corresponding `api/*.ts` (for AI context) |

## Child components

- `WikiEditor` modal — create / edit interface (Markdown + preview)

## Key state

- `pageId` (URL param) — selected page
- `wikiEditorOpen`, `editingPageId` — editor modal
- `aiGenerating`, `aiContent` — AI generation in flight
- `abortRef` — `AbortController` for cancelling the AI stream

## Behavior

- **Page list** sorted by `page.order`. No nested / parent-child hierarchy — flat structure.
- **Selecting** a page updates the URL (`/projects/:id/wiki/:pageId`) so the view is shareable.
- **AI generation** collects project metadata (datasets, queries, visualizations, dashboards), assembles a context prompt, streams a "Project Overview" page in Markdown.
- **Markdown preview** strips frontmatter, code blocks, and images for the list snippet — full content shows in the right pane.
- **Edit / Delete** buttons appear in the toolbar when a page is selected.

## Chrome

Breadcrumb: `Projects > <Project Name> > Wiki[ > <Page>]`. Toolbar: New page, Generate with AI (when AI enabled), Edit (when selected), Delete (when selected).

## Quirks

- No cross-project equivalent — wiki content is always project-scoped.
- AI generation runs against the project context only; you can't ask for a page covering multiple projects from here.
- Page order is a `number` field; reordering UI is not built — order can only be changed by editing the field.
