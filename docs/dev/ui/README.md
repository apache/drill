# Drill Web UI — Developer Docs

The Drill web UI is a React single-page app served from the Drillbit. Source lives in `exec/java-exec/src/main/resources/webapp/`; it is built with Vite, written in TypeScript, and styled with Ant Design + a custom Sonoma/Tahoe-inspired CSS layer.

This directory documents how the UI is put together: its architecture, how the shell and routing work, the conventions for adding pages or nav entries, and reference docs for every page and major component.

## Where to start

- **New to the codebase?** Read [`architecture.md`](architecture.md), then [`routing.md`](routing.md), then [`sidebar-navigation.md`](sidebar-navigation.md). That's enough to find your way to any page.
- **Adding a new page?** Read [`routing.md`](routing.md) and [`sidebar-navigation.md`](sidebar-navigation.md). Use an existing page doc under [`pages/`](pages/) as a template for documenting yours.
- **Touching API code?** Read [`data-flow.md`](data-flow.md) — covers the axios client, CSRF handling, react-query conventions, and the Redux slices.
- **Modifying chrome (toolbar, sidebar, inspector)?** Read [`contexts.md`](contexts.md) for `usePageChrome` and the chrome-registration pattern, then [`components/app-shell.md`](components/app-shell.md).
- **Styling?** Read [`styling.md`](styling.md) for design tokens and conventions.

## Map of the docs

### Foundation

| Doc | What it covers |
|---|---|
| [`architecture.md`](architecture.md) | Tech stack, file tree, build setup, the AppShell pattern, mobile behavior |
| [`routing.md`](routing.md) | Server-side routing (Jersey + SPA), client-side routing (React Router), the three-layer route structure |
| [`sidebar-navigation.md`](sidebar-navigation.md) | Sidebar sections, nav items, the projects list, conventions for adding entries |
| [`contexts.md`](contexts.md) | AppChromeContext, AiModalContext, ProjectContext, ThemeProvider — what each owns |
| [`data-flow.md`](data-flow.md) | API client (CSRF, base URL, interceptors), react-query setup, Redux slices |
| [`styling.md`](styling.md) | Design tokens, Ant Design theming, dark mode, component CSS conventions |

### Pages

Pages live in `src/pages/`. Each doc covers route, purpose, data sources (which APIs), child components, key state, and quirks.

**Cross-project pages** (top-level, not nested under a project):

| Doc | Route(s) |
|---|---|
| [`pages/projects.md`](pages/projects.md) | `/projects` |
| [`pages/sql-lab.md`](pages/sql-lab.md) | `/query` |
| [`pages/data-sources.md`](pages/data-sources.md) | `/datasources`, `/datasources/:name` |
| [`pages/saved-queries.md`](pages/saved-queries.md) | `/saved-queries` |
| [`pages/workflows.md`](pages/workflows.md) | `/workflows` |
| [`pages/profiles.md`](pages/profiles.md) | `/profiles`, `/profiles/:queryId` |
| [`pages/visualizations.md`](pages/visualizations.md) | `/visualizations`, `/visualizations/:vizId` |
| [`pages/dashboards.md`](pages/dashboards.md) | `/dashboards`, `/dashboards/:id` |
| [`pages/metrics.md`](pages/metrics.md) | `/metrics` |
| [`pages/options.md`](pages/options.md) | `/options` |
| [`pages/logs.md`](pages/logs.md) | `/logs` |
| [`pages/ai-analytics.md`](pages/ai-analytics.md) | `/ai-analytics` |
| [`pages/cluster.md`](pages/cluster.md) | `/cluster` |
| [`pages/credentials.md`](pages/credentials.md) | `/credentials` |
| [`pages/threads.md`](pages/threads.md) | `/threads` |
| [`pages/login.md`](pages/login.md) | `/login`, `/mainLogin` |

**Project-scoped pages** (nested under `/projects/:id/`):

| Doc | Route(s) |
|---|---|
| [`pages/project-layout.md`](pages/project-layout.md) | `/projects/:id/*` shell + tabbed nav |
| [`pages/project-detail.md`](pages/project-detail.md) | `/projects/:id/settings` |
| [`pages/project-query.md`](pages/project-query.md) | `/projects/:id/query` |
| [`pages/project-wiki.md`](pages/project-wiki.md) | `/projects/:id/wiki`, `/projects/:id/wiki/:pageId` |
| [`pages/project-data-sources.md`](pages/project-data-sources.md) | `/projects/:id/datasources` |
| [`pages/project-workflows.md`](pages/project-workflows.md) | `/projects/:id/workflows` |

The other project pages (`queries`, `visualizations`, `dashboards`, the `:id`-level wrappers) are thin decorators over their cross-project counterparts and are documented inside the matching cross-project page doc.

### Components

| Doc | Covers |
|---|---|
| [`components/app-shell.md`](components/app-shell.md) | AppShell, Sidebar, Toolbar, LeftRail, RightInspector, PreferencesModal, command palette |
| [`components/prospector.md`](components/prospector.md) | Frontend architecture of the Prospector AI assistant (chat panel, hook, API client). For backend, see [`../PROSPECTOR.md`](../PROSPECTOR.md). |

## Related docs

- [`../PROSPECTOR.md`](../PROSPECTOR.md) — Prospector backend (LLM provider registry, REST endpoints, persistent store)
- [`../TRANSPILER.md`](../TRANSPILER.md) — GraalPy + sqlglot SQL transpiler used by Prospector's "optimize" flow
- [`../AI_FEATURES.md`](../AI_FEATURES.md) — overview of all AI features

## Conventions

- All TypeScript/TSX files carry the Apache 2.0 license header.
- Pages use lazy loading via `React.lazy` so each route ships as its own chunk; see [`architecture.md`](architecture.md) for the pattern.
- Pages register their chrome (breadcrumb, toolbar actions, inspector tabs, left rail) via `usePageChrome` from [`contexts.md`](contexts.md). They do not poke at the AppShell directly.
- Server state lives in react-query; client state lives in Redux only when it crosses route boundaries (currently just `querySlice` and `uiSlice` for the SQL Lab editor).
- All mutating API calls go through `src/api/client.ts` so they inherit CSRF token injection and 401-redirect handling.

When adding or substantially modifying a page, update its doc in the same PR.
