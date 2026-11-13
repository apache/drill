# Architecture

## Tech stack

| Concern | Library | Notes |
|---|---|---|
| Language | TypeScript (strict) | `tsconfig.json` — path alias `@/*` → `src/*` |
| Build | Vite 5 | `vite.config.ts` — chunked bundles (monaco, charts, grid, antd) with sourcemaps |
| UI framework | React 18 | Function components + hooks; no class components |
| Component library | Ant Design 5 | `ConfigProvider` theme algorithm switches light/dark |
| Routing | React Router 6 | `BrowserRouter`; routes declared in `App.tsx` |
| Server state | TanStack Query v5 | Single `QueryClient` in `main.tsx`; default `staleTime: 5min`, `refetchOnWindowFocus: false`, `retry: 1` |
| Client state | Redux Toolkit | Only for the SQL Lab editor; everything else uses react-query or local `useState` |
| HTTP | axios | Single wrapper in `src/api/client.ts` with CSRF interceptor and 401 redirect |
| Code editor | `@monaco-editor/react` | Used by SQL Lab and the data source JSON tab |
| Charts | echarts + echarts-for-react | Used by Metrics, Profiles plan tree, Dashboards |
| Data grid | ag-grid-community + ag-grid-react | Used by SQL Lab results, Profiles tables |
| Markdown | react-markdown | Used by Prospector, Wiki, AI analysis panes |
| Date | dayjs | Used by Workflows date pickers, Profiles grouping |
| Dashboard grid | react-grid-layout | Used by `DashboardViewPage` for drag-resize panels |

## File tree

```
exec/java-exec/src/main/resources/webapp/
├── package.json, package-lock.json
├── vite.config.ts, vitest.config.ts
├── tsconfig.json, tsconfig.node.json
├── index.html                 ← shell HTML; SPA mount point
├── public/                    ← static assets copied verbatim into dist/
├── scripts/                   ← build / dev helpers
├── dist/                      ← built output (served by the Drillbit; gitignored)
├── node_modules/              ← gitignored
└── src/
    ├── main.tsx               ← React root; sets up providers (Redux, QueryClient, BrowserRouter, ThemeProvider)
    ├── App.tsx                ← top-level routes + AppShell
    ├── index.css              ← global styles + design tokens + Sonoma/Tahoe theme
    ├── setupTests.ts          ← Vitest setup
    ├── api/                   ← axios clients, one module per backend area
    ├── assets/                ← icons, images bundled into JS chunks
    ├── components/            ← reusable components, grouped by feature
    │   ├── shell/             ← AppShell, Sidebar, Toolbar, LeftRail, RightInspector
    │   ├── project/           ← ProjectLayout, ProjectNavBar, ProjectContext consumers
    │   ├── sqllab/            ← SQL editor, results grid, schema explorer, notebook
    │   ├── dashboards/        ← grid layout, panel cards, settings drawer
    │   ├── visualizations/    ← chart preview, visualization editor
    │   ├── prospector/        ← AI assistant chat UI
    │   └── …                  ← per-feature folders for visualizations, storage forms, etc.
    ├── contexts/              ← AppChromeContext, AiModalContext, ProjectContext
    ├── data/                  ← static reference data (e.g. plugin templates)
    ├── hooks/                 ← cross-cutting hooks (useTheme, useProspector, useCurrentUser, …)
    ├── pages/                 ← one file per route (lazy-loaded)
    ├── services/              ← non-API utilities (e.g. SQL parsing helpers)
    ├── store/                 ← Redux store + slices (querySlice, uiSlice)
    ├── types/                 ← shared TypeScript types
    └── utils/                 ← small pure helpers
```

## Build & dev

### Production build

```bash
cd exec/java-exec/src/main/resources/webapp
npm install
npm run build
```

Output lands in `dist/`. Maven packs it into the `java-exec` jar under `webapp/dist/`; the `SpaResource` JAX-RS catch-all serves files from that classpath location. See [`routing.md`](routing.md) for the request-routing details.

`dist/` is excluded from the `jdbc-all` shaded jar via the `webapp/**` filter in `exec/jdbc-all/pom.xml` to stay under the size limit.

### Dev server

```bash
cd exec/java-exec/src/main/resources/webapp
npm run dev
```

Vite serves on `http://localhost:3000` with HMR. The proxy in `vite.config.ts` forwards `/api`, `/query.json`, `/storage.json`, `/profiles`, `/geojson` to a running Drillbit on `http://localhost:8047`. Start the Drillbit separately (`drillbit.sh run` or via your IDE) before opening the dev server.

The Vite `base` is `/`, so dev URLs are `http://localhost:3000/...`. `BrowserRouter`'s `basename` matches.

### Tests

```bash
npm test               # Vitest watch mode
npm run test:run       # one-shot
```

Tests live next to the file under test (`*.test.ts(x)`). The current suite is API-focused — see `src/api/*.test.ts`.

## The AppShell pattern

Every route renders inside `AppShell`, declared once in `App.tsx`. The shell owns the three-pane Sonoma-style layout:

```
+-------------------------------------------------------------------+
| Sidebar  | Toolbar                                                |
|          |--------------------------------------------------------|
|          | LeftRail (optional) | Page content | RightInspector    |
|          |                     |              | (optional)        |
+-------------------------------------------------------------------+
```

- **Sidebar** (`src/components/shell/Sidebar.tsx`): persistent left nav. Width 232px expanded, 56px collapsed, resizable 200–380px. See [`sidebar-navigation.md`](sidebar-navigation.md).
- **Toolbar** (`src/components/shell/Toolbar.tsx`): 48px fixed header. Breadcrumb + spotlight search + global action buttons (theme, preferences, AI menu, inspector toggle). See [`components/app-shell.md`](components/app-shell.md).
- **LeftRail** (`src/components/shell/LeftRail.tsx`): optional per-page rail (e.g. SQL Lab schema browser). Pages opt in via `usePageChrome({ leftRail: ... })`. Resizable 220–480px, default 280px.
- **RightInspector** (`src/components/shell/RightInspector.tsx`): optional tabbed inspector. Always carries the global Prospector tab; pages register additional tabs via `usePageChrome({ inspectorTabs: [...] })`. Resizable 280–720px, default 360px.

Pages do not render any of these themselves — they call `usePageChrome` from `src/contexts/AppChromeContext.tsx` to register their breadcrumb, toolbar actions, inspector tabs, and left rail. The shell renders whatever is currently registered. See [`contexts.md`](contexts.md).

## Mobile behavior

Defined in `index.css` (`@media (max-width: 768px)` block around line 1970) and `AppShell.tsx` (~lines 118-129):

- Sidebar and inspector switch from inline columns to overlay drawers (`position: absolute`) with a backdrop dismiss.
- Sidebar defaults to closed; opening it on a small screen and then navigating auto-collapses it.
- Left rail auto-collapses below 1100px (`LEFT_RAIL_AUTO_COLLAPSE_PX` in `AppChromeContext.tsx`).
- Toolbar density is unchanged but some buttons hide via CSS at narrow widths.

## Lazy loading

`App.tsx` wraps every page in `React.lazy`:

```tsx
const ProjectsPage = lazy(() => import('./pages/ProjectsPage'));
```

and renders inside `<Suspense fallback={<RouteFallback />}>`. The result: only the shell, sidebar, toolbar, and the active route's chunk download on first paint. Subsequent navigations stream their chunks on demand.

Heavy libraries are split into named chunks in `vite.config.ts`:

```ts
manualChunks: {
  monaco: ['@monaco-editor/react'],
  charts: ['echarts', 'echarts-for-react'],
  grid:   ['ag-grid-react', 'ag-grid-community'],
  antd:   ['antd', '@ant-design/icons'],
}
```

When adding a new page that pulls in a fat dependency, consider adding it to this map or accept that it ships in that page's chunk.

## Persistence

User preferences are persisted to `localStorage` under the `drill-shell-*` and `drill-sqllab-*` keys. Server-persisted state (saved queries, dashboards, projects, options, AI config) lives in the Drillbit's `PersistentStoreProvider` and is fetched via react-query.

Keys defined in:

| Key | Owner |
|---|---|
| `drill-shell-sidebar-collapsed`, `drill-shell-inspector-open`, `drill-shell-left-rail-collapsed`, `drill-shell-inspector-width`, `drill-shell-left-rail-width`, `drill-shell-sidebar-width` | `AppChromeContext.tsx` |
| `drill-theme-mode` | `useTheme.tsx` |
| `drill-sqllab-tabs`, `drill-sqllab-editor-settings`, `drill-sqllab-results-settings`, `drill-sqllab-auto-limit` | SQL Lab hooks (see [`pages/sql-lab.md`](pages/sql-lab.md)) |
| `drill-sidebar-pinned-projects`, `drill-sidebar-project-order` | `usePinnedRecentProjects.ts` |

When adding new persisted state, prefix the key with `drill-` and document it in the page or component doc.
