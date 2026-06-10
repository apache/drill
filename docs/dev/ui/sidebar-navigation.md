# Sidebar navigation

The sidebar (`src/components/shell/Sidebar.tsx`) is the persistent left-hand nav. It is grouped into four sections:

1. **Workspace** — quick access to home and SQL Lab.
2. **Projects** — a dynamic list of up to six visible projects, each expandable into its tabbed sub-nav.
3. **Library** — cross-project aggregate views.
4. **Administration** — admin / system-level pages (collapsed by default).

## Static sections

Defined at the top of `Sidebar.tsx` (~lines 110-130):

### WORKSPACE

```ts
const WORKSPACE: NavItem[] = [
  { to: '/projects',  label: 'Home',     icon: <HomeOutlined /> },
  { to: '/query',     label: 'SQL Lab',  icon: <CodeOutlined />, matchPrefix: '/query' },
];
```

### LIBRARY

```ts
const LIBRARY: NavItem[] = [
  { to: '/saved-queries',   label: 'All Saved Queries',    icon: <SaveOutlined />,      matchPrefix: '/saved-queries' },
  { to: '/visualizations',  label: 'All Visualizations',   icon: <BarChartOutlined />,  matchPrefix: '/visualizations' },
  { to: '/dashboards',      label: 'All Dashboards',       icon: <DashboardOutlined />, matchPrefix: '/dashboards' },
  { to: '/workflows',       label: 'Workflows',            icon: <FieldTimeOutlined />, matchPrefix: '/workflows' },
  { to: '/profiles',        label: 'Query History',        icon: <HistoryOutlined />,   matchPrefix: '/profiles' },
];
```

The "All" prefix is intentional — these are cross-project aggregate views, distinct from the project-scoped equivalents inside the Projects section.

### ADMINISTRATION

```ts
const ADMIN: NavItem[] = [
  { to: '/cluster',       label: 'Cluster',          icon: <ClusterOutlined /> },
  { to: '/datasources',   label: 'Data Sources',     icon: <DatabaseOutlined />,  matchPrefix: '/datasources' },
  { to: '/credentials',   label: 'Credentials',      icon: <KeyOutlined /> },
  { to: '/metrics',       label: 'Metrics',          icon: <LineChartOutlined /> },
  { to: '/options',       label: 'System Options',   icon: <SettingOutlined /> },
  { to: '/logs',          label: 'Server Logs',      icon: <FileTextOutlined /> },
  { to: '/threads',       label: 'Threads',          icon: <PartitionOutlined /> },
  { to: '/ai-analytics',  label: 'AI Analytics',     icon: <RobotOutlined /> },
];
```

Collapsed by default (`defaultOpen={false}` on the Section wrapper). Login pages will join this section in Phase 4.

### `NavItem` shape

```ts
interface NavItem {
  to: string;        // target route
  label: string;     // shown next to the icon when sidebar expanded; used in tooltips when collapsed
  icon: ReactNode;   // 16px AntD icon
  matchPrefix?: string;  // optional — pathname must equal `to` OR start with `matchPrefix + '/'` for active styling
}
```

`isActive(pathname, item)` (`Sidebar.tsx:159-164`) uses `matchPrefix` to handle deep routes. E.g. `/datasources/foo` should highlight the `/datasources` item.

## Project section (dynamic)

The Projects section is the most complex part of the sidebar. It is rendered between WORKSPACE and LIBRARY.

### What it shows

- A search box (when expanded) that filters the list.
- Up to **six** visible project rows (`VISIBLE_PROJECTS_WHEN_IDLE = 6`, `Sidebar.tsx:157`). The currently active project is always included even if it would otherwise be beyond the cutoff.
- A "Show all" affordance to expand beyond six.
- A "New project" button at the bottom.

### Ordering

Order resolution (`Sidebar.tsx:505-533`):

1. Projects pinned by the user (drag-reordered) come first in the user's order.
2. Remaining projects sort alphabetically.
3. The active project is hoisted into the visible window if it would otherwise be below the cutoff.

Pin and order state is persisted to `localStorage` via `usePinnedRecentProjects.ts` under `drill-sidebar-pinned-projects` and `drill-sidebar-project-order`. Drag-reordering is wired through the project rows.

### Per-project sub-nav

Each project row expands (via chevron click) into a sub-nav. `BASE_PROJECT_SECTIONS` (`Sidebar.tsx:138-144`):

```ts
[
  { key: 'query',           label: 'Query',          icon: <CodeOutlined /> },
  { key: 'queries',         label: 'Saved Queries',  icon: <SaveOutlined /> },
  { key: 'visualizations',  label: 'Visualizations', icon: <BarChartOutlined /> },
  { key: 'dashboards',      label: 'Dashboards',     icon: <DashboardOutlined /> },
  { key: 'datasources',     label: 'Data Sources',   icon: <DatabaseOutlined /> },
]
```

Plus conditionally:

- `WORKFLOWS_SECTION` is inserted only if the project has any scheduled queries. The check (`sectionsFor`, `Sidebar.tsx:493+`) cross-references `project.savedQueryIds` against the `/api/schedules` result.
- `TRAILING_PROJECT_SECTIONS` (Wiki, Settings) always render at the end.

Each sub-nav entry links to `/projects/${project.id}/${section.key}`.

### Project glyph

A small color swatch derived from the project's `appearance` field (`projectGlyph`, `Sidebar.tsx:88-101`) appears next to the project name. Same swatch is used on the Projects page tiles for visual consistency.

## Collapsed mode

When the sidebar is collapsed (≤56px wide):

- Section headers disappear; only icons remain.
- Nav items become icon-only with tooltips on hover (`ItemRow`, `Sidebar.tsx:177-198`).
- Project rows collapse to glyphs; project sub-nav is unreachable from the collapsed state — users must expand to navigate inside a project.
- The collapse toggle is in the Toolbar (`⌘0`).

Sidebar width is persisted to `drill-shell-sidebar-width`. Collapse state to `drill-shell-sidebar-collapsed`.

## Adding a new nav item

### To Workspace / Library / Administration

Edit the matching array at the top of `Sidebar.tsx`. Pick an AntD icon from `@ant-design/icons`. Use `matchPrefix` if the page has sub-routes that should still highlight the entry.

```ts
// Example: adding the Cluster page to Administration
const ADMIN: NavItem[] = [
  // …existing…
  { to: '/cluster', label: 'Cluster', icon: <ClusterOutlined /> },
];
```

If the page is admin-only and you want to hide it from non-admins, add a check inside the Administration section's render — there isn't a built-in `adminOnly` flag on `NavItem` today; add one if multiple admin-only items emerge.

### To the project sub-nav

Edit `BASE_PROJECT_SECTIONS` or `TRAILING_PROJECT_SECTIONS`. The `key` must match the route segment under `/projects/:id/`. If the new section should only appear conditionally, follow the Workflows pattern: compute visibility in `sectionsFor` (`Sidebar.tsx:493+`).

### Keep in mind

- Section ordering matters — Library and Administration are after Projects to keep workspace-level nav at the top.
- The "Administration" section is collapsed by default; new pages added there start hidden.
- Update [`sidebar-navigation.md`](sidebar-navigation.md) and the [`README.md`](README.md) page index when you add an item that introduces a new top-level route.
- If you change the project sub-nav, also check `ProjectNavBar.tsx` — it renders the same set of tabs horizontally at the top of project pages.

## Related components

- `LeftRail.tsx` — the optional second column inside a page (e.g. SQL Lab's schema browser). Not part of the sidebar. See [`components/app-shell.md`](components/app-shell.md).
- `Toolbar.tsx` — owns the sidebar collapse button. See [`components/app-shell.md`](components/app-shell.md).
- `ProjectNavBar.tsx` — horizontal tabbed nav rendered at the top of project pages. Mirrors the project sub-nav.
