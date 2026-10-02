# App Shell

Covers the components in `src/components/shell/` that own the global UI chrome: AppShell, Sidebar, Toolbar, LeftRail, RightInspector, PreferencesModal, BrowseDataDrawer, TrashBody, WorkspaceRecoveryBody, GlobalProspectorTab.

The shell renders once in `App.tsx` and stays mounted across navigations; pages render inside it. Pages do not own chrome — they register what they need via [`usePageChrome`](../contexts.md), and the shell renders whatever is currently registered.

## AppShell

**File:** `src/components/shell/AppShell.tsx`

The outer orchestrator. Renders the three-pane Sonoma-style layout:

```
+-------------------------------------------------------------------+
| Sidebar  | Toolbar                                                |
|          |--------------------------------------------------------|
|          | LeftRail (optional) | Page content | RightInspector    |
|          |                     |              | (optional)        |
+-------------------------------------------------------------------+
```

### Responsibilities

- Reads chrome state from `useAppChrome()` and lays out the panes.
- Handles global keyboard shortcuts:
  - `?` — open shortcuts modal
  - `⌘0` — toggle sidebar
  - `⌘⌥0` — toggle inspector
  - `⌘B` — toggle left rail (or open Browse Data drawer if there's no left rail)
  - `⌘L` — toggle theme (delegated to Toolbar)
  - `⌘,` — open Preferences
  - `⌘K` — open spotlight search (delegated to Toolbar)
- Mounts the modals that live at shell level: `PreferencesModal`, `BrowseDataDrawer`, `TrashBody`, `WorkspaceRecoveryBody`, keyboard-shortcuts overlay.
- Sets up the mobile-overlay vs. inline-column behavior based on viewport.

### Mobile behavior

Below 768px the sidebar and inspector switch from inline columns to overlay drawers with a backdrop. The first user-initiated open / close on mobile is treated as a manual override and disables the auto-close-on-navigation behavior so it doesn't fight the user.

---

## Sidebar

**File:** `src/components/shell/Sidebar.tsx`

The persistent left nav. Detailed reference in [`../sidebar-navigation.md`](../sidebar-navigation.md) — that doc covers section structure, the projects list, and conventions for adding nav items.

### Key responsibilities

- Render the four sections (Workspace, Projects, Library, Administration).
- Project list: search, expand/collapse per-project sub-nav, drag-reorder, "show all" overflow.
- Width: 232px default, 56px collapsed, resizable 200–380px. Persisted to `drill-shell-sidebar-width`.
- Collapse state persisted to `drill-shell-sidebar-collapsed`.
- Collapsed mode: icon-only rows with tooltips, no sub-nav (users must expand to navigate into a project).

### Internal helpers

- `Section` — collapsible header + body. Used for the four sections.
- `ItemRow` — single nav row (icon + label + optional trailing badge). Renders as `<Link>` with active styling.
- `ProjectRow` — expandable row per project with chevron, glyph, drag handle, and sub-nav.
- `usePinnedRecentProjects` (`src/components/shell/usePinnedRecentProjects.ts`) — owns the pin / order state in localStorage.

---

## Toolbar

**File:** `src/components/shell/Toolbar.tsx`

48px-tall header above the page content.

### Slots (left to right)

1. **Sidebar toggle** (`⌘0`)
2. **Breadcrumb** — from `pageChrome.breadcrumb` if present; otherwise derived from the URL via `TOP_LEVEL_LABELS` and project name lookup
3. **Spotlight search** (`⌘K`) — opens `CommandPalette` for fuzzy navigation
4. **Page actions** — from `pageChrome.toolbarActions`
5. **AI menu** — Suggestions / Explain / Optimize (opens `AiAssistantModal` via `useAiModal`)
6. **Theme toggle** (`⌘L`) — sun / moon icon, drives `useTheme().setMode`
7. **Preferences** (`⌘,`) — opens `PreferencesModal`
8. **Help** — opens the shortcuts modal
9. **Browse Data** (`⌘B`) — opens `BrowseDataDrawer` (or toggles left rail if the page has one)
10. **Inspector toggle** (`⌘⌥0`) — opens / closes the right inspector

### Breadcrumb derivation

When the page doesn't supply a breadcrumb, the Toolbar derives one from the URL:

- `/projects` → `Projects`
- `/projects/:id` → `Projects > <Project Name> > <Section>` (resolves the name from the cached project)
- Other top-level paths → `TOP_LEVEL_LABELS[path]` (e.g. `/datasources` → `Data Sources`)

When a page does supply `breadcrumb` via `usePageChrome`, that takes precedence.

---

## LeftRail

**File:** `src/components/shell/LeftRail.tsx`

Optional second column between the Sidebar and the main page content. Used today by SQL Lab for the schema explorer.

- Width: 280px default, resizable 220–480px. Persisted to `drill-shell-left-rail-width`.
- Collapse state persisted to `drill-shell-left-rail-collapsed`.
- Auto-collapses below 1100px (`LEFT_RAIL_AUTO_COLLAPSE_PX` in `AppChromeContext.tsx`).
- Pages opt in via `usePageChrome({ leftRail: <SomeComponent /> })`. The component is rendered as-is; the rail provides only the surrounding chrome (collapse handle, resize handle, background).

---

## RightInspector

**File:** `src/components/shell/RightInspector.tsx`

Optional tabbed panel on the right. Always carries the global Prospector tab; pages can register additional tabs.

- Width: 360px default, resizable 280–720px. Persisted to `drill-shell-inspector-width`.
- Open state persisted to `drill-shell-inspector-open`.
- Tabs rendered: `[GlobalProspectorTab, ...pageChrome.inspectorTabs]`.
- Active tab persisted to `drill-shell-active-inspector-tab`.

### `InspectorTab` shape

```ts
interface InspectorTab {
  key: string;
  label: string;
  icon?: ReactNode;
  content: ReactNode;
}
```

Tabs are simple — there's no per-tab state management built into the inspector. Tab `content` is rendered when the tab is active and unmounted when it isn't, so each tab is responsible for its own state preservation if needed.

---

## GlobalProspectorTab

**File:** `src/components/shell/GlobalProspectorTab.tsx`

The always-available Prospector chat tab in the right inspector. Instantiates `useProspector()` with empty `ChatContext` and renders `ProspectorPanel`. See [`prospector.md`](prospector.md).

---

## PreferencesModal

**File:** `src/components/shell/PreferencesModal.tsx`

User preferences: theme, default schema, autoLimit defaults, AI provider preview, keyboard layout. Opens from the Toolbar (`⌘,`) and from various "Settings" links elsewhere.

Settings persist to localStorage with the `drill-` prefix. Most of the modal is form fields backed by `useTheme`, `useUser`, and a few local hooks.

---

## BrowseDataDrawer

**File:** `src/components/shell/BrowseDataDrawer.tsx`

Cross-cutting schema browser drawer. Opens from the Toolbar (`⌘B`) when no left rail is present (e.g. on the Projects page). Same tree component as the SQL Lab schema explorer, just rendered in a drawer instead of a rail.

---

## TrashBody and WorkspaceRecoveryBody

**Files:** `src/components/shell/TrashBody.tsx`, `src/components/shell/WorkspaceRecoveryBody.tsx`

Modal bodies for recoverable resources:

- **Trash** — soft-deleted projects, saved queries, visualizations, dashboards. Restore (calls the matching `restore*` API) or permanently delete.
- **Workspace Recovery** — recovers an interrupted SQL Lab session from the result cache.

Mounted by AppShell so they're available globally.

---

## Conventions

- **No chrome leaks.** Pages do not render Sidebar, Toolbar, LeftRail, or RightInspector elements themselves. If you find yourself doing so, add it to `usePageChrome` instead.
- **Persistence keys.** Use the `drill-shell-*` prefix for shell-level localStorage. Document new keys in [`../architecture.md`](../architecture.md).
- **Width clamps.** Sidebar 200–380, LeftRail 220–480, Inspector 280–720. Honor these unless you have a strong UX reason not to.
- **Mobile behavior** is CSS-driven. Add new shell elements to the `@media (max-width: 768px)` block in `index.css`.
- **Keyboard shortcuts** are registered in `AppShell.tsx`. Add new ones there; document them in the help modal.
