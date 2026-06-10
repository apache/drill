# Contexts and providers

The React app uses four React Context providers to share cross-cutting state. They are all wired up in `src/main.tsx` and `src/App.tsx`:

```tsx
// main.tsx
<Provider store={store}>                        // Redux
  <QueryClientProvider client={queryClient}>    // react-query
    <BrowserRouter basename="...">
      <ThemeProvider>                            // ← contexts.md
        <App />
      </ThemeProvider>
    </BrowserRouter>
  </QueryClientProvider>
</Provider>

// App.tsx
<AppChromeProvider>                              // ← contexts.md
  <AiModalProvider>                              // ← contexts.md
    <AppShell>{routes}</AppShell>
  </AiModalProvider>
</AppChromeProvider>

// ProjectLayout.tsx (per-route)
<ProjectContextProvider projectId={id}>          // ← contexts.md
  <Outlet />
</ProjectContextProvider>
```

Each one owns a single, well-defined slice of state. They do not depend on each other.

## ThemeProvider

**File:** `src/hooks/useTheme.tsx`

**Owns:** light / dark / system theme mode, applies it to `document.documentElement`, and configures Ant Design's `ConfigProvider` with the matching algorithm and custom component tokens.

**Hook:** `useTheme()` returns `{ mode, setMode, isDark }` where `mode` is `'light' | 'dark' | 'system'`.

**Behavior:**

- `mode = 'system'` listens to `prefers-color-scheme` and switches on the fly.
- `mode` is persisted to `localStorage` under `drill-theme-mode`.
- Sets `html.dark` class when dark — global CSS uses `:root.dark` selectors for dark-only rules. See [`styling.md`](styling.md).
- Toggles via `⌘L` keyboard shortcut (handled in Toolbar) and via the moon/sun button in the Toolbar.

**Component tokens** (`useTheme.tsx` ~line 39-292) override AntD defaults for buttons, inputs, selects, tabs, modals, and tables to match the Sonoma/Tahoe look. Continuous corner radii, Apple motion curves, glass surfaces.

## AppChromeProvider

**File:** `src/contexts/AppChromeContext.tsx`

**Owns:** the chrome around the page — breadcrumb, toolbar actions, inspector tabs, left rail config — plus the open/collapsed state of the sidebar, inspector, and left rail.

**Hooks:**

- `useAppChrome()` — read or update chrome state. Returns:
  - `sidebarCollapsed`, `setSidebarCollapsed`, `toggleSidebar`
  - `inspectorOpen`, `setInspectorOpen`, `toggleInspector`
  - `leftRailCollapsed`, `setLeftRailCollapsed`, `toggleLeftRail`
  - `activeInspectorTab`, `setActiveInspectorTab`
  - `inspectorWidth`, `setInspectorWidth`
  - `leftRailWidth`, `setLeftRailWidth`
  - `sidebarWidth`, `setSidebarWidth`
  - `pageChrome` — the currently registered chrome from `usePageChrome`
- `usePageChrome(chrome)` — call from a page or top-level component to register its chrome. The chrome stays active until the calling component unmounts. Shape:

  ```ts
  interface PageChrome {
    breadcrumb?: BreadcrumbItem[];
    toolbarActions?: ReactNode;
    inspectorTabs?: InspectorTab[];
    leftRail?: ReactNode;
  }
  ```

**Behavior:**

- `sidebarCollapsed`, `inspectorOpen`, `leftRailCollapsed` are persisted to `localStorage` (`drill-shell-*` keys, see [`architecture.md`](architecture.md)).
- Sidebar and inspector widths are also persisted.
- Mobile: sidebar defaults to closed on first load below 768px; left rail auto-collapses below `LEFT_RAIL_AUTO_COLLAPSE_PX` (1100px, `AppChromeContext.tsx:87`).
- Chrome registration is idempotent — calling `usePageChrome` again with the same payload is cheap. Render-time chrome updates (e.g. an editing flag that swaps the breadcrumb) are fine.

**Pages do not render any chrome themselves.** They call `usePageChrome` and the AppShell renders whatever is currently registered. This keeps the shell layout consistent and means individual pages don't accidentally duplicate breadcrumbs or fight over toolbar slots.

### Example

```tsx
function MyPage() {
  usePageChrome({
    breadcrumb: [{ label: 'Library' }, { label: 'My Page' }],
    toolbarActions: <Button icon={<PlusOutlined />}>New</Button>,
    inspectorTabs: [{ key: 'help', label: 'Help', content: <HelpPanel /> }],
  });
  return <div>page content</div>;
}
```

## AiModalProvider

**File:** `src/contexts/AiModalContext.tsx`

**Owns:** the open/closed state of the global AI assistant modal triggered from the Toolbar's AI menu.

**Hook:** `useAiModal()` returns `{ isOpen, mode, openModal, closeModal }` where `mode` is `'suggestions' | 'explain' | 'optimize'`.

**Behavior:**

- Toolbar calls `openModal('suggestions')`, `openModal('explain')`, etc. from its AI menu items.
- `AiAssistantModal` (rendered inside ProjectQueryPage and SqlLabPage when appropriate) reads `mode` to decide which UI to show.
- This is **separate from** Prospector's chat context (`ChatContext` in `types/ai.ts`). The modal handles one-shot SQL-on-selection AI features; Prospector is the persistent chat assistant in the right inspector. See [`components/prospector.md`](components/prospector.md).

## ProjectContextProvider

**File:** `src/contexts/ProjectContext.tsx`

**Owns:** the currently loaded project for project-scoped pages.

**Provided by:** `ProjectLayout` (`src/components/project/ProjectLayout.tsx`), wrapping every route under `/projects/:id/*`.

**Hook:** `useProjectContext()` returns:

```ts
{
  project: Project | undefined;
  isLoading: boolean;
  error: unknown;
  savedQueryIdSet: Set<string>;       // memoized for O(1) lookups
  visualizationIdSet: Set<string>;
  dashboardIdSet: Set<string>;
}
```

**Behavior:**

- Fetches the project via react-query (`getProject(projectId)`, key `['project', projectId]`).
- 404s do not retry (`retry: false` for the not-found case, `ProjectContext.tsx:51`).
- Memoized `Set`s let project sub-pages filter aggregate lists (saved queries, visualizations, dashboards) in O(1).
- Throws (via `useProjectContext`) if called outside a `ProjectContextProvider` — protects against mis-mounting outside `/projects/:id/*`.

Use this context from any page rendered under `ProjectLayout`. Do not use it from cross-project pages.

## Redux store

Not a Context per se, but worth knowing where it sits: `src/store/index.ts` configures a Redux Toolkit store with two slices. See [`data-flow.md`](data-flow.md) for the slices and when to use Redux vs react-query vs local state.

## When to add a new context vs. lift state

Use a new Context only when:

- The state crosses many unrelated components and threading it through props is genuinely painful, **and**
- The state changes infrequently or you've accepted the cost of re-rendering all consumers when it does.

Prefer:

- **react-query** for anything that originates on the server (cache, refetch, invalidation).
- **Redux** for editor-style mutable client state that survives navigation (SQL Lab tabs, editor height).
- **Local `useState`** for everything else.
- **`usePageChrome`** instead of a new context for "page wants to influence the shell."

If you do add a new provider, wire it up next to the existing ones (`main.tsx` for global, `App.tsx` for shell-level, `ProjectLayout.tsx` for project-level) and document it here.
