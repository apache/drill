# Routing

There are two routing layers and they must agree:

1. **Server-side routing** — Jetty + JAX-RS (Jersey) decide which Java resource or static asset handles each HTTP request.
2. **Client-side routing** — React Router decides which page component renders for each in-app navigation.

When the browser requests `/projects` directly (a hard refresh, a bookmark, an inbound link), both layers must cooperate: the server has to return the React SPA's `index.html`, and then the client router has to recognize `/projects` and render `ProjectsPage`.

> **History.** Earlier in the React migration the SPA was mounted at `/sqllab/*` while the legacy Freemarker UI lived at `/`. Phase 0 of the legacy-UI-removal refactor moved the SPA to `/`; the legacy HTML endpoints are being removed phase by phase. If you find a doc, a memory, or a comment referring to `/sqllab/*`, it is pre-Phase-0 and should be updated.

## Server-side routing

### Servlets and their mappings

Defined in `WebServer.java` (`exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/WebServer.java`, ~line 195+):

| Mapping | Handler | Purpose |
|---|---|---|
| `/dynamic/*` | `DefaultServlet` | Dynamically generated JavaScript (option/storage form templates) |
| `/static/*` | `DefaultServlet` | Drill's static icon/logo assets |
| `/*` | `ServletContainer` (Jersey) | All JAX-RS resources, including the React SPA via `SpaResource` |

### Jersey resource paths

Every page or REST endpoint is a JAX-RS resource under `org.apache.drill.exec.server.rest`. After Phase 5 the only resources that still produce HTML are:

| Path | Resource | Produces | Reason kept |
|---|---|---|---|
| `/status` | `StatusResources` | `status.ftl` ("Running!") | External monitors / load balancers poll this URL |
| `/storage/{name}/update_oauth2_authtoken` | `StorageResources` | `storage/success.html` | Registered OAuth callback URL; popup closes itself |
| `/credentials/{name}/update_oauth2_authtoken` | `CredentialResources` | `storage/success.html` | Same as above |
| `POST /login` (auth-failure dispatch target) | `LogInLogOutResources` | Tiny HTML stub that redirects to `/login?error=1` | Jetty's `FormAuthenticator` requires SOMETHING here |

Every other legacy HTML endpoint has been deleted across Phases 0–5. JSON siblings (`/cluster.json`, `/query.json`, `/storage.json`, `/storage/{name}.json`, `/profiles.json`, `/profiles/{queryid}.json`, `/status.json`, `/credentials.json`, etc.) stay and are consumed by the React app and by external clients (BI tools, scripts).

The legacy `index.ftl`, `query/`, `storage/list.ftl` + `storage/update*.ftl`, `profile/`, `credentials/`, `threads/`, `login.ftl`, `mainLogin.ftl`, `alertModals.ftl`, `confirmationModals.ftl`, `errorMessage.ftl`, `runningQuery.ftl` are all gone. `generic.ftl` survives because `status.ftl` includes it.

`FreemarkerMvcFeature` and the `jersey-mvc-freemarker` dependency stay registered for the one remaining template (`status.ftl`). `ViewableWithPermissions` likewise stays — only `StatusResources.getStatus()` uses it.

### How the SPA gets served

The React SPA is served by `SpaServletFilter` (`exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/SpaServletFilter.java`), a Jakarta Servlet filter registered at `/*` ahead of Jersey. The filter classifies each request and either short-circuits it (SPA route or static asset) or chains through to Jersey (JSON endpoint).

> **Why not a JAX-RS catch-all?** An earlier implementation used a JAX-RS resource at `@Path("/{path:.*}")`. Jersey treats that template as MORE specific than `@Path("/")` (one capturing group vs. zero), so the catch-all shadowed every root-level JSON endpoint (`/cluster.json`, `/state`, `/storage.json`, …). Running before Jersey with an explicit allow-list is the only way to keep the JSON endpoints reachable without rewriting every resource.

`SpaServletFilter` behavior, in order:

1. Non-GET / non-HEAD requests chain through to Jersey untouched.
2. Anything ending in `.json` chains through to Jersey.
3. Paths in `JERSEY_EXACT_PATHS` or starting with a `JERSEY_PREFIXES` entry chain through to Jersey.
4. Paths with a file extension are served as static assets from `/webapp/dist/` on the classpath. If no such asset exists, a flat `404` is returned (no fall-through, since Jersey wouldn't have it either).
5. Everything else is treated as a React Router route and served `/webapp/dist/index.html`. The filter falls back to `/webapp/index.html` in dev runs where the webapp hasn't been built.

Path traversal is blocked via `Path.normalize()` containment.

The legacy `SqlLabSpaServlet` and its `/sqllab/*` servlet mapping were deleted in Phase 0.

### Coexistence rules

The filter and the existing JSON-only JAX-RS endpoints must not collide. The rules:

- **Reserved JSON paths** (kept exactly as today): `/cluster.json`, `/query.json`, `/status.json`, `/storage.json`, `/storage/{name}.json`, `/storage/create_update`, `/storage/{name}/enable/{val}`, `/storage/{name}/update_*`, `/profiles.json`, `/profiles/{queryid}.json`, `/profiles/completed.json`, `/profiles/running.json`, `/profiles/cancel/{queryid}`, `/credentials.json`, `/credentials/{name}/update_credentials.json`, `/options.json`, `/internal_options.json`, `/metrics`, `/queriesCount`, `/portNum`, `/gracePeriod`, `/gracefulShutdown`, `/quiescent`, `/shutdown`, `/state`, `/{path}-plugins.json` patterns.
- **API namespace**: `/api/v1/*` is owned by the modern JAX-RS APIs (`MetadataResources`, `SavedQueriesResources`, `VisualizationsResources`, `DashboardsResources`, `ProspectorResources`, `AiConfigResources`, `OptionsResources`, etc.). No client-side routes ever start with `/api/`.
- **Static assets**: `/static/*` and `/dynamic/*` keep their servlets. `/assets/*` (Vite's hashed build output) is served by the catch-all from `dist/assets/`.
- **GeoJSON for map visualizations**: served from `/geojson/*` (was `/sqllab/geojson/*` pre-Phase-0).
- **CSRF token endpoint**: `GET /api/v1/csrf-token` returns `{token: "..."}`. The React API client fetches it once at boot and caches it. See [`data-flow.md`](data-flow.md).
- **Anything else** falls through to the catch-all and gets `index.html`.

If you add a new top-level React route, make sure its first path segment does not collide with any reserved JSON path above.

## Client-side routing

### Provider setup

`src/main.tsx`:

```tsx
<BrowserRouter basename="/">
  <ThemeProvider>
    <App />
  </ThemeProvider>
</BrowserRouter>
```

Vite `base` in `vite.config.ts` is also `/`. Both must match.

### Route table

Declared in `src/App.tsx`:

```
/                                  →  redirect to /projects
/projects                          →  ProjectsPage
/projects/:id                      →  ProjectLayout
  ├─ (index)                       →  redirect to ./query
  ├─ query                         →  ProjectQueryPage
  ├─ queries                       →  ProjectSavedQueriesPage
  ├─ visualizations                →  ProjectVisualizationsPage
  ├─ visualizations/:vizId         →  ProjectVisualizationDetailPage
  ├─ dashboards                    →  ProjectDashboardsPage
  ├─ dashboards/:dashboardId       →  DashboardViewPage
  ├─ datasources                   →  ProjectDataSourcesPage
  ├─ datasources/:name             →  ProjectDataSourceEditPage
  ├─ workflows                     →  ProjectWorkflowsPage
  ├─ wiki                          →  ProjectWikiPage
  ├─ wiki/:pageId                  →  ProjectWikiPage
  └─ settings                      →  ProjectDetailPage
/datasources                       →  DataSourcesPage
/datasources/:name                 →  DataSourceEditPage
/query                             →  SqlLabPage
/saved-queries                     →  SavedQueriesPage
/workflows                         →  WorkflowsPage
/profiles                          →  ProfilesPage
/profiles/:queryId                 →  ProfileDetailPage
/visualizations                    →  VisualizationsPage
/visualizations/:vizId             →  VisualizationDetailPage
/dashboards                        →  DashboardsPage
/dashboards/:id                    →  DashboardViewPage
/metrics                           →  MetricsPage
/options                           →  OptionsPage
/logs                              →  LogsPage
/ai-analytics                      →  AiAnalyticsPage
*                                  →  redirect to /projects
```

Every page component is lazy-loaded via `React.lazy`. The Suspense fallback is a centered AntD `<Spin>`.

### Three-layer mental model

1. **Workspace** (everything not under `/projects/:id/`) — home + cross-project aggregate views + admin.
2. **Project-scoped** (`/projects/:id/...`) — same kinds of pages, but filtered to a single project. Often implemented as thin wrappers around the workspace page (see [`pages/project-layout.md`](pages/project-layout.md)).
3. **Admin / system** — `/metrics`, `/options`, `/logs`, `/ai-analytics`, and the upcoming `/cluster`, `/credentials`, `/status`, `/threads`. Listed under the "Administration" section of the sidebar.

### Adding a new route

1. Create the page in `src/pages/YourPage.tsx`.
2. Add a lazy import + a `<Route>` in `src/App.tsx`.
3. If the page should appear in nav, add an entry to the right section in `Sidebar.tsx` — see [`sidebar-navigation.md`](sidebar-navigation.md).
4. Create `docs/dev/ui/pages/your-page.md` and link it from [`README.md`](README.md).
5. If your route's first path segment could collide with a Jersey-served path (`storage`, `profiles`, `credentials`, anything ending in `.json`), pick a different name.

### Navigation and links

Use `<Link to="/path">` and `useNavigate()` from `react-router-dom`. The router's `basename` is `/`, so paths are absolute as written. Do not hand-construct URLs with any prefix.

For state passed between routes (e.g. SQL Lab loading a saved query), use `navigate('/query', { state: { sql, name } })` and read via `useLocation().state`. Avoid query strings for ephemeral state.
