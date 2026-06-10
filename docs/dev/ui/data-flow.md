# Data flow

Three layers handle data in the UI:

1. **API client** (`src/api/client.ts`) — single axios wrapper that all REST modules share. Owns CSRF, base URL, 401 handling.
2. **react-query** — caches server data, handles refetch, optimistic updates, invalidation. Used by almost everything.
3. **Redux** (`src/store/`) — mutable client state that needs to survive navigation. Currently just SQL Lab editor state.

Local `useState` covers everything else.

## API client

**File:** `src/api/client.ts`

A single axios instance is exported as `apiClient` and re-exported per-module from `src/api/*.ts`. Configuration:

| Option | Value | Why |
|---|---|---|
| `baseURL` | `''` (relative) | Lets dev mode use Vite's proxy and prod use same-origin |
| `withCredentials` | `true` | Sends the Drill session cookie on every request |
| `timeout` | none | Long-running queries shouldn't time out at the HTTP layer |

### Request interceptor — CSRF

The CSRF token is fetched once at app boot from `GET /api/v1/csrf-token` (a JSON endpoint backed by `CsrfTokenResource` on the server, which reads the session attribute set by `CsrfTokenInjectFilter`). The token is cached in a module-level promise, mirrored into the `<meta name="csrf-token">` tag for non-axios consumers, and attached to mutating requests by an async request interceptor:

```ts
// client.ts
apiClient.interceptors.request.use(async (config) => {
  const method = config.method?.toLowerCase() ?? '';
  if (MUTATING_METHODS.has(method)) {
    const token = await getCsrfToken();
    if (token) config.headers['X-CSRF-Token'] = token;
  }
  return config;
});
```

Module entry points:

- `prefetchCsrfToken()` — warms the cache. Called from `main.tsx` at app boot so the first POST doesn't pay the round-trip.
- `resolveCsrfToken()` — async getter used by non-axios callers (the Prospector SSE stream in `api/ai.ts`, anywhere else doing manual `fetch`).
- `invalidateCsrfToken()` — drops the cache. Call after login or any flow that rotates the session.

Fallback order (handles both edge cases and legacy callers):

1. `fetchCsrfToken()` — `/api/v1/csrf-token` (primary).
2. `<meta name="csrf-token">` (populated by the prefetch, kept for sync DOM readers like the Pyodide-embedded Python in `usePyodide.ts`).
3. `drill.csrf.token` cookie (legacy).

### Response interceptor — 401 redirect

```ts
apiClient.interceptors.response.use(
  (r) => r,
  (err) => {
    if (err.response?.status === 401) {
      const redirect = encodeURIComponent(window.location.pathname + window.location.search);
      window.location.href = `/mainLogin?redirect=${redirect}`;
    }
    return Promise.reject(err);
  }
);
```

On a 401, the client redirects to `/mainLogin` so Drill's auth handler can prompt for credentials and bounce back. After the login refactor, `/mainLogin` will be a React route too.

### API modules

One module per backend area, each exporting typed functions that wrap `apiClient` calls. As of today:

| Module | Backend endpoints |
|---|---|
| `projects.ts` | `/api/v1/projects/*` |
| `queries.ts` | `/query.json`, query execution |
| `savedQueries.ts` | `/api/v1/savedQueries/*` |
| `sharedQueries.ts` | `/api/v1/sharedQueries/*` |
| `visualizations.ts` | `/api/v1/visualizations/*` |
| `dashboards.ts` | `/api/v1/dashboards/*` |
| `schedules.ts` | `/api/v1/schedules/*` |
| `metadata.ts` | `/schema.json`, `/api/v1/metadata/*` (schema/table introspection) |
| `storage.ts` | `/storage.json`, `/storage/{name}.json`, enable/disable, OAuth helpers |
| `metrics.ts` | `/api/v1/metrics`, `/metrics` |
| `options.ts` | `/options.json`, `/internal_options.json`, `/api/v1/options/*` |
| `logs.ts` | `/api/v1/logs/*` |
| `profile.ts` | `/profiles.json`, `/profiles/{queryid}.json`, `/profiles/cancel/{queryid}` |
| `resultCache.ts` | result cache GET/PUT for SQL Lab tab persistence |
| `user.ts` | `/api/v1/user`, current-user info |
| `ai.ts` | `/api/v1/ai/*` (Prospector chat, status, transpile, format SQL) |
| `aiAnalytics.ts` | `/api/v1/ai-analytics/*` |
| `smtp.ts` | `/api/v1/smtp/*` for alert emails |
| `geojson.ts` | `/sqllab/geojson/*` for map visualizations |

Conventions:

- Functions are named in verb form: `getX`, `createX`, `updateX`, `deleteX`, `restoreX`, `executeX`.
- Each function returns a typed promise. Types live with the module (`projects.ts` defines `Project`) or in `src/types/`.
- Modules do not import each other — if you need to call multiple APIs, do it at the caller (component or hook).
- All requests go through `apiClient`. Never call `axios.get` directly.

## react-query

**File:** `src/main.tsx`

```ts
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      retry: 1,
      staleTime: 5 * 60 * 1000, // 5 minutes
    },
  },
});
```

### Conventions

**Query keys** are arrays starting with the resource name:

```ts
useQuery({ queryKey: ['projects'], queryFn: getProjects });
useQuery({ queryKey: ['project', projectId], queryFn: () => getProject(projectId) });
useQuery({ queryKey: ['profiles', { status }], queryFn: () => getProfiles(status) });
```

Keep keys consistent across the app so `invalidateQueries` works. When mutating a project, invalidate both `['projects']` and `['project', id]`.

**Mutations:**

```ts
const mutation = useMutation({
  mutationFn: updateProject,
  onSuccess: () => queryClient.invalidateQueries({ queryKey: ['projects'] }),
});
```

Prefer `invalidateQueries` over manual cache updates unless you're optimizing a hot path.

**Polling** is implemented with `refetchInterval`:

```ts
useQuery({
  queryKey: ['profiles'],
  queryFn: getQueryProfiles,
  refetchInterval: 5000,  // ProfilesPage
});
```

Stop polling by setting `refetchInterval` to `false` when the page is hidden or the user pauses.

**Streaming** (Prospector chat) does not use react-query — it uses native `fetch` for SSE. See [`components/prospector.md`](components/prospector.md).

## Redux

**File:** `src/store/index.ts`

```ts
export const store = configureStore({
  reducer: { query: querySlice, ui: uiSlice },
  middleware: (gdm) => gdm({
    serializableCheck: { ignoredPaths: ['query.tabs'] },  // results may include non-serializable rows
  }),
});
```

### `querySlice` — SQL Lab tabs

`src/store/querySlice.ts`. Owns the array of open editor tabs and which tab is active. Each tab has:

```ts
{
  id: string;
  name: string;             // user-editable
  sql: string;
  results?: QueryResult;    // last execution result, or undefined
  cacheId?: string;         // resultCache id for restore-after-reload
  isRunning: boolean;
  vizIds?: string[];        // visualizations attached to this tab
  isLocked: boolean;        // saved-query-backed tabs are locked from rename/close
}
```

Why Redux instead of react-query: tabs are mutable client state with frequent local edits (typing in the editor), and they need to survive navigation away from `/query` and back without losing the typed-but-unsaved SQL.

Tabs are also persisted to `localStorage` via `useTabPersistence` so they survive page reloads.

### `uiSlice` — SQL Lab UI state

`src/store/uiSlice.ts`. Owns:

- `editorHeight` (150–600 px, persisted to localStorage)
- `selectedSchema`, `selectedTable` (current schema browser selection)
- `resultsPanelTab` (`'results' | 'visualization' | 'history'`)

These are all small flags that the editor and schema browser need to read. Local `useState` would force prop-drilling through many sub-components.

### When to use Redux

Use Redux only when **all** of the following are true:

1. The state is client-only (not a cached server response).
2. It must survive navigation between routes.
3. It is read or written by more than two components.

Otherwise: local `useState`, or `usePageChrome` for shell-level concerns. Adding a new Redux slice should be a deliberate decision — the SQL Lab pair is the bar.

## Putting it together

Lifecycle of a typical fetch (e.g. Projects page):

1. Component mounts: `useQuery({ queryKey: ['projects'], queryFn: getProjects })`.
2. react-query checks its cache. If fresh (< 5 min old), returns immediately.
3. Otherwise calls `getProjects()` from `src/api/projects.ts`, which calls `apiClient.get('/api/v1/projects')`.
4. axios interceptor attaches CSRF for mutating requests (not this one).
5. axios sends the request; Drill returns JSON.
6. axios interceptor checks for 401 (would redirect); none here.
7. react-query stores the result and the component re-renders.
8. User clicks "delete": `useMutation({ mutationFn: deleteProject })` fires.
9. CSRF interceptor adds `X-CSRF-Token` header.
10. On success, `invalidateQueries(['projects'])` triggers a refetch and the UI updates.

## Testing

API modules have Vitest tests next to them (`src/api/*.test.ts`). Tests mock axios to assert URL, method, body, and the parsing of the response into the typed shape. When adding a new module, follow the existing pattern.
