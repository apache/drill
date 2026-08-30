# Server-Side Tabs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move SQL Lab query tabs from browser-only scratch state to per-user, server-persisted project documents that appear in the project tree, where closing hides a tab and deleting is explicit.

**Architecture:** Two-tier persistence. Every tab is written to `localStorage` immediately; a tab is promoted to a `drill.sqllab.tabs` PersistentStore when a query is executed from it, or when it is closed holding content. The server tier is what the project tree lists. Redux stays the in-memory source of truth for the active session; `useWorkspacePersistence` gains a server sync alongside its existing local save.

**Tech Stack:** Java / JAX-RS (Jersey) + Jackson + Drill `PersistentStore` on the backend; React 18 + Redux Toolkit + TypeScript + antd + Vitest on the frontend.

**Spec:** [`../TabPersistence.md`](../TabPersistence.md)

## Global Constraints

- After modifying anything in `exec/java-exec`, run `mvn checkstyle:check -pl exec/java-exec`. All `if` statements need braces; no unused imports.
- Every new source file (Java, TS, TSX, CSS) needs an Apache 2.0 license header. Copy one from a neighbouring file.
- Do not add Claude as a git co-author. Commit messages are imperative ("Add tab store", not "Added tab store").
- Frontend commands run from `exec/java-exec/src/main/resources/webapp`.
- Backend tests that hit HTTP extend `ClusterTest` and start a cluster with `ExecConstants.HTTP_ENABLE` and `HTTP_PORT_HUNT`, then read `cluster.drillbit().getWebServerPort()`. Copy the harness from `TestProjectSchemaCacheEndpoints`.
- `contrib/` modules compile against the *installed* `drill-java-exec` jar. If a task changes a public java-exec method used elsewhere, run `mvn -pl exec/java-exec install -DskipTests` before building dependants.

---

## Phase 1 — Server-side store and API

### Task 1: Tab record and PersistentStore

**Files:**
- Create: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/QueryTabStore.java`
- Test: `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestQueryTabStore.java`

**Interfaces:**
- Consumes: nothing.
- Produces: `QueryTabStore.get(PersistentStoreProvider, WorkManager)` returning a singleton; `QueryTabStore.TabRecord` with the fields below; `List<TabRecord> list(String owner, String projectId)`, `TabRecord find(String id)`, `void save(TabRecord)`, `void delete(String id)`. `storeKey(owner, projectId, tabId)` is package-private for tests.

The store key is `owner`, `projectId` and `tabId` joined by a NUL separator
(`\u0000`, written in Java as `\0`), with the literal string `_global` standing in
for a null `projectId`. NUL cannot appear in a username, so no user can craft a key
that resolves into another user's namespace — a printable separator such as `:` could.
`PersistentStore` offers only `get`, `put`, `delete`, `putIfAbsent`, `getAll()` and
`getRange(skip, take)` — no prefix scan — so `list` is `getAll()` plus a filter, which
is the approach `ProjectResources` already uses for projects.

- [ ] **Step 1: Write the failing test**

```java
@Test
public void testSaveAndListScopedByOwnerAndProject() {
  QueryTabStore store = QueryTabStore.get(provider, workManager);

  store.save(newRecord("t1", "alice", "proj1", "SELECT 1"));
  store.save(newRecord("t2", "bob", "proj1", "SELECT 2"));
  store.save(newRecord("t3", "alice", "proj2", "SELECT 3"));

  List<QueryTabStore.TabRecord> found = store.list("alice", "proj1");
  assertEquals(1, found.size());
  assertEquals("t1", found.get(0).getId());
}

@Test
public void testGlobalTabsAreScopedToTheUser() {
  QueryTabStore store = QueryTabStore.get(provider, workManager);
  store.save(newRecord("g1", "alice", null, "SELECT 1"));
  store.save(newRecord("g2", "bob", null, "SELECT 2"));

  List<QueryTabStore.TabRecord> found = store.list("alice", null);
  assertEquals(1, found.size());
  assertEquals("g1", found.get(0).getId());
}

@Test
public void testDeleteRemovesOnlyTheNamedTab() {
  QueryTabStore store = QueryTabStore.get(provider, workManager);
  store.save(newRecord("t1", "alice", "proj1", "SELECT 1"));
  store.save(newRecord("t2", "alice", "proj1", "SELECT 2"));

  store.delete("t1");

  List<QueryTabStore.TabRecord> found = store.list("alice", "proj1");
  assertEquals(1, found.size());
  assertEquals("t2", found.get(0).getId());
}
```

The test extends `ClusterTest` and takes both dependencies from the running drillbit in `@BeforeClass`:

```java
private static PersistentStoreProvider provider;
private static WorkManager workManager;

@BeforeClass
public static void setup() throws Exception {
  startCluster(ClusterFixture.builder(dirTestWatcher));
  provider = cluster.drillbit().getContext().getStoreProvider();
  workManager = cluster.drillbit().getWorkManager();
}
```

Helper, in the same test class:

```java
private static QueryTabStore.TabRecord newRecord(
    String id, String owner, String projectId, String sql) {
  QueryTabStore.TabRecord r = new QueryTabStore.TabRecord();
  r.setId(id);
  r.setOwner(owner);
  r.setProjectId(projectId);
  r.setName("Query " + id);
  r.setSql(sql);
  r.setCreatedAt(System.currentTimeMillis());
  r.setUpdatedAt(System.currentTimeMillis());
  return r;
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -o -pl exec/java-exec test -Dtest=TestQueryTabStore`
Expected: FAIL — compilation error, `QueryTabStore` does not exist.

- [ ] **Step 3: Write the implementation**

Model the singleton on `ProjectSchemaCache.get(...)`: a `static volatile` instance and
store, double-checked locking, and

```java
cachedStore = provider.getOrCreateStore(
    PersistentStoreConfig.newJacksonBuilder(
        workManager.getContext().getLpPersistence().getMapper(), TabRecord.class)
        .name("drill.sqllab.tabs")
        .build());
```

`TabRecord` is a Jackson bean — no-arg constructor, getters and setters for:

```java
private String id;            // UUID
private String projectId;     // null for global (/query) tabs
private String owner;         // username, always set server-side
private String name;
private String sql;
private String defaultSchema;
private boolean hidden;
private long createdAt;
private long updatedAt;
private List<String> vizIds;
private boolean locked;
private String lockReason;
private String lockType;      // "manual" | "api"
private boolean pinned;
private String cacheId;       // results cache handle; results themselves are NOT stored
```

Because `delete(id)` receives only the tab id but the key includes owner and project,
`delete` and `find` scan `getAll()` for a record whose `getId()` matches. Tab counts
per user are small; a scan is fine and avoids a second index.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -o -pl exec/java-exec test -Dtest=TestQueryTabStore`
Expected: PASS (3 tests)

- [ ] **Step 5: Checkstyle and commit**

```bash
mvn -o checkstyle:check -pl exec/java-exec
git add exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/QueryTabStore.java \
        exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestQueryTabStore.java
git commit -m "Add per-user query tab store"
```

---

### Task 2: Tab REST endpoints

**Files:**
- Create: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/QueryTabResources.java`
- Test: `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestQueryTabResources.java`

**Interfaces:**
- Consumes: `QueryTabStore` from Task 1.
- Produces: the HTTP contract below, which Task 3 consumes.

| Method | Path | Behaviour |
|---|---|---|
| GET | `/api/v1/tabs?projectId=` | `{ "tabs": [TabRecord] }` for the calling user. Omit `projectId` for global tabs. |
| POST | `/api/v1/tabs` | Body is a `TabRecord`. Server sets `owner`, `createdAt`, `updatedAt`, and generates `id` when absent. Returns the stored record. |
| PUT | `/api/v1/tabs/{id}` | Body is a `TabRecord`. 403 when not the owner. Bumps `updatedAt`. |
| DELETE | `/api/v1/tabs/{id}` | 403 when not the owner, **409 when `locked`**. |

Class annotations mirror `ProjectResources`: `@Path("/api/v1/tabs")`,
`@Tag(...)`, `@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)`, with `@Inject`
fields for `PersistentStoreProvider`, `DrillUserPrincipal` and `WorkManager`.

The owner always comes from `principal.getName()` and is never read from the request
body. Trusting a client-supplied owner would let any authenticated user write into
another user's namespace.

- [ ] **Step 1: Write the failing test**

```java
@Test
public void testCreateListUpdateDelete() throws Exception {
  JsonNode created = post("/api/v1/tabs",
      "{\"name\":\"Query 1\",\"sql\":\"SELECT 1\",\"projectId\":\"p1\"}");
  String id = created.get("id").asText();
  assertFalse(id.isEmpty());

  assertEquals(1, get("/api/v1/tabs?projectId=p1").get("tabs").size());

  put("/api/v1/tabs/" + id,
      "{\"name\":\"Renamed\",\"sql\":\"SELECT 2\",\"projectId\":\"p1\"}");
  assertEquals("Renamed",
      get("/api/v1/tabs?projectId=p1").get("tabs").get(0).get("name").asText());

  delete("/api/v1/tabs/" + id);
  assertEquals(0, get("/api/v1/tabs?projectId=p1").get("tabs").size());
}

/**
 * Locking is enforced server-side. A client-side-only guard on a durable object is
 * not a guard.
 */
@Test
public void testLockedTabCannotBeDeleted() throws Exception {
  JsonNode created = post("/api/v1/tabs",
      "{\"name\":\"Locked\",\"sql\":\"SELECT 1\",\"projectId\":\"p1\",\"locked\":true}");
  String id = created.get("id").asText();

  Request request = new Request.Builder().url(url("/api/v1/tabs/" + id)).delete().build();
  try (Response response = httpClient.newCall(request).execute()) {
    assertEquals(409, response.code());
  }
}

/** A hidden tab is still listed. That is the whole point of hide-versus-delete. */
@Test
public void testHiddenTabsAreStillListed() throws Exception {
  post("/api/v1/tabs",
      "{\"name\":\"Hidden\",\"sql\":\"SELECT 1\",\"projectId\":\"p1\",\"hidden\":true}");

  JsonNode tabs = get("/api/v1/tabs?projectId=p1").get("tabs");
  assertEquals(1, tabs.size());
  assertTrue(tabs.get(0).get("hidden").asBoolean());
}

/** Global tabs and project tabs must not leak into each other's listings. */
@Test
public void testGlobalTabsAreSeparateFromProjectTabs() throws Exception {
  post("/api/v1/tabs", "{\"name\":\"Global\",\"sql\":\"SELECT 1\"}");
  post("/api/v1/tabs", "{\"name\":\"Scoped\",\"sql\":\"SELECT 2\",\"projectId\":\"p1\"}");

  assertEquals(1, get("/api/v1/tabs").get("tabs").size());
  assertEquals(1, get("/api/v1/tabs?projectId=p1").get("tabs").size());
}
```

Add `get`, `put` and `delete` helpers beside the `post` helper copied from
`TestProjectSchemaCacheEndpoints`.

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -o -pl exec/java-exec test -Dtest=TestQueryTabResources`
Expected: FAIL — 404 from every call; the resource does not exist.

- [ ] **Step 3: Write the implementation**

Copy the request-handling shape from `ProjectResources.addDataset` (lines 785–830):
`synchronized (id.intern())` around each read-modify-write,
`Response.status(...).entity(new MessageResponse(...)).build()` for error paths, and
`logger.error` plus `DrillRuntimeException` in the catch.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -o -pl exec/java-exec test -Dtest=TestQueryTabResources`
Expected: PASS (4 tests)

- [ ] **Step 5: Checkstyle and commit**

```bash
mvn -o checkstyle:check -pl exec/java-exec
git add exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/QueryTabResources.java \
        exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestQueryTabResources.java
git commit -m "Add REST endpoints for query tabs"
```

---

### Task 3: Frontend API client

**Files:**
- Create: `exec/java-exec/src/main/resources/webapp/src/api/tabs.ts`
- Test: `exec/java-exec/src/main/resources/webapp/src/api/tabs.test.ts`

**Interfaces:**
- Consumes: the HTTP contract from Task 2.
- Produces:

```ts
export interface ServerTab {
  id: string;
  projectId?: string;
  name: string;
  sql: string;
  defaultSchema?: string;
  hidden: boolean;
  createdAt: number;
  updatedAt: number;
  vizIds?: string[];
  locked?: boolean;
  lockReason?: string;
  lockType?: 'manual' | 'api';
  pinned?: boolean;
  cacheId?: string;
}

export function listTabs(projectId?: string): Promise<ServerTab[]>;
export function createTab(tab: Partial<ServerTab>): Promise<ServerTab>;
export function updateTab(id: string, tab: Partial<ServerTab>): Promise<ServerTab>;
export function deleteTab(id: string): Promise<void>;
```

- [ ] **Step 1: Write the failing test**

```ts
import { describe, it, expect, vi, beforeEach } from 'vitest';
import apiClient from './client';
import { listTabs, createTab, deleteTab } from './tabs';

vi.mock('./client', () => ({
  default: { get: vi.fn(), post: vi.fn(), put: vi.fn(), delete: vi.fn() },
}));

describe('tabs api', () => {
  beforeEach(() => vi.clearAllMocks());

  it('scopes the list request to a project', async () => {
    vi.mocked(apiClient.get).mockResolvedValue({ data: { tabs: [] } });
    await listTabs('p1');
    expect(apiClient.get).toHaveBeenCalledWith('/api/v1/tabs', { params: { projectId: 'p1' } });
  });

  it('omits projectId entirely for global tabs', async () => {
    vi.mocked(apiClient.get).mockResolvedValue({ data: { tabs: [] } });
    await listTabs(undefined);
    expect(apiClient.get).toHaveBeenCalledWith('/api/v1/tabs', { params: {} });
  });

  it('returns the created tab', async () => {
    vi.mocked(apiClient.post).mockResolvedValue({ data: { id: 'x', name: 'T' } });
    await expect(createTab({ name: 'T' })).resolves.toMatchObject({ id: 'x' });
  });

  it('surfaces a 409 from deleting a locked tab', async () => {
    vi.mocked(apiClient.delete).mockRejectedValue({ response: { status: 409 } });
    await expect(deleteTab('t1')).rejects.toMatchObject({ response: { status: 409 } });
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run src/api/tabs.test.ts`
Expected: FAIL — cannot resolve `./tabs`.

- [ ] **Step 3: Write the implementation**

Follow `src/api/projects.ts`: `import apiClient from './client'`, a
`const TABS_BASE = '/api/v1/tabs'`, and one thin exported function per endpoint.

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run src/api/tabs.test.ts`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add src/api/tabs.ts src/api/tabs.test.ts
git commit -m "Add tab API client"
```

---

## Phase 2 — Two-tier persistence

### Task 4: UUID tab ids and legacy id migration

**Files:**
- Modify: `exec/java-exec/src/main/resources/webapp/src/store/querySlice.ts` — `tabCounter` at :58, `addTab` at :123-133, `duplicateTab` at :134-155
- Modify: `exec/java-exec/src/main/resources/webapp/src/utils/workspacePersistence.ts`
- Test: `exec/java-exec/src/main/resources/webapp/src/store/querySlice.test.ts`

`tabCounter` keeps driving the *name* (`Query 3`) but no longer the id. Existing
persisted `tab-N` ids are rewritten to UUIDs on load, once, before anything else reads
them.

**Interfaces:**
- Produces: `migrateTabIds(state: PersistedTabState): PersistedTabState`, exported from `workspacePersistence.ts`.

- [ ] **Step 1: Write the failing test**

```ts
it('gives new tabs UUID ids', () => {
  const state = reducer(undefined, addTab(undefined));
  const created = state.tabs[state.tabs.length - 1];
  expect(created.id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-/);
});

it('still numbers tab names sequentially', () => {
  let state = reducer(undefined, addTab(undefined));
  state = reducer(state, addTab(undefined));
  expect(state.tabs[state.tabs.length - 1].name).toMatch(/^Query \d+$/);
});

it('rewrites legacy tab-N ids and keeps activeTabId pointing at the same tab', () => {
  const migrated = migrateTabIds({
    tabs: [
      { id: 'tab-1', name: 'Query 1', sql: 'SELECT 1' },
      { id: 'tab-2', name: 'Query 2', sql: 'SELECT 2' },
    ],
    activeTabId: 'tab-2',
    tabCounter: 2,
  } as PersistedTabState);

  expect(migrated.tabs[0].id).not.toBe('tab-1');
  expect(migrated.tabs[1].id).not.toBe('tab-2');
  expect(migrated.activeTabId).toBe(migrated.tabs[1].id);
});

it('leaves already-migrated state untouched', () => {
  const uuid = '11111111-2222-3333-4444-555555555555';
  const migrated = migrateTabIds({
    tabs: [{ id: uuid, name: 'Query 1', sql: 'SELECT 1' }],
    activeTabId: uuid,
    tabCounter: 1,
  } as PersistedTabState);

  expect(migrated.tabs[0].id).toBe(uuid);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run src/store/querySlice.test.ts`
Expected: FAIL — ids still match `tab-N`, and `migrateTabIds` is not exported.

- [ ] **Step 3: Write the implementation**

Use `crypto.randomUUID()`, available in every browser this SPA targets and in the
jsdom Vitest runs. Detect legacy ids with `/^tab-\d+$/` so the migration is
idempotent.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run src/store/querySlice.test.ts`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/store/querySlice.ts src/utils/workspacePersistence.ts src/store/querySlice.test.ts
git commit -m "Use UUIDs for tab ids and migrate legacy ids"
```

---

### Task 5: The promotion rule

**Files:**
- Create: `exec/java-exec/src/main/resources/webapp/src/utils/tabPromotion.ts`
- Test: `exec/java-exec/src/main/resources/webapp/src/utils/tabPromotion.test.ts`

A pure decision function kept apart from the effects, so the rule at the heart of the
spec can be tested without a store, a network, or a React tree.

**Interfaces:**
- Produces:

```ts
export interface PromotionInput {
  hasExecuted: boolean;       // a query has been run from this tab, success or failure
  isClosing: boolean;
  sql: string;
  conversationLength: number; // Prospector messages in this tab
  alreadyPromoted: boolean;
}

export function shouldPromote(input: PromotionInput): boolean;
```

- [ ] **Step 1: Write the failing test**

```ts
const base: PromotionInput = {
  hasExecuted: false,
  isClosing: false,
  sql: '',
  conversationLength: 0,
  alreadyPromoted: false,
};

it('promotes once a query has been executed', () => {
  expect(shouldPromote({ ...base, hasExecuted: true })).toBe(true);
});

// A failed query is still work the user will want back in order to fix it.
it('does not care whether the execution succeeded', () => {
  expect(shouldPromote({ ...base, hasExecuted: true, sql: 'SELCT 1' })).toBe(true);
});

it('does not promote an open, unexecuted tab', () => {
  expect(shouldPromote({ ...base, sql: 'SELECT 1' })).toBe(false);
});

it('promotes on close when SQL is present', () => {
  expect(shouldPromote({ ...base, isClosing: true, sql: 'SELECT 1' })).toBe(true);
});

// The editor is only half a tab's content; a conversation alone is worth keeping.
it('promotes on close when only a conversation is present', () => {
  expect(shouldPromote({ ...base, isClosing: true, conversationLength: 4 })).toBe(true);
});

it('treats whitespace-only SQL as empty', () => {
  expect(shouldPromote({ ...base, isClosing: true, sql: '   \n  ' })).toBe(false);
});

// Keeps a stray "New Query" click from leaving a permanent Query 7 in the tree.
it('promotes nothing for an empty tab closed with no content', () => {
  expect(shouldPromote({ ...base, isClosing: true })).toBe(false);
});

it('does not re-promote a tab already on the server', () => {
  expect(shouldPromote({ ...base, hasExecuted: true, alreadyPromoted: true })).toBe(false);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run src/utils/tabPromotion.test.ts`
Expected: FAIL — cannot resolve `./tabPromotion`.

- [ ] **Step 3: Write the implementation**

```ts
export function shouldPromote(input: PromotionInput): boolean {
  if (input.alreadyPromoted) {
    return false;
  }
  if (input.hasExecuted) {
    return true;
  }
  const hasContent = input.sql.trim().length > 0 || input.conversationLength > 0;
  return input.isClosing && hasContent;
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run src/utils/tabPromotion.test.ts`
Expected: PASS (8 tests)

- [ ] **Step 5: Commit**

```bash
git add src/utils/tabPromotion.ts src/utils/tabPromotion.test.ts
git commit -m "Add tab promotion rule"
```

---

### Task 6: Reconciliation

**Files:**
- Create: `exec/java-exec/src/main/resources/webapp/src/utils/tabReconcile.ts`
- Test: `exec/java-exec/src/main/resources/webapp/src/utils/tabReconcile.test.ts`

**Interfaces:**
- Consumes: `ServerTab` from Task 3, `PersistedTab` from `workspacePersistence.ts`.
- Produces: `reconcileTabs(local: PersistedTab[], server: ServerTab[]): PersistedTab[]`

- [ ] **Step 1: Write the failing test**

```ts
it('unions local-only and server-only tabs', () => {
  const out = reconcileTabs(
    [{ id: 'a', name: 'A', sql: 'SELECT 1', updatedAt: 100 }] as PersistedTab[],
    [{ id: 'b', name: 'B', sql: 'SELECT 2', updatedAt: 100 }] as ServerTab[],
  );
  expect(out.map((t) => t.id).sort()).toEqual(['a', 'b']);
});

it('prefers whichever copy was updated most recently', () => {
  const out = reconcileTabs(
    [{ id: 'a', name: 'local', sql: 'SELECT local', updatedAt: 200 }] as PersistedTab[],
    [{ id: 'a', name: 'server', sql: 'SELECT server', updatedAt: 100 }] as ServerTab[],
  );
  expect(out).toHaveLength(1);
  expect(out[0].sql).toBe('SELECT local');
});

it('takes the server copy when it is newer', () => {
  const out = reconcileTabs(
    [{ id: 'a', name: 'local', sql: 'SELECT local', updatedAt: 100 }] as PersistedTab[],
    [{ id: 'a', name: 'server', sql: 'SELECT server', updatedAt: 200 }] as ServerTab[],
  );
  expect(out[0].sql).toBe('SELECT server');
});

// The drillbit being unreachable must not wipe the user's working set.
it('returns the local set unchanged when the server list is empty', () => {
  const local = [{ id: 'a', name: 'A', sql: 'SELECT 1', updatedAt: 100 }] as PersistedTab[];
  expect(reconcileTabs(local, [])).toEqual(local);
});

it('carries the hidden flag through from the server copy', () => {
  const out = reconcileTabs(
    [],
    [{ id: 'a', name: 'A', sql: 'SELECT 1', updatedAt: 100, hidden: true }] as ServerTab[],
  );
  expect(out[0].hidden).toBe(true);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run src/utils/tabReconcile.test.ts`
Expected: FAIL — cannot resolve `./tabReconcile`.

- [ ] **Step 3: Write the implementation**

Build a `Map<string, PersistedTab>` from the local list, then walk the server list,
inserting where absent and overwriting only when `server.updatedAt > local.updatedAt`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run src/utils/tabReconcile.test.ts`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add src/utils/tabReconcile.ts src/utils/tabReconcile.test.ts
git commit -m "Add tab reconciliation"
```

---

### Task 7: Wire the server tier into the persistence hook

**Files:**
- Modify: `exec/java-exec/src/main/resources/webapp/src/hooks/useWorkspacePersistence.ts`
- Test: `exec/java-exec/src/main/resources/webapp/src/hooks/useWorkspacePersistence.test.tsx`

Where Tasks 3, 5 and 6 meet. The existing local save stays **unconditional** — it is
the write-through buffer that makes a failed server write non-destructive.

- [ ] **Step 1: Write the failing test**

```tsx
it('writes locally even when the server save fails', async () => {
  vi.mocked(updateTab).mockRejectedValue(new Error('network'));
  renderHook(() => useWorkspacePersistence('p1'), { wrapper });
  act(() => { store.dispatch(setSql({ tabId: 'a', sql: 'SELECT 1' })); });
  await waitFor(() => expect(saveTabState).toHaveBeenCalled());
});

it('promotes a tab to the server after a query executes', async () => {
  renderHook(() => useWorkspacePersistence('p1'), { wrapper });
  act(() => { store.dispatch(setResults({ tabId: 'a', results: sampleResults })); });
  await waitFor(() =>
    expect(createTab).toHaveBeenCalledWith(expect.objectContaining({ id: 'a' })));
});

it('promotes a tab whose query failed', async () => {
  renderHook(() => useWorkspacePersistence('p1'), { wrapper });
  act(() => { store.dispatch(setError({ tabId: 'a', error: { message: 'boom' } })); });
  await waitFor(() =>
    expect(createTab).toHaveBeenCalledWith(expect.objectContaining({ id: 'a' })));
});

it('does not promote a tab that has only been typed in', async () => {
  renderHook(() => useWorkspacePersistence('p1'), { wrapper });
  act(() => { store.dispatch(setSql({ tabId: 'a', sql: 'SELECT 1' })); });
  await new Promise((resolve) => setTimeout(resolve, 50));
  expect(createTab).not.toHaveBeenCalled();
});

it('reconciles the server list into the restored state on mount', async () => {
  vi.mocked(listTabs).mockResolvedValue([
    { id: 'server-only', name: 'From server', sql: 'SELECT 9', updatedAt: 999, hidden: true },
  ] as ServerTab[]);
  renderHook(() => useWorkspacePersistence('p1'), { wrapper });
  await waitFor(() =>
    expect(store.getState().query.tabs.some((t) => t.id === 'server-only')).toBe(true));
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run src/hooks/useWorkspacePersistence.test.tsx`
Expected: FAIL — no server calls are made.

- [ ] **Step 3: Write the implementation**

On mount, call `listTabs(projectId)` and pass the result through `reconcileTabs`
before dispatching `restoreQueryState`. In the existing debounced save, evaluate
`shouldPromote` for each changed tab and call `createTab` or `updateTab` accordingly.
Wrap every server call in a try/catch that logs and leaves the local write intact.
Keep the debounce at 1s or more: SQL changes on every keystroke, and each change is
now a potential network write.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run src/hooks/useWorkspacePersistence.test.tsx`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add src/hooks/useWorkspacePersistence.ts src/hooks/useWorkspacePersistence.test.tsx
git commit -m "Sync promoted tabs to the server"
```

---

## Phase 3 — Hide, delete, and the project tree

### Task 8: Closing hides instead of removing

**Files:**
- Modify: `exec/java-exec/src/main/resources/webapp/src/store/querySlice.ts:156-164` (`removeTab`)
- Modify: `exec/java-exec/src/main/resources/webapp/src/pages/SqlLabPage.tsx` — the tab strip filters on `hidden`
- Test: `exec/java-exec/src/main/resources/webapp/src/store/querySlice.test.ts`

`removeTab` becomes `hideTab`, setting `hidden: true`; a new `deleteTab` action
removes the record. Today's guard refusing to remove the last tab
(`state.tabs.length > 1`) moves to the *visible* count: hiding the last visible tab
opens a fresh empty one, so the editor is never blank.

- [ ] **Step 1: Write the failing test**

```ts
it('hides a tab instead of removing it', () => {
  let state = reducer(undefined, addTab(undefined));
  const id = state.tabs[0].id;
  state = reducer(state, hideTab(id));
  expect(state.tabs.find((t) => t.id === id)?.hidden).toBe(true);
});

it('opens a fresh tab when the last visible one is hidden', () => {
  let state = reducer(undefined, addTab(undefined));
  state.tabs.forEach((t) => { state = reducer(state, hideTab(t.id)); });
  expect(state.tabs.filter((t) => !t.hidden)).toHaveLength(1);
});

it('moves activeTabId off a hidden tab', () => {
  let state = reducer(undefined, addTab(undefined));
  state = reducer(state, addTab(undefined));
  const active = state.activeTabId;
  state = reducer(state, hideTab(active));
  expect(state.activeTabId).not.toBe(active);
  expect(state.tabs.find((t) => t.id === state.activeTabId)?.hidden).toBeFalsy();
});

it('deleteTab removes the record outright', () => {
  let state = reducer(undefined, addTab(undefined));
  const id = state.tabs[state.tabs.length - 1].id;
  state = reducer(state, deleteTab(id));
  expect(state.tabs.find((t) => t.id === id)).toBeUndefined();
});

it('refuses to delete a locked tab', () => {
  let state = reducer(undefined, addTab(undefined));
  const id = state.tabs[state.tabs.length - 1].id;
  state = reducer(state, lockTab({ tabId: id, reason: 'test', lockType: 'manual' }));
  state = reducer(state, deleteTab(id));
  expect(state.tabs.find((t) => t.id === id)).toBeDefined();
});

it('allows a locked tab to be hidden', () => {
  let state = reducer(undefined, addTab(undefined));
  const id = state.tabs[state.tabs.length - 1].id;
  state = reducer(state, lockTab({ tabId: id, reason: 'test', lockType: 'manual' }));
  state = reducer(state, hideTab(id));
  expect(state.tabs.find((t) => t.id === id)?.hidden).toBe(true);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run src/store/querySlice.test.ts`
Expected: FAIL — `hideTab` and `deleteTab` are not exported.

- [ ] **Step 3: Write the implementation**

Keep `removeTab` exported as an alias for `hideTab` for one release so no caller
breaks silently.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run src/store/querySlice.test.ts`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add src/store/querySlice.ts src/pages/SqlLabPage.tsx src/store/querySlice.test.ts
git commit -m "Hide tabs on close instead of destroying them"
```

---

### Task 9: List tabs in the project tree

**Files:**
- Create: `exec/java-exec/src/main/resources/webapp/src/components/shell/ProjectTabsSection.tsx`
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/shell/Sidebar.tsx:144-150` (`BASE_PROJECT_SECTIONS`)
- Test: `exec/java-exec/src/main/resources/webapp/src/components/shell/ProjectTabsSection.test.tsx`

The sidebar currently lists *sections*, not items. Tabs are the first item-level
content in it, so they get their own component rather than being forced into the
`ProjectSection` shape. Clicking a hidden tab unhides and activates it.

- [ ] **Step 1: Write the failing test**

```tsx
const tabs = [
  { id: 'a', name: 'Open one', hidden: false },
  { id: 'b', name: 'Hidden one', hidden: true },
];

it('lists both open and hidden tabs', () => {
  render(<ProjectTabsSection projectId="p1" tabs={tabs} onOpen={vi.fn()} onDelete={vi.fn()} />);
  expect(screen.getByText('Open one')).toBeInTheDocument();
  expect(screen.getByText('Hidden one')).toBeInTheDocument();
});

it('opens a hidden tab when clicked', () => {
  const onOpen = vi.fn();
  render(<ProjectTabsSection projectId="p1" tabs={tabs} onOpen={onOpen} onDelete={vi.fn()} />);
  fireEvent.click(screen.getByText('Hidden one'));
  expect(onOpen).toHaveBeenCalledWith('b');
});

it('offers Delete on right-click', () => {
  render(<ProjectTabsSection projectId="p1" tabs={tabs} onOpen={vi.fn()} onDelete={vi.fn()} />);
  fireEvent.contextMenu(screen.getByText('Open one'));
  expect(screen.getByText(/delete/i)).toBeInTheDocument();
});

it('does not offer Delete for a locked tab', () => {
  render(<ProjectTabsSection projectId="p1"
    tabs={[{ id: 'c', name: 'Locked one', hidden: false, locked: true }]}
    onOpen={vi.fn()} onDelete={vi.fn()} />);
  fireEvent.contextMenu(screen.getByText('Locked one'));
  expect(screen.queryByText(/delete/i)).not.toBeInTheDocument();
});

it('renders an empty state when the project has no tabs', () => {
  render(<ProjectTabsSection projectId="p1" tabs={[]} onOpen={vi.fn()} onDelete={vi.fn()} />);
  expect(screen.getByText(/no tabs/i)).toBeInTheDocument();
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run src/components/shell/ProjectTabsSection.test.tsx`
Expected: FAIL — the component does not exist.

- [ ] **Step 3: Write the implementation**

Use antd `Dropdown` with `trigger={['contextMenu']}`. Style hidden tabs muted so they
read as distinct from open ones.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run src/components/shell/ProjectTabsSection.test.tsx`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add src/components/shell/ProjectTabsSection.tsx src/components/shell/Sidebar.tsx \
        src/components/shell/ProjectTabsSection.test.tsx
git commit -m "List project tabs in the sidebar tree"
```

---

### Task 10: Record which tab a published API came from

**Files:**
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/SharedQueryApiResources.java` — add `tabId` to `SharedQueryApi` (:102) and to `CreateRequest` (:200)
- Modify: `exec/java-exec/src/main/resources/webapp/src/pages/SqlLabPage.tsx:984-990` — send the tab id when creating
- Test: `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestSharedQueryApiTabLink.java`

Today the link from a published API back to its tab lives only in
`sharedQueryApiIds`, a `useState` in `SqlLabPage.tsx:156` that never reaches storage.
After a reload nothing knows which tabs were published, so Task 11's warning would
have nothing to read.

Note that the API does **not** depend on the tab: `SharedQueryApi` carries its own
`sql`, and `GET /{id}/data` re-executes that copy. The `tabId` is provenance for the
warning, not a foreign key, and deleting the tab must leave the endpoint serving.

**Interfaces:**
- Produces: `SharedQueryApi.getTabId()`; `GET /api/v1/shared-queries?tabId=` filtering by it.

- [ ] **Step 1: Write the failing test**

```java
@Test
public void testCreatedApiRemembersItsTab() throws Exception {
  JsonNode created = post("/api/v1/shared-queries",
      "{\"name\":\"Sales feed\",\"sql\":\"SELECT 1\",\"tabId\":\"tab-abc\"}");
  assertEquals("tab-abc", created.get("tabId").asText());
}

@Test
public void testApisCanBeListedByTab() throws Exception {
  post("/api/v1/shared-queries",
      "{\"name\":\"A\",\"sql\":\"SELECT 1\",\"tabId\":\"tab-abc\"}");
  post("/api/v1/shared-queries",
      "{\"name\":\"B\",\"sql\":\"SELECT 2\",\"tabId\":\"tab-xyz\"}");

  JsonNode found = get("/api/v1/shared-queries?tabId=tab-abc").get("queries");
  assertEquals(1, found.size());
  assertEquals("A", found.get(0).get("name").asText());
}

/**
 * The endpoint holds its own copy of the SQL, so removing the tab must not disturb
 * it. This is what lets the delete warning say "stays live" truthfully.
 */
@Test
public void testApiKeepsServingAfterItsTabIsDeleted() throws Exception {
  String tabId = post("/api/v1/tabs",
      "{\"name\":\"T\",\"sql\":\"SELECT 1\",\"projectId\":\"p1\"}").get("id").asText();
  String apiId = post("/api/v1/shared-queries",
      "{\"name\":\"Feed\",\"sql\":\"SELECT 1\",\"tabId\":\"" + tabId + "\"}")
      .get("id").asText();

  delete("/api/v1/tabs/" + tabId);

  Request request = new Request.Builder()
      .url(url("/api/v1/shared-queries/" + apiId + "/data")).build();
  try (Response response = httpClient.newCall(request).execute()) {
    assertEquals(200, response.code());
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -o -pl exec/java-exec test -Dtest=TestSharedQueryApiTabLink`
Expected: FAIL — `tabId` is absent from the created record.

- [ ] **Step 3: Write the implementation**

Add the field with a Jackson getter/setter, carry it through `CreateRequest`, and add
an optional `@QueryParam("tabId")` filter to the list endpoint. Existing stored
records deserialize with a null `tabId`, which the warning treats as "not published" —
no migration needed.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -o -pl exec/java-exec test -Dtest=TestSharedQueryApiTabLink`
Expected: PASS (3 tests)

- [ ] **Step 5: Checkstyle and commit**

```bash
mvn -o checkstyle:check -pl exec/java-exec
git add exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/SharedQueryApiResources.java         exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestSharedQueryApiTabLink.java         exec/java-exec/src/main/resources/webapp/src/pages/SqlLabPage.tsx
git commit -m "Record the originating tab on a published query API"
```

---

### Task 11: Delete with dependency warnings

**Files:**
- Create: `exec/java-exec/src/main/resources/webapp/src/components/query-editor/DeleteTabModal.tsx`
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/query-editor/QueryToolbar.tsx` — add Delete to the three-dot menu
- Test: `exec/java-exec/src/main/resources/webapp/src/components/query-editor/DeleteTabModal.test.tsx`

**Interfaces:**
- Consumes: `listTabs`/`deleteTab` (Task 3), `GET /api/v1/shared-queries?tabId=` (Task 10).
- Produces: `DeleteTabModal` with props `{ open, tab, vizNames, publishedApis, onConfirm, onCancel }`, where `publishedApis: { id: string; name: string }[]`.

The modal warns and proceeds; it does not block. Blocking on dependants would create
tabs that can never be deleted. Locked tabs are the one hard stop, enforced by the 409
from Task 2.

Three dependants, saying three different things. The published-API line is **not** a
breakage warning — the endpoint holds its own SQL and keeps serving (proved by Task
10's third test). It is there because a user can be wrong in either direction:
believing they broke production, or believing they revoked an endpoint they meant to
take down.

- [ ] **Step 1: Write the failing test**

```tsx
it('names the visualizations that will break', () => {
  render(<DeleteTabModal open tab={{ id: 'a', name: 'Sales', vizIds: ['v1', 'v2'] }}
    vizNames={{ v1: 'Revenue chart', v2: 'Trend' }} onConfirm={vi.fn()} onCancel={vi.fn()} />);
  expect(screen.getByText(/Revenue chart/)).toBeInTheDocument();
  expect(screen.getByText(/Trend/)).toBeInTheDocument();
});

it('warns that the Prospector conversation goes too', () => {
  render(<DeleteTabModal open tab={{ id: 'a', name: 'Sales', conversationLength: 12 }}
    vizNames={{}} onConfirm={vi.fn()} onCancel={vi.fn()} />);
  expect(screen.getByText(/conversation/i)).toBeInTheDocument();
});

it('still allows deletion despite dependants', () => {
  const onConfirm = vi.fn();
  render(<DeleteTabModal open tab={{ id: 'a', name: 'Sales', vizIds: ['v1'] }}
    vizNames={{ v1: 'Revenue chart' }} onConfirm={onConfirm} onCancel={vi.fn()} />);
  fireEvent.click(screen.getByRole('button', { name: /delete/i }));
  expect(onConfirm).toHaveBeenCalled();
});

it('says nothing about dependants when there are none', () => {
  render(<DeleteTabModal open tab={{ id: 'a', name: 'Scratch' }}
    vizNames={{}} publishedApis={[]} onConfirm={vi.fn()} onCancel={vi.fn()} />);
  expect(screen.queryByText(/will break/i)).not.toBeInTheDocument();
});

// The endpoint keeps its own copy of the SQL, so it survives the tab.
it('says a published API stays live rather than breaking', () => {
  render(<DeleteTabModal open tab={{ id: 'a', name: 'Sales' }} vizNames={{}}
    publishedApis={[{ id: 'api1', name: 'Sales feed' }]}
    onConfirm={vi.fn()} onCancel={vi.fn()} />);
  expect(screen.getByText(/Sales feed/)).toBeInTheDocument();
  expect(screen.getByText(/stays live|remain live|keep serving/i)).toBeInTheDocument();
  expect(screen.queryByText(/Sales feed.*will break/i)).not.toBeInTheDocument();
});

it('still warns that visualizations break, even alongside a live API', () => {
  render(<DeleteTabModal open tab={{ id: 'a', name: 'Sales', vizIds: ['v1'] }}
    vizNames={{ v1: 'Revenue chart' }}
    publishedApis={[{ id: 'api1', name: 'Sales feed' }]}
    onConfirm={vi.fn()} onCancel={vi.fn()} />);
  expect(screen.getByText(/Revenue chart/)).toBeInTheDocument();
  expect(screen.getByText(/Sales feed/)).toBeInTheDocument();
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run src/components/query-editor/DeleteTabModal.test.tsx`
Expected: FAIL — the component does not exist.

- [ ] **Step 3: Write the implementation**

antd `Modal` with `okType="danger"`. Resolve `vizIds` to names through the
visualizations query cache already present in `SqlLabPage`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run src/components/query-editor/DeleteTabModal.test.tsx`
Expected: PASS (6 tests)

- [ ] **Step 5: Full suite, build, commit**

```bash
npx tsc --noEmit && npx eslint src && npx vitest run && npm run build
git add src/components/query-editor/DeleteTabModal.tsx \
        src/components/query-editor/DeleteTabModal.test.tsx \
        src/components/query-editor/QueryToolbar.tsx
git commit -m "Warn about dependants before deleting a tab"
```

---

## Phase 4 — Per-tab Prospector

> **Blocked on a decision.** Before starting, confirm what backs `PersistentStore` in
> the target deployment. If it is ZooKeeper, the default 1 MB per-znode limit applies
> and conversations need a size cap or a different store. See
> [`../TabPersistence.md`](../TabPersistence.md#constraints-discovered-in-the-codebase).

### Task 12: Key conversations by tab

**Files:**
- Modify: `exec/java-exec/src/main/resources/webapp/src/hooks/useProspector.ts:224` (`storageKey`)
- Modify: `exec/java-exec/src/main/resources/webapp/src/pages/SqlLabPage.tsx:323` — pass a per-tab key
- Test: `exec/java-exec/src/main/resources/webapp/src/hooks/useProspector.test.tsx`

`useProspector` already swaps history when `storageKey` changes
(`useProspector.ts:259-262`), so this is mostly a change to what is passed in.

- [ ] **Step 1: Write the failing test**

```tsx
it('keeps a separate conversation per tab', () => {
  const { result, rerender } = renderHook(
    ({ key }) => useProspector(undefined, undefined, 15, key),
    { initialProps: { key: 'chat:p1:tab-a' } },
  );
  act(() => { result.current.sendMessage('hello', {} as ChatContext); });
  rerender({ key: 'chat:p1:tab-b' });
  expect(result.current.messages).toHaveLength(0);
});

it('restores a tab conversation when returning to it', () => {
  const { result, rerender } = renderHook(
    ({ key }) => useProspector(undefined, undefined, 15, key),
    { initialProps: { key: 'chat:p1:tab-a' } },
  );
  act(() => { result.current.sendMessage('hello', {} as ChatContext); });
  const before = result.current.messages.length;
  rerender({ key: 'chat:p1:tab-b' });
  rerender({ key: 'chat:p1:tab-a' });
  expect(result.current.messages).toHaveLength(before);
});

// Two threads claiming to describe the same query would immediately diverge.
it('does not copy a conversation into a duplicated tab', () => {
  let state = reducer(undefined, addTab(undefined));
  const source = state.tabs[state.tabs.length - 1].id;
  state = reducer(state, duplicateTab(source));
  const copy = state.tabs[state.tabs.length - 1].id;
  expect(loadChat(`chat:p1:${copy}`)).toEqual([]);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run src/hooks/useProspector.test.tsx`
Expected: FAIL — the conversation is shared across tabs.

- [ ] **Step 3: Write the implementation**

Change the key from `chat:${projectId}` to `chat:${projectId}:${activeTabId}`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run src/hooks/useProspector.test.tsx`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/hooks/useProspector.ts src/pages/SqlLabPage.tsx src/hooks/useProspector.test.tsx
git commit -m "Scope Prospector conversations to the active tab"
```

---

### Task 13: Promote conversations with their tab

**Files:**
- Create: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/TabConversationStore.java`
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/QueryTabResources.java` — delete cascades to the conversation
- Test: `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestTabConversationStore.java`

**Interfaces:**
- Consumes: `QueryTabStore` (Task 1) and the tab HTTP contract (Task 2).
- Produces: `GET` and `PUT` on `/api/v1/tabs/{id}/conversation`, plus cascade-on-delete.

A separate store, `drill.sqllab.tab_conversations`, keyed by tab id, so a large
conversation never bloats the tab record the tree listing reads.

- [ ] **Step 1: Write the failing test**

```java
@Test
public void testConversationRoundTrips() throws Exception {
  String id = post("/api/v1/tabs", "{\"name\":\"T\",\"sql\":\"SELECT 1\",\"projectId\":\"p1\"}")
      .get("id").asText();

  put("/api/v1/tabs/" + id + "/conversation",
      "{\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}");

  assertEquals(1, get("/api/v1/tabs/" + id + "/conversation").get("messages").size());
}

@Test
public void testDeletingATabDeletesItsConversation() throws Exception {
  String id = post("/api/v1/tabs", "{\"name\":\"T\",\"sql\":\"SELECT 1\",\"projectId\":\"p1\"}")
      .get("id").asText();
  put("/api/v1/tabs/" + id + "/conversation",
      "{\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}");

  delete("/api/v1/tabs/" + id);

  Request request = new Request.Builder()
      .url(url("/api/v1/tabs/" + id + "/conversation")).build();
  try (Response response = httpClient.newCall(request).execute()) {
    assertEquals(404, response.code());
  }
}

/**
 * Guards the ZooKeeper znode limit. An oversized history must be rejected loudly,
 * not truncated silently.
 */
@Test
public void testOversizedConversationIsRejected() throws Exception {
  String id = post("/api/v1/tabs", "{\"name\":\"T\",\"sql\":\"SELECT 1\",\"projectId\":\"p1\"}")
      .get("id").asText();

  StringBuilder huge = new StringBuilder("{\"messages\":[{\"role\":\"user\",\"content\":\"");
  for (int i = 0; i < 600000; i++) {
    huge.append('x');
  }
  huge.append("\"}]}");

  RequestBody body = RequestBody.create(huge.toString(), JSON);
  Request request = new Request.Builder()
      .url(url("/api/v1/tabs/" + id + "/conversation")).put(body).build();
  try (Response response = httpClient.newCall(request).execute()) {
    assertEquals(413, response.code());
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -o -pl exec/java-exec test -Dtest=TestTabConversationStore`
Expected: FAIL — 404 from the conversation endpoints.

- [ ] **Step 3: Write the implementation**

Reject payloads over a `MAX_CONVERSATION_BYTES` constant with 413. Start at 512 KB,
comfortably under ZooKeeper's 1 MB znode default.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -o -pl exec/java-exec test -Dtest=TestTabConversationStore`
Expected: PASS (3 tests)

- [ ] **Step 5: Checkstyle, backend suite, commit**

```bash
mvn -o checkstyle:check -pl exec/java-exec
mvn -o -pl exec/java-exec test
git add exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/TabConversationStore.java \
        exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/QueryTabResources.java \
        exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestTabConversationStore.java
git commit -m "Persist Prospector conversations per tab"
```

---

## Final verification

- [ ] `mvn -o checkstyle:check -pl exec/java-exec`
- [ ] `mvn -o -pl exec/java-exec test`
- [ ] `npx tsc --noEmit && npx eslint src && npx vitest run && npm run build`
- [ ] Update the SQL Lab page doc under `docs/dev/ui/pages/` and the per-tab conversation behaviour in `docs/dev/PROSPECTOR.md`
- [ ] Manual pass: type in a new tab without running it, then refresh — the tab survives and is absent from the tree. Run the query — it appears in the tree. Close it — it stays in the tree. Reopen it from the tree. Delete a tab with a visualization attached — the warning names the visualization. Try to delete a locked tab — it is refused.

## Deferred

**Notebooks**, by decision. They already follow this pattern in `localStorage` and
will need the same treatment, including a third clause in the promotion rule and a
migration of the `drill-tab-notebooks` map, which is keyed by the old `tab-N` ids and
would orphan silently. See [`../TabPersistence.md`](../TabPersistence.md#out-of-scope).
