# Prospector Conversations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a conversation a durable, project-scoped artifact and the landing surface of a Drill project, with a canvas beside the chat that shows what Prospector just built.

**Architecture:** Two new PersistentStores mirror the existing tab split — conversation metadata (listable, small) separate from message bodies (capped at 512 KB) — fronted by a project-scoped REST resource. The server becomes the source of truth and `localStorage` is demoted to an offline buffer for the active conversation. A new `ConversationsPage` hosts the existing `ProspectorPanel` beside a canvas that mounts read-only viewers per artifact kind; editing hands off to each artifact's existing route.

**Tech Stack:** Java 8+/Jakarta JAX-RS + Jackson + Drill `PersistentStore` (backend, JUnit tests); React 18 + TypeScript + AntD + react-query + React Router (frontend, Vitest tests).

**Spec:** [`../specs/2026-11-07-prospector-conversations-design.md`](../specs/2026-11-07-prospector-conversations-design.md)

## Global Constraints

- Checkstyle must pass on every backend change: `mvn checkstyle:check -pl exec/java-exec`. Braces on every `if`; no unused imports.
- Every new source file (Java, TS, TSX, CSS) carries an Apache 2.0 license header. See `docs/dev/LicenseHeadersAndNotices.md`.
- Conversation bodies are capped at `512 * 1024` bytes, exactly as `TabConversationStore.MAX_CONVERSATION_BYTES` defines it. Over-cap writes return HTTP 413 with a message naming the cap.
- Conversations are per-user. The owner is taken from the authenticated principal, never from the request body. Non-owners get 403.
- Frontend lint must pass with zero warnings: `npm run lint` (`--max-warnings 0`).
- No new Maven dependencies, no new npm dependencies.
- Do not add Claude as a co-author on commits. Commit messages are imperative ("Add conversation store", not "Added").
- The name is **Prospector**. Never "AI Copilot".
- No em-dashes in user-facing copy.

## Review Focus

Input classes the spec implies that no single task's happy path exercises. Each has a test assigned to the task that owns the code.

1. **A conversation grows past 512 KB.** The server is now primary, so a rejected write means lost messages, not lost sync. The user must be told, in the thread, before the turn is lost. → Task 2, Task 6.
2. **The same conversation is open on two devices.** Server-primary with a naive PUT is last-write-wins and silently eats the other device's turn. → Task 2 (version/`updatedAt` precondition), Task 6.
3. **An artifact is deleted from its normal tab, then its thread card is clicked.** The card holds an id that no longer resolves. The canvas must say so, not crash the page. → Task 8.
4. **A user opens a conversation id belonging to someone else, or in a project they lost access to.** Must 403/404 at the server, and the page must render a message rather than an empty shell. → Task 2, Task 7.
5. **`create_workflow` output is never confirmed.** The workflow must remain `enabled: false` and unreachable by the scheduler, and no code path may flip it true without the user's explicit confirmation. → Task 11.

---

### Task 1: Conversation stores

**Files:**
- Create: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ConversationStore.java`
- Test: `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/ConversationStoreTest.java`

**Interfaces:**
- Consumes: `PersistentStore`, `PersistentStoreConfig`, `PersistentStoreProvider`, `WorkManager`, `ChatMessage` (all existing).
- Produces:
  - `ConversationStore.get(PersistentStoreProvider, WorkManager) -> ConversationStore`
  - `ConversationRecord` with `id, projectId, owner, name, createdAt, updatedAt, messageCount, version` (all `@JsonProperty`, standard getters/setters)
  - `List<ConversationRecord> listForProject(String projectId, String owner)`
  - `ConversationRecord findRecord(String id)`
  - `void saveRecord(ConversationRecord record)`
  - `Conversation findMessages(String conversationId)` — never null; empty `Conversation` when absent
  - `void saveMessages(String conversationId, Conversation conversation)`
  - `void delete(String conversationId)` — removes record and messages
  - `static final int MAX_CONVERSATION_BYTES = 512 * 1024`

Model this on `QueryTabStore` (metadata) plus `TabConversationStore` (bodies) in the same package — two `PersistentStore` instances, store names `drill.sqllab.conversations` and `drill.sqllab.conversation_messages`, same double-checked singleton pattern. Reuse the existing `TabConversationStore.Conversation` message-list wrapper rather than defining a second one.

- [ ] **Step 1: Write the failing tests**

```java
@Test
public void listForProjectReturnsOnlyThisOwnersRecordsInThatProject() { }

@Test
public void findMessagesReturnsEmptyConversationWhenAbsent() { }

@Test
public void deleteRemovesBothRecordAndMessages() { }

@Test
public void saveRecordIncrementsVersion() { }
```

Assert: `listForProject("p1", "alice")` excludes a record owned by `"bob"` and excludes one in `"p2"`; `findMessages("missing").getMessages()` is empty, not null; after `delete(id)`, both `findRecord(id)` is null and `findMessages(id)` is empty; `saveRecord` on a record at version 3 stores version 4.

Use an in-memory `PersistentStore` fake, as `TestDashboardResources` does for its store.

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -pl exec/java-exec -Dtest=ConversationStoreTest`
Expected: FAIL, `ConversationStore` not found.

- [ ] **Step 3: Implement `ConversationStore`**

- [ ] **Step 4: Run tests and checkstyle**

Run: `mvn test -pl exec/java-exec -Dtest=ConversationStoreTest && mvn checkstyle:check -pl exec/java-exec`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ConversationStore.java exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/ConversationStoreTest.java
git commit -m "Add project-scoped conversation store"
```

---

### Task 2: Conversation REST resource

**Files:**
- Create: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ConversationResources.java`
- Test: `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/ConversationResourcesTest.java`
- Modify: wherever `QueryTabResources` is registered with Jersey, register `ConversationResources` the same way.

**Interfaces:**
- Consumes: `ConversationStore` from Task 1; `DrillUserPrincipal` for the owner.
- Produces, all under `@Path("/api/v1/projects/{projectId}/conversations")`, `@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)`:
  - `GET /` → `{ conversations: ConversationRecord[] }`
  - `POST /` body `{ name? }` → 201 + `ConversationRecord` (server mints id, owner, timestamps)
  - `GET /{cid}` → `{ record, messages }`
  - `PUT /{cid}` body `{ name }` → renamed `ConversationRecord`
  - `PUT /{cid}/messages` body `{ messages, version }` → `{ version }`
  - `DELETE /{cid}` → 204

`PUT /{cid}/messages` carries the concurrency and size gates. Serialize with the store's Jackson mapper and compare `length` to `MAX_CONVERSATION_BYTES` before writing, mirroring `QueryTabResources.putConversation`. Reject a stale `version` with 409 so a second device cannot silently clobber the first.

- [ ] **Step 1: Write the failing tests**

```java
@Test
public void putMessagesOverCapReturns413() { }

@Test
public void putMessagesWithStaleVersionReturns409() { }

@Test
public void nonOwnerGetReturns403() { }

@Test
public void getMissingConversationReturns404() { }

@Test
public void postMintsIdAndTakesOwnerFromPrincipal() { }
```

Assert: a message list serializing over `512 * 1024` returns 413 and the stored messages are unchanged; `PUT` with `version: 1` against a stored version 2 returns 409 and leaves the stored messages unchanged; a `GET` as `"bob"` of a conversation owned by `"alice"` returns 403; `POST` with `{"owner": "mallory"}` in the body stores the principal's name, not `"mallory"`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -pl exec/java-exec -Dtest=ConversationResourcesTest`
Expected: FAIL.

- [ ] **Step 3: Implement `ConversationResources` and register it**

- [ ] **Step 4: Run tests and checkstyle**

Run: `mvn test -pl exec/java-exec -Dtest=ConversationResourcesTest && mvn checkstyle:check -pl exec/java-exec`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git commit -am "Add project-scoped conversation REST endpoints"
```

---

### Task 3: Scratch project

**Files:**
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProjectResources.java`
- Test: `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/ScratchProjectTest.java`

**Interfaces:**
- Produces: `GET /api/v1/projects/scratch` → the calling user's personal project, created on first call. Id format `scratch_<owner>`, name `"Scratch"`, `isSystem: true`, `isPublic: false`.

Idempotent: a second call returns the existing project, never a duplicate. Global SQL Lab and Logs use this project id for their conversations.

- [ ] **Step 1: Write the failing tests**

```java
@Test
public void scratchIsCreatedOnFirstCallAndReusedOnSecond() { }

@Test
public void scratchIsPerUser() { }

@Test
public void scratchIsNotListedAmongOrdinaryProjects() { }
```

Assert: two calls as `"alice"` return the same id; a call as `"bob"` returns a different id; `GET /api/v1/projects` does not include the scratch project in its ordinary listing.

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -pl exec/java-exec -Dtest=ScratchProjectTest`
Expected: FAIL.

- [ ] **Step 3: Implement the scratch endpoint**

- [ ] **Step 4: Run tests and checkstyle**

Run: `mvn test -pl exec/java-exec -Dtest=ScratchProjectTest && mvn checkstyle:check -pl exec/java-exec`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git commit -am "Add per-user Scratch project for conversations outside a project"
```

---

### Task 4: Frontend conversation API client

**Files:**
- Create: `exec/java-exec/src/main/resources/webapp/src/api/conversations.ts`
- Create: `exec/java-exec/src/main/resources/webapp/src/api/conversations.test.ts`
- Modify: `exec/java-exec/src/main/resources/webapp/src/types/ai.ts` — add `ConversationRecord`.

**Interfaces:**
- Consumes: `apiClient` (same import style as `api/tabs.ts`), `ChatMessage` from `types/ai.ts`.
- Produces:
  - `ConversationRecord { id, projectId, owner, name, createdAt, updatedAt, messageCount, version }`
  - `listConversations(projectId: string): Promise<ConversationRecord[]>`
  - `createConversation(projectId: string, name?: string): Promise<ConversationRecord>`
  - `getConversation(projectId, cid): Promise<{ record: ConversationRecord; messages: ChatMessage[] }>`
  - `renameConversation(projectId, cid, name): Promise<ConversationRecord>`
  - `saveConversationMessages(projectId, cid, messages, version): Promise<{ status: 'ok' | 'too-large' | 'conflict'; version?: number }>`
  - `deleteConversation(projectId, cid): Promise<void>`
  - `getScratchProjectId(): Promise<string>`

`saveConversationMessages` resolves rather than throws on 413 and 409 — the caller needs to tell the user which happened. Everything else rejects normally. Follow `api/tabs.ts` for shape and for its 413 handling.

- [ ] **Step 1: Write the failing tests**

```ts
it('maps a 413 to too-large rather than throwing', async () => {});
it('maps a 409 to conflict rather than throwing', async () => {});
it('sends the version with a message save', async () => {});
it('posts to the project-scoped conversations path', async () => {});
```

Assert: `saveConversationMessages` resolves `{ status: 'too-large' }` on 413 and `{ status: 'conflict' }` on 409; the PUT body includes `version`; `createConversation('p1')` posts to `/api/v1/projects/p1/conversations`. Mock `apiClient` as `api/tabs.test.ts` does.

- [ ] **Step 2: Run tests to verify they fail**

Run: `npx vitest run src/api/conversations.test.ts` from `exec/java-exec/src/main/resources/webapp`
Expected: FAIL.

- [ ] **Step 3: Implement `api/conversations.ts` and the `ConversationRecord` type**

- [ ] **Step 4: Run tests and lint**

Run: `npx vitest run src/api/conversations.test.ts && npm run lint`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git commit -am "Add conversation API client"
```

---

### Task 5: Make the server the source of truth in useProspector

**Files:**
- Modify: `exec/java-exec/src/main/resources/webapp/src/hooks/useProspector.ts` (persistence block, roughly lines 240-430)
- Modify: `exec/java-exec/src/main/resources/webapp/src/hooks/useProspector.test.tsx`

**Interfaces:**
- Consumes: `api/conversations.ts` from Task 4.
- Produces, added to `useProspector`'s options and `UseProspectorReturn`:
  - option `conversationId?: string | null`
  - `UseProspectorReturn.conversationId: string | null`
  - `UseProspectorReturn.saveStatus: 'idle' | 'saving' | 'too-large' | 'conflict' | 'error'`

When `conversationId` is set, the server is primary: load from `getConversation` on mount and on id change, and persist with `saveConversationMessages` (debounced, as the current `localStorage` write is). `localStorage` stays only as an offline buffer for the active conversation, replayed when a save failed with a network error. A `too-large` or `conflict` result must surface through `saveStatus` rather than being swallowed.

Leave the existing `storageKey`/`tabId` path intact for now; Task 14 removes it.

**Decision this task's implementer must make and record in the code:** what a conversation does once it reaches the cap. The spec leaves the mechanism open and names three candidates — roll older turns into a summary message, spill to a second record with a continuation pointer, or hard-stop and prompt for a new conversation. Pick one, implement it, and leave a `ponytail:` comment naming the ceiling and the upgrade path. The requirement the choice must satisfy: a user never silently loses a turn they already sent. Tests below pin the surfaced status either way.

**Review Focus coverage:** items 1 and 2.

- [ ] **Step 1: Write the failing tests**

```tsx
it('loads messages from the server when a conversationId is given', async () => {});
it('surfaces saveStatus "too-large" when the server rejects an oversized save', async () => {});
it('surfaces saveStatus "conflict" when another device wrote first', async () => {});
it('keeps the unsaved turn in localStorage when the save fails with a network error', async () => {});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npx vitest run src/hooks/useProspector.test.tsx`
Expected: FAIL.

- [ ] **Step 3: Implement server-primary persistence**

- [ ] **Step 4: Run tests and lint**

Run: `npx vitest run src/hooks/useProspector.test.tsx && npm run lint`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git commit -am "Make the server the source of truth for Prospector conversations"
```

---

### Task 6: Conversations page shell and routing

**Files:**
- Create: `exec/java-exec/src/main/resources/webapp/src/pages/ConversationsPage.tsx`
- Create: `exec/java-exec/src/main/resources/webapp/src/components/conversation/ConversationList.tsx`
- Create: `exec/java-exec/src/main/resources/webapp/src/pages/ConversationsPage.test.tsx`
- Modify: `src/App.tsx:93-108` — add `chat` routes
- Modify: `src/components/project/ProjectNavBar.tsx:43-48` — add `Conversations` first and `Workflows` before `Data Sources`
- Modify: `src/App.tsx:94` — index route redirects to `chat`, not `query`

**Interfaces:**
- Consumes: `api/conversations.ts`, `useProspector` with `conversationId`, `ProspectorPanel`, `useProjectContext`.
- Produces: `<ConversationsPage />` rendered at `/projects/:id/chat` and `/projects/:id/chat/:conversationId`.

Three panes: `ConversationList` (left), `ProspectorPanel` (middle), canvas slot (right, a placeholder in this task — Task 7 fills it). `/chat` with no id redirects to the most recent conversation, or renders an empty state with a "New conversation" action when the project has none. A conversation with no name takes its name from the first user message, truncated.

`saveStatus` of `too-large` or `conflict` renders a visible banner in the thread. Final nav order: Conversations, Query, Queries, Visualizations, Dashboards, Workflows, Data Sources, Wiki.

**Review Focus coverage:** items 1, 2 (user-visible half).

- [ ] **Step 1: Write the failing tests**

```tsx
it('redirects /chat to the most recent conversation', async () => {});
it('renders an empty state with a New conversation action when the project has none', async () => {});
it('names a new conversation from the first user message', async () => {});
it('shows a banner when saveStatus is too-large', async () => {});
it('shows a banner when saveStatus is conflict', async () => {});
it('lands on Conversations when opening a project', async () => {});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npx vitest run src/pages/ConversationsPage.test.tsx`
Expected: FAIL.

- [ ] **Step 3: Implement the page, the list, the routes and the nav entries**

- [ ] **Step 4: Run tests and lint**

Run: `npx vitest run src/pages/ConversationsPage.test.tsx && npm run lint`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git commit -am "Add the Conversations page and make it the project landing tab"
```

---

### Task 7: Canvas host and the results kind

**Files:**
- Create: `src/components/conversation/CanvasHost.tsx`
- Create: `src/components/conversation/ResultsCanvas.tsx`
- Create: `src/components/conversation/CanvasHost.test.tsx`
- Modify: `src/pages/ConversationsPage.tsx` — replace the placeholder with `CanvasHost`

**Interfaces:**
- Produces:
  - `type CanvasTarget = { kind: 'results'; sql: string; rows: unknown[]; columns: string[] } | { kind: 'chart' | 'dashboard' | 'query' | 'workflow' | 'wiki' | 'datasource'; id: string }`
  - `<CanvasHost target={CanvasTarget | null} pinned={boolean} onTogglePin={() => void} onFullScreen={() => void} />`
  - `<ResultsCanvas sql={string} rows={unknown[]} columns={string[]} />` — secondary tabs `Results | SQL`

`CanvasHost` dispatches on `target.kind` and renders a per-kind viewer; it owns the secondary tab strip and the pin and full-screen controls. With `target` null it renders an empty state. Task 8 supplies the viewers for the non-results kinds; this task stubs them as a named placeholder each.

`ResultsCanvas` is the one kind with no existing component. Render rows in an AntD `Table`; derive columns from the result metadata. Results are transient and are replaced by the next target.

**Review Focus coverage:** none directly; item 3 is Task 8.

- [ ] **Step 1: Write the failing tests**

```tsx
it('renders the results viewer for a results target', () => {});
it('renders an empty state when target is null', () => {});
it('does not change the rendered target while pinned', () => {});
it('shows SQL on the SQL secondary tab', () => {});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npx vitest run src/components/conversation/CanvasHost.test.tsx`
Expected: FAIL.

- [ ] **Step 3: Implement `CanvasHost` and `ResultsCanvas`**

- [ ] **Step 4: Run tests and lint**

Run: `npx vitest run src/components/conversation/CanvasHost.test.tsx && npm run lint`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git commit -am "Add the conversation canvas host and results viewer"
```

---

### Task 8: Canvas viewers for the existing artifact kinds

**Files:**
- Create: `src/components/conversation/ArtifactCanvas.tsx`
- Create: `src/components/conversation/ArtifactCanvas.test.tsx`
- Modify: `src/components/conversation/CanvasHost.tsx` — replace the stubs

**Interfaces:**
- Consumes: the existing read paths — `getVisualization`, `getDashboard`, `getSavedQuery`, `getWorkflow`, the project's `wikiPages`, and the data-source read used by `ProjectDataSourceEditPage`.
- Produces: `<ArtifactCanvas kind={...} id={string} onFullScreen={() => void} />`

Read-only per the spec. Each kind renders its artifact plus a `History` secondary tab listing the conversation turns that touched it (from the thread, not from any revision store) and, for the SQL-bearing kinds, a `SQL` tab. `onFullScreen` navigates to that artifact's own existing route (`/projects/:id/visualizations/:vizId`, `/dashboards/:dashboardId`, `/workflows/:wid`, `/wiki/:pageId`, `/datasources/:name`) — the editors are not embedded.

An id that no longer resolves renders "This artifact was deleted" with a link back, and does not throw.

**Review Focus coverage:** item 3.

- [ ] **Step 1: Write the failing tests**

```tsx
it('renders a deleted-artifact message when the id does not resolve', async () => {});
it('does not throw when the artifact fetch 404s', async () => {});
it('navigates to the artifact route on full screen rather than embedding an editor', async () => {});
it('lists the conversation turns that touched the artifact on the History tab', async () => {});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npx vitest run src/components/conversation/ArtifactCanvas.test.tsx`
Expected: FAIL.

- [ ] **Step 3: Implement `ArtifactCanvas` and wire it into `CanvasHost`**

- [ ] **Step 4: Run tests and lint**

Run: `npx vitest run src/components/conversation/ArtifactCanvas.test.tsx && npm run lint`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git commit -am "Render project artifacts on the conversation canvas"
```

---

### Task 9: Artifact cards, auto-follow and pin

**Files:**
- Create: `src/components/conversation/ArtifactCard.tsx`
- Modify: `src/components/prospector/ChatMessageBubble.tsx` — render artifact cards
- Modify: `src/pages/ConversationsPage.tsx` — own `canvasTarget` and `pinned`
- Create: `src/components/conversation/autoFollow.test.tsx`

**Interfaces:**
- Produces:
  - `<ArtifactCard kind={...} id={string} name={string} live={boolean} onOpen={() => void} />`
  - `src/components/conversation/autoFollow.ts`: `nextCanvasTarget(turnArtifacts: CanvasTarget[], pinned: boolean, current: CanvasTarget | null): CanvasTarget | null`

`nextCanvasTarget` is the whole follow rule and is where it gets tested: returns `current` when `pinned`; otherwise the **last** artifact of the turn, settling once at the end of the turn rather than stepping through each.

Cards are for durable artifacts only. A bare `execute_sql` puts results on the canvas and drops **no** card. The card whose artifact is on the canvas renders `live`.

- [ ] **Step 1: Write the failing tests**

```tsx
it('settles on the last artifact of a turn, not each in sequence', () => {});
it('keeps the current target when pinned', () => {});
it('returns the current target when a turn produced no artifacts', () => {});
it('drops no card for a bare execute_sql', () => {});
it('marks the card whose artifact is on the canvas as live', () => {});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npx vitest run src/components/conversation/autoFollow.test.tsx`
Expected: FAIL.

- [ ] **Step 3: Implement `nextCanvasTarget`, `ArtifactCard`, and the wiring**

- [ ] **Step 4: Run tests and lint**

Run: `npx vitest run src/components/conversation && npm run lint`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git commit -am "Add artifact cards with canvas auto-follow and pinning"
```

---

### Task 10: Replace save_report with write_wiki_page

**Files:**
- Modify: `src/hooks/useProspector.ts:154-166` (the `save_report` tool definition) and its executor
- Modify: `src/hooks/useProspector.test.tsx`
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProspectorResources.java` — prompt references to `save_report`

**Interfaces:**
- Produces: tool `write_wiki_page` with parameters `title: string`, `content: string`, `folder?: string` (default: the existing `REPORTS_FOLDER`), `includeTranscript?: boolean` (default `false`), `pageId?: string` (when present, updates that page instead of creating one).

`save_report` is removed. Its behavior becomes arguments: `folder` defaults to the reports folder and `includeTranscript: true` reproduces `buildReportMarkdown`. Existing report pages are untouched — this changes the tool, not stored data.

- [ ] **Step 1: Write the failing tests**

```tsx
it('creates a wiki page in the reports folder by default', async () => {});
it('appends the transcript only when includeTranscript is true', async () => {});
it('updates the named page when pageId is given instead of creating a duplicate', async () => {});
it('no longer exposes a save_report tool', () => {});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npx vitest run src/hooks/useProspector.test.tsx`
Expected: FAIL.

- [ ] **Step 3: Implement `write_wiki_page` and remove `save_report`**

- [ ] **Step 4: Run tests, lint and checkstyle**

Run: `npx vitest run src/hooks/useProspector.test.tsx && npm run lint` then `mvn checkstyle:check -pl exec/java-exec`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git commit -am "Replace save_report with a general write_wiki_page tool"
```

---

### Task 11: create_workflow with a confirmation gate

**Files:**
- Modify: `src/hooks/useProspector.ts` — add the tool definition and executor
- Create: `src/components/conversation/WorkflowActivationCard.tsx`
- Create: `src/components/conversation/WorkflowActivationCard.test.tsx`
- Modify: `src/hooks/useProspector.test.tsx`

**Interfaces:**
- Consumes: `createWorkflow`, `setWorkflowEnabled` from `api/workflows.ts`.
- Produces: tool `create_workflow` taking the workflow definition (schedule, condition, actions) and creating it through `createWorkflow` with **`enabled: false` forced**, regardless of what the model passes.
- Produces: `<WorkflowActivationCard workflowId={string} name={string} />` — an explicit Activate control that calls `setWorkflowEnabled(id, true)` only after the user confirms in an AntD `Modal.confirm`.

This is the spec's hard gate. The tool executor must overwrite `enabled` rather than trust it, and `setWorkflowEnabled(id, true)` must be reachable from this flow only via the confirm handler.

**Review Focus coverage:** item 5.

- [ ] **Step 1: Write the failing tests**

```tsx
it('forces enabled false even when the model asks for enabled true', async () => {});
it('does not call setWorkflowEnabled when the tool runs', async () => {});
it('calls setWorkflowEnabled(id, true) only after the user confirms', async () => {});
it('does not enable the workflow when the user cancels the confirm', async () => {});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npx vitest run src/components/conversation/WorkflowActivationCard.test.tsx src/hooks/useProspector.test.tsx`
Expected: FAIL.

- [ ] **Step 3: Implement the tool and the activation card**

- [ ] **Step 4: Run tests and lint**

Run: `npx vitest run src/components/conversation src/hooks && npm run lint`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git commit -am "Add create_workflow with an explicit activation confirmation"
```

---

### Task 12: System prompt for conversation mode

**Files:**
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProspectorResources.java` (`ChatContext` fields around line 159, prompt assembly around lines 690-1030)
- Create: `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/ConversationPromptTest.java`

**Interfaces:**
- Produces: `ChatContext.conversationMode: boolean`; when true the prompt instructs Prospector to build artifacts with its tools rather than hand back SQL for the user to paste, and names the canvas as where its work appears.
- Removes: the `ctx.notebookMode` branch and its `notebookDfName`, `notebookDfShape`, `notebookColumns`, `notebookCellCode`, `notebookCellError` fields.

The remaining surface branches (dashboard panels, logs) must not regress — the tests pin them.

- [ ] **Step 1: Write the failing tests**

```java
@Test
public void conversationModeTellsTheModelToBuildArtifacts() { }

@Test
public void conversationModeDoesNotEmitTheNotebookBlock() { }

@Test
public void logsContextBlockIsUnchanged() { }

@Test
public void dashboardContextBlockIsUnchanged() { }
```

Assert against the assembled prompt string, in the style of the existing `ProjectContextBlockTest`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -pl exec/java-exec -Dtest=ConversationPromptTest`
Expected: FAIL.

- [ ] **Step 3: Add conversation mode and drop the notebook branch**

- [ ] **Step 4: Run tests and checkstyle**

Run: `mvn test -pl exec/java-exec -Dtest=ConversationPromptTest,ProjectContextBlockTest && mvn checkstyle:check -pl exec/java-exec`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git commit -am "Add conversation mode to the Prospector system prompt"
```

---

### Task 13: Point global SQL Lab and Logs at the Scratch project

**Files:**
- Modify: `src/pages/SqlLabPage.tsx`, `src/pages/LogsPage.tsx` — replace the dock with a link into a Scratch conversation
- Create: `src/hooks/useScratchProject.ts`
- Create: `src/hooks/useScratchProject.test.ts`

**Interfaces:**
- Consumes: `getScratchProjectId()` from Task 4.
- Produces: `useScratchProject(): { projectId: string | undefined; isLoading: boolean }` — react-query wrapper, cached for the session.

Each page gets a single "Ask Prospector" action that navigates to `/projects/<scratchId>/chat`. No chat panel remains on either page.

**Review Focus coverage:** item 4 (the page half) — a failed scratch lookup renders the action disabled with a message, not a broken link.

- [ ] **Step 1: Write the failing tests**

```ts
it('returns the scratch project id', async () => {});
it('caches the lookup across callers', async () => {});
it('reports loading rather than a bad id while the lookup is in flight', async () => {});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npx vitest run src/hooks/useScratchProject.test.ts`
Expected: FAIL.

- [ ] **Step 3: Implement the hook and rewire both pages**

- [ ] **Step 4: Run tests and lint**

Run: `npx vitest run src/hooks/useScratchProject.test.ts && npm run lint`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git commit -am "Send global SQL Lab and Logs into a Scratch project conversation"
```

---

### Task 14: Delete the Prospector dock and the tab-scoped path

**Files:**
- Delete: `src/components/shell/GlobalProspectorTab.tsx`, `src/components/shell/GlobalProspectorTab.test.tsx`
- Modify: `src/components/shell/` — remove the global inspector's Prospector tab
- Modify: `src/pages/ProjectQueryPage.tsx` — remove the dock
- Modify: `src/hooks/useProspector.ts` — remove `storageKey`, `tabId`, `prospectorChatKey`, `conversationLengthFor`
- Modify: `exec/java-exec/.../QueryTabResources.java` — remove `GET`/`PUT /{id}/conversation`
- Delete: `exec/java-exec/.../TabConversationStore.java` once nothing references it
- Modify: `src/api/tabs.ts`, `src/api/tabs.test.ts` — drop the conversation calls

Existing tab conversations are dropped, not migrated. They are per-tab chat scratch, they are superseded by project conversations, and keeping a dead read path to satisfy them would preserve exactly the model this work removes. Note the decision in `docs/dev/TabPersistence.md`.

- [ ] **Step 1: Confirm nothing still references the removed symbols**

Run: `cd exec/java-exec/src/main/resources/webapp/src && grep -rn "prospectorChatKey\|conversationLengthFor\|GlobalProspectorTab\|storageKey" . | grep -v node_modules`
Expected: no hits outside the files being deleted in this task.

- [ ] **Step 2: Delete the dock, the tab conversation path and the dead options**

- [ ] **Step 3: Run the full suites**

Run: `npx vitest run && npm run lint` then `mvn test -pl exec/java-exec -Dtest='*Conversation*,*Tab*' && mvn checkstyle:check -pl exec/java-exec`
Expected: PASS, with no test still asserting dock behavior.

- [ ] **Step 4: Commit**

```bash
git commit -am "Remove the Prospector dock and tab-scoped conversations"
```

---

### Task 15: Documentation

**Files:**
- Create: `docs/dev/ui/pages/conversations.md`
- Modify: `docs/dev/ui/pages/project-layout.md` — new nav order, new landing route
- Modify: `docs/dev/ui/components/prospector.md` — the dock is gone; the panel is re-hosted
- Modify: `docs/dev/PROSPECTOR.md` — conversation stores, REST, `write_wiki_page`, `create_workflow` and its gate, conversation mode
- Modify: `docs/dev/TabPersistence.md` — tab conversations removed
- Modify: `docs/dev/WORKFLOWS.md` — workflows created by Prospector arrive disabled
- Modify: `CLAUDE.md` — add the Conversations row to the doc table

- [ ] **Step 1: Write the docs**

Follow the conventions in `docs/dev/ui/README.md`: purpose, data sources table, layout, key state, chrome, quirks.

- [ ] **Step 2: Verify every link resolves**

Run: `cd /Users/charlesgivre/github/drill && grep -o '](\.\./[^)]*\|](docs/[^)]*' docs/dev/ui/pages/conversations.md docs/dev/PROSPECTOR.md | head -40`
Expected: every referenced path exists.

- [ ] **Step 3: Commit**

```bash
git commit -am "Document the Conversations page and the project-scoped conversation model"
```

---

### Task 16: Full build

- [ ] **Step 1: Build the UI**

Run: `cd exec/java-exec/src/main/resources/webapp && npm install && npm run build`
Expected: PASS.

- [ ] **Step 2: Run the backend module build**

Run: `cd /Users/charlesgivre/github/drill && mvn clean install -pl exec/java-exec -DskipTests && mvn checkstyle:check -pl exec/java-exec`
Expected: PASS.

- [ ] **Step 3: Confirm the jdbc-all size enforcer still passes**

Run: `mvn enforcer:enforce -pl exec/jdbc-all`
Expected: PASS. No new resources were added outside `webapp/`, which is already excluded, so no new filter should be needed.

- [ ] **Step 4: Run the full test suites**

Run: `cd exec/java-exec/src/main/resources/webapp && npx vitest run && npm run lint`
Expected: PASS.

- [ ] **Step 5: Commit any build fixes**

```bash
git commit -am "Fix build issues from the conversations work"
```
