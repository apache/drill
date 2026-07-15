# Prospector Project Context Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Prospector use the descriptive metadata users already author — project descriptions, tags, wiki markdown, and saved-query descriptions — instead of guessing at table names.

**Architecture:** The browser sends `projectId` on `ChatContext` (already landed). The server loads the project and its saved queries from their PersistentStores and injects a size-capped summary into the system prompt. Full wiki markdown is fetched on demand by a client-executed `get_project_docs` tool rather than inlined, because the prompt is re-sent on every tool round.

**Tech Stack:** Java 17, JAX-RS (Jersey), Jackson, JUnit 5 (jupiter), PersistentStore, React + TypeScript, Vitest.

**Spec:** `docs/superpowers/specs/2026-07-15-prospector-logging-and-context-design.md` (Part 2)

## Global Constraints

- Every new/modified source file (Java, TS, TSX) must carry the Apache 2.0 license header. See `docs/dev/LicenseHeadersAndNotices.md`.
- After modifying anything in `exec/java-exec`, run `mvn -o checkstyle:check -pl exec/java-exec`. It must pass. Common failures: `if` without braces, unused imports, missing license header.
- Maven runs offline: `mvn -o ...`.
- Java tests are JUnit 5 (`org.junit.jupiter.api.Test`), matching `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/ProspectorEventTest.java`.
- Frontend commands run from `exec/java-exec/src/main/resources/webapp`: `npx tsc --noEmit` (exit 0) and `npx vitest run`.
- Do not add Claude as a git co-author. Commit messages imperative.
- **Loading project context must never fail a chat.** Every store read is best-effort: on any failure, log at debug and inject nothing.
- **The injected block is capped at 2000 characters**, truncated with the exact marker `...[truncated]`. `useProspector` runs up to 15 tool rounds (`DEFAULT_MAX_TOOL_ROUNDS`, `useProspector.ts:36`) and the full system prompt is re-sent on every round, so anything inlined is multiplied by up to 15× per question.

## Starting State (verify before planning around it)

Already landed — **do not redo**:

- `ChatContext.projectId?: string` in `types/ai.ts:55`, and `SqlLabPage.tsx` sends it in its `aiContext` memo (with `projectId` in the dep array).
- `useProspector`'s `executeToolCall(toolCall, context?)` receives the context and already reads `context.projectId` (the `create_visualization` tool uses it).
- `UseProspectorReturn.executeToolCall` is exposed, so tool behaviour can be asserted directly. `useProspector.test.tsx` exists with mocks for the api modules.
- **Java `ChatContext.projectId`** — added in commit `8dc54dbed`.
- **Java `ChatContext` notebook fields** — `notebookMode`, `notebookDfName`, `notebookDfShape`, `notebookColumns`, `notebookCellCode`, `notebookCellError`, all added in `8dc54dbed`. They deserialize but are **not yet emitted into the prompt** (Task 4).
- `@JsonIgnoreProperties(ignoreUnknown = true)` on `ChatContext`, so an unknown field from a newer client is dropped rather than 500ing the chat.
- `ProspectorResources` injects `storeProvider` and uses a lazy cached-store pattern (`getStore()`, ~line 800).
- `renderFullPrompt` records assistant tool calls, so the manual verification below can observe `get_project_docs`.

Correction to an earlier claim in the spec's discussion: unknown JSON properties were **not** tolerated before `8dc54dbed` — Jackson's default is to reject them. The notebook fields appeared harmless only because they are attached solely when the notebook tab is active (`SqlLabPage.tsx:417`), which is why chatting from that tab failed with HTTP 500. Do not assume tolerance anywhere else without checking for the annotation.

## Corrections to the spec

The spec claims `GlobalProspectorTab` "gains project awareness for free". **This is false.** `AppShell.tsx:158` renders `RightInspector` (which hosts `GlobalProspectorTab`) above the router outlet, while `ProjectContextProvider` mounts only inside the `/projects/:id` route element (`App.tsx:92`, `ProjectLayout.tsx:60`). `GlobalProspectorTab` is therefore outside the provider on every page, and `useProjectContext()` throws when there is no provider (`ProjectContext.tsx:100-106`). Task 5 derives the id from the route instead.

---

### Task 1: Read the project and its saved queries

**Files:**
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProspectorResources.java`
- Test: `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/ProjectContextBlockTest.java` (create)

**Interfaces:**
- Consumes: Java `ChatContext.projectId` (already exists — do not re-add).
- Produces:
  - `static String buildProjectBlock(ProjectResources.Project project, List<SavedQueryResources.SavedQuery> savedQueries)` — package-private, **pure**, returns the prompt text (never null; empty string when there is nothing to say). Pure so its output can be asserted without a store.
  - `private List<SavedQueryResources.SavedQuery> loadSavedQueries(List<String> ids)` and `private ProjectResources.Project loadProject(String id)` — best-effort, return null/empty on failure.

Constants to add near the other constants (~line 71):

```java
  private static final String PROJECTS_STORE_NAME = "drill.sqllab.projects";
  private static final String SAVED_QUERIES_STORE_NAME = "drill.sqllab.saved_queries";
  private static final int PROJECT_BLOCK_MAX_CHARS = 2000;
  private static final int SAVED_QUERY_SQL_MAX_CHARS = 500;
```

- [ ] **Step 1: Write the failing test**

Create `ProjectContextBlockTest.java` with the standard Apache header, package `org.apache.drill.exec.server.rest`, then:

```java
package org.apache.drill.exec.server.rest;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The project block is what stops the model guessing at table names, so it must
 * carry the user's own words — description, tags, saved-query descriptions — and
 * must stay small enough to survive being re-sent on every tool round.
 */
public class ProjectContextBlockTest {

  private static ProjectResources.Project project(String name, String description,
      List<String> tags, List<ProjectResources.WikiPage> wikiPages) {
    ProjectResources.Project p = new ProjectResources.Project();
    p.setName(name);
    p.setDescription(description);
    p.setTags(tags);
    p.setWikiPages(wikiPages);
    return p;
  }

  private static SavedQueryResources.SavedQuery savedQuery(String name, String description,
      String sql) {
    SavedQueryResources.SavedQuery q = new SavedQueryResources.SavedQuery();
    q.setName(name);
    q.setDescription(description);
    q.setSql(sql);
    return q;
  }

  @Test
  public void testIncludesNameDescriptionAndTags() {
    String block = ProspectorResources.buildProjectBlock(
        project("Retail", "Store sales analytics", List.of("sales", "retail"), null),
        List.of());
    assertTrue(block.contains("Retail"));
    assertTrue(block.contains("Store sales analytics"));
    assertTrue(block.contains("sales"));
    assertTrue(block.contains("retail"));
  }

  @Test
  public void testIncludesSavedQueryNamesAndDescriptions() {
    String block = ProspectorResources.buildProjectBlock(
        project("Retail", null, null, null),
        List.of(savedQuery("Top regions", "Revenue by region, last 30d",
            "SELECT region, SUM(amount) FROM sales GROUP BY region")));
    assertTrue(block.contains("Top regions"));
    assertTrue(block.contains("Revenue by region, last 30d"));
    assertTrue(block.contains("SELECT region"));
  }

  /**
   * Wiki titles are listed but never the bodies: a page can be tens of KB and the
   * prompt is re-sent on every tool round. get_project_docs fetches bodies on demand.
   */
  @Test
  public void testListsWikiTitlesButNotContent() {
    ProjectResources.WikiPage page = new ProjectResources.WikiPage(
        "w1", "Runbook", "SECRET_BODY_TEXT", 0L, 0L);
    String block = ProspectorResources.buildProjectBlock(
        project("Retail", null, null, List.of(page)), List.of());
    assertTrue(block.contains("Runbook"));
    assertFalse(block.contains("SECRET_BODY_TEXT"));
    assertTrue(block.contains("get_project_docs"));
  }

  @Test
  public void testCapsBlockSize() {
    List<SavedQueryResources.SavedQuery> many = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      many.add(savedQuery("Query " + i, "A fairly wordy description number " + i,
          "SELECT * FROM some_table_" + i + " WHERE a = 1 AND b = 2 AND c = 3"));
    }
    String block = ProspectorResources.buildProjectBlock(
        project("Retail", "Store sales analytics", null, null), many);
    assertTrue(block.length() <= 2000 + "...[truncated]".length());
    assertTrue(block.contains("...[truncated]"));
  }

  @Test
  public void testNullProjectYieldsEmptyBlock() {
    assertEquals("", ProspectorResources.buildProjectBlock(null, List.of()));
  }

  @Test
  public void testTruncatesLongSavedQuerySql() {
    String longSql = "SELECT " + "x, ".repeat(400) + "1";
    String block = ProspectorResources.buildProjectBlock(
        project("Retail", null, null, null),
        List.of(savedQuery("Wide", null, longSql)));
    assertFalse(block.contains(longSql));
  }
}
```

**Before writing the implementation, verify the POJO setters exist.** `ProjectResources.Project`, `ProjectResources.WikiPage`, and `SavedQueryResources.SavedQuery` are Jackson POJOs; read them and adapt the helpers above to their real constructors/setters. If they are immutable (constructor-only), build them via their constructors instead — do not add setters to production classes just for tests.

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -o test -pl exec/java-exec -Dtest=ProjectContextBlockTest -DfailIfNoTests=false`
Expected: FAIL — compilation error, `buildProjectBlock` does not exist.

- [ ] **Step 3: Write minimal implementation**

`ChatContext.projectId` already exists — do not re-add it. Add the pure builder:

```java
  /**
   * Renders the project's own descriptive metadata for the system prompt. Pure and
   * package-private so its output can be asserted without a store.
   *
   * <p>Wiki bodies are deliberately excluded — a page can be tens of KB and the whole
   * system prompt is re-sent on every tool round, so bodies are fetched on demand by
   * the get_project_docs tool instead.
   */
  static String buildProjectBlock(ProjectResources.Project project,
      List<SavedQueryResources.SavedQuery> savedQueries) {
    if (project == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    sb.append("\nPROJECT CONTEXT — the user's own description of this work:\n");
    sb.append("Project: ").append(project.getName()).append("\n");
    if (project.getDescription() != null && !project.getDescription().trim().isEmpty()) {
      sb.append("Description: ").append(project.getDescription()).append("\n");
    }
    if (project.getTags() != null && !project.getTags().isEmpty()) {
      sb.append("Tags: ").append(String.join(", ", project.getTags())).append("\n");
    }
    if (savedQueries != null && !savedQueries.isEmpty()) {
      sb.append("\nSaved queries in this project:\n");
      for (SavedQueryResources.SavedQuery q : savedQueries) {
        sb.append("- ").append(q.getName());
        if (q.getDescription() != null && !q.getDescription().trim().isEmpty()) {
          sb.append(": ").append(q.getDescription());
        }
        if (q.getSql() != null) {
          sb.append("\n  SQL: ").append(truncate(q.getSql(), SAVED_QUERY_SQL_MAX_CHARS));
        }
        sb.append("\n");
      }
    }
    if (project.getWikiPages() != null && !project.getWikiPages().isEmpty()) {
      sb.append("\nProject documentation pages (call get_project_docs with a title to "
          + "read one in full):\n");
      for (ProjectResources.WikiPage page : project.getWikiPages()) {
        sb.append("- ").append(page.getTitle()).append("\n");
      }
    }
    return truncate(sb.toString(), PROJECT_BLOCK_MAX_CHARS);
  }

  private static String truncate(String text, int maxChars) {
    if (text == null || text.length() <= maxChars) {
      return text;
    }
    return text.substring(0, maxChars) + "...[truncated]";
  }
```

Add the best-effort loaders, following the existing `getStore()` lazy-cache pattern (~line 800) with their own cached static fields:

```java
  private ProjectResources.Project loadProject(String projectId) {
    if (projectId == null || projectId.trim().isEmpty()) {
      return null;
    }
    try {
      return getProjectStore().get(projectId);
    } catch (Exception e) {
      // Context is an enhancement; never fail the chat because it is unavailable.
      logger.debug("Could not load project {} for AI context", projectId, e);
      return null;
    }
  }

  private List<SavedQueryResources.SavedQuery> loadSavedQueries(List<String> ids) {
    List<SavedQueryResources.SavedQuery> out = new ArrayList<>();
    if (ids == null || ids.isEmpty()) {
      return out;
    }
    try {
      PersistentStore<SavedQueryResources.SavedQuery> store = getSavedQueryStore();
      for (String id : ids) {
        SavedQueryResources.SavedQuery q = store.get(id);
        if (q != null) {
          out.add(q);
        }
      }
    } catch (Exception e) {
      logger.debug("Could not load saved queries for AI context", e);
    }
    return out;
  }
```

Model `getProjectStore()` / `getSavedQueryStore()` on the existing `getStore()` (double-checked lazy init over `storeProvider.getOrCreateStore(PersistentStoreConfig.newJacksonBuilder(...))`), using `ProjectResources.Project.class` and `SavedQueryResources.SavedQuery.class`. Read `getStore()` first and match it.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -o test -pl exec/java-exec -Dtest=ProjectContextBlockTest -DfailIfNoTests=false`
Expected: PASS — 6 tests.

Run: `mvn -o checkstyle:check -pl exec/java-exec`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProspectorResources.java \
        exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/ProjectContextBlockTest.java
git commit -m "Render the active project's metadata for the AI system prompt"
```

---

### Task 2: Inject the block into the system prompt

**Files:**
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProspectorResources.java` (`buildMessages`, ~line 600)
- Test: `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/ProjectContextBlockTest.java` (extend)

**Interfaces:**
- Consumes: `buildProjectBlock`, `loadProject`, `loadSavedQueries` (Task 1).
- Produces: `buildMessages` emits the project block when `ctx.projectId` is set.

`buildMessages` is currently `private List<ChatMessage> buildMessages(LlmConfig, ChatRequest)`. It stays an instance method (the loaders need `storeProvider`).

- [ ] **Step 1: Write the failing test**

Make `buildMessages` package-private (drop `private`) so it can be called directly.
`ProspectorResources` has a no-arg constructor and `storeProvider` is field-injected,
so a plain `new ProspectorResources()` has a null `storeProvider` — which is exactly
the "store unavailable" path this must survive.

Append to `ProjectContextBlockTest.java`:

```java
  private static ProspectorResources.ChatRequest request(String projectId) {
    ProspectorResources.ChatRequest req = new ProspectorResources.ChatRequest();
    ProspectorResources.ChatContext ctx = new ProspectorResources.ChatContext();
    ctx.feature = "sql_lab_chat";
    ctx.projectId = projectId;
    req.context = ctx;
    req.messages = new ArrayList<>();
    return req;
  }

  private static String systemPromptOf(List<ChatMessage> messages) {
    return messages.get(0).getContent();
  }

  @Test
  public void testNoProjectBlockWhenNoProjectId() {
    List<ChatMessage> messages =
        new ProspectorResources().buildMessages(new LlmConfig(), request(null));
    assertFalse(systemPromptOf(messages).contains("PROJECT CONTEXT"));
  }

  /**
   * Context is an enhancement. An unreachable store (here: no injected provider at
   * all) must degrade to no project block, never to a failed chat.
   */
  @Test
  public void testUnreachableStoreDoesNotFailTheChat() {
    List<ChatMessage> messages =
        new ProspectorResources().buildMessages(new LlmConfig(), request("proj-42"));
    assertFalse(systemPromptOf(messages).contains("PROJECT CONTEXT"));
    assertTrue(systemPromptOf(messages).contains("Apache Drill"));
  }
```

Import `org.apache.drill.exec.server.rest.ai.ChatMessage` and `LlmConfig`. Verify
`ChatRequest`'s fields/constructor before writing this — adapt to what is really there.

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -o test -pl exec/java-exec -Dtest=ProjectContextBlockTest -DfailIfNoTests=false`
Expected: FAIL — compilation error, `buildMessages` is private and inaccessible from the test.

- [ ] **Step 3: Write minimal implementation**

Drop `private` from `buildMessages` so the test above can reach it. Then, immediately after the existing `projectDatasets` / `availableSchemas` block (`if (ctx.projectDatasets != null ...) { ... } else if (ctx.availableSchemas ...) { ... }`), add:

```java
      if (ctx.projectId != null && !ctx.projectId.trim().isEmpty()) {
        ProjectResources.Project project = loadProject(ctx.projectId);
        if (project != null) {
          systemPrompt.append(buildProjectBlock(project,
              loadSavedQueries(project.getSavedQueryIds())));
        }
      }
```

- [ ] **Step 4: Verify**

Run: `mvn -o test -pl exec/java-exec -Dtest=ProjectContextBlockTest -DfailIfNoTests=false`
Expected: PASS.

Run: `mvn -o checkstyle:check -pl exec/java-exec`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProspectorResources.java \
        exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/ProjectContextBlockTest.java
git commit -m "Inject the active project's context into the Prospector prompt"
```

---

### Task 3: `get_project_docs` tool

Wiki pages may be large, so full markdown is fetched on demand. The block from Task 1 lists the page titles, which is what makes the model reach for this tool — a tool the model never learns exists is dead code.

**Files:**
- Modify: `exec/java-exec/src/main/resources/webapp/src/hooks/useProspector.ts` (`TOOL_DEFINITIONS` ~line 38; `executeToolCall` ~line 200)
- Test: `exec/java-exec/src/main/resources/webapp/src/hooks/useProspector.test.tsx` (extend)

**Interfaces:**
- Consumes: `ChatContext.projectId` (already present), `executeToolCall`'s exposed seam (already present on `UseProspectorReturn`).
- Produces: tool `get_project_docs(pageTitle?: string)`. With no `pageTitle`, returns `{ pages: [{title}] }`. With one, returns `{ title, content }`, content capped at 8000 chars.

Read `api/projects.ts` first and use the existing wiki accessor. If the only way to read wiki pages is `getProject(projectId)` (they are embedded in the project record), use that rather than inventing an endpoint.

- [ ] **Step 1: Write the failing test**

Append to `useProspector.test.tsx` (it already mocks `../api/projects`; extend that mock's factory to include the accessor you use):

```ts
describe('get_project_docs tool', () => {
  const docsCall = (pageTitle?: string): ToolCall => ({
    id: 'call-2',
    name: 'get_project_docs',
    arguments: JSON.stringify(pageTitle ? { pageTitle } : {}),
  });

  it('lists page titles when given no title', async () => {
    const { result } = renderHook(() => useProspector());
    const out = JSON.parse(await result.current.executeToolCall(docsCall(), ctx('proj-42')));
    expect(out.pages.map((p: { title: string }) => p.title)).toEqual(['Runbook', 'Glossary']);
  });

  it('returns the full content of a named page', async () => {
    const { result } = renderHook(() => useProspector());
    const out = JSON.parse(
      await result.current.executeToolCall(docsCall('Runbook'), ctx('proj-42')));
    expect(out.title).toBe('Runbook');
    expect(out.content).toContain('step one');
  });

  it('reports a missing page rather than failing silently', async () => {
    const { result } = renderHook(() => useProspector());
    const out = JSON.parse(
      await result.current.executeToolCall(docsCall('Nope'), ctx('proj-42')));
    expect(out.error).toContain('Nope');
  });

  it('requires an active project', async () => {
    const { result } = renderHook(() => useProspector());
    const out = JSON.parse(await result.current.executeToolCall(docsCall(), ctx()));
    expect(out.error).toMatch(/project/i);
  });
});
```

Mock the project read to return wiki pages `[{title: 'Runbook', content: 'step one ...'}, {title: 'Glossary', content: '...'}]`.

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run src/hooks/useProspector.test.tsx`
Expected: FAIL — 4 tests; `get_project_docs` hits the `default:` branch and returns `{"error":"Unknown tool: get_project_docs"}`.

- [ ] **Step 3: Write minimal implementation**

Add to `TOOL_DEFINITIONS`:

```ts
  {
    name: 'get_project_docs',
    description: 'Read the current project\'s documentation (wiki) pages. Call with no '
      + 'arguments to list page titles, or with pageTitle to read one page in full. '
      + 'Use this when the project has documentation that may explain the data, its '
      + 'conventions, or the business meaning of a table or column.',
    parameters: {
      type: 'object',
      properties: {
        pageTitle: {
          type: 'string',
          description: 'Title of the page to read. Omit to list available pages.',
        },
      },
    },
  },
```

Add the case to `executeToolCall`, capping content at a module constant `const PROJECT_DOC_MAX_CHARS = 8000;`:

```ts
        case 'get_project_docs': {
          if (!context?.projectId) {
            return JSON.stringify({ error: 'No active project — get_project_docs is '
              + 'only available inside a project.' });
          }
          const project = await getProject(context.projectId);
          const pages = project.wikiPages || [];
          if (!args.pageTitle) {
            return JSON.stringify({ pages: pages.map((p) => ({ title: p.title })) });
          }
          const page = pages.find((p) => p.title === args.pageTitle);
          if (!page) {
            return JSON.stringify({
              error: `No documentation page titled "${args.pageTitle}". Available: `
                + `${pages.map((p) => p.title).join(', ') || 'none'}`,
            });
          }
          const content = page.content || '';
          return JSON.stringify({
            title: page.title,
            content: content.length > PROJECT_DOC_MAX_CHARS
              ? `${content.slice(0, PROJECT_DOC_MAX_CHARS)}...[truncated]`
              : content,
          });
        }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run src/hooks/useProspector.test.tsx`
Expected: PASS.

Run: `npx tsc --noEmit`
Expected: exit 0.

- [ ] **Step 5: Commit**

```bash
git add exec/java-exec/src/main/resources/webapp/src
git commit -m "Add a get_project_docs tool for on-demand project documentation"
```

---

### Task 4: Stop dropping notebook context, and honour sendDataToAi

Two approved fixes from the spec, both the same class of bug as the project gap: context the client sends that the server throws away, and a privacy flag the tool path ignores.

**Files:**
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProspectorResources.java` (`ChatContext`; `buildMessages`; `appendDashboardData` ~line 830)
- Modify: `exec/java-exec/src/main/resources/webapp/src/types/ai.ts` (add `sendDataToAi`)
- Modify: `exec/java-exec/src/main/resources/webapp/src/hooks/useProspector.ts` (`execute_sql` case, ~line 210)
- Modify: `exec/java-exec/src/main/resources/webapp/src/pages/SqlLabPage.tsx` (send `sendDataToAi` in `aiContext`)
- Test: `ProjectContextBlockTest.java` and `useProspector.test.tsx` (extend)

**Interfaces:**
- Consumes: the six Java `ChatContext` notebook fields (already added in `8dc54dbed` — **do not re-add**; they deserialize but are never emitted).
- Produces: Java `ChatContext.sendDataToAi` (`Boolean`, so absent is distinguishable from false), `static boolean ProspectorResources.isSendDataToAi(ChatContext)`, and TS `ChatContext.sendDataToAi?: boolean`.

Background: the notebook tab populates six `notebook*` context fields. The server now accepts them but `buildMessages` never emits them, so the model still never sees notebook context — this task closes that half. Separately `useProspector.ts:~210` returns `rows: result.rows?.slice(0, 5)` from `execute_sql` unconditionally, ignoring the `sendDataToAi` flag that the optimize path honours (`SqlLabPage.tsx:~508`), and `appendDashboardData` serialises sample rows regardless.

- [ ] **Step 1: Write the failing tests**

Java — append to `ProjectContextBlockTest.java`. (Deserialization of the notebook
fields is already covered by `ProspectorEventTest.testChatContextCarriesNotebookFields`;
do not duplicate it. What is untested is that they reach the prompt.)

```java
  @Test
  public void testSendDataToAiDefaultsToTrueWhenAbsent() throws Exception {
    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();
    ProspectorResources.ChatContext ctx = mapper.readValue(
        "{\"feature\":\"sql_lab_chat\"}", ProspectorResources.ChatContext.class);
    assertTrue(ProspectorResources.isSendDataToAi(ctx));
  }

  @Test
  public void testSendDataToAiRespectsExplicitFalse() throws Exception {
    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();
    ProspectorResources.ChatContext ctx = mapper.readValue(
        "{\"feature\":\"sql_lab_chat\",\"sendDataToAi\":false}",
        ProspectorResources.ChatContext.class);
    assertFalse(ProspectorResources.isSendDataToAi(ctx));
  }
```

TypeScript — append to `useProspector.test.tsx`:

```ts
describe('execute_sql honours sendDataToAi', () => {
  const sqlCall = (): ToolCall => ({
    id: 'call-3',
    name: 'execute_sql',
    arguments: JSON.stringify({ sql: 'SELECT * FROM sales' }),
  });

  it('omits sample rows when sendDataToAi is false, keeping columns and count', async () => {
    const { result } = renderHook(() => useProspector());
    const out = JSON.parse(await result.current.executeToolCall(
      sqlCall(), { feature: 'sql_lab_chat', sendDataToAi: false } as ChatContext));
    expect(out.rows).toBeUndefined();
    expect(out.columns).toBeDefined();
    expect(out.rowCount).toBeDefined();
  });

  it('includes sample rows when sendDataToAi is true', async () => {
    const { result } = renderHook(() => useProspector());
    const out = JSON.parse(await result.current.executeToolCall(
      sqlCall(), { feature: 'sql_lab_chat', sendDataToAi: true } as ChatContext));
    expect(out.rows).toBeDefined();
  });
});
```

Mock `executeQuery` to resolve with columns, metadata and at least one row.

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -o test -pl exec/java-exec -Dtest=ProjectContextBlockTest -DfailIfNoTests=false`
Expected: FAIL — compilation error; the `notebook*` fields and `isSendDataToAi` do not exist.

Run: `npx vitest run src/hooks/useProspector.test.tsx`
Expected: FAIL — `rows` is present regardless of the flag.

- [ ] **Step 3: Write minimal implementation**

The six notebook fields already exist on the Java `ChatContext`. Add only `sendDataToAi` (a `Boolean`, so "absent" is distinguishable from "false", `@JsonProperty` public field matching the existing style), and:

```java
  /** Sample data may be sent unless the user explicitly opted out. */
  static boolean isSendDataToAi(ChatContext ctx) {
    return ctx == null || ctx.sendDataToAi == null || ctx.sendDataToAi;
  }
```

In `buildMessages`, emit a notebook section when `ctx.notebookMode` is set, following the style of the existing log/dashboard sections — dataframe name, shape, columns, current cell code, and last cell error.

Gate `appendDashboardData`'s sample rows on `isSendDataToAi(ctx)`; keep columns, types and rowCount either way. It will need the context passed in — adjust its signature.

In `useProspector.ts`'s `execute_sql` case, omit `rows` when `context?.sendDataToAi === false`, keeping columns/types/rowCount. In `SqlLabPage.tsx`, add `sendDataToAi` to the `base` context from the same state the optimize path reads (`SqlLabPage.tsx:~508`), and add it to the memo's dep array.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -o test -pl exec/java-exec -Dtest=ProjectContextBlockTest -DfailIfNoTests=false`
Expected: PASS.

Run: `npx vitest run && npx tsc --noEmit`
Expected: PASS, exit 0.

Run: `mvn -o checkstyle:check -pl exec/java-exec`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add exec/java-exec/src
git commit -m "Carry notebook context to the model and honour sendDataToAi in tools"
```

---

### Task 5: Give global chat the active project

`GlobalProspectorTab` sends `{ feature: 'global_chat' }` with no project, so chatting from the shell on a project page has no project context.

It **cannot** use `useProjectContext()`: `AppShell.tsx:158` renders `RightInspector` above the router outlet, while `ProjectContextProvider` mounts only inside the `/projects/:id` route element (`App.tsx:92`, `ProjectLayout.tsx:60`), and the hook throws when there is no provider (`ProjectContext.tsx:100-106`). Derive the id from the route instead — `RightInspector` is inside the router, so react-router's matching hooks are available.

**Files:**
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/shell/GlobalProspectorTab.tsx`
- Test: `exec/java-exec/src/main/resources/webapp/src/components/shell/GlobalProspectorTab.test.tsx` (create)

**Interfaces:**
- Consumes: `ChatContext.projectId`.
- Produces: nothing downstream.

- [ ] **Step 1: Write the failing test**

Create `GlobalProspectorTab.test.tsx` with the Apache header. Render the component inside a `MemoryRouter` at `/projects/proj-42/sql` and assert the context passed to `ProspectorPanel` carries `projectId: 'proj-42'`; render at `/` and assert `projectId` is undefined. Mock `../prospector`'s `ProspectorPanel` to capture its `context` prop, and mock `../../hooks/useProspector`.

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run src/components/shell/GlobalProspectorTab.test.tsx`
Expected: FAIL — `projectId` is undefined on the project route.

- [ ] **Step 3: Write minimal implementation**

```tsx
import { useMatch } from 'react-router-dom';
// ...
export default function GlobalProspectorTab() {
  const prospector = useProspector();
  // RightInspector renders above the router outlet, so ProjectContextProvider is not
  // an ancestor here — read the active project from the route instead.
  const match = useMatch('/projects/:id/*');
  const projectId = match?.params.id;
  const context: ChatContext = useMemo(
    () => ({ feature: 'global_chat', projectId }), [projectId]);
  return <ProspectorPanel prospector={prospector} context={context} />;
}
```

Verify the route pattern against `App.tsx:92` — if project routes are also reachable without a trailing segment, match both (`useMatch('/projects/:id')` as well) or use a pattern that covers both.

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run src/components/shell/GlobalProspectorTab.test.tsx && npx tsc --noEmit`
Expected: PASS, exit 0.

- [ ] **Step 5: Commit**

```bash
git add exec/java-exec/src/main/resources/webapp/src/components/shell
git commit -m "Give global Prospector chat the active project from the route"
```

---

### Task 6: Document the context contract

**Files:**
- Modify: `docs/dev/PROSPECTOR.md`
- Modify: `docs/dev/ui/components/prospector.md`

- [ ] **Step 1: Document it**

In `docs/dev/PROSPECTOR.md`, add a "Project context" section: `context.projectId` makes the server load the project and its saved queries and inject a capped summary (name, description, tags, saved-query names/descriptions/SQL, wiki page **titles**); wiki bodies are fetched on demand by `get_project_docs` because the system prompt is re-sent on every tool round; loading is best-effort and never fails a chat. State the 2000-char cap and the `...[truncated]` marker.

In `docs/dev/ui/components/prospector.md`, document the `get_project_docs` tool and that `sendDataToAi=false` suppresses sample rows in `execute_sql` and dashboard data while keeping columns/types/rowCount.

- [ ] **Step 2: Verify claims against the code**

Re-read the constants you documented and confirm they match `PROJECT_BLOCK_MAX_CHARS`, `SAVED_QUERY_SQL_MAX_CHARS`, and `PROJECT_DOC_MAX_CHARS`. Do not document a number you have not read.

- [ ] **Step 3: Commit**

```bash
git add docs/dev/PROSPECTOR.md docs/dev/ui/components/prospector.md
git commit -m "Document Prospector's project context and get_project_docs"
```

---

## Verification

Unit tests do not prove the model receives or uses the context. After all tasks, with a rebuilt webapp:

1. Open a project with a description, at least one tag, a saved query with a description, and a wiki page.
2. From SQL Lab inside that project, ask a question answerable only from the description (e.g. "what is this project about?"). Confirm the answer reflects the user's own words rather than a guess.
3. Ask something answerable only from the wiki page. Confirm from `ai-events.log` that `get_project_docs` was called — tool calls are now recorded in the prompt (`renderFullPrompt`).
4. Confirm the prompt in `ai-events.log` contains the `PROJECT CONTEXT` block and does **not** contain wiki page bodies.
5. Delete the project's store entry (or pass a bogus `projectId`) and confirm chat still works with no project block.
6. Turn off `sendDataToAi` and confirm `execute_sql` tool results in the log carry no `rows`.
