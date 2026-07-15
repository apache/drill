/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.server.rest;

import org.apache.drill.exec.server.rest.ai.ChatMessage;
import org.apache.drill.exec.server.rest.ai.LlmConfig;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.store.InMemoryStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The project block is what stops the model guessing at table names, so it must
 * carry the user's own words — description, tags, saved-query descriptions — and
 * must stay small enough to survive being re-sent on every tool round.
 */
public class ProjectContextBlockTest {

  private static final String USER = "alice";

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
        "w1", "Runbook", "SECRET_BODY_TEXT", 0, 0L, 0L);
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

  /**
   * Names are optional on the wire, and an unguarded append renders the four-character
   * string "null" into the prompt as if it were the user's own word for their work.
   */
  @Test
  public void testNullNamesNeverRenderAsTheLiteralNull() {
    ProjectResources.WikiPage untitledPage =
        new ProjectResources.WikiPage("w1", null, "body", 0, 0L, 0L);
    String block = ProspectorResources.buildProjectBlock(
        project(null, "Store sales analytics", null, List.of(untitledPage)),
        List.of(savedQuery(null, null, "SELECT 1")));
    assertFalse(block.contains("null"));
    // The guards must drop only the missing values, not the block around them.
    assertTrue(block.contains("Store sales analytics"));
    assertTrue(block.contains("SELECT 1"));
  }

  @Test
  public void testTruncatesLongSavedQuerySql() {
    String longSql = "SELECT " + "x, ".repeat(400) + "1";
    String block = ProspectorResources.buildProjectBlock(
        project("Retail", null, null, null),
        List.of(savedQuery("Wide", null, longSql)));
    assertFalse(block.contains(longSql));
  }

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
        new ProspectorResources().buildMessages(new LlmConfig(), request(null), USER);
    assertFalse(systemPromptOf(messages).contains("PROJECT CONTEXT"));
  }

  /**
   * Context is an enhancement. An unreachable store (here: no injected provider at
   * all) must degrade to no project block, never to a failed chat.
   */
  @Test
  public void testUnreachableStoreDoesNotFailTheChat() {
    List<ChatMessage> messages =
        new ProspectorResources().buildMessages(new LlmConfig(), request("proj-42"), USER);
    assertFalse(systemPromptOf(messages).contains("PROJECT CONTEXT"));
    assertTrue(systemPromptOf(messages).contains("Apache Drill"));
  }

  /**
   * Proves the positive path end to end: a loaded project actually reaches the
   * system prompt via buildMessages, and the project's own savedQueryIds are what
   * get passed to loadSavedQueries — not some other field. Without this, a test
   * suite that only exercises buildProjectBlock directly (as the six tests above
   * do) can't tell the injection wiring in buildMessages apart from wiring that was
   * deleted or subtly broken.
   */
  @Test
  public void testLoadedProjectReachesSystemPrompt() {
    ProjectResources.WikiPage wikiPage = new ProjectResources.WikiPage(
        "w1", "Runbook", "SECRET_BODY_TEXT", 0, 0L, 0L);
    ProjectResources.Project fixtureProject = project("Retail", "Store sales analytics",
        List.of("sales", "retail"), List.of(wikiPage));
    fixtureProject.setSavedQueryIds(List.of("q1"));

    SavedQueryResources.SavedQuery fixtureQuery = savedQuery("Top regions",
        "Revenue by region, last 30d", "SELECT region, SUM(amount) FROM sales GROUP BY region");

    List<List<String>> capturedSavedQueryIds = new ArrayList<>();

    ProspectorResources resources = new ProspectorResources() {
      @Override
      ProjectResources.Project loadProject(String projectId, String username) {
        assertEquals("proj-42", projectId);
        assertEquals(USER, username);
        return fixtureProject;
      }

      @Override
      List<SavedQueryResources.SavedQuery> loadSavedQueries(List<String> ids, String username) {
        capturedSavedQueryIds.add(ids);
        assertEquals(USER, username);
        return List.of(fixtureQuery);
      }
    };

    List<ChatMessage> messages =
        resources.buildMessages(new LlmConfig(), request("proj-42"), USER);
    String systemPrompt = systemPromptOf(messages);

    assertTrue(systemPrompt.contains("PROJECT CONTEXT"));
    assertTrue(systemPrompt.contains("Store sales analytics"));
    assertTrue(systemPrompt.contains("Revenue by region, last 30d"));
    assertTrue(systemPrompt.contains("Runbook"));
    assertFalse(systemPrompt.contains("SECRET_BODY_TEXT"));

    assertEquals(1, capturedSavedQueryIds.size());
    assertEquals(fixtureProject.getSavedQueryIds(), capturedSavedQueryIds.get(0));
  }

  // ==================== Authorization ====================
  //
  // projectId is client-supplied, so the load path must enforce the same rules as
  // GET /api/v1/projects/{id}: soft-deleted projects are invisible, and the requester
  // must own the project, or it must be public, or it must be shared with them.
  // Anything else must look exactly like a missing project — no block, chat proceeds.

  private static ProjectResources.Project storedProject(String owner, boolean isPublic,
      List<String> sharedWith, long deletedAt, List<String> savedQueryIds) {
    return new ProjectResources.Project("proj-42", "Retail", "PROJECT_SECRET_DESC",
        List.of("sales"), owner, isPublic, sharedWith, null, savedQueryIds, null, null,
        List.of(new ProjectResources.WikiPage("w1", "Runbook", "body", 0, 0L, 0L)),
        false, 0L, 0L, null, null, deletedAt);
  }

  private static SavedQueryResources.SavedQuery storedQuery(String id, String name,
      String owner, boolean isPublic, long deletedAt) {
    return new SavedQueryResources.SavedQuery(id, name, "desc for " + name,
        "SELECT * FROM " + name, null, owner, 0L, 0L, null, isPublic, deletedAt);
  }

  /** A ProspectorResources backed by real in-memory stores rather than a Drillbit. */
  private static ProspectorResources withStores(ProjectResources.Project project,
      List<SavedQueryResources.SavedQuery> queries) {
    PersistentStore<ProjectResources.Project> projectStore = new InMemoryStore<>(10);
    if (project != null) {
      projectStore.put(project.getId(), project);
    }
    PersistentStore<SavedQueryResources.SavedQuery> queryStore = new InMemoryStore<>(10);
    for (SavedQueryResources.SavedQuery q : queries) {
      queryStore.put(q.getId(), q);
    }
    return new ProspectorResources() {
      @Override
      PersistentStore<ProjectResources.Project> getProjectStore() {
        return projectStore;
      }

      @Override
      PersistentStore<SavedQueryResources.SavedQuery> getSavedQueryStore() {
        return queryStore;
      }
    };
  }

  private static String promptFor(ProjectResources.Project project,
      List<SavedQueryResources.SavedQuery> queries, String requester) {
    return systemPromptOf(withStores(project, queries)
        .buildMessages(new LlmConfig(), request("proj-42"), requester));
  }

  /**
   * The IDOR itself: naming another user's private project id must not read its
   * contents back out of the system prompt.
   */
  @Test
  public void testNoProjectBlockForOtherUsersPrivateProject() {
    String prompt = promptFor(
        storedProject("alice", false, List.of("bob"), 0L, List.of()), List.of(), "mallory");
    assertFalse(prompt.contains("PROJECT CONTEXT"));
    assertFalse(prompt.contains("PROJECT_SECRET_DESC"));
    // Unauthorized must be indistinguishable from missing: the chat still happens.
    assertTrue(prompt.contains("Apache Drill"));
  }

  @Test
  public void testProjectBlockForOwner() {
    assertTrue(promptFor(storedProject("alice", false, List.of(), 0L, List.of()),
        List.of(), "alice").contains("PROJECT_SECRET_DESC"));
  }

  @Test
  public void testProjectBlockForPublicProject() {
    assertTrue(promptFor(storedProject("alice", true, List.of(), 0L, List.of()),
        List.of(), "mallory").contains("PROJECT_SECRET_DESC"));
  }

  @Test
  public void testProjectBlockWhenSharedWithRequester() {
    assertTrue(promptFor(storedProject("alice", false, List.of("bob"), 0L, List.of()),
        List.of(), "bob").contains("PROJECT_SECRET_DESC"));
  }

  /** Trashed projects are 404 on the REST path; they must be invisible here too. */
  @Test
  public void testNoProjectBlockForSoftDeletedProjectEvenForOwner() {
    String prompt = promptFor(storedProject("alice", false, List.of(), 999L, List.of()),
        List.of(), "alice");
    assertFalse(prompt.contains("PROJECT CONTEXT"));
    assertFalse(prompt.contains("PROJECT_SECRET_DESC"));
  }

  @Test
  public void testNoProjectBlockForMissingProject() {
    assertFalse(promptFor(null, List.of(), "alice").contains("PROJECT CONTEXT"));
  }

  /**
   * A readable project can reference a saved query owned by someone else — sharing a
   * project does not share every query it lists. Each query is authorized on its own.
   */
  @Test
  public void testOtherUsersPrivateSavedQueryExcludedFromReadableProject() {
    String prompt = promptFor(
        storedProject("alice", true, List.of(), 0L, List.of("q-mine", "q-private", "q-public")),
        List.of(storedQuery("q-mine", "my_query", "bob", false, 0L),
            storedQuery("q-private", "alices_secret", "alice", false, 0L),
            storedQuery("q-public", "shared_query", "alice", true, 0L)),
        "bob");

    assertTrue(prompt.contains("my_query"));
    assertTrue(prompt.contains("shared_query"));
    assertFalse(prompt.contains("alices_secret"));
  }

  @Test
  public void testSoftDeletedSavedQueryExcluded() {
    String prompt = promptFor(
        storedProject("bob", false, List.of(), 0L, List.of("q-gone")),
        List.of(storedQuery("q-gone", "trashed_query", "bob", false, 999L)),
        "bob");
    assertTrue(prompt.contains("PROJECT CONTEXT"));
    assertFalse(prompt.contains("trashed_query"));
  }

  /**
   * The notebook tab populates six notebook* context fields, which deserialize fine
   * (covered by ProspectorEventTest.testChatContextCarriesNotebookFields) but were
   * silently dropped by buildMessages. This proves they actually reach the prompt
   * the model sees, not just the deserialized ChatContext.
   */
  @Test
  public void testNotebookContextReachesSystemPrompt() {
    ProspectorResources.ChatRequest req = new ProspectorResources.ChatRequest();
    ProspectorResources.ChatContext ctx = new ProspectorResources.ChatContext();
    ctx.feature = "sql_lab_chat";
    ctx.notebookMode = true;
    ctx.notebookDfName = "sales_df";
    ctx.notebookDfShape = "10 rows x 3 cols";
    ctx.notebookColumns = List.of("region", "amount");
    ctx.notebookCellCode = "sales_df.groupby('region').sum()";
    ctx.notebookCellError = "KeyError: 'region'";
    req.context = ctx;
    req.messages = new ArrayList<>();

    List<ChatMessage> messages = new ProspectorResources().buildMessages(new LlmConfig(), req, USER);
    String systemPrompt = systemPromptOf(messages);

    assertTrue(systemPrompt.contains("sales_df"));
    assertTrue(systemPrompt.contains("10 rows x 3 cols"));
    assertTrue(systemPrompt.contains("region"));
    assertTrue(systemPrompt.contains("amount"));
    assertTrue(systemPrompt.contains("sales_df.groupby('region').sum()"));
    assertTrue(systemPrompt.contains("KeyError: 'region'"));
  }

  /**
   * notebookDfName etc. are populated even though notebookMode is off — this can't
   * happen from the real UI, but it isolates the notebookMode guard itself: without
   * it, a stale notebook value would leak into a non-notebook chat's prompt.
   */
  @Test
  public void testNoNotebookSectionWhenNotebookModeFalse() {
    ProspectorResources.ChatRequest req = new ProspectorResources.ChatRequest();
    ProspectorResources.ChatContext ctx = new ProspectorResources.ChatContext();
    ctx.feature = "sql_lab_chat";
    ctx.notebookMode = false;
    ctx.notebookDfName = "ghost_df";
    ctx.notebookCellCode = "ghost_df.head()";
    req.context = ctx;
    req.messages = new ArrayList<>();

    List<ChatMessage> messages = new ProspectorResources().buildMessages(new LlmConfig(), req, USER);
    String systemPrompt = systemPromptOf(messages);
    assertFalse(systemPrompt.contains("ghost_df"));
    assertFalse(systemPrompt.contains("DataFrame variable:"));
  }

  private static ProspectorResources.DashboardDataContext dashboardPanel(String panelName,
      List<String> columns, List<Map<String, Object>> sampleRows) {
    ProspectorResources.DashboardDataContext ddc = new ProspectorResources.DashboardDataContext();
    ddc.panelName = panelName;
    ddc.columns = columns;
    ddc.rowCount = sampleRows.size();
    ddc.sampleRows = sampleRows;
    return ddc;
  }

  private static ProspectorResources.ChatRequest dashboardRequest() {
    ProspectorResources.ChatRequest req = new ProspectorResources.ChatRequest();
    ProspectorResources.ChatContext ctx = new ProspectorResources.ChatContext();
    ctx.feature = "sql_lab_chat";
    ctx.dashboardSummaryMode = true;
    ctx.dashboardData = List.of(dashboardPanel("Sales", List.of("region", "amount"),
        List.of(Map.of("region", "West", "amount", "SECRET_VALUE_42"))));
    req.context = ctx;
    req.messages = new ArrayList<>();
    return req;
  }

  private static LlmConfig configWithSendDataToAi(boolean sendDataToAi) {
    LlmConfig config = new LlmConfig();
    config.setSendDataToAi(sendDataToAi);
    return config;
  }

  /**
   * appendDashboardData used to serialize sample rows unconditionally. The gate is the
   * server-side LlmConfig setting rather than anything the request carries, so a client
   * cannot opt itself back in. Columns and row count are metadata, not user data, so
   * they must survive even when sample rows are withheld.
   */
  @Test
  public void testDashboardSampleRowsOmittedWhenConfigForbidsSendingData() {
    String systemPrompt = systemPromptOf(new ProspectorResources()
        .buildMessages(configWithSendDataToAi(false), dashboardRequest(), USER));

    assertTrue(systemPrompt.contains("region"));
    assertTrue(systemPrompt.contains("Row count: 1"));
    assertFalse(systemPrompt.contains("SECRET_VALUE_42"));
  }

  @Test
  public void testDashboardSampleRowsIncludedWhenConfigPermitsSendingData() {
    String systemPrompt = systemPromptOf(new ProspectorResources()
        .buildMessages(configWithSendDataToAi(true), dashboardRequest(), USER));

    assertTrue(systemPrompt.contains("SECRET_VALUE_42"));
  }

  /**
   * The gate must not be re-openable from the request. This is the whole point of
   * moving it off ChatContext: a stray client field must be inert, so an unknown
   * "sendDataToAi":true in the JSON cannot restore rows that the config withholds.
   */
  @Test
  public void testClientCannotReEnableSampleRowsViaRequestJson() throws Exception {
    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();
    ProspectorResources.ChatContext ctx = mapper.readValue(
        "{\"feature\":\"sql_lab_chat\",\"dashboardSummaryMode\":true,\"sendDataToAi\":true}",
        ProspectorResources.ChatContext.class);
    ctx.dashboardData = List.of(dashboardPanel("Sales", List.of("region", "amount"),
        List.of(Map.of("region", "West", "amount", "SECRET_VALUE_42"))));
    ProspectorResources.ChatRequest req = new ProspectorResources.ChatRequest();
    req.context = ctx;
    req.messages = new ArrayList<>();

    String systemPrompt = systemPromptOf(new ProspectorResources()
        .buildMessages(configWithSendDataToAi(false), req, USER));

    assertFalse(systemPrompt.contains("SECRET_VALUE_42"));
  }

  /**
   * The status endpoint is the only source of this flag a non-admin browser can read
   * (/api/v1/ai/config is admin-only), so the client-side execute_sql row gate depends
   * on it being mirrored here.
   */
  @Test
  public void testStatusResponseCarriesSendDataToAi() {
    assertFalse(new ProspectorResources.AiStatusResponse(true, true, false).sendDataToAi);
    assertTrue(new ProspectorResources.AiStatusResponse(true, true, true).sendDataToAi);
  }
}
