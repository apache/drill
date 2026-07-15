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
      ProjectResources.Project loadProject(String projectId) {
        assertEquals("proj-42", projectId);
        return fixtureProject;
      }

      @Override
      List<SavedQueryResources.SavedQuery> loadSavedQueries(List<String> ids) {
        capturedSavedQueryIds.add(ids);
        return List.of(fixtureQuery);
      }
    };

    List<ChatMessage> messages = resources.buildMessages(new LlmConfig(), request("proj-42"));
    String systemPrompt = systemPromptOf(messages);

    assertTrue(systemPrompt.contains("PROJECT CONTEXT"));
    assertTrue(systemPrompt.contains("Store sales analytics"));
    assertTrue(systemPrompt.contains("Revenue by region, last 30d"));
    assertTrue(systemPrompt.contains("Runbook"));
    assertFalse(systemPrompt.contains("SECRET_BODY_TEXT"));

    assertEquals(1, capturedSavedQueryIds.size());
    assertEquals(fixtureProject.getSavedQueryIds(), capturedSavedQueryIds.get(0));
  }
}
