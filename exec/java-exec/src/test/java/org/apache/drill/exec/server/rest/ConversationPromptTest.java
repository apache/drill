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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.exec.server.rest.ai.ChatMessage;
import org.apache.drill.exec.server.rest.ai.LlmConfig;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The conversation surface is built on the premise that Prospector builds things, so the
 * prompt has to say so, and has to describe the tools as they actually behave. These tests
 * assert against the assembled system prompt, the string the model sees.
 */
public class ConversationPromptTest {

  private static final String USER = "alice";

  private static ProspectorResources.ChatRequest requestWith(ProspectorResources.ChatContext ctx) {
    ProspectorResources.ChatRequest req = new ProspectorResources.ChatRequest();
    req.context = ctx;
    req.messages = new ArrayList<>();
    return req;
  }

  private static String promptFor(ProspectorResources.ChatContext ctx) {
    return promptFor(new LlmConfig(), ctx);
  }

  private static String promptFor(LlmConfig config, ProspectorResources.ChatContext ctx) {
    List<ChatMessage> messages =
        new ProspectorResources().buildMessages(config, requestWith(ctx), USER);
    return messages.get(0).getContent();
  }

  private static ProspectorResources.ChatContext conversationContext() {
    ProspectorResources.ChatContext ctx = new ProspectorResources.ChatContext();
    ctx.feature = "project_chat";
    ctx.conversationMode = true;
    return ctx;
  }

  private static String lower(String s) {
    return s.toLowerCase(java.util.Locale.ROOT);
  }

  /** Collapses line wrapping, so an assertion about a sentence survives a reflow of the prompt. */
  private static String conversationPrompt() {
    return promptFor(conversationContext()).replaceAll("\\s+", " ");
  }

  @Test
  public void conversationModeTellsTheModelToBuildArtifacts() {
    String prompt = conversationPrompt();

    // The canvas is where its work appears.
    assertTrue(prompt.contains("the user has a canvas"), "names the canvas");
    assertTrue(prompt.contains("The SQL and the rows appear on the canvas"),
        "says where a query's work appears");

    // It builds with tools instead of handing SQL back.
    assertTrue(prompt.contains("execute_sql"));
    assertTrue(lower(prompt).contains("do not answer with a sql code block"),
        "forbids answering with SQL to paste");
    assertTrue(lower(prompt).contains("build it"), "forbids narrating instead of building");

    // Every artifact-creating tool is named.
    for (String tool : List.of("execute_sql", "create_visualization", "create_dashboard",
        "save_query", "write_wiki_page", "create_workflow")) {
      assertTrue(prompt.contains(tool), "prompt names " + tool);
    }

    // The old surfaces told the model it could create things ONLY when explicitly asked, which
    // directly contradicts a surface whose point is that it builds.
    assertFalse(prompt.contains("ONLY when the user explicitly asks"),
        "the explicit-ask-only rule must not reach a conversation");
    assertFalse(prompt.contains("means reporting the result"));
  }

  @Test
  public void conversationModeDescribesCreateWorkflowAsBuilt() {
    String prompt = conversationPrompt();

    // create_workflow takes sql directly; save_query first 422s (the saved query is not in the
    // project, and WorkflowResources.validate rejects a savedQueryId that is not).
    assertTrue(prompt.contains("Pass the query as sql"));
    assertTrue(prompt.contains("Do NOT call save_query first"));

    // Always paused, and the model cannot activate it.
    assertTrue(prompt.contains("always created paused"));
    assertTrue(prompt.contains("cannot activate"));
    assertTrue(prompt.contains("never say it is running"));

    // Point at the viewPath in the result; the activation control is not on every surface, so
    // the prompt must not promise a button.
    assertTrue(prompt.contains("viewPath"));
    assertFalse(lower(prompt).contains("activate button"));
    assertFalse(lower(prompt).contains("activation button"));
  }

  /**
   * Build-first is scoped: without these sentences a conversation accumulates a chart or a
   * saved query for every result, and creates a schedule nobody agreed to.
   */
  @Test
  public void conversationModeScopesWhatItBuildsUnasked() {
    String prompt = conversationPrompt();

    assertTrue(prompt.contains("Do not chart or save every result unasked"),
        "must not chart or save every result");
    assertTrue(prompt.contains("when the user asks for one or asks for something that is one"),
        "charts, dashboards and saved queries are for requests");
    assertTrue(prompt.contains("A scheduled workflow needs the user's agreement first"));
    assertTrue(prompt.contains("then call create_workflow once they agree"),
        "no workflow before the user agrees");
  }

  /** execute_sql drops no card, so a model told every result gets one would misread the thread. */
  @Test
  public void conversationModeDoesNotPromiseACardForEveryResult() {
    String prompt = conversationPrompt();

    assertTrue(prompt.contains("The rows from execute_sql appear on the canvas without a card"));
    assertTrue(prompt.contains("charts, dashboards, saved queries, workflows and wiki pages "
        + "also get a card in the thread"));
    assertFalse(prompt.contains("each with a card"));
  }

  /** The tool's own failure message tells the model to retry, so the prompt must not say "once". */
  @Test
  public void conversationModeAllowsRetryingARejectedWorkflow() {
    String prompt = conversationPrompt();

    assertTrue(prompt.contains("Do not call create_workflow again for a workflow that was created"));
    assertTrue(prompt.contains("If the server rejects it, fix the fields and call it again"));
    assertFalse(prompt.contains("once per workflow"));
  }

  @Test
  public void conversationModeDescribesWriteWikiPageAsBuilt() {
    String prompt = conversationPrompt();

    assertTrue(prompt.contains("write_wiki_page"));
    // includeTranscript defaults to false and save_report always attached the evidence, so a
    // findings report silently loses its appendix unless the model passes it.
    assertTrue(prompt.contains("includeTranscript true"));
    // Update by pageId, and get_project_docs is how it learns the id of a page it did not write.
    assertTrue(prompt.contains("with its pageId"));
    assertTrue(prompt.contains("find its pageId with get_project_docs"));
    // save_report is retired.
    assertFalse(prompt.contains("save_report"));
  }

  /**
   * Conversation mode must not change what the other surfaces say. Chat, dashboard and logs
   * keep the rule that the authoring tools are for explicit requests only.
   */
  @Test
  public void withoutConversationModeThePromptKeepsTheExplicitAskRule() {
    ProspectorResources.ChatContext ctx = new ProspectorResources.ChatContext();
    ctx.feature = "sql_lab_chat";
    String prompt = promptFor(ctx);

    assertTrue(prompt.contains("ONLY when the user explicitly asks"));
    assertFalse(prompt.contains("canvas"));
    assertFalse(prompt.contains("CONVERSATION"));
  }

  @Test
  public void conversationModeDoesNotEmitTheNotebookBlock() throws Exception {
    // A browser running an older build still sends the notebook fields. They must be tolerated
    // on the wire and must not put a notebook section in the prompt.
    ProspectorResources.ChatContext ctx = new ObjectMapper().readValue(
        "{\"feature\":\"project_chat\",\"conversationMode\":true,\"notebookMode\":true,"
        + "\"notebookDfName\":\"ghost_df\",\"notebookDfShape\":\"10 rows x 3 cols\","
        + "\"notebookColumns\":[\"a\",\"b\"],\"notebookCellCode\":\"ghost_df.head()\","
        + "\"notebookCellError\":\"KeyError: 'z'\"}",
        ProspectorResources.ChatContext.class);
    assertTrue(ctx.conversationMode);

    String prompt = promptFor(ctx);

    assertFalse(prompt.contains("notebook"));
    assertFalse(prompt.contains("DataFrame"));
    assertFalse(prompt.contains("ghost_df"));
    assertFalse(prompt.contains("KeyError"));
  }

  @Test
  public void conversationModeIsOffUnlessTheClientSaysSo() throws Exception {
    ProspectorResources.ChatContext ctx = new ObjectMapper().readValue(
        "{\"feature\":\"project_chat\",\"projectId\":\"p1\"}",
        ProspectorResources.ChatContext.class);
    assertFalse(ctx.conversationMode);
  }

  /** The logs block, byte for byte, as it was before conversation mode existed. */
  @Test
  public void logsContextBlockIsUnchanged() {
    ProspectorResources.ChatContext ctx = new ProspectorResources.ChatContext();
    ctx.feature = "log_analysis";
    ctx.logAnalysisMode = true;
    ctx.logFileName = "drillbit.log";
    ctx.logLines = List.of("ERROR boom", "WARN slow");

    String expected =
        "\nYou are also a log analysis expert for Apache Drill. When analyzing log lines:\n"
        + "- Identify error patterns and their root causes\n"
        + "- Explain what each log level (ERROR, WARN, INFO, DEBUG) means in context\n"
        + "- Suggest configuration changes or actions to resolve issues\n"
        + "- Identify performance concerns from thread contention or slow operations\n"
        + "- Reference Drill documentation when relevant\n"
        + "- Users can query log files via SQL using the dfs.logs workspace\n\n"
        + "Log file being analyzed: drillbit.log\n"
        + "\nLog lines for context:\n```\nERROR boom\nWARN slow\n```\n";

    String prompt = promptFor(ctx);
    assertTrue(prompt.contains(expected), "logs block changed:\n" + prompt);
    // The surface still gets the shared tail unchanged.
    assertTrue(prompt.contains("ONLY when the user explicitly asks"));
    assertFalse(prompt.contains("canvas"));
  }

  /** The dashboard summary block, byte for byte, with rows withheld by config. */
  @Test
  public void dashboardContextBlockIsUnchanged() {
    ProspectorResources.DashboardDataContext panel = new ProspectorResources.DashboardDataContext();
    panel.panelName = "Sales";
    panel.columns = List.of("region", "amount");
    panel.rowCount = 1;
    panel.sampleRows = List.of(Map.of("region", "West", "amount", "SECRET_VALUE_42"));

    ProspectorResources.ChatContext ctx = new ProspectorResources.ChatContext();
    ctx.feature = "dashboard_summary";
    ctx.dashboardSummaryMode = true;
    ctx.dashboardData = List.of(panel);

    String expectedHead =
        "\nYou are generating an executive summary for a dashboard. "
        + "Analyze the data from all dashboard panels and provide:\n"
        + "- Key insights and trends\n"
        + "- Anomalies or areas needing attention\n"
        + "- Status indicators using emoji (✅ good, ⚠️ warning, ❌ critical)\n"
        + "- Clear, well-formatted sections using markdown\n"
        + "- You may use markdown images ![alt](url) if helpful\n\n"
        + "Dashboard panels data:\n\n### Panel 1: Sales\n";

    LlmConfig config = new LlmConfig();
    config.setSendDataToAi(false);
    String prompt = promptFor(config, ctx);
    assertTrue(prompt.contains(expectedHead), "dashboard block changed:\n" + prompt);
    assertTrue(prompt.contains("Columns: region, amount"));
    assertFalse(prompt.contains("SECRET_VALUE_42"));
    assertFalse(prompt.contains("canvas"));
  }

  @Test
  public void dashboardQuestionAnsweringStillForbidsToolsAndDoesNotGetConversationText() {
    ProspectorResources.ChatContext ctx = new ProspectorResources.ChatContext();
    ctx.feature = "dashboard_qna";
    ctx.dashboardQnAMode = true;

    String prompt = promptFor(ctx);
    assertTrue(prompt.contains("IMPORTANT: Do NOT suggest running queries, retrieving data, "
        + "or using tools."));
    assertFalse(prompt.contains("canvas"));
    assertEquals(1, prompt.split("answering questions about dashboard data", -1).length - 1);
  }
}
