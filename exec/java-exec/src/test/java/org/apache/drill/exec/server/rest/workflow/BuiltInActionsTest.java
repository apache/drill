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
package org.apache.drill.exec.server.rest.workflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BuiltInActionsTest {
  private static final ObjectMapper M = new ObjectMapper();

  @Test
  public void saveToTableTimeoutFollowsTheQueryTimeout() {
    Workflow wf = new Workflow();
    wf.schedule.timeoutSeconds = 300;
    assertEquals((300 + WorkflowRunner.CANCEL_GRACE_SECONDS + SaveToTableAction.MARGIN_SECONDS) * 1000L, new SaveToTableAction().timeoutMs(wf, 60_000));
    wf.schedule.timeoutSeconds = 10;
    assertEquals(60_000L, new SaveToTableAction().timeoutMs(wf, 60_000));
    assertEquals(60_000L, new RecordAlertAction().timeoutMs(wf, 60_000));
  }

  @Test
  public void recordAlertRendersMessage() throws Exception {
    RunContext ctx = new RunContext();
    ctx.rowCount = 9;
    Map<String, Object> out = new RecordAlertAction().execute(
        new ActionContext(new Workflow(), "s", ctx, null), M.readTree("{\"message\":\"n={{rowCount}}\"}"));
    assertEquals(Map.of("message", "n=9"), out);
  }

  @Test
  public void saveToTableStatementsPerMode() throws Exception {
    assertEquals(List.of(
            "ALTER SESSION SET `store.format` = 'json'",
            "DROP TABLE IF EXISTS dfs.tmp.`schedules/wf_1`",
            "CREATE TABLE dfs.tmp.`schedules/wf_1` AS SELECT * FROM (select 1\n)"),
        SaveToTableAction.statements("wf-1", "select 1 ;  ",
            M.readTree("{\"location\":\"dfs.tmp\",\"format\":\"json\",\"mode\":\"overwrite\"}"), 5L));
    assertEquals(List.of(
            "ALTER SESSION SET `store.format` = 'parquet'",
            "INSERT INTO dfs.tmp.`schedules/wf_1` SELECT * FROM (select 1\n)"),
        SaveToTableAction.statements("wf-1", "select 1", M.readTree("{\"mode\":\"append\"}"), 5L));
    assertEquals(List.of(
            "ALTER SESSION SET `store.format` = 'parquet'",
            "CREATE TABLE dfs.tmp.`schedules/wf_1/run_5` AS SELECT * FROM (select 1\n)"),
        SaveToTableAction.statements("wf-1", "select 1", M.readTree("{\"mode\":\"newTablePerRun\"}"), 5L));
  }

  @Test
  public void saveToTableRunsStatementsAsOwnerOnOneSession() throws Exception {
    List<String> seen = new ArrayList<>();
    WorkflowServices services = new WorkflowServices() {
      @Override public void executeStatements(List<String> sql, String user, int timeout) {
        seen.add(user + ":" + sql.size() + ":" + timeout);
      }

      @Override public void sendMail(List<String> to, String s, String b) { }
      @Override public String summarize(String prompt, int maxChars) { return "summary"; }
    };
    Workflow wf = new Workflow();
    wf.id = "wf-1";
    wf.owner = "alice";
    wf.schedule.timeoutSeconds = 77;
    Map<String, Object> out = new SaveToTableAction().execute(
        new ActionContext(wf, "select 1", new RunContext(), services), M.readTree("{}"));
    assertEquals(List.of("alice:3:77"), seen);
    assertEquals("dfs.tmp.`schedules/wf_1`", out.get("path"));
  }

  @Test
  public void newTablePerRunReportsTheRunTable() throws Exception {
    WorkflowServices services = new WorkflowServices() {
      @Override public void executeStatements(List<String> sql, String user, int timeout) {
        seenSql = sql;
      }

      @Override public void sendMail(List<String> to, String s, String b) { }
      @Override public String summarize(String prompt, int maxChars) { return "summary"; }
    };
    Workflow wf = new Workflow();
    wf.id = "wf-1";
    wf.owner = "alice";
    Map<String, Object> out = new SaveToTableAction().execute(
        new ActionContext(wf, "select 1", new RunContext(), services), M.readTree("{\"mode\":\"newTablePerRun\"}"));
    String path = (String) out.get("path");
    assertTrue(path.matches("dfs\\.tmp\\.`schedules/wf_1/run_\\d+`"), path);
    assertEquals("CREATE TABLE " + path + " AS SELECT * FROM (select 1\n)", seenSql.get(1));
  }

  private List<String> seenSql;

  @Test
  public void lineCommentCannotSwallowClosingParen() throws Exception {
    List<String> sql = SaveToTableAction.statements("wf-1", "select 1 -- note", M.readTree("{}"), 5L);
    assertEquals("CREATE TABLE dfs.tmp.`schedules/wf_1` AS SELECT * FROM (select 1 -- note\n)", sql.get(2));
  }

  @Test
  public void statementsRejectUnsafeValues() throws Exception {
    for (String cfg : new String[] {
        "{\"location\":\"dfs.tmp; DROP TABLE x\"}",
        "{\"location\":\"dfs.tmp\\n\"}",
        "{\"format\":\"json'; DROP\"}",
        "{\"mode\":\"truncate\"}"}) {
      assertThrows(IllegalArgumentException.class,
          () -> SaveToTableAction.statements("wf-1", "select 1", M.readTree(cfg), 5L), cfg);
    }
    assertThrows(IllegalArgumentException.class,
        () -> SaveToTableAction.statements("wf`1", "select 1", M.readTree("{}"), 5L));
  }

  @Test
  public void validatorRejectsInjectedLocationAndAcceptsValid() throws Exception {
    ActionTypeRegistry reg = ActionTypeRegistry.defaults();
    assertTrue(errorsFor(reg, "{\"location\":\"dfs.tmp; DROP TABLE x\"}").contains("actions[0].config.location"));
    assertTrue(errorsFor(reg, "{\"location\":\"dfs.tmp\"}").isEmpty());
    assertTrue(errorsFor(reg, "{\"location\":\"dfs\"}").isEmpty());
  }

  private static List<String> errorsFor(ActionTypeRegistry reg, String config) throws Exception {
    Workflow wf = new Workflow();
    wf.name = "W";
    wf.savedQueryId = "q1";
    Workflow.ActionSpec a = new Workflow.ActionSpec();
    a.id = "save";
    a.type = "save_to_table";
    a.config = (com.fasterxml.jackson.databind.node.ObjectNode) M.readTree(config);
    wf.actions.add(a);
    List<String> paths = new ArrayList<>();
    WorkflowValidator.validate(wf, reg).forEach(e -> paths.add(e.path));
    return paths;
  }

  @Test
  public void emailRendersAndSends() throws Exception {
    List<String> sent = new ArrayList<>();
    WorkflowServices services = new WorkflowServices() {
      @Override public void executeStatements(List<String> sql, String user, int timeout) { }
      @Override public void sendMail(List<String> to, String s, String b) { sent.add(to + "|" + s + "|" + b); }
      @Override public String summarize(String prompt, int maxChars) { return "summary"; }
    };
    RunContext ctx = new RunContext();
    ctx.rowCount = 4;
    Map<String, Object> out = new EmailAction().execute(new ActionContext(new Workflow(), "s", ctx, services),
        M.readTree("{\"to\":[\"a@example.com\"],\"subject\":\"{{rowCount}} rows\",\"body\":\"see {{rowCount}}\"}"));
    assertEquals(List.of("[a@example.com]|4 rows|see 4"), sent);
    assertEquals(Map.of("sent", true), out);
  }

  // ------------------------------------------------------------ ai_summary

  /** Fake services whose summarize records the prompt and returns or throws as configured. */
  private static final class SummaryServices implements WorkflowServices {
    final List<String> prompts = new ArrayList<>();
    final boolean sendData;
    final String reply;
    final Exception failure;

    SummaryServices(boolean sendData, String reply, Exception failure) {
      this.sendData = sendData;
      this.reply = reply;
      this.failure = failure;
    }

    @Override public void executeStatements(List<String> sql, String user, int timeout) { }
    @Override public void sendMail(List<String> to, String s, String b) { }
    @Override public boolean sendDataToAi() { return sendData; }

    @Override
    public String summarize(String prompt, int maxChars) throws Exception {
      prompts.add(prompt);
      if (failure != null) {
        throw failure;
      }
      return reply.length() > maxChars ? reply.substring(0, maxChars) : reply;
    }
  }

  private static RunContext summaryRun() {
    RunContext run = new RunContext();
    run.status = "success";
    run.rowCount = 42;
    run.durationMs = 1234;
    run.columns = List.of("host", "secret_col");
    run.rows = List.of(Map.of("host", "web-01", "secret_col", "s3cr3t-cell"));
    return run;
  }

  private static Map<String, Object> runSummary(SummaryServices services, String config) throws Exception {
    return new AiSummaryAction().execute(
        new ActionContext(new Workflow(), "select 1", summaryRun(), services), M.readTree(config));
  }

  @Test
  public void aiSummaryReturnsTheProvidersTextUnderSummary() throws Exception {
    SummaryServices s = new SummaryServices(true, "All quiet.", null);
    assertEquals(Map.of("summary", "All quiet."), runSummary(s, "{\"instructions\":\"be brief\"}"));
    assertTrue(s.prompts.get(0).contains("be brief"));
  }

  @Test
  public void aiSummaryTruncatesToMaxChars() throws Exception {
    SummaryServices s = new SummaryServices(true, "x".repeat(50), null);
    assertEquals("x".repeat(10), runSummary(s, "{\"maxChars\":10}").get("summary"));
  }

  @Test
  public void aiSummaryFailsWithAStatedReasonWhenTheProviderThrows() {
    SummaryServices s = new SummaryServices(true, "", new IllegalStateException("Prospector is not configured"));
    Exception e = assertThrows(IllegalStateException.class, () -> runSummary(s, "{}"));
    assertEquals("Prospector is not configured", e.getMessage());
  }

  @Test
  public void aiSummaryIncludesRowCountAndColumnsInThePrompt() throws Exception {
    SummaryServices s = new SummaryServices(true, "ok", null);
    runSummary(s, "{}");
    String prompt = s.prompts.get(0);
    assertTrue(prompt.contains("rowCount: 42"), prompt);
    assertTrue(prompt.contains("durationMs: 1234"), prompt);
    assertTrue(prompt.contains("host, secret_col"), prompt);
    assertTrue(prompt.contains("s3cr3t-cell"), prompt);
  }

  @Test
  public void aiSummaryWithholdsColumnsAndRowsWhenSendDataToAiIsOff() throws Exception {
    SummaryServices s = new SummaryServices(false, "ok", null);
    runSummary(s, "{}");
    String prompt = s.prompts.get(0);
    assertTrue(prompt.contains("status: success"), prompt);
    assertTrue(prompt.contains("rowCount: 42"), prompt);
    assertTrue(prompt.contains("durationMs: 1234"), prompt);
    assertFalse(prompt.contains("s3cr3t-cell"), prompt);
    assertFalse(prompt.contains("web-01"), prompt);
    assertFalse(prompt.contains("secret_col"), prompt);
  }

  @Test
  public void aiSummaryPromptCapsRowsAtTwenty() {
    RunContext run = summaryRun();
    List<Map<String, String>> rows = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      rows.add(Map.of("host", "h" + i, "secret_col", "v" + i));
    }
    run.rows = rows;
    String prompt = AiSummaryAction.buildPrompt("", run, true);
    assertTrue(prompt.contains("h19 | v19"), prompt);
    assertFalse(prompt.contains("h20 | v20"), prompt);
  }

  @Test
  public void aiSummaryPreviewDoesNotCallTheProvider() throws Exception {
    SummaryServices s = new SummaryServices(true, "ok", null);
    AiSummaryAction action = new AiSummaryAction();
    assertTrue(action.hasSideEffects());
    action.preview(new ActionContext(new Workflow(), "select 1", summaryRun(), s), M.readTree("{}"));
    assertTrue(s.prompts.isEmpty());
  }
}
