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
    };
    RunContext ctx = new RunContext();
    ctx.rowCount = 4;
    Map<String, Object> out = new EmailAction().execute(new ActionContext(new Workflow(), "s", ctx, services),
        M.readTree("{\"to\":[\"a@example.com\"],\"subject\":\"{{rowCount}} rows\",\"body\":\"see {{rowCount}}\"}"));
    assertEquals(List.of("[a@example.com]|4 rows|see 4"), sent);
    assertEquals(Map.of("sent", true), out);
  }
}
