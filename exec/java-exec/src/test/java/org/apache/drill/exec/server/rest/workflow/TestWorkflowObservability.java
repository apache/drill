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

import org.apache.drill.exec.physical.rowSet.DirectRowSet;
import org.apache.drill.exec.physical.rowSet.RowSetReader;
import org.apache.drill.exec.store.easy.json.JSONFormatConfig;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestWorkflowObservability extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
    File dir = dirTestWatcher.makeSubDir(java.nio.file.Paths.get("wflogs"));
    Files.write(new File(dir, "workflow-runs.log").toPath(), List.of(
        "{\"ts\":\"2026-10-20T08:00:00Z\",\"workflowId\":\"w1\",\"projectId\":\"p1\",\"status\":\"success\",\"durationMs\":100,"
            + "\"actions\":[{\"id\":\"a\",\"type\":\"record_alert\",\"outcome\":\"fired\"}]}",
        "{\"ts\":\"2026-10-21T08:00:00Z\",\"workflowId\":\"w1\",\"projectId\":\"p1\",\"status\":\"error\",\"durationMs\":300,"
            + "\"actions\":[{\"id\":\"a\",\"type\":\"record_alert\",\"outcome\":\"skipped\"}]}",
        "{\"ts\":\"2026-10-21T09:00:00Z\",\"workflowId\":\"w2\",\"projectId\":\"p2\",\"status\":\"success\",\"durationMs\":50,"
            + "\"actions\":[]}"));
    cluster.defineFormat("dfs", WorkflowObservability.FORMAT,
        new JSONFormatConfig(List.of("log"), null, null, null, null, null));
    cluster.defineWorkspace("dfs", WorkflowObservability.WORKSPACE, dir.getAbsolutePath(), WorkflowObservability.FORMAT);
  }

  @Test
  public void summaryFiltersByProject() throws Exception {
    DirectRowSet rows = client.queryBuilder().sql(WorkflowObservability.summarySql("p1", "2026-10-01T00:00:00Z")).rowSet();
    try {
      assertEquals(1, rows.rowCount());
      RowSetReader r = rows.reader();
      r.next();
      assertEquals("w1", r.scalar("workflowId").getString());
      assertEquals(2L, r.scalar("runs").getLong());
      assertEquals(1L, r.scalar("successes").getLong());
    } finally {
      rows.clear();
    }
  }

  @Test
  public void dailyAndActionBreakdown() throws Exception {
    DirectRowSet daily = client.queryBuilder().sql(WorkflowObservability.dailySql("w1", "2026-10-01T00:00:00Z")).rowSet();
    try {
      assertEquals(2, daily.rowCount());
    } finally {
      daily.clear();
    }
    DirectRowSet actions = client.queryBuilder().sql(WorkflowObservability.actionSql("w1", "2026-10-01T00:00:00Z")).rowSet();
    try {
      assertEquals(2, actions.rowCount());
    } finally {
      actions.clear();
    }
  }

  @Test
  public void escapeQuotes() {
    assertEquals("a''b", WorkflowObservability.escape("a'b"));
  }

  @Test
  public void quoteInProjectIdIsInertInSql() throws Exception {
    String injected = "p1' OR '1'='1";
    String sql = WorkflowObservability.summarySql(injected, "2026-10-01T00:00:00Z");
    assertTrue(sql, sql.contains("projectId = 'p1'' OR ''1''=''1'"));
    DirectRowSet rows = client.queryBuilder().sql(sql).rowSet();
    try {
      // A literal match on the whole string finds nothing; unescaped, the OR clause would return every run.
      assertEquals(0, rows.rowCount());
    } finally {
      rows.clear();
    }
    DirectRowSet daily = client.queryBuilder()
        .sql(WorkflowObservability.dailySql("w1' OR '1'='1", "2026-10-01T00:00:00Z")).rowSet();
    try {
      assertEquals(0, daily.rowCount());
    } finally {
      daily.clear();
    }
  }
}
