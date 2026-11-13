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

import java.io.File;

/** SQL over the workflow run log (dfs.workflow_logs), mirroring the AI Analytics setup. */
public final class WorkflowObservability {
  public static final String WORKSPACE = "workflow_logs";
  public static final String FORMAT = "workflow_log";
  static final String TABLE = "dfs." + WORKSPACE + ".`workflow-runs*.log`";

  private WorkflowObservability() { }

  public static String summarySql(String projectId, String sinceIso) {
    return "SELECT workflowId, COUNT(*) AS runs, "
        + "SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successes, "
        + "AVG(durationMs) AS avgDurationMs, MAX(ts) AS lastTs "
        + "FROM " + TABLE + " WHERE ts >= '" + escape(sinceIso) + "'"
        + (projectId == null ? "" : " AND projectId = '" + escape(projectId) + "'")
        + " GROUP BY workflowId";
  }

  public static String dailySql(String workflowId, String sinceIso) {
    return "SELECT SUBSTR(ts, 1, 10) AS `day`, COUNT(*) AS runs, "
        + "SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successes, "
        + "AVG(durationMs) AS avgDurationMs "
        + "FROM " + TABLE + " WHERE workflowId = '" + escape(workflowId) + "' AND ts >= '" + escape(sinceIso) + "' "
        + "GROUP BY SUBSTR(ts, 1, 10) ORDER BY `day`";
  }

  public static String actionSql(String workflowId, String sinceIso) {
    return "SELECT t.a.id AS id, t.a.type AS type, t.a.outcome AS outcome, COUNT(*) AS cnt FROM "
        + "(SELECT FLATTEN(actions) AS a FROM " + TABLE + " WHERE workflowId = '" + escape(workflowId)
        + "' AND ts >= '" + escape(sinceIso) + "') t "
        + "GROUP BY t.a.id, t.a.type, t.a.outcome";
  }

  public static String escape(String s) {
    return s == null ? "" : s.replace("'", "''");
  }

  static boolean logPresent(String logDir) {
    File[] files = logDir == null ? null : new File(logDir).listFiles(
        (d, n) -> n.startsWith("workflow-runs") && n.endsWith(".log"));
    return files != null && files.length > 0;
  }
}
