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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Appends one JSON line per run to workflow-runs.log (logback logger "workflow.runs").
 * Action outputs are deliberately left out: they may contain query data.
 */
public final class WorkflowRunLog {
  private static final Logger RUNS = LoggerFactory.getLogger("workflow.runs");
  private static final Logger logger = LoggerFactory.getLogger(WorkflowRunLog.class);
  private static final ObjectMapper M = new ObjectMapper();

  private WorkflowRunLog() { }

  public static void log(Workflow wf, WorkflowRun run, String drillbit) {
    try {
      RUNS.info(M.writeValueAsString(line(wf, run, drillbit)));
    } catch (Exception e) {
      logger.warn("Failed to write workflow run log line for {}", wf.id, e);
    }
  }

  static Map<String, Object> line(Workflow wf, WorkflowRun run, String drillbit) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("ts", run.executedAt);
    m.put("runId", run.id);
    m.put("workflowId", wf.id);
    m.put("projectId", wf.projectId);
    m.put("owner", wf.owner);
    m.put("drillbit", drillbit);
    m.put("trigger", run.trigger);
    m.put("status", run.status);
    m.put("rowCount", run.rowCount);
    m.put("durationMs", run.duration);
    List<Map<String, Object>> actions = new ArrayList<>();
    if (run.actions != null) {
      for (ActionOutcome o : run.actions) {
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("id", o.id);
        a.put("type", o.type);
        a.put("outcome", o.outcome);
        a.put("reason", o.reason);
        a.put("durationMs", o.durationMs);
        a.put("error", o.error);
        actions.add(a);
      }
    }
    m.put("actions", actions);
    return m;
  }
}
