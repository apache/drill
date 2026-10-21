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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class WorkflowRunLogTest {

  @Test
  public void lineHasRunFactsButNoOutputs() {
    Workflow wf = new Workflow();
    wf.id = "wf-1";
    wf.projectId = "p-1";
    wf.owner = "alice";
    WorkflowRun run = new WorkflowRun();
    run.id = "r-1";
    run.executedAt = "2026-10-21T08:00:00Z";
    run.status = "success";
    run.trigger = "schedule";
    run.rowCount = 5L;
    run.duration = 120L;
    ActionOutcome o = new ActionOutcome("sum", "record_alert");
    o.outcome = ActionOutcome.FIRED;
    o.outputs = Map.of("message", "secret data");
    run.actions = List.of(o);

    Map<String, Object> line = WorkflowRunLog.line(wf, run, "host:31010");
    assertEquals("wf-1", line.get("workflowId"));
    assertEquals("p-1", line.get("projectId"));
    assertEquals(120L, line.get("durationMs"));
    @SuppressWarnings("unchecked")
    Map<String, Object> action = ((List<Map<String, Object>>) line.get("actions")).get(0);
    assertEquals("fired", action.get("outcome"));
    assertFalse(action.containsKey("outputs"));
    assertFalse(line.toString().contains("secret data"));
  }
}
