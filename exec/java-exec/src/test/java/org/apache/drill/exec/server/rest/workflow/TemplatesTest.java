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
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TemplatesTest {
  private static final ObjectMapper M = new ObjectMapper();

  private static RunContext ctx() {
    RunContext c = new RunContext();
    c.rowCount = 7;
    c.rows = List.of(Map.of("user", "a"), Map.of("user", "b"));
    c.actionOutputs.put("sum", Map.of("text", "all good"));
    return c;
  }

  @Test
  public void rendersPathsListsAndMissing() {
    assertEquals("7 rows: all good [a, b] ()",
        Templates.render("{{rowCount}} rows: {{ actions.sum.text }} [{{rows[*].user}}] ({{nope}})", ctx()));
  }

  @Test
  public void collectsReferencesAndActionIds() throws Exception {
    Workflow.ActionSpec a = new Workflow.ActionSpec();
    a.config = (ObjectNode) M.readTree("{\"message\":\"{{actions.sum.text}} {{rowCount}}\",\"to\":[\"{{actions.x.y}}\"]}");
    a.condition = Workflow.Condition.tree(ConditionParser.parse("actions.judge.verdict && rowCount > 1"));
    assertEquals(Set.of("actions.sum.text", "rowCount", "actions.x.y"), Templates.references(a.config));
    assertEquals(Set.of("sum", "x", "judge"), Templates.referencedActionIds(a));
  }

  @Test
  public void referencedActionIdsIgnoresIncompleteActionPath() throws Exception {
    Workflow.ActionSpec a = new Workflow.ActionSpec();
    a.config = (ObjectNode) M.readTree("{\"message\":\"{{actions.}} {{actions.ok.v}}\"}");
    assertEquals(Set.of("ok"), Templates.referencedActionIds(a));
  }

  @Test
  public void renderAllRendersNestedText() throws Exception {
    ObjectNode cfg = (ObjectNode) M.readTree("{\"subject\":\"n={{rowCount}}\",\"to\":[\"x@y\"],\"n\":3}");
    assertEquals(M.readTree("{\"subject\":\"n=7\",\"to\":[\"x@y\"],\"n\":3}"), Templates.renderAll(cfg, ctx()));
  }
}
