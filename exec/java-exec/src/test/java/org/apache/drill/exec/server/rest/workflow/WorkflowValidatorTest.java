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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WorkflowValidatorTest {
  private static final ObjectMapper M = new ObjectMapper();

  private static ActionType type(String id, String schema, Map<String, String> outputs) {
    return new ActionType() {
      @Override public String id() { return id; }
      @Override public String displayName() { return id; }
      @Override public String description() { return id; }
      @Override public ObjectNode configSchema() {
        try {
          return (ObjectNode) M.readTree(schema);
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }
      @Override public Map<String, String> outputs() { return outputs; }
      @Override public boolean hasSideEffects() { return false; }
      @Override public Map<String, Object> execute(ActionContext c, JsonNode cfg) { return Map.of(); }
    };
  }

  private static final ActionTypeRegistry REG = new ActionTypeRegistry(List.of(
      type("note", "{\"type\":\"object\",\"required\":[\"message\"],\"properties\":{"
          + "\"message\":{\"type\":\"string\"},"
          + "\"to\":{\"type\":\"array\",\"minItems\":1,\"items\":{\"type\":\"string\",\"format\":\"email\"}},"
          + "\"location\":{\"type\":\"string\",\"pattern\":\"^[A-Za-z0-9_]+(\\\\.[A-Za-z0-9_]+)?$\"},"
          + "\"mode\":{\"type\":\"string\",\"enum\":[\"a\",\"b\"]}}}", Map.of("message", "string"))));

  private static Workflow valid() throws Exception {
    Workflow wf = new Workflow();
    wf.name = "W";
    wf.savedQueryId = "q1";
    wf.actions.add(action("first", "note", "{\"message\":\"hi\"}"));
    return wf;
  }

  private static Workflow.ActionSpec action(String id, String type, String config) throws Exception {
    Workflow.ActionSpec a = new Workflow.ActionSpec();
    a.id = id;
    a.type = type;
    a.config = (ObjectNode) M.readTree(config);
    return a;
  }

  private static Map<String, String> errors(Workflow wf) {
    return WorkflowValidator.validate(wf, REG).stream()
        .collect(Collectors.toMap(e -> e.path, e -> e.message, (a, b) -> a + "; " + b));
  }

  @Test
  public void validWorkflowHasNoErrors() throws Exception {
    assertEquals(Map.of(), errors(valid()));
  }

  @Test
  public void requiredTopLevelAndSchedule() throws Exception {
    Workflow wf = valid();
    wf.name = " ";
    wf.savedQueryId = null;
    wf.schedule.frequency = "yearly";
    wf.schedule.timeOfDay = "25:00";
    wf.schedule.dayOfMonth = 31;
    Map<String, String> e = errors(wf);
    assertTrue(e.containsKey("name"));
    assertTrue(e.containsKey("savedQueryId"));
    assertTrue(e.containsKey("schedule.frequency"));
    assertTrue(e.containsKey("schedule.timeOfDay"));
    assertTrue(e.containsKey("schedule.dayOfMonth"));
  }

  @Test
  public void actionIdTypeAndConfig() throws Exception {
    Workflow wf = valid();
    wf.actions.add(action("first", "note", "{\"message\":\"x\"}"));
    wf.actions.add(action("bad id", "nope", "{}"));
    wf.actions.add(action("c", "note", "{\"to\":[\"not-an-email\"],\"location\":\"dfs.tmp; DROP TABLE x\",\"mode\":\"z\",\"extra\":1}"));
    Map<String, String> e = errors(wf);
    assertTrue(e.get("actions[1].id").contains("Duplicate"));
    assertTrue(e.containsKey("actions[2].id"));
    assertTrue(e.get("actions[2].type").contains("Unknown action type"));
    assertTrue(e.get("actions[3].config.message").contains("required"));
    assertTrue(e.containsKey("actions[3].config.to[0]"));
    assertTrue(e.containsKey("actions[3].config.location"));
    assertTrue(e.containsKey("actions[3].config.mode"));
    assertTrue(e.get("actions[3].config.extra").contains("Unknown"));
  }

  @Test
  public void expressionIsParsedIntoTree() throws Exception {
    Workflow wf = valid();
    wf.actions.get(0).condition = new Workflow.Condition();
    wf.actions.get(0).condition.expr = "rowCount > 5";
    assertEquals(Map.of(), errors(wf));
    assertNotNull(wf.actions.get(0).condition.tree);
  }

  @Test
  public void badExpressionReportsPosition() throws Exception {
    Workflow wf = valid();
    wf.actions.get(0).condition = new Workflow.Condition();
    wf.actions.get(0).condition.expr = "rowCount >";
    assertTrue(errors(wf).get("actions[0].condition").contains("position 10"));
  }

  @Test
  public void referencesMustPointBackwardsAtDeclaredOutputs() throws Exception {
    Workflow wf = valid();
    wf.actions.add(action("second", "note", "{\"message\":\"{{actions.first.message}} {{actions.first.nope}}\"}"));
    wf.actions.get(0).config = (ObjectNode) M.readTree("{\"message\":\"{{actions.second.message}} {{rowcount}}\"}");
    Map<String, String> e = errors(wf);
    assertTrue(e.get("actions[0].config").contains("actions.second.message"));
    assertTrue(e.get("actions[0].config").contains("rowcount"));
    assertTrue(e.get("actions[1].config").contains("actions.first.nope"));
    assertTrue(!e.get("actions[1].config").contains("actions.first.message"));
  }

  @Test
  public void conditionUnknownFieldRejected() throws Exception {
    Workflow wf = valid();
    wf.actions.get(0).condition = new Workflow.Condition();
    wf.actions.get(0).condition.expr = "rowcount > 5";
    assertTrue(errors(wf).get("actions[0].condition").contains("rowcount"));
  }

  @Test
  public void overlongExpressionRejectedBeforeParsing() throws Exception {
    Workflow wf = valid();
    wf.actions.get(0).condition = new Workflow.Condition();
    wf.actions.get(0).condition.expr = "(".repeat(5000);
    assertTrue(errors(wf).get("actions[0].condition").contains("too long"));
  }

  private static String conditionError(Workflow wf, String treeJson) throws Exception {
    wf.actions.get(0).condition = new Workflow.Condition();
    wf.actions.get(0).condition.tree = M.readTree(treeJson);
    return errors(wf).get("actions[0].condition");
  }

  @Test
  public void malformedTreesRejected() throws Exception {
    assertTrue(conditionError(valid(), "{}").contains("field"));
    assertTrue(conditionError(valid(), "{\"field\":\"rowCount\",\"op\":\">\"}").contains("value"));
    assertTrue(conditionError(valid(), "{\"field\":\"rowCount\",\"op\":\"~\",\"value\":1}").contains("operator"));
    assertTrue(conditionError(valid(), "{\"all\":[]}").contains("non-empty"));
    assertTrue(conditionError(valid(), "{\"field\":\"rowCount\",\"op\":\">\",\"value\":null}").contains("null"));
    assertEquals(null, conditionError(valid(), "{\"all\":[{\"field\":\"rowCount\",\"op\":\">\",\"value\":1}]}"));
  }

  @Test
  public void expressionLengthBoundary() throws Exception {
    Workflow wf = valid();
    wf.actions.get(0).condition = new Workflow.Condition();
    String base = "rowCount > 5";
    wf.actions.get(0).condition.expr = base + " ".repeat(2000 - base.length());
    assertEquals(Map.of(), errors(wf));
    wf.actions.get(0).condition.expr = base + " ".repeat(2001 - base.length());
    assertTrue(errors(wf).get("actions[0].condition").contains("too long"));
  }

  @Test
  public void nullActionsAndNullElementGivePathedErrors() throws Exception {
    Workflow wf = valid();
    wf.actions = null;
    assertTrue(errors(wf).containsKey("actions"));
    wf = valid();
    wf.actions.add(null);
    assertTrue(errors(wf).containsKey("actions[1]"));
  }

  @Test
  public void weeklyAndMonthlyNeedTheirDay() throws Exception {
    Workflow wf = valid();
    wf.schedule.frequency = "weekly";
    assertTrue(errors(wf).containsKey("schedule.dayOfWeek"));
    wf.schedule.frequency = "monthly";
    assertTrue(errors(wf).containsKey("schedule.dayOfMonth"));
  }
}
