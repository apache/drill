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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ActionPipelineTest {
  private static final ObjectMapper M = new ObjectMapper();
  private final ExecutorService executor = Executors.newCachedThreadPool();
  private final List<String> calls = new ArrayList<>();

  /** Records calls; config {"fail":true} throws, {"sleepMs":n} sleeps, {"out":"x"} returns {"value":"x"}. */
  private class Recording implements ActionType {
    private final String id;
    private final boolean sideEffects;

    Recording(String id, boolean sideEffects) {
      this.id = id;
      this.sideEffects = sideEffects;
    }

    @Override public String id() { return id; }
    @Override public String displayName() { return id; }
    @Override public String description() { return id; }
    @Override public ObjectNode configSchema() { return M.createObjectNode(); }
    @Override public Map<String, String> outputs() { return Map.of("value", "string"); }
    @Override public boolean hasSideEffects() { return sideEffects; }
    @Override public Map<String, Object> execute(ActionContext ctx, JsonNode config) throws Exception {
      calls.add(id + ":" + ctx.render(config.path("msg").asText("")));
      if (config.path("fail").asBoolean()) {
        throw new IllegalStateException("boom");
      }
      if (config.has("sleepMs")) {
        Thread.sleep(config.get("sleepMs").asLong());
      }
      return Map.of("value", config.path("out").asText(""));
    }
  }

  private ActionPipeline pipeline() {
    return new ActionPipeline(new ActionTypeRegistry(List.of(
        new Recording("pure", false), new Recording("effect", true))), executor, 200);
  }

  private static Workflow.ActionSpec action(String id, String type, String expr, String config) throws Exception {
    Workflow.ActionSpec a = new Workflow.ActionSpec();
    a.id = id;
    a.type = type;
    a.condition = expr == null ? Workflow.Condition.always() : Workflow.Condition.tree(ConditionParser.parse(expr));
    a.config = (ObjectNode) M.readTree(config);
    return a;
  }

  private static RunContext ctx() {
    RunContext c = new RunContext();
    c.status = "success";
    c.rowCount = 10;
    return c;
  }

  @AfterEach
  public void shutdown() {
    executor.shutdownNow();
  }

  @Test
  public void runsInOrderAndChainsOutputs() throws Exception {
    Workflow wf = new Workflow();
    wf.actions.add(action("a", "pure", null, "{\"out\":\"hello\"}"));
    wf.actions.add(action("b", "effect", "actions.a.value == 'hello'", "{\"msg\":\"got {{actions.a.value}}\"}"));
    List<ActionOutcome> out = pipeline().run(wf, "select 1", ctx(), null, false);
    assertEquals(List.of("pure:", "effect:got hello"), calls);
    assertEquals(ActionOutcome.FIRED, out.get(1).outcome);
  }

  @Test
  public void falseConditionSkipsAndDependentsAreSkipped() throws Exception {
    Workflow wf = new Workflow();
    wf.actions.add(action("a", "pure", "rowCount > 100", "{}"));
    wf.actions.add(action("b", "pure", null, "{\"msg\":\"{{actions.a.value}}\"}"));
    wf.actions.add(action("c", "pure", null, "{}"));
    List<ActionOutcome> out = pipeline().run(wf, "select 1", ctx(), null, false);
    assertEquals("condition false", out.get(0).reason);
    assertEquals(ActionOutcome.SKIPPED, out.get(1).outcome);
    assertEquals("upstream a skipped", out.get(1).reason);
    assertEquals(ActionOutcome.FIRED, out.get(2).outcome);
    assertEquals(List.of("pure:"), calls);
  }

  @Test
  public void errorIsIsolatedAndPropagatesAsSkip() throws Exception {
    Workflow wf = new Workflow();
    wf.actions.add(action("a", "pure", null, "{\"fail\":true}"));
    wf.actions.add(action("b", "pure", "actions.a.value == 'x'", "{}"));
    wf.actions.add(action("c", "pure", null, "{}"));
    List<ActionOutcome> out = pipeline().run(wf, "select 1", ctx(), null, false);
    assertEquals(ActionOutcome.ERRORED, out.get(0).outcome);
    assertEquals("boom", out.get(0).error);
    assertEquals("upstream a errored", out.get(1).reason);
    assertEquals(ActionOutcome.FIRED, out.get(2).outcome);
  }

  @Test
  public void timeoutErrors() throws Exception {
    Workflow wf = new Workflow();
    wf.actions.add(action("slow", "pure", null, "{\"sleepMs\":2000}"));
    List<ActionOutcome> out = pipeline().run(wf, "select 1", ctx(), null, false);
    assertEquals(ActionOutcome.ERRORED, out.get(0).outcome);
    assertTrue(out.get(0).error.contains("timed out"), out.get(0).error);
  }

  @Test
  public void actionTypeCanExtendItsOwnTimeout() throws Exception {
    ActionType patient = new Recording("patient", false) {
      @Override public long timeoutMs(Workflow wf, long configuredMs) { return 5000; }
    };
    Workflow wf = new Workflow();
    wf.actions.add(action("slow", "patient", null, "{\"sleepMs\":500,\"out\":\"done\"}"));
    List<ActionOutcome> out = new ActionPipeline(new ActionTypeRegistry(List.of(patient)), executor, 200)
        .run(wf, "select 1", ctx(), null, false);
    assertEquals(ActionOutcome.FIRED, out.get(0).outcome, out.get(0).error);
  }

  @Test
  public void dryRunPreviewsSideEffectsButExecutesPureActions() throws Exception {
    Workflow wf = new Workflow();
    wf.actions.add(action("a", "pure", null, "{\"out\":\"v\"}"));
    wf.actions.add(action("b", "effect", null, "{\"msg\":\"n={{rowCount}}\"}"));
    List<ActionOutcome> out = pipeline().run(wf, "select 1", ctx(), null, true);
    assertEquals(List.of("pure:"), calls);
    assertEquals(M.readTree("{\"msg\":\"n=10\"}"), out.get(1).outputs.get("wouldSend"));
  }

  @Test
  public void unknownTypeErrors() throws Exception {
    Workflow wf = new Workflow();
    wf.actions.add(action("x", "nope", null, "{}"));
    assertEquals("Unknown action type: nope", pipeline().run(wf, "s", ctx(), null, false).get(0).error);
  }

  @Test
  public void interruptStopsRemainingActions() throws Exception {
    Workflow wf = new Workflow();
    wf.actions.add(action("slow", "pure", null, "{\"sleepMs\":2000}"));
    wf.actions.add(action("mail", "effect", null, "{}"));
    Thread caller = Thread.currentThread();
    executor.submit(() -> {
      Thread.sleep(50);
      caller.interrupt();
      return null;
    });
    List<ActionOutcome> out = new ActionPipeline(new ActionTypeRegistry(List.of(
        new Recording("pure", false), new Recording("effect", true))), executor, 5000)
        .run(wf, "select 1", ctx(), null, false);
    assertTrue(Thread.interrupted());
    assertEquals(ActionOutcome.SKIPPED, out.get(1).outcome);
    assertEquals("interrupted", out.get(1).reason);
    assertEquals(List.of("pure:"), calls);
  }
}
