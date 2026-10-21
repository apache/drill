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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Runs a workflow's actions in list order. An action is skipped when an action it
 * references did not fire or when its condition is false; an action that throws or
 * times out is recorded as errored and never stops the actions after it.
 */
public class ActionPipeline {
  private final ActionTypeRegistry registry;
  private final ExecutorService executor;
  private final long actionTimeoutMs;

  public ActionPipeline(ActionTypeRegistry registry, ExecutorService executor, long actionTimeoutMs) {
    this.registry = registry;
    this.executor = executor;
    this.actionTimeoutMs = actionTimeoutMs;
  }

  public List<ActionOutcome> run(Workflow wf, String sql, RunContext ctx, WorkflowServices services, boolean dryRun) {
    Map<String, String> outcomeById = new HashMap<>();
    List<ActionOutcome> results = new ArrayList<>();
    for (Workflow.ActionSpec action : wf.actions) {
      long start = System.currentTimeMillis();
      ActionOutcome o = new ActionOutcome(action.id, action.type);
      String blocker = firstUnfired(action, outcomeById);
      if (Thread.currentThread().isInterrupted()) {
        o.outcome = ActionOutcome.SKIPPED;
        o.reason = "interrupted";
      } else if (blocker != null) {
        o.outcome = ActionOutcome.SKIPPED;
        o.reason = "upstream " + blocker + " " + outcomeById.getOrDefault(blocker, "missing");
      } else {
        runOne(wf, sql, ctx, services, dryRun, action, o);
      }
      o.durationMs = System.currentTimeMillis() - start;
      outcomeById.put(action.id, o.outcome);
      results.add(o);
    }
    return results;
  }

  private void runOne(Workflow wf, String sql, RunContext ctx, WorkflowServices services, boolean dryRun,
      Workflow.ActionSpec action, ActionOutcome o) {
    ActionType type = registry.get(action.type);
    if (type == null) {
      o.outcome = ActionOutcome.ERRORED;
      o.error = "Unknown action type: " + action.type;
      return;
    }
    try {
      if (!ConditionEvaluator.evaluate(action.condition, ctx)) {
        o.outcome = ActionOutcome.SKIPPED;
        o.reason = "condition false";
        return;
      }
      ActionContext actx = new ActionContext(wf, sql, ctx, services);
      Map<String, Object> outputs = dryRun && type.hasSideEffects()
          ? type.preview(actx, action.config)
          : withTimeout(type, actx, action);
      o.outcome = ActionOutcome.FIRED;
      o.outputs = outputs;
      ctx.actionOutputs.put(action.id, outputs);
    } catch (Exception e) {
      o.outcome = ActionOutcome.ERRORED;
      o.error = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
    }
  }

  private Map<String, Object> withTimeout(ActionType type, ActionContext actx, Workflow.ActionSpec action)
      throws Exception {
    long timeoutMs = type.timeoutMs(actx.workflow(), actionTimeoutMs);
    Future<Map<String, Object>> f = executor.submit(() -> type.execute(actx, action.config));
    try {
      return f.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      f.cancel(true);
      throw new TimeoutException("Action timed out after " + timeoutMs + " ms");
    } catch (InterruptedException e) {
      f.cancel(true);
      Thread.currentThread().interrupt();
      throw e;
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      throw cause instanceof Exception ? (Exception) cause : e;
    }
  }

  private static String firstUnfired(Workflow.ActionSpec action, Map<String, String> outcomeById) {
    for (String id : Templates.referencedActionIds(action)) {
      if (!ActionOutcome.FIRED.equals(outcomeById.get(id))) {
        return id;
      }
    }
    return null;
  }
}
