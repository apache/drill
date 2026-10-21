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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Validates a workflow document and normalizes expression conditions into trees. */
public final class WorkflowValidator {
  private static final Set<String> FREQUENCIES = Set.of("hourly", "daily", "weekly", "monthly");
  private static final Pattern TIME = Pattern.compile("^([01]\\d|2[0-3]):[0-5]\\d$");
  private static final Set<String> OPS =
      Set.of("==", "!=", ">", ">=", "<", "<=", "contains", "startsWith");
  private static final int MAX_EXPR_LENGTH = 2000;
  private static final Pattern ACTION_ID = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");

  public static class ValidationError {
    public String path;
    public String message;

    public ValidationError() { }

    public ValidationError(String path, String message) {
      this.path = path;
      this.message = message;
    }
  }

  private WorkflowValidator() { }

  public static List<ValidationError> validate(Workflow wf, ActionTypeRegistry registry) {
    List<ValidationError> errors = new ArrayList<>();
    if (wf.name == null || wf.name.trim().isEmpty()) {
      errors.add(new ValidationError("name", "Name is required"));
    }
    if (wf.savedQueryId == null || wf.savedQueryId.isEmpty()) {
      errors.add(new ValidationError("savedQueryId", "Choose a saved query"));
    }
    validateSchedule(wf.schedule, errors);

    Map<String, ActionType> earlier = new HashMap<>();
    Set<String> seen = new HashSet<>();
    if (wf.actions == null) {
      errors.add(new ValidationError("actions", "Actions must be a list"));
      return errors;
    }
    for (int i = 0; i < wf.actions.size(); i++) {
      Workflow.ActionSpec a = wf.actions.get(i);
      String p = "actions[" + i + "]";
      if (a == null) {
        errors.add(new ValidationError(p, "Action must not be null"));
        continue;
      }
      if (a.id == null || !ACTION_ID.matcher(a.id).matches()) {
        errors.add(new ValidationError(p + ".id", "Use letters, digits and _; start with a letter"));
      } else if (!seen.add(a.id)) {
        errors.add(new ValidationError(p + ".id", "Duplicate action id '" + a.id + "'"));
      }
      ActionType type = registry.get(a.type);
      if (type == null) {
        errors.add(new ValidationError(p + ".type", "Unknown action type: " + a.type));
      } else {
        SchemaCheck.check(type.configSchema(), a.config, p + ".config", errors);
      }
      validateCondition(a, p + ".condition", earlier, errors);
      Set<String> bad = badRefs(Templates.references(a.config), earlier);
      if (!bad.isEmpty()) {
        errors.add(new ValidationError(p + ".config", "Unknown reference(s): " + String.join(", ", bad)));
      }
      if (a.id != null && type != null) {
        earlier.put(a.id, type);
      }
    }
    return errors;
  }

  private static void validateSchedule(Workflow.Schedule s, List<ValidationError> errors) {
    if (s == null) {
      errors.add(new ValidationError("schedule", "Schedule is required"));
      return;
    }
    if (!FREQUENCIES.contains(s.frequency)) {
      errors.add(new ValidationError("schedule.frequency", "Must be hourly, daily, weekly or monthly"));
    }
    if (s.timeOfDay == null || !TIME.matcher(s.timeOfDay).matches()) {
      errors.add(new ValidationError("schedule.timeOfDay", "Use HH:mm (24-hour)"));
    }
    if ("weekly".equals(s.frequency) && s.dayOfWeek == null) {
      errors.add(new ValidationError("schedule.dayOfWeek", "Required for weekly schedules"));
    }
    if ("monthly".equals(s.frequency) && s.dayOfMonth == null) {
      errors.add(new ValidationError("schedule.dayOfMonth", "Required for monthly schedules"));
    }
    if (s.dayOfWeek != null && (s.dayOfWeek < 0 || s.dayOfWeek > 6)) {
      errors.add(new ValidationError("schedule.dayOfWeek", "0 (Sunday) to 6 (Saturday)"));
    }
    if (s.dayOfMonth != null && (s.dayOfMonth < 1 || s.dayOfMonth > 28)) {
      errors.add(new ValidationError("schedule.dayOfMonth", "1 to 28"));
    }
    if (s.timeoutSeconds < 1 || s.timeoutSeconds > 86_400) {
      errors.add(new ValidationError("schedule.timeoutSeconds", "1 to 86400 seconds"));
    }
  }

  private static void validateCondition(Workflow.ActionSpec a, String p, Map<String, ActionType> earlier,
      List<ValidationError> errors) {
    Workflow.Condition c = a.condition;
    if (c == null) {
      a.condition = Workflow.Condition.always();
      return;
    }
    if (c.expr != null) {
      if (c.expr.length() > MAX_EXPR_LENGTH) {
        errors.add(new ValidationError(p, "Expression is too long (max " + MAX_EXPR_LENGTH + " characters)"));
        return;
      }
      try {
        c.tree = ConditionParser.parse(c.expr);
        c.always = null;
      } catch (ConditionParser.ParseException e) {
        errors.add(new ValidationError(p, e.getMessage()));
        return;
      }
    }
    if (!Boolean.TRUE.equals(c.always) && c.tree == null) {
      errors.add(new ValidationError(p, "Condition needs always, tree or expr"));
      return;
    }
    if (c.tree != null) {
      String problem = treeProblem(c.tree);
      if (problem != null) {
        errors.add(new ValidationError(p, "Invalid condition tree: " + problem));
        return;
      }
    }
    Set<String> bad = badRefs(new LinkedHashSet<>(Templates.conditionFields(c.tree)), earlier);
    if (!bad.isEmpty()) {
      errors.add(new ValidationError(p, "Unknown field(s): " + String.join(", ", bad)));
    }
  }

  /** Returns a description of the first structural problem in a condition tree, or null. */
  private static String treeProblem(JsonNode n) {
    return treeProblem(n, 0);
  }

  private static String treeProblem(JsonNode n, int depth) {
    if (n == null || !n.isObject()) {
      return "node must be an object";
    }
    if (depth > ConditionParser.MAX_DEPTH) {
      return "nested too deeply";
    }
    for (String key : new String[] {"all", "any"}) {
      if (n.has(key)) {
        JsonNode arr = n.get(key);
        if (!arr.isArray() || arr.isEmpty()) {
          return "'" + key + "' must be a non-empty array";
        }
        for (JsonNode child : arr) {
          String problem = treeProblem(child, depth + 1);
          if (problem != null) {
            return problem;
          }
        }
        return null;
      }
    }
    if (n.has("not")) {
      return treeProblem(n.get("not"), depth + 1);
    }
    if (!n.path("field").isTextual() || n.get("field").asText().trim().isEmpty()) {
      return "condition needs a field";
    }
    String op = n.path("op").asText("");
    if (!OPS.contains(op)) {
      return "unknown operator '" + op + "'";
    }
    if (!n.has("value")) {
      return "condition needs a value";
    }
    if (n.get("value").isNull() && !op.equals("==") && !op.equals("!=")) {
      return "null value only allowed with == or !=";
    }
    return null;
  }

  private static Set<String> badRefs(Set<String> refs, Map<String, ActionType> earlier) {
    Set<String> bad = new LinkedHashSet<>();
    for (String ref : refs) {
      if (RunContext.BASE_FIELDS.contains(ref) || ref.startsWith(RunContext.ROWS_PREFIX)) {
        continue;
      }
      String[] parts = ref.split("\\.", 3);
      boolean ok = ref.startsWith(RunContext.ACTIONS_PREFIX) && parts.length == 3
          && earlier.containsKey(parts[1]) && earlier.get(parts[1]).outputs().containsKey(parts[2]);
      if (!ok) {
        bad.add(ref);
      }
    }
    return bad;
  }
}
