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

import java.util.List;

/** Evaluates a condition tree against a {@link RunContext}. */
public final class ConditionEvaluator {

  private ConditionEvaluator() { }

  public static boolean evaluate(Workflow.Condition condition, RunContext ctx) {
    if (condition == null || Boolean.TRUE.equals(condition.always) || condition.tree == null) {
      return true;
    }
    return evaluate(condition.tree, ctx);
  }

  public static boolean evaluate(JsonNode node, RunContext ctx) {
    if (node == null || node.isNull()) {
      return true;
    }
    if (node.has("all")) {
      for (JsonNode child : node.get("all")) {
        if (!evaluate(child, ctx)) {
          return false;
        }
      }
      return true;
    }
    if (node.has("any")) {
      for (JsonNode child : node.get("any")) {
        if (evaluate(child, ctx)) {
          return true;
        }
      }
      return false;
    }
    if (node.has("not")) {
      return !evaluate(node.get("not"), ctx);
    }
    String op = node.path("op").asText("==");
    Object actual = ctx.resolve(node.path("field").asText());
    Object expected = toJava(node.get("value"));
    if (actual instanceof List) {
      for (Object v : (List<?>) actual) {
        if (compare(v, op, expected)) {
          return true;
        }
      }
      return false;
    }
    return compare(actual == RunContext.MISSING ? null : actual, op, expected);
  }

  static boolean compare(Object a, String op, Object b) {
    if (a == null || b == null) {
      boolean bothNull = a == null && b == null;
      switch (op) {
        case "==": return bothNull;
        case "!=": return !bothNull;
        case ">": case ">=": case "<": case "<=": case "contains": case "startsWith": return false;
        default: throw new IllegalArgumentException("Unknown operator: " + op);
      }
    }
    Double an = number(a);
    Double bn = number(b);
    boolean numeric = an != null && bn != null;
    switch (op) {
      case "==": return numeric ? an.doubleValue() == bn.doubleValue() : a.toString().equals(b.toString());
      case "!=": return !compare(a, "==", b);
      case ">": return numeric && an > bn;
      case ">=": return numeric && an >= bn;
      case "<": return numeric && an < bn;
      case "<=": return numeric && an <= bn;
      case "contains": return a.toString().contains(b.toString());
      case "startsWith": return a.toString().startsWith(b.toString());
      default: throw new IllegalArgumentException("Unknown operator: " + op);
    }
  }

  private static Double number(Object o) {
    if (o instanceof Number) {
      return ((Number) o).doubleValue();
    }
    if (o instanceof String) {
      try {
        return Double.parseDouble(((String) o).trim());
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  private static Object toJava(JsonNode v) {
    if (v == null || v.isNull()) {
      return null;
    }
    if (v.isBoolean()) {
      return v.booleanValue();
    }
    if (v.isNumber()) {
      return v.numberValue();
    }
    return v.asText();
  }
}
