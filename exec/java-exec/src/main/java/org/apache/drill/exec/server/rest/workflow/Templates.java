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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** {@code {{path}}} substitution. No logic, no escaping: plain text in, plain text out. */
public final class Templates {
  static final Pattern REF = Pattern.compile("\\{\\{\\s*([A-Za-z_][A-Za-z0-9_.]*(?:\\[\\*\\][A-Za-z0-9_.]*)?)\\s*\\}\\}");

  private Templates() { }

  public static String render(String template, RunContext ctx) {
    if (template == null) {
      return "";
    }
    Matcher m = REF.matcher(template);
    StringBuilder sb = new StringBuilder();
    while (m.find()) {
      m.appendReplacement(sb, Matcher.quoteReplacement(display(ctx.resolve(m.group(1)))));
    }
    m.appendTail(sb);
    return sb.toString();
  }

  public static JsonNode renderAll(JsonNode node, RunContext ctx) {
    if (node == null) {
      return null;
    }
    if (node.isTextual()) {
      return TextNode.valueOf(render(node.asText(), ctx));
    }
    if (node.isObject()) {
      ObjectNode copy = ((ObjectNode) node).objectNode();
      Iterator<Map.Entry<String, JsonNode>> it = node.fields();
      while (it.hasNext()) {
        Map.Entry<String, JsonNode> e = it.next();
        copy.set(e.getKey(), renderAll(e.getValue(), ctx));
      }
      return copy;
    }
    if (node.isArray()) {
      ArrayNode copy = ((ArrayNode) node).arrayNode();
      node.forEach(child -> copy.add(renderAll(child, ctx)));
      return copy;
    }
    return node;
  }

  public static Set<String> references(JsonNode node) {
    Set<String> refs = new LinkedHashSet<>();
    collect(node, refs);
    return refs;
  }

  public static List<String> conditionFields(JsonNode tree) {
    List<String> fields = new ArrayList<>();
    collectFields(tree, fields);
    return fields;
  }

  public static Set<String> referencedActionIds(Workflow.ActionSpec action) {
    Set<String> paths = new LinkedHashSet<>(references(action.config));
    if (action.condition != null) {
      paths.addAll(conditionFields(action.condition.tree));
    }
    return paths.stream()
        .filter(p -> p.startsWith(RunContext.ACTIONS_PREFIX))
        .map(p -> p.split("\\."))
        .filter(parts -> parts.length > 1 && !parts[1].isEmpty())
        .map(parts -> parts[1])
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  private static void collect(JsonNode node, Set<String> refs) {
    if (node == null) {
      return;
    }
    if (node.isTextual()) {
      Matcher m = REF.matcher(node.asText());
      while (m.find()) {
        refs.add(m.group(1));
      }
    } else if (node.isContainerNode()) {
      node.forEach(child -> collect(child, refs));
    }
  }

  private static void collectFields(JsonNode node, List<String> fields) {
    if (node == null || node.isNull()) {
      return;
    }
    if (node.has("field")) {
      fields.add(node.get("field").asText());
    }
    for (String key : new String[] {"all", "any"}) {
      if (node.has(key)) {
        node.get(key).forEach(child -> collectFields(child, fields));
      }
    }
    if (node.has("not")) {
      collectFields(node.get("not"), fields);
    }
  }

  private static String display(Object value) {
    if (value == null || value == RunContext.MISSING) {
      return "";
    }
    if (value instanceof List) {
      return ((List<?>) value).stream().map(String::valueOf).collect(Collectors.joining(", "));
    }
    return String.valueOf(value);
  }
}
