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

import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Validates action configs against the small JSON Schema subset action types declare.
 * ponytail: hand-rolled subset (type/required/properties/enum/items/minItems/pattern/email);
 * swap for a real JSON Schema library if action types need more.
 */
public final class SchemaCheck {
  private static final Pattern EMAIL = Pattern.compile("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$");

  private SchemaCheck() { }

  public static void check(JsonNode schema, JsonNode value, String path,
      List<WorkflowValidator.ValidationError> errors) {
    if (schema == null || schema.isEmpty()) {
      return;
    }
    String type = schema.path("type").asText("");
    if (value == null || value.isMissingNode() || value.isNull()) {
      value = type.equals("object") ? com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.objectNode() : null;
      if (value == null) {
        return;
      }
    }
    if (!typeMatches(type, value)) {
      errors.add(new WorkflowValidator.ValidationError(path, "Expected " + type));
      return;
    }
    if (schema.has("enum")) {
      boolean ok = false;
      for (JsonNode allowed : schema.get("enum")) {
        ok |= allowed.equals(value);
      }
      if (!ok) {
        errors.add(new WorkflowValidator.ValidationError(path, "Must be one of " + schema.get("enum")));
      }
    }
    if (type.equals("string")) {
      if (schema.has("pattern") && !Pattern.compile(schema.get("pattern").asText()).matcher(value.asText()).find()) {
        errors.add(new WorkflowValidator.ValidationError(path, "Invalid format"));
      }
      if ("email".equals(schema.path("format").asText()) && !EMAIL.matcher(value.asText()).matches()) {
        errors.add(new WorkflowValidator.ValidationError(path, "Invalid email address"));
      }
    }
    if (type.equals("array")) {
      if (schema.has("minItems") && value.size() < schema.get("minItems").asInt()) {
        errors.add(new WorkflowValidator.ValidationError(path, "At least " + schema.get("minItems").asInt() + " required"));
      }
      for (int i = 0; i < value.size(); i++) {
        check(schema.get("items"), value.get(i), path + "[" + i + "]", errors);
      }
    }
    if (type.equals("object")) {
      JsonNode props = schema.path("properties");
      for (JsonNode req : schema.path("required")) {
        JsonNode v = value.get(req.asText());
        if (v == null || v.isNull() || (v.isTextual() && v.asText().trim().isEmpty())) {
          errors.add(new WorkflowValidator.ValidationError(path + "." + req.asText(), "This field is required"));
        }
      }
      Iterator<String> names = value.fieldNames();
      while (names.hasNext()) {
        String name = names.next();
        if (!props.has(name)) {
          errors.add(new WorkflowValidator.ValidationError(path + "." + name, "Unknown field"));
        } else {
          check(props.get(name), value.get(name), path + "." + name, errors);
        }
      }
    }
  }

  private static boolean typeMatches(String type, JsonNode v) {
    switch (type) {
      case "object": return v.isObject();
      case "string": return v.isTextual();
      case "number": return v.isNumber();
      case "integer": return v.isIntegralNumber();
      case "boolean": return v.isBoolean();
      case "array": return v.isArray();
      default: return true;
    }
  }
}
