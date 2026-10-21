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

import java.util.Map;

/** Records an alert on the run (shown as a badge in run history). */
public class RecordAlertAction implements ActionType {
  private static final String SCHEMA = "{\"type\":\"object\",\"required\":[\"message\"],\"properties\":{"
      + "\"message\":{\"type\":\"string\",\"title\":\"Alert message\",\"x-template\":true}}}";

  @Override public String id() { return "record_alert"; }

  @Override public String displayName() { return "Record alert"; }

  @Override public String description() { return "Flag the run with an alert message in run history."; }

  @Override public ObjectNode configSchema() { return parse(SCHEMA); }

  @Override public Map<String, String> outputs() { return Map.of("message", "string"); }

  @Override public boolean hasSideEffects() { return false; }

  @Override
  public Map<String, Object> execute(ActionContext ctx, JsonNode config) {
    return Map.of("message", ctx.render(config.path("message").asText()));
  }

  static ObjectNode parse(String json) {
    try {
      return (ObjectNode) new ObjectMapper().readTree(json);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
