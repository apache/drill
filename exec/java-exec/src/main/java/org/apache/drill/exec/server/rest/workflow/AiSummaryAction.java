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
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Map;

/** Asks the configured Prospector model to summarise the run in plain English. */
public class AiSummaryAction implements ActionType {
  static final int DEFAULT_MAX_CHARS = 2000;
  static final int MAX_ROWS = 20;

  private static final String SCHEMA = "{\"type\":\"object\",\"properties\":{"
      + "\"instructions\":{\"type\":\"string\",\"title\":\"Instructions\",\"x-template\":true,\"x-multiline\":true},"
      + "\"maxChars\":{\"type\":\"integer\",\"title\":\"Maximum characters\",\"default\":" + DEFAULT_MAX_CHARS + "}}}";

  @Override public String id() { return "ai_summary"; }

  @Override public String displayName() { return "AI summary"; }

  @Override public String description() { return "Ask Prospector to summarise the run in plain English."; }

  @Override public ObjectNode configSchema() { return RecordAlertAction.parse(SCHEMA); }

  @Override public Map<String, String> outputs() { return Map.of("summary", "string"); }

  /** Calls a paid external provider, so dry runs preview instead of executing. */
  @Override public boolean hasSideEffects() { return true; }

  @Override
  public Map<String, Object> execute(ActionContext ctx, JsonNode config) throws Exception {
    int maxChars = config.path("maxChars").asInt(DEFAULT_MAX_CHARS);
    String instructions = ctx.render(config.path("instructions").asText(""));
    String prompt = buildPrompt(instructions, ctx.run(), ctx.services().sendDataToAi());
    return Map.of("summary", ctx.services().summarize(prompt, maxChars));
  }

  /**
   * Builds the model prompt. Columns and row values are included only when the administrator
   * allows query data to be sent to the model; otherwise only aggregate facts appear.
   */
  static String buildPrompt(String instructions, RunContext run, boolean sendData) {
    StringBuilder sb = new StringBuilder();
    sb.append("You are summarising the result of a scheduled Apache Drill query for a report. "
        + "Write a short plain-English summary.\n");
    if (!instructions.isBlank()) {
      sb.append("Instructions: ").append(instructions).append('\n');
    }
    sb.append("status: ").append(run.status).append('\n');
    sb.append("rowCount: ").append(run.rowCount).append('\n');
    sb.append("durationMs: ").append(run.durationMs).append('\n');
    if (sendData) {
      sb.append("columns: ").append(String.join(", ", run.columns)).append('\n');
      int n = Math.min(MAX_ROWS, run.rows.size());
      sb.append("first ").append(n).append(" rows:\n");
      for (int i = 0; i < n; i++) {
        Map<String, String> row = run.rows.get(i);
        StringBuilder line = new StringBuilder();
        for (String c : run.columns) {
          if (line.length() > 0) {
            line.append(" | ");
          }
          line.append(row.get(c));
        }
        sb.append(line).append('\n');
      }
    }
    return sb.toString();
  }
}
