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

/** A kind of step a workflow can run after its query. */
public interface ActionType {
  String id();

  String displayName();

  String description();

  /** JSON Schema subset understood by {@link SchemaCheck}; also drives the editor form. */
  ObjectNode configSchema();

  /** Output name to "string" | "number" | "boolean". */
  Map<String, String> outputs();

  /** True if executing touches anything outside the run (tables, mail, HTTP). Dry runs call {@link #preview}. */
  boolean hasSideEffects();

  Map<String, Object> execute(ActionContext ctx, JsonNode config) throws Exception;

  /** How long one execution may take before the pipeline cancels it; most actions use the configured default. */
  default long timeoutMs(Workflow wf, long configuredMs) {
    return configuredMs;
  }

  default Map<String, Object> preview(ActionContext ctx, JsonNode config) {
    return Map.of("wouldSend", Templates.renderAll(config, ctx.run()));
  }
}
