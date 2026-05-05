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
package org.apache.drill.exec.server.rest.ai;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A single AI usage event written as one JSON line to ai-events.log.
 * Drill queries the log via the dfs.ai_logs workspace for analytics.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AiEvent {

  /** ISO-8601 UTC timestamp, e.g. 2026-05-01T12:34:56.789Z. */
  @JsonProperty("ts")
  public String ts;

  /** Authenticated username, or "anonymous" when auth is disabled. */
  @JsonProperty("user")
  public String user;

  /** Feature that issued the call: prospector_chat, transpile, query_suggestions, explain_query, optimize_query. */
  @JsonProperty("feature")
  public String feature;

  /** Where the event was captured: server (instrumentation) or client (browser observability). */
  @JsonProperty("source")
  public String source;

  @JsonProperty("provider")
  public String provider;

  @JsonProperty("model")
  public String model;

  @JsonProperty("promptTokens")
  public Integer promptTokens;

  @JsonProperty("responseTokens")
  public Integer responseTokens;

  @JsonProperty("totalTokens")
  public Integer totalTokens;

  @JsonProperty("durationMs")
  public Long durationMs;

  @JsonProperty("success")
  public Boolean success;

  /**
   * True when the call was aborted by the client (e.g. browser closed the SSE stream)
   * rather than failing upstream. Cancellations should be excluded from success-rate math
   * so legitimate user-aborts don't poison the dashboard.
   */
  @JsonProperty("cancelled")
  public Boolean cancelled;

  /** Short error class name when success is false. */
  @JsonProperty("errorClass")
  public String errorClass;

  /** Human-readable error message when success is false. */
  @JsonProperty("error")
  public String error;

  /** The user's actual message text (for "what are users asking" filtering). */
  @JsonProperty("userMessage")
  public String userMessage;

  /** Full assembled prompt sent to the LLM (system + messages + tools, truncated). */
  @JsonProperty("prompt")
  public String prompt;

  /** Response text from the LLM, truncated. */
  @JsonProperty("response")
  public String response;
}
