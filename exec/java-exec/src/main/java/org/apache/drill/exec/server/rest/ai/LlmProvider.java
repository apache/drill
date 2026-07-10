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

import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Locale;

/**
 * Interface for LLM providers that can stream chat completions.
 * Each provider normalizes its vendor-specific SSE format to
 * a common wire format consumed by the frontend.
 */
public interface LlmProvider {

  /**
   * @return unique identifier for this provider (e.g. "openai", "anthropic")
   */
  String getId();

  /**
   * @return human-readable display name
   */
  String getDisplayName();

  /**
   * Stream a chat completion to the given output stream using normalized SSE events.
   *
   * @param config         the LLM configuration (endpoint, key, model, etc.)
   * @param messages       the conversation messages
   * @param tools          tool definitions (may be empty)
   * @param out            the output stream to write SSE events to
   * @param usageObserver  receives token-usage updates as they arrive from upstream;
   *                       use UsageObserver.NOOP if you don't need them
   * @return summary of the call (token usage, response text) for analytics
   * @throws Exception if streaming fails
   */
  LlmCallResult streamChatCompletion(LlmConfig config, List<ChatMessage> messages,
      List<ToolDefinition> tools, OutputStream out, UsageObserver usageObserver) throws Exception;

  /**
   * Validate the given configuration for this provider.
   *
   * @param config the configuration to validate
   * @return validation result
   */
  ValidationResult validateConfig(LlmConfig config);

  /**
   * Normalize an admin-configured endpoint: fall back to {@code defaultEndpoint} when
   * blank, strip a trailing slash, and reject non-http(s) schemes. The scheme guard is
   * defense-in-depth against a mistyped or hostile value (e.g. {@code file://}) being
   * used to build the outbound request URL.
   *
   * @param endpoint        the configured endpoint (may be null/empty)
   * @param defaultEndpoint the provider default to use when blank
   * @return the normalized endpoint, without a trailing slash
   * @throws IllegalArgumentException if the endpoint is not an http(s) URL
   */
  static String normalizeEndpoint(String endpoint, String defaultEndpoint) {
    if (endpoint == null || endpoint.isEmpty()) {
      endpoint = defaultEndpoint;
    }
    if (endpoint.endsWith("/")) {
      endpoint = endpoint.substring(0, endpoint.length() - 1);
    }
    String lower = endpoint.toLowerCase(Locale.ROOT);
    if (!lower.startsWith("http://") && !lower.startsWith("https://")) {
      throw new IllegalArgumentException("AI endpoint must be an http(s) URL: " + endpoint);
    }
    return endpoint;
  }

  /**
   * Build a diagnostic string for a non-2xx HTTP response from a provider probe:
   * the URL hit, the status code, and the full response body. Intended for the
   * {@code details} field of a failed {@link ValidationResult} and for server logs.
   */
  static String describeHttpError(String url, int statusCode, String body) {
    return "URL: " + url + "\nHTTP status: " + statusCode + "\nResponse body:\n"
        + (body == null || body.isEmpty() ? "(empty)" : body);
  }

  /**
   * Build a diagnostic string for an exception thrown while reaching a provider,
   * walking the full cause chain. For enterprise failures the real reason (SSL PKIX
   * path, connection refused, unknown host) is usually a nested cause, not the
   * top-level message, so the whole chain is included.
   */
  static String describeException(String url, Throwable t) {
    StringBuilder sb = new StringBuilder("URL: ").append(url).append('\n');
    String prefix = "";
    // ponytail: cap at 10 to avoid a pathological self-referential cause cycle
    Throwable cur = t;
    for (int i = 0; cur != null && i < 10; i++, cur = cur.getCause()) {
      sb.append(prefix).append(cur.getClass().getName());
      if (cur.getMessage() != null) {
        sb.append(": ").append(cur.getMessage());
      }
      sb.append('\n');
      prefix = "Caused by: ";
    }
    // The concise chain above is the TL;DR; the full stack trace (with frames) follows for
    // deeper debugging. Safe to include here: this endpoint is admin-only.
    StringWriter trace = new StringWriter();
    t.printStackTrace(new PrintWriter(trace));
    sb.append('\n').append("Stack trace:\n").append(trace.toString().trim());
    return sb.toString().trim();
  }
}
