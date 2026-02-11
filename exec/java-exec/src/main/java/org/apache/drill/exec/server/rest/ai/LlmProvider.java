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
import java.util.List;

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
   * @param config   the LLM configuration (endpoint, key, model, etc.)
   * @param messages the conversation messages
   * @param tools    tool definitions (may be empty)
   * @param out      the output stream to write SSE events to
   * @throws Exception if streaming fails
   */
  void streamChatCompletion(LlmConfig config, List<ChatMessage> messages,
      List<ToolDefinition> tools, OutputStream out) throws Exception;

  /**
   * Validate the given configuration for this provider.
   *
   * @param config the configuration to validate
   * @return validation result
   */
  ValidationResult validateConfig(LlmConfig config);
}
