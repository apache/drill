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

/**
 * Callback an LLM provider invokes whenever it learns more about token usage
 * during a streaming call. Anthropic emits incrementally (input on
 * message_start, running output on each message_delta); OpenAI emits once at
 * the end of the stream when stream_options.include_usage is set; Ollama may
 * not emit at all.
 *
 * <p>responseTokens may be null on the first call when only the prompt count
 * has been seen.
 */
@FunctionalInterface
public interface UsageObserver {
  void onUsage(Integer promptTokens, Integer responseTokens);

  /** A no-op observer used by callers that don't need usage callbacks. */
  UsageObserver NOOP = (in, out) -> { };
}
