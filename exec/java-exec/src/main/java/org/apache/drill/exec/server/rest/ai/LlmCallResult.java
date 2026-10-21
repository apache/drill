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
 * Outcome of a streaming LLM call.
 * Token counts come from the upstream provider when available (OpenAI usage chunk,
 * Anthropic message_start + message_delta) and are null when the provider does not
 * report them (e.g. Ollama).
 */
public class LlmCallResult {

  private Integer promptTokens;
  private Integer responseTokens;
  private final StringBuilder responseText = new StringBuilder();

  public Integer getPromptTokens() {
    return promptTokens;
  }

  public void setPromptTokens(Integer promptTokens) {
    this.promptTokens = promptTokens;
  }

  public Integer getResponseTokens() {
    return responseTokens;
  }

  public void setResponseTokens(Integer responseTokens) {
    this.responseTokens = responseTokens;
  }

  public Integer getTotalTokens() {
    if (promptTokens == null && responseTokens == null) {
      return null;
    }
    return (promptTokens != null ? promptTokens : 0)
        + (responseTokens != null ? responseTokens : 0);
  }

  public void appendResponseText(String text) {
    if (text != null && !text.isEmpty()) {
      responseText.append(text);
    }
  }

  public String getResponseText() {
    return responseText.toString();
  }
}
