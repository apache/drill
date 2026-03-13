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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Normalized chat message used across all LLM providers.
 * Roles: "system", "user", "assistant", "tool".
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ChatMessage {

  @JsonProperty
  private String role;

  @JsonProperty
  private String content;

  @JsonProperty
  private List<ToolCall> toolCalls;

  @JsonProperty
  private String toolCallId;

  @JsonProperty
  private String name;

  public ChatMessage() {
  }

  @JsonCreator
  public ChatMessage(
      @JsonProperty("role") String role,
      @JsonProperty("content") String content,
      @JsonProperty("toolCalls") List<ToolCall> toolCalls,
      @JsonProperty("toolCallId") String toolCallId,
      @JsonProperty("name") String name) {
    this.role = role;
    this.content = content;
    this.toolCalls = toolCalls;
    this.toolCallId = toolCallId;
    this.name = name;
  }

  public static ChatMessage system(String content) {
    return new ChatMessage("system", content, null, null, null);
  }

  public static ChatMessage user(String content) {
    return new ChatMessage("user", content, null, null, null);
  }

  public static ChatMessage assistant(String content) {
    return new ChatMessage("assistant", content, null, null, null);
  }

  public static ChatMessage tool(String toolCallId, String name, String content) {
    return new ChatMessage("tool", content, null, toolCallId, name);
  }

  public String getRole() {
    return role;
  }

  public void setRole(String role) {
    this.role = role;
  }

  public String getContent() {
    return content;
  }

  public void setContent(String content) {
    this.content = content;
  }

  public List<ToolCall> getToolCalls() {
    return toolCalls;
  }

  public void setToolCalls(List<ToolCall> toolCalls) {
    this.toolCalls = toolCalls;
  }

  public String getToolCallId() {
    return toolCallId;
  }

  public void setToolCallId(String toolCallId) {
    this.toolCallId = toolCallId;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
