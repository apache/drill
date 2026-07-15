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
package org.apache.drill.exec.server.rest;

import jakarta.ws.rs.core.SecurityContext;
import org.apache.drill.exec.server.rest.ai.AiEvent;
import org.apache.drill.exec.server.rest.ai.LlmCallResult;
import org.apache.drill.exec.server.rest.ai.LlmConfig;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.security.Principal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the AI event built for each Prospector chat call.
 */
public class ProspectorEventTest {

  private static LlmConfig config() {
    LlmConfig config = new LlmConfig();
    config.setProvider("openai");
    config.setModel("gpt-4o");
    return config;
  }

  private static LlmCallResult result() {
    LlmCallResult result = new LlmCallResult();
    result.setPromptTokens(10);
    result.setResponseTokens(5);
    result.appendResponseText("hello");
    return result;
  }

  @Test
  public void testFeatureIsRecorded() {
    AiEvent event = ProspectorResources.buildEvent(
        config(), "alice", "dashboard_qna", "hi", "prompt", result(), null, 42L);
    assertEquals("dashboard_qna", event.feature);
    assertEquals("server", event.source);
    assertEquals("alice", event.user);
    assertEquals("openai", event.provider);
    assertEquals("gpt-4o", event.model);
    assertEquals(42L, event.durationMs);
  }

  @Test
  public void testFeatureDefaultsWhenAbsent() {
    AiEvent nullFeature = ProspectorResources.buildEvent(
        config(), "alice", null, "hi", "prompt", result(), null, 1L);
    assertEquals("prospector_chat", nullFeature.feature);

    AiEvent blankFeature = ProspectorResources.buildEvent(
        config(), "alice", "   ", "hi", "prompt", result(), null, 1L);
    assertEquals("prospector_chat", blankFeature.feature);
  }

  @Test
  public void testSuccessWhenResultPresent() {
    AiEvent event = ProspectorResources.buildEvent(
        config(), "alice", "sql_lab_chat", "hi", "prompt", result(), null, 1L);
    assertTrue(event.success);
    assertEquals(10, event.promptTokens);
    assertEquals(5, event.responseTokens);
    assertEquals("hello", event.response);
    assertNull(event.errorClass);
  }

  /**
   * An Error (not Exception) escapes the streaming catch block, leaving failure
   * null while finally still runs. Without a callResult this is not a success.
   */
  @Test
  public void testNotSuccessfulWhenResultMissingAndNoFailure() {
    AiEvent event = ProspectorResources.buildEvent(
        config(), "alice", "sql_lab_chat", "hi", "prompt", null, null, 1L);
    assertFalse(event.success);
    assertNull(event.totalTokens);
  }

  @Test
  public void testFailureIsRecorded() {
    AiEvent event = ProspectorResources.buildEvent(
        config(), "alice", "sql_lab_chat", "hi", "prompt", null,
        new IllegalStateException("boom"), 1L);
    assertFalse(event.success);
    assertEquals("IllegalStateException", event.errorClass);
    assertEquals("boom", event.error);
    assertFalse(event.cancelled);
  }

  @Test
  public void testClientCancellationIsFlagged() {
    AiEvent event = ProspectorResources.buildEvent(
        config(), "alice", "sql_lab_chat", "hi", "prompt", null,
        new IOException("Broken pipe"), 1L);
    assertTrue(event.cancelled);
    assertEquals("ClientCancelled", event.errorClass);
    assertFalse(event.success);
  }

  @Test
  public void testChatContextCarriesFeature() throws Exception {
    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();
    ProspectorResources.ChatContext ctx = mapper.readValue(
        "{\"feature\":\"executive_summary\"}", ProspectorResources.ChatContext.class);
    assertEquals("executive_summary", ctx.feature);
  }

  @Test
  public void testSetupFailureEvent() {
    AiEvent event = ProspectorResources.buildSetupFailureEvent(
        null, "alice", "sql_lab_chat", "ProspectorDisabled", "Prospector is not enabled");
    assertFalse(event.success);
    assertFalse(event.cancelled);
    assertEquals("sql_lab_chat", event.feature);
    assertEquals("ProspectorDisabled", event.errorClass);
    assertEquals("Prospector is not enabled", event.error);
    assertEquals("server", event.source);
    // A setup failure never reached the provider, so it burned no tokens.
    assertNull(event.totalTokens);
    assertEquals(0L, event.durationMs);
  }

  @Test
  public void testSetupFailureEventDefaultsFeature() {
    AiEvent event = ProspectorResources.buildSetupFailureEvent(
        null, "alice", null, "UnknownProvider", "Unknown LLM provider: bogus");
    assertEquals("prospector_chat", event.feature);
  }

  /**
   * When auth is disabled there is no SecurityContext (or no principal on it).
   * AiEvent's contract is "anonymous" in that case, never null — a null user
   * breaks the analytics dashboard's byUser grouping.
   */
  @Test
  public void testResolveUserFallsBackToAnonymousWhenNoPrincipal() {
    assertEquals("anonymous", ProspectorResources.resolveUser(null));

    SecurityContext noPrincipal = new SecurityContext() {
      @Override
      public Principal getUserPrincipal() {
        return null;
      }

      @Override
      public boolean isUserInRole(String role) {
        return false;
      }

      @Override
      public boolean isSecure() {
        return false;
      }

      @Override
      public String getAuthenticationScheme() {
        return null;
      }
    };
    assertEquals("anonymous", ProspectorResources.resolveUser(noPrincipal));
  }
}
