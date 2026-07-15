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

import org.apache.drill.exec.server.rest.ai.AiEvent;
import org.apache.drill.exec.server.rest.ai.LlmConfig;
import org.apache.drill.exec.server.rest.ai.ValidationResult;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the analytics event recorded by the admin "Test Connection" action.
 *
 * <p>The central hazard: providers report a failed test by <em>returning</em> an error
 * ValidationResult, not by throwing. An event built from "did anything throw?" records
 * every wrong API key as a success.
 */
public class AiConfigTestEventTest {

  private static LlmConfig config() {
    LlmConfig config = new LlmConfig();
    config.setProvider("openai");
    config.setModel("gpt-4o");
    return config;
  }

  @Test
  public void testSuccessfulValidationIsRecordedAsSuccess() {
    AiEvent event = AiConfigResources.buildConfigTestEvent(
        config(), "alice", ValidationResult.ok("Connection successful"), null, 120L);
    assertTrue(event.success);
    assertEquals("config_test", event.feature);
    assertEquals("server", event.source);
    assertEquals("alice", event.user);
    assertEquals("openai", event.provider);
    assertEquals("gpt-4o", event.model);
    assertEquals(120L, event.durationMs);
    assertNull(event.errorClass);
    assertNull(event.error);
    assertFalse(event.cancelled);
  }

  /**
   * The regression this class exists for: an admin pastes a wrong API key, the probe
   * gets HTTP 401 and returns an error result without throwing. The event must not
   * claim success.
   */
  @Test
  public void testFailedValidationIsRecordedAsFailure() {
    ValidationResult failed = ValidationResult.error(
        "API returned HTTP 401", "Incorrect API key provided");
    AiEvent event = AiConfigResources.buildConfigTestEvent(config(), "alice", failed, null, 90L);
    assertFalse(event.success);
    assertEquals("ValidationFailed", event.errorClass);
    assertEquals("API returned HTTP 401", event.error);
    assertEquals("config_test", event.feature);
    assertEquals(90L, event.durationMs);
  }

  @Test
  public void testThrownFailureIsRecordedWithExceptionClass() {
    AiEvent event = AiConfigResources.buildConfigTestEvent(
        config(), "alice", null, new IllegalStateException("boom"), 5L);
    assertFalse(event.success);
    assertEquals("IllegalStateException", event.errorClass);
    assertEquals("boom", event.error);
  }

  /**
   * An Error escapes catch(Exception) while finally still runs, leaving both the
   * result and the failure null. Absent a result, that is not a success.
   */
  @Test
  public void testMissingResultWithoutFailureIsNotSuccess() {
    AiEvent event = AiConfigResources.buildConfigTestEvent(config(), "alice", null, null, 5L);
    assertFalse(event.success);
  }

  /** A config test never reaches a chat completion, so it burns no tokens. */
  @Test
  public void testConfigTestEventCarriesNoTokens() {
    AiEvent event = AiConfigResources.buildConfigTestEvent(
        config(), "alice", ValidationResult.ok("ok"), null, 1L);
    assertNull(event.promptTokens);
    assertNull(event.responseTokens);
    assertNull(event.totalTokens);
  }

  /** The unknown-provider guard records a failed test rather than logging nothing. */
  @Test
  public void testUnknownProviderIsRecordedAsFailure() {
    LlmConfig bogus = new LlmConfig();
    bogus.setProvider("bogus");
    AiEvent event = AiConfigResources.buildConfigTestEvent(
        bogus, "alice", ValidationResult.error("Unknown provider: bogus"), null, 0L);
    assertFalse(event.success);
    assertEquals("bogus", event.provider);
    assertEquals("Unknown provider: bogus", event.error);
    assertEquals(0L, event.durationMs);
  }
}
