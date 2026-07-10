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

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end test of the OAuth2 gateway auth flow against a mock gateway: the OAuth2
 * client-credentials token fetch and the gateway headers on the chat call.
 */
public class OAuth2GatewayProviderTest {

  private MockWebServer server;

  @BeforeEach
  public void setUp() throws Exception {
    server = new MockWebServer();
    server.start();
  }

  @AfterEach
  public void tearDown() throws Exception {
    server.shutdown();
  }

  private LlmConfig config() {
    LlmConfig config = new LlmConfig();
    config.setProvider("oauth2");
    config.setModel("gpt-4o");
    config.setApiEndpoint(server.url("/v1").toString());
    config.setAuthUrl(server.url("/token").toString());
    config.setConsumerKey("ck");
    config.setConsumerSecret("cs");
    config.setClientId("cid");
    config.setUsecaseId("uc");
    config.setApiKey("apikey");
    return config;
  }

  @Test
  public void fetchesTokenThenSendsGatewayHeaders() throws Exception {
    server.enqueue(new MockResponse()
        .setResponseCode(200)
        .addHeader("Content-Type", "application/json")
        .setBody("{\"token_type\":\"Bearer\",\"access_token\":\"tok123\",\"expires_in\":3600}"));
    server.enqueue(new MockResponse()
        .setResponseCode(200)
        .addHeader("Content-Type", "text/event-stream")
        .setBody("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n"
            + "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"
            + "data: [DONE]\n\n"));

    OAuth2GatewayProvider provider = new OAuth2GatewayProvider();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    provider.streamChatCompletion(config(), List.of(ChatMessage.user("hi")),
        List.of(), out, null);

    // Token call: Basic auth with base64(consumerKey:consumerSecret), form grant.
    RecordedRequest tokenReq = server.takeRequest();
    assertEquals("/token", tokenReq.getPath());
    assertEquals("grant_type=client_credentials", tokenReq.getBody().readUtf8());
    String expectedBasic = "Basic "
        + Base64.getEncoder().encodeToString("ck:cs".getBytes(StandardCharsets.UTF_8));
    assertEquals(expectedBasic, tokenReq.getHeader("Authorization"));

    // Chat call: fetched bearer token + the x-wf-* gateway headers.
    RecordedRequest chatReq = server.takeRequest();
    assertTrue(chatReq.getPath().endsWith("/chat/completions"), chatReq.getPath());
    assertEquals("Bearer tok123", chatReq.getHeader("Authorization"));
    assertEquals("cid", chatReq.getHeader("client-id"));
    assertEquals("uc", chatReq.getHeader("usecase-id"));
    assertEquals("apikey", chatReq.getHeader("api-key"));
    assertNotNull(chatReq.getHeader("x-request-id"));
    assertNotNull(chatReq.getHeader("x-correlation-id"));
    // x-wf-request-date must match Python's datetime.now().isoformat(): microsecond, no zone.
    assertTrue(chatReq.getHeader("x-wf-request-date")
        .matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{6}"),
        chatReq.getHeader("x-wf-request-date"));

    assertTrue(out.toString(StandardCharsets.UTF_8).contains("hi"));
  }

  @Test
  public void honorsConfiguredHeaderNamesAndPlaceholders() throws Exception {
    server.enqueue(new MockResponse().setResponseCode(200)
        .addHeader("Content-Type", "application/json")
        .setBody("{\"access_token\":\"tok123\",\"expires_in\":3600}"));
    server.enqueue(new MockResponse().setResponseCode(200)
        .addHeader("Content-Type", "text/event-stream")
        .setBody("data: [DONE]\n\n"));

    LlmConfig config = config();
    // A different gateway: custom header names and a bare (non-Bearer) token scheme.
    Map<String, String> headers = new LinkedHashMap<>();
    headers.put("X-Api-Token", "{token}");
    headers.put("X-Trace", "{uuid}");
    headers.put("X-Model", "{model}");
    config.setGatewayHeaders(headers);

    OAuth2GatewayProvider provider = new OAuth2GatewayProvider();
    provider.streamChatCompletion(config, List.of(ChatMessage.user("hi")), List.of(),
        new ByteArrayOutputStream(), null);

    server.takeRequest(); // token call
    RecordedRequest chatReq = server.takeRequest();
    assertEquals("tok123", chatReq.getHeader("X-Api-Token"));
    assertEquals("gpt-4o", chatReq.getHeader("X-Model"));
    assertNotNull(chatReq.getHeader("X-Trace"));
    // The default headers must NOT be present when a custom mapping is set.
    assertEquals(null, chatReq.getHeader("api-key"));
    assertEquals(null, chatReq.getHeader("Authorization"));
  }

  @Test
  public void cachesTokenAcrossCalls() throws Exception {
    for (int i = 0; i < 2; i++) {
      // Only one token response enqueued; a second fetch would block/fail, proving the cache.
      if (i == 0) {
        server.enqueue(new MockResponse().setResponseCode(200)
            .addHeader("Content-Type", "application/json")
            .setBody("{\"access_token\":\"tok123\",\"expires_in\":3600}"));
      }
      server.enqueue(new MockResponse().setResponseCode(200)
          .addHeader("Content-Type", "text/event-stream")
          .setBody("data: [DONE]\n\n"));
    }

    OAuth2GatewayProvider provider = new OAuth2GatewayProvider();
    LlmConfig config = config();
    provider.streamChatCompletion(config, List.of(ChatMessage.user("a")), List.of(),
        new ByteArrayOutputStream(), null);
    provider.streamChatCompletion(config, List.of(ChatMessage.user("b")), List.of(),
        new ByteArrayOutputStream(), null);

    // 3 requests total: 1 token + 2 chat (token reused on the second call).
    assertEquals(3, server.getRequestCount());
  }
}
