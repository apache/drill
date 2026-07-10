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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link LlmProvider#normalizeEndpoint}.
 */
public class LlmProviderEndpointTest {

  private static final String DEFAULT = "https://api.example.com";

  @Test
  public void blankFallsBackToDefault() {
    assertEquals(DEFAULT, LlmProvider.normalizeEndpoint(null, DEFAULT));
    assertEquals(DEFAULT, LlmProvider.normalizeEndpoint("", DEFAULT));
  }

  @Test
  public void trailingSlashStripped() {
    assertEquals("http://host:8000", LlmProvider.normalizeEndpoint("http://host:8000/", DEFAULT));
  }

  @Test
  public void httpAndHttpsAllowed() {
    assertEquals("http://host", LlmProvider.normalizeEndpoint("http://host", DEFAULT));
    assertEquals("HTTPS://Host", LlmProvider.normalizeEndpoint("HTTPS://Host", DEFAULT));
  }

  @Test
  public void nonHttpSchemeRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> LlmProvider.normalizeEndpoint("file:///etc/passwd", DEFAULT));
    assertThrows(IllegalArgumentException.class,
        () -> LlmProvider.normalizeEndpoint("gopher://internal", DEFAULT));
  }

  @Test
  public void describeHttpErrorIncludesUrlStatusBody() {
    String d = LlmProvider.describeHttpError("https://host/v1", 401, "{\"error\":\"nope\"}");
    assertTrue(d.contains("https://host/v1"));
    assertTrue(d.contains("401"));
    assertTrue(d.contains("nope"));
  }

  @Test
  public void describeExceptionWalksCauseChain() {
    Throwable root = new java.security.cert.CertPathBuilderException("unable to find valid certification path");
    Throwable mid = new javax.net.ssl.SSLHandshakeException("PKIX path building failed");
    mid.initCause(root);
    String d = LlmProvider.describeException("https://llm.corp", mid);
    // The top message alone is useless; the root cause is the real reason.
    assertTrue(d.contains("https://llm.corp"));
    assertTrue(d.contains("PKIX path building failed"));
    assertTrue(d.contains("Caused by"));
    assertTrue(d.contains("unable to find valid certification path"));
  }
}
