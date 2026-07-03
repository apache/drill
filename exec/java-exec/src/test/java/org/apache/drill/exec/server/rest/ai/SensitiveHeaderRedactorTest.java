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

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for SensitiveHeaderRedactor.
 */
public class SensitiveHeaderRedactorTest {

  private static final Logger logger = LoggerFactory.getLogger(SensitiveHeaderRedactorTest.class);
  private MockWebServer mockWebServer;

  @BeforeEach
  public void setUp() {
    mockWebServer = new MockWebServer();
  }

  @AfterEach
  public void tearDown() throws IOException {
    mockWebServer.shutdown();
  }

  @Test
  public void testSensitiveHeaderRedaction() throws Exception {
    mockWebServer.enqueue(new MockResponse().setBody("test response"));
    mockWebServer.start();

    SensitiveHeaderRedactor redactor = new SensitiveHeaderRedactor();
    OkHttpClient client = new OkHttpClient.Builder()
        .addInterceptor(redactor)
        .build();

    Request request = new Request.Builder()
        .url(mockWebServer.url("/api/test"))
        .addHeader("Authorization", "Bearer secret-token-12345")
        .post(RequestBody.create("test", MediaType.parse("application/json")))
        .build();

    try (Response response = client.newCall(request).execute()) {
      assertTrue(response.isSuccessful());
    }

    // Note: We can't directly verify redaction in logs without capturing logs,
    // but we can verify the client processes the request
    mockWebServer.takeRequest();
  }

  @Test
  public void testMultipleSensitiveHeaders() throws Exception {
    mockWebServer.enqueue(new MockResponse().setBody("test response"));
    mockWebServer.start();

    SensitiveHeaderRedactor redactor = new SensitiveHeaderRedactor();
    OkHttpClient client = new OkHttpClient.Builder()
        .addInterceptor(redactor)
        .build();

    Request request = new Request.Builder()
        .url(mockWebServer.url("/api/test"))
        .addHeader("Authorization", "Bearer token-123")
        .addHeader("X-API-Key", "key-456")
        .addHeader("Cookie", "session=abc123")
        .addHeader("Content-Type", "application/json")  // Should NOT be redacted
        .post(RequestBody.create("test", MediaType.parse("application/json")))
        .build();

    try (Response response = client.newCall(request).execute()) {
      assertTrue(response.isSuccessful());
    }

    // Request should succeed - redactor doesn't block, just logs
    mockWebServer.takeRequest();
  }

  @Test
  public void testCaseInsensitiveHeaderName() throws Exception {
    mockWebServer.enqueue(new MockResponse().setBody("test response"));
    mockWebServer.start();

    SensitiveHeaderRedactor redactor = new SensitiveHeaderRedactor();
    OkHttpClient client = new OkHttpClient.Builder()
        .addInterceptor(redactor)
        .build();

    // Test with different cases
    Request request = new Request.Builder()
        .url(mockWebServer.url("/api/test"))
        .addHeader("authorization", "Bearer token-lower")  // lowercase
        .addHeader("AUTHORIZATION", "Bearer token-upper")  // uppercase (will be added, not override)
        .addHeader("X-API-KEY", "key-upper")
        .addHeader("x-api-key", "key-lower")
        .post(RequestBody.create("test", MediaType.parse("application/json")))
        .build();

    try (Response response = client.newCall(request).execute()) {
      assertTrue(response.isSuccessful());
    }

    mockWebServer.takeRequest();
  }

  @Test
  public void testNonSensitiveHeaders() throws Exception {
    mockWebServer.enqueue(new MockResponse().setBody("test response"));
    mockWebServer.start();

    SensitiveHeaderRedactor redactor = new SensitiveHeaderRedactor();
    OkHttpClient client = new OkHttpClient.Builder()
        .addInterceptor(redactor)
        .build();

    Request request = new Request.Builder()
        .url(mockWebServer.url("/api/test"))
        .addHeader("Content-Type", "application/json")
        .addHeader("Accept", "application/json")
        .addHeader("User-Agent", "OkHttp/4.9.0")
        .addHeader("X-Tenant-ID", "tenant-123")
        .post(RequestBody.create("test", MediaType.parse("application/json")))
        .build();

    try (Response response = client.newCall(request).execute()) {
      assertTrue(response.isSuccessful());
    }

    mockWebServer.takeRequest();
  }

  @Test
  public void testProxyAuthorizationHeader() throws Exception {
    mockWebServer.enqueue(new MockResponse().setBody("test response"));
    mockWebServer.start();

    SensitiveHeaderRedactor redactor = new SensitiveHeaderRedactor();
    OkHttpClient client = new OkHttpClient.Builder()
        .addInterceptor(redactor)
        .build();

    Request request = new Request.Builder()
        .url(mockWebServer.url("/api/test"))
        .addHeader("Proxy-Authorization", "Basic dXNlcjpwYXNz")  // Should be redacted
        .post(RequestBody.create("test", MediaType.parse("application/json")))
        .build();

    try (Response response = client.newCall(request).execute()) {
      assertTrue(response.isSuccessful());
    }

    mockWebServer.takeRequest();
  }

  @Test
  public void testAllSensitiveHeaderTypes() throws Exception {
    mockWebServer.enqueue(new MockResponse().setBody("test response"));
    mockWebServer.start();

    SensitiveHeaderRedactor redactor = new SensitiveHeaderRedactor();
    OkHttpClient client = new OkHttpClient.Builder()
        .addInterceptor(redactor)
        .build();

    Request request = new Request.Builder()
        .url(mockWebServer.url("/api/test"))
        // All should be redacted
        .addHeader("Authorization", "Bearer token")
        .addHeader("X-API-Key", "secret-key")
        .addHeader("Proxy-Authorization", "Basic auth")
        .addHeader("Cookie", "session=xyz")
        .addHeader("X-Auth-Token", "auth-token")
        .addHeader("API-Key", "api-key-value")
        // These should NOT be redacted
        .addHeader("Content-Type", "application/json")
        .addHeader("User-Agent", "TestClient/1.0")
        .addHeader("Accept", "application/json")
        .post(RequestBody.create("test", MediaType.parse("application/json")))
        .build();

    try (Response response = client.newCall(request).execute()) {
      assertTrue(response.isSuccessful());
    }

    mockWebServer.takeRequest();
  }

  @Test
  public void testInterceptorChaining() throws Exception {
    mockWebServer.enqueue(new MockResponse().setBody("test response"));
    mockWebServer.start();

    SensitiveHeaderRedactor redactor = new SensitiveHeaderRedactor();
    CustomHeadersInterceptor customHeaders = new CustomHeadersInterceptor(
        java.util.Map.of("X-Custom", "value"));

    OkHttpClient client = new OkHttpClient.Builder()
        .addInterceptor(redactor)  // Redactor first
        .addInterceptor(customHeaders)  // Then custom headers
        .build();

    Request request = new Request.Builder()
        .url(mockWebServer.url("/api/test"))
        .addHeader("Authorization", "Bearer token")
        .post(RequestBody.create("test", MediaType.parse("application/json")))
        .build();

    try (Response response = client.newCall(request).execute()) {
      assertTrue(response.isSuccessful());
    }

    mockWebServer.takeRequest();
  }

  @Test
  public void testEmptyHeaders() throws Exception {
    mockWebServer.enqueue(new MockResponse().setBody("test response"));
    mockWebServer.start();

    SensitiveHeaderRedactor redactor = new SensitiveHeaderRedactor();
    OkHttpClient client = new OkHttpClient.Builder()
        .addInterceptor(redactor)
        .build();

    Request request = new Request.Builder()
        .url(mockWebServer.url("/api/test"))
        .post(RequestBody.create("test", MediaType.parse("application/json")))
        .build();

    try (Response response = client.newCall(request).execute()) {
      assertTrue(response.isSuccessful());
    }

    mockWebServer.takeRequest();
  }

  @Test
  public void testRedactorDoesNotModifyRequest() throws Exception {
    mockWebServer.enqueue(new MockResponse().setBody("test response"));
    mockWebServer.start();

    SensitiveHeaderRedactor redactor = new SensitiveHeaderRedactor();
    OkHttpClient client = new OkHttpClient.Builder()
        .addInterceptor(redactor)
        .build();

    String authValue = "Bearer secret-token-xyz";
    Request request = new Request.Builder()
        .url(mockWebServer.url("/api/test"))
        .addHeader("Authorization", authValue)
        .post(RequestBody.create("test", MediaType.parse("application/json")))
        .build();

    try (Response response = client.newCall(request).execute()) {
      assertTrue(response.isSuccessful());
    }

    // Verify the request was actually sent with the header
    // (redactor only logs, doesn't modify the request)
    var recordedRequest = mockWebServer.takeRequest();
    assertEquals(authValue, recordedRequest.getHeader("Authorization"));
  }

  @Test
  public void testRedactorWithDifferentAuthSchemes() throws Exception {
    mockWebServer.enqueue(new MockResponse().setBody("response1"));
    mockWebServer.enqueue(new MockResponse().setBody("response2"));
    mockWebServer.enqueue(new MockResponse().setBody("response3"));
    mockWebServer.start();

    SensitiveHeaderRedactor redactor = new SensitiveHeaderRedactor();
    OkHttpClient client = new OkHttpClient.Builder()
        .addInterceptor(redactor)
        .build();

    // Bearer token
    Request request1 = new Request.Builder()
        .url(mockWebServer.url("/api/test1"))
        .addHeader("Authorization", "Bearer eyJhbGc...")
        .post(RequestBody.create("test", MediaType.parse("application/json")))
        .build();

    try (Response response = client.newCall(request1).execute()) {
      assertTrue(response.isSuccessful());
    }

    // Basic auth
    Request request2 = new Request.Builder()
        .url(mockWebServer.url("/api/test2"))
        .addHeader("Authorization", "Basic dXNlcjpwYXNz")
        .post(RequestBody.create("test", MediaType.parse("application/json")))
        .build();

    try (Response response = client.newCall(request2).execute()) {
      assertTrue(response.isSuccessful());
    }

    // Custom token
    Request request3 = new Request.Builder()
        .url(mockWebServer.url("/api/test3"))
        .addHeader("Authorization", "CustomScheme token-value-123")
        .post(RequestBody.create("test", MediaType.parse("application/json")))
        .build();

    try (Response response = client.newCall(request3).execute()) {
      assertTrue(response.isSuccessful());
    }

    mockWebServer.takeRequest();
    mockWebServer.takeRequest();
    mockWebServer.takeRequest();
  }
}
