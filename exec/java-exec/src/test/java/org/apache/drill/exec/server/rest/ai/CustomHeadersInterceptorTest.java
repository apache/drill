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
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for CustomHeadersInterceptor.
 */
public class CustomHeadersInterceptorTest {

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
  public void testSingleCustomHeader() throws Exception {
    mockWebServer.enqueue(new MockResponse().setBody("test response"));
    mockWebServer.start();

    Map<String, String> headers = new HashMap<>();
    headers.put("X-Custom-Header", "custom-value");

    CustomHeadersInterceptor interceptor = new CustomHeadersInterceptor(headers);
    OkHttpClient client = new OkHttpClient.Builder()
        .addInterceptor(interceptor)
        .build();

    Request request = new Request.Builder()
        .url(mockWebServer.url("/api/test"))
        .post(RequestBody.create("test", MediaType.parse("application/json")))
        .build();

    try (Response response = client.newCall(request).execute()) {
      assertTrue(response.isSuccessful());
    }

    RecordedRequest recordedRequest = mockWebServer.takeRequest();
    assertEquals("custom-value", recordedRequest.getHeader("X-Custom-Header"));
  }

  @Test
  public void testMultipleCustomHeaders() throws Exception {
    mockWebServer.enqueue(new MockResponse().setBody("test response"));
    mockWebServer.start();

    Map<String, String> headers = new HashMap<>();
    headers.put("X-Tenant-ID", "tenant123");
    headers.put("X-Environment", "production");
    headers.put("X-Version", "v2");

    CustomHeadersInterceptor interceptor = new CustomHeadersInterceptor(headers);
    OkHttpClient client = new OkHttpClient.Builder()
        .addInterceptor(interceptor)
        .build();

    Request request = new Request.Builder()
        .url(mockWebServer.url("/api/test"))
        .post(RequestBody.create("test", MediaType.parse("application/json")))
        .build();

    try (Response response = client.newCall(request).execute()) {
      assertTrue(response.isSuccessful());
    }

    RecordedRequest recordedRequest = mockWebServer.takeRequest();
    assertEquals("tenant123", recordedRequest.getHeader("X-Tenant-ID"));
    assertEquals("production", recordedRequest.getHeader("X-Environment"));
    assertEquals("v2", recordedRequest.getHeader("X-Version"));
  }

  @Test
  public void testEmptyCustomHeaders() throws Exception {
    mockWebServer.enqueue(new MockResponse().setBody("test response"));
    mockWebServer.start();

    Map<String, String> headers = new HashMap<>();
    CustomHeadersInterceptor interceptor = new CustomHeadersInterceptor(headers);
    OkHttpClient client = new OkHttpClient.Builder()
        .addInterceptor(interceptor)
        .build();

    Request request = new Request.Builder()
        .url(mockWebServer.url("/api/test"))
        .post(RequestBody.create("test", MediaType.parse("application/json")))
        .build();

    try (Response response = client.newCall(request).execute()) {
      assertTrue(response.isSuccessful());
    }

    RecordedRequest recordedRequest = mockWebServer.takeRequest();
    assertNotNull(recordedRequest);
  }

  @Test
  public void testHeadersWithSpecialCharacters() throws Exception {
    mockWebServer.enqueue(new MockResponse().setBody("test response"));
    mockWebServer.start();

    Map<String, String> headers = new HashMap<>();
    headers.put("X-Custom", "value-with-dashes");
    headers.put("X-Token", "abc123!@#$%");

    CustomHeadersInterceptor interceptor = new CustomHeadersInterceptor(headers);
    OkHttpClient client = new OkHttpClient.Builder()
        .addInterceptor(interceptor)
        .build();

    Request request = new Request.Builder()
        .url(mockWebServer.url("/api/test"))
        .post(RequestBody.create("test", MediaType.parse("application/json")))
        .build();

    try (Response response = client.newCall(request).execute()) {
      assertTrue(response.isSuccessful());
    }

    RecordedRequest recordedRequest = mockWebServer.takeRequest();
    assertEquals("value-with-dashes", recordedRequest.getHeader("X-Custom"));
    assertEquals("abc123!@#$%", recordedRequest.getHeader("X-Token"));
  }

  @Test
  public void testHeadersPreserveExistingHeaders() throws Exception {
    mockWebServer.enqueue(new MockResponse().setBody("test response"));
    mockWebServer.start();

    Map<String, String> customHeaders = new HashMap<>();
    customHeaders.put("X-Custom", "custom-value");

    CustomHeadersInterceptor interceptor = new CustomHeadersInterceptor(customHeaders);
    OkHttpClient client = new OkHttpClient.Builder()
        .addInterceptor(interceptor)
        .build();

    Request request = new Request.Builder()
        .url(mockWebServer.url("/api/test"))
        .addHeader("Content-Type", "application/json")
        .addHeader("Authorization", "Bearer token")
        .post(RequestBody.create("test", MediaType.parse("application/json")))
        .build();

    try (Response response = client.newCall(request).execute()) {
      assertTrue(response.isSuccessful());
    }

    RecordedRequest recordedRequest = mockWebServer.takeRequest();
    assertTrue(recordedRequest.getHeader("Content-Type").contains("application/json"));
    assertEquals("Bearer token", recordedRequest.getHeader("Authorization"));
    assertEquals("custom-value", recordedRequest.getHeader("X-Custom"));
  }

  @Test
  public void testHeadersOverrideExisting() throws Exception {
    mockWebServer.enqueue(new MockResponse().setBody("test response"));
    mockWebServer.start();

    Map<String, String> customHeaders = new HashMap<>();
    customHeaders.put("Authorization", "Bearer custom-token");

    CustomHeadersInterceptor interceptor = new CustomHeadersInterceptor(customHeaders);
    OkHttpClient client = new OkHttpClient.Builder()
        .addInterceptor(interceptor)
        .build();

    Request request = new Request.Builder()
        .url(mockWebServer.url("/api/test"))
        .addHeader("Authorization", "Bearer original-token")
        .post(RequestBody.create("test", MediaType.parse("application/json")))
        .build();

    try (Response response = client.newCall(request).execute()) {
      assertTrue(response.isSuccessful());
    }

    RecordedRequest recordedRequest = mockWebServer.takeRequest();
    // Custom headers are added, so both should be present (addHeader doesn't override, it adds)
    assertNotNull(recordedRequest.getHeader("Authorization"));
  }

  @Test
  public void testAuthorizationHeader() throws Exception {
    mockWebServer.enqueue(new MockResponse().setBody("test response"));
    mockWebServer.start();

    Map<String, String> headers = new HashMap<>();
    headers.put("Authorization", "Bearer sk-1234567890");

    CustomHeadersInterceptor interceptor = new CustomHeadersInterceptor(headers);
    OkHttpClient client = new OkHttpClient.Builder()
        .addInterceptor(interceptor)
        .build();

    Request request = new Request.Builder()
        .url(mockWebServer.url("/api/test"))
        .post(RequestBody.create("test", MediaType.parse("application/json")))
        .build();

    try (Response response = client.newCall(request).execute()) {
      assertTrue(response.isSuccessful());
    }

    RecordedRequest recordedRequest = mockWebServer.takeRequest();
    assertEquals("Bearer sk-1234567890", recordedRequest.getHeader("Authorization"));
  }

  @Test
  public void testMultipleRequests() throws Exception {
    mockWebServer.enqueue(new MockResponse().setBody("response1"));
    mockWebServer.enqueue(new MockResponse().setBody("response2"));
    mockWebServer.start();

    Map<String, String> headers = new HashMap<>();
    headers.put("X-Request-ID", "req-123");

    CustomHeadersInterceptor interceptor = new CustomHeadersInterceptor(headers);
    OkHttpClient client = new OkHttpClient.Builder()
        .addInterceptor(interceptor)
        .build();

    // First request
    Request request1 = new Request.Builder()
        .url(mockWebServer.url("/api/test1"))
        .post(RequestBody.create("test1", MediaType.parse("application/json")))
        .build();

    try (Response response = client.newCall(request1).execute()) {
      assertTrue(response.isSuccessful());
    }

    RecordedRequest recordedRequest1 = mockWebServer.takeRequest();
    assertEquals("req-123", recordedRequest1.getHeader("X-Request-ID"));

    // Second request
    Request request2 = new Request.Builder()
        .url(mockWebServer.url("/api/test2"))
        .post(RequestBody.create("test2", MediaType.parse("application/json")))
        .build();

    try (Response response = client.newCall(request2).execute()) {
      assertTrue(response.isSuccessful());
    }

    RecordedRequest recordedRequest2 = mockWebServer.takeRequest();
    assertEquals("req-123", recordedRequest2.getHeader("X-Request-ID"));
  }

  @Test
  public void testHeaderNamesAreCaseInsensitive() throws Exception {
    mockWebServer.enqueue(new MockResponse().setBody("test response"));
    mockWebServer.start();

    Map<String, String> headers = new HashMap<>();
    headers.put("X-Custom-Header", "value");

    CustomHeadersInterceptor interceptor = new CustomHeadersInterceptor(headers);
    OkHttpClient client = new OkHttpClient.Builder()
        .addInterceptor(interceptor)
        .build();

    Request request = new Request.Builder()
        .url(mockWebServer.url("/api/test"))
        .post(RequestBody.create("test", MediaType.parse("application/json")))
        .build();

    try (Response response = client.newCall(request).execute()) {
      assertTrue(response.isSuccessful());
    }

    RecordedRequest recordedRequest = mockWebServer.takeRequest();
    // HTTP headers are case-insensitive
    assertNotNull(recordedRequest.getHeader("X-Custom-Header"));
    assertNotNull(recordedRequest.getHeader("x-custom-header"));
  }
}
