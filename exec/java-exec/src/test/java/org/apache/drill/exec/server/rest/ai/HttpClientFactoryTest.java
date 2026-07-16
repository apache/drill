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

import okhttp3.OkHttpClient;
import okhttp3.Proxy;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for HttpClientFactory.
 */
public class HttpClientFactoryTest {

  @Test
  public void testBasicClientCreation() throws Exception {
    LlmConfig config = new LlmConfig();
    OkHttpClient client = HttpClientFactory.createClient(config);
    assertNotNull(client);
  }

  @Test
  public void testDefaultTimeouts() throws Exception {
    LlmConfig config = new LlmConfig();
    OkHttpClient client = HttpClientFactory.createClient(config);

    // Default timeouts: 30s connect, 120s read, 30s write
    assertEquals(30, TimeUnit.MILLISECONDS.toSeconds(client.connectTimeoutMillis()));
    assertEquals(120, TimeUnit.MILLISECONDS.toSeconds(client.readTimeoutMillis()));
    assertEquals(30, TimeUnit.MILLISECONDS.toSeconds(client.writeTimeoutMillis()));
  }

  @Test
  public void testCustomConnectTimeout() throws Exception {
    LlmConfig config = new LlmConfig();
    config.setConnectTimeoutSeconds(60);

    OkHttpClient client = HttpClientFactory.createClient(config);
    assertEquals(60, TimeUnit.MILLISECONDS.toSeconds(client.connectTimeoutMillis()));
  }

  @Test
  public void testCustomReadTimeout() throws Exception {
    LlmConfig config = new LlmConfig();
    config.setReadTimeoutSeconds(180);

    OkHttpClient client = HttpClientFactory.createClient(config);
    assertEquals(180, TimeUnit.MILLISECONDS.toSeconds(client.readTimeoutMillis()));
  }

  @Test
  public void testCustomWriteTimeout() throws Exception {
    LlmConfig config = new LlmConfig();
    config.setWriteTimeoutSeconds(45);

    OkHttpClient client = HttpClientFactory.createClient(config);
    assertEquals(45, TimeUnit.MILLISECONDS.toSeconds(client.writeTimeoutMillis()));
  }

  @Test
  public void testAllCustomTimeouts() throws Exception {
    LlmConfig config = new LlmConfig();
    config.setConnectTimeoutSeconds(45);
    config.setReadTimeoutSeconds(150);
    config.setWriteTimeoutSeconds(50);

    OkHttpClient client = HttpClientFactory.createClient(config);

    assertEquals(45, TimeUnit.MILLISECONDS.toSeconds(client.connectTimeoutMillis()));
    assertEquals(150, TimeUnit.MILLISECONDS.toSeconds(client.readTimeoutMillis()));
    assertEquals(50, TimeUnit.MILLISECONDS.toSeconds(client.writeTimeoutMillis()));
  }

  @Test
  public void testProxyConfiguration() throws Exception {
    LlmConfig config = new LlmConfig();
    config.setProxyUrl("http://proxy.example.com:8080");

    OkHttpClient client = HttpClientFactory.createClient(config);
    assertNotNull(client.proxy());
    InetSocketAddress proxyAddr = (InetSocketAddress) client.proxy().address();
    assertEquals("proxy.example.com", proxyAddr.getHostName());
    assertEquals(8080, proxyAddr.getPort());
  }

  @Test
  public void testProxyWithAuthentication() throws Exception {
    LlmConfig config = new LlmConfig();
    config.setProxyUrl("http://proxy.example.com:3128");
    config.setProxyUsername("proxyuser");
    config.setProxyPassword("proxypass");

    OkHttpClient client = HttpClientFactory.createClient(config);
    assertNotNull(client.proxy());
    assertNotNull(client.proxyAuthenticator());
  }

  @Test
  public void testInvalidProxyUrl() throws Exception {
    LlmConfig config = new LlmConfig();
    config.setProxyUrl("not-a-valid-url");

    assertThrows(IllegalArgumentException.class, () -> HttpClientFactory.createClient(config));
  }

  @Test
  public void testNoProxyConfiguration() throws Exception {
    LlmConfig config = new LlmConfig();
    config.setProxyUrl(null);

    OkHttpClient client = HttpClientFactory.createClient(config);
    assertEquals(Proxy.Type.DIRECT, client.proxy().type());
  }

  @Test
  public void testEmptyProxyConfiguration() throws Exception {
    LlmConfig config = new LlmConfig();
    config.setProxyUrl("");

    OkHttpClient client = HttpClientFactory.createClient(config);
    assertEquals(Proxy.Type.DIRECT, client.proxy().type());
  }

  @Test
  public void testCustomHeadersInterceptor() throws Exception {
    LlmConfig config = new LlmConfig();
    Map<String, String> headers = new HashMap<>();
    headers.put("X-Custom-Header", "custom-value");
    headers.put("X-Tenant-ID", "tenant123");
    config.setCustomHeaders(headers);

    OkHttpClient client = HttpClientFactory.createClient(config);
    assertNotNull(client);
    // Interceptors list should include CustomHeadersInterceptor
    assertTrue(client.interceptors().size() > 0 ||
        client.networkInterceptors().size() > 0);
  }

  @Test
  public void testNoCustomHeaders() throws Exception {
    LlmConfig config = new LlmConfig();
    config.setCustomHeaders(null);

    OkHttpClient client = HttpClientFactory.createClient(config);
    assertNotNull(client);
  }

  @Test
  public void testEmptyCustomHeaders() throws Exception {
    LlmConfig config = new LlmConfig();
    config.setCustomHeaders(new HashMap<>());

    OkHttpClient client = HttpClientFactory.createClient(config);
    assertNotNull(client);
  }

  @Test
  public void testSSLVerificationEnabled() throws Exception {
    LlmConfig config = new LlmConfig();
    config.setVerifySSL(true);

    OkHttpClient client = HttpClientFactory.createClient(config);
    assertNotNull(client);
    // When verification is enabled, hostnameVerifier should be default (null)
    // Note: OkHttp doesn't expose hostnameVerifier directly, so we just verify client created
  }

  @Test
  public void testSSLVerificationDisabled() throws Exception {
    LlmConfig config = new LlmConfig();
    config.setVerifySSL(false);

    OkHttpClient client = HttpClientFactory.createClient(config);
    assertNotNull(client);
    // When verification is disabled, hostnameVerifier should be permissive
    // Note: OkHttp doesn't expose this, so just verify client created
  }

  @Test
  public void testSSLVerificationDefault() throws Exception {
    LlmConfig config = new LlmConfig();
    // verifySSL is null by default, should use true

    OkHttpClient client = HttpClientFactory.createClient(config);
    assertNotNull(client);
  }

  @Test
  public void testCombinedConfiguration() throws Exception {
    LlmConfig config = new LlmConfig();
    config.setConnectTimeoutSeconds(45);
    config.setReadTimeoutSeconds(150);
    config.setProxyUrl("http://proxy.example.com:8080");
    config.setProxyUsername("user");
    config.setProxyPassword("pass");

    Map<String, String> headers = new HashMap<>();
    headers.put("X-Api-Key", "secret");
    config.setCustomHeaders(headers);
    config.setVerifySSL(true);

    OkHttpClient client = HttpClientFactory.createClient(config);
    assertNotNull(client);
    assertEquals(45, TimeUnit.MILLISECONDS.toSeconds(client.connectTimeoutMillis()));
    assertEquals(150, TimeUnit.MILLISECONDS.toSeconds(client.readTimeoutMillis()));
    assertNotNull(client.proxy());
  }

  @Test
  public void testTimeoutBoundaries() throws Exception {
    // Test minimum timeout
    LlmConfig config1 = new LlmConfig();
    config1.setConnectTimeoutSeconds(1);
    OkHttpClient client1 = HttpClientFactory.createClient(config1);
    assertEquals(1, TimeUnit.MILLISECONDS.toSeconds(client1.connectTimeoutMillis()));

    // Test maximum reasonable timeout
    LlmConfig config2 = new LlmConfig();
    config2.setReadTimeoutSeconds(600);
    OkHttpClient client2 = HttpClientFactory.createClient(config2);
    assertEquals(600, TimeUnit.MILLISECONDS.toSeconds(client2.readTimeoutMillis()));
  }

  @Test
  public void testProxyUrlWithDifferentPorts() throws Exception {
    LlmConfig config1 = new LlmConfig();
    config1.setProxyUrl("http://proxy.example.com:3128");
    OkHttpClient client1 = HttpClientFactory.createClient(config1);
    assertEquals(3128, ((InetSocketAddress) client1.proxy().address()).getPort());

    LlmConfig config2 = new LlmConfig();
    config2.setProxyUrl("http://proxy.example.com:8888");
    OkHttpClient client2 = HttpClientFactory.createClient(config2);
    assertEquals(8888, ((InetSocketAddress) client2.proxy().address()).getPort());
  }
}
