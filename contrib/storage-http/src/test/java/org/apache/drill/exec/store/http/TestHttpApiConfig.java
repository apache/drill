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

package org.apache.drill.exec.store.http;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.util.DrillFileUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestHttpApiConfig {

  private final ObjectMapper objectMapper = new ObjectMapper();

  private static String EXAMPLE_HTTP_API_CONFIG_JSON;

  private static Map<String, String> EXAMPLE_HEADERS;

  @BeforeAll
  public static void setup() throws Exception {
    EXAMPLE_HTTP_API_CONFIG_JSON = Files.asCharSource(
        DrillFileUtils.getResourceAsFile("/data/exampleHttpApiConfig.json"), Charsets.UTF_8
    ).read().trim();

    EXAMPLE_HEADERS = new HashMap<>();
    EXAMPLE_HEADERS.put("Authorization", "Bearer token");
  }

  @Test
  public void testBuilderDefaults() {
    HttpApiConfig config = HttpApiConfig.builder().url("http://example.com").build();

    assertEquals("http://example.com", config.url());
    assertEquals("GET", config.method());
    assertTrue(config.verifySSLCert());
    assertTrue(config.requireTail());
    assertEquals(HttpApiConfig.DEFAULT_INPUT_FORMAT, config.inputType());
    assertEquals("QUERY_STRING", config.getPostParameterLocation());
    assertEquals("none", config.authType());

    assertNull(config.postBody());
    assertNull(config.headers());
    assertNull(config.params());
    assertNull(config.dataPath());
    assertNull(config.jsonOptions());
    assertNull(config.xmlOptions());
    assertNull(config.csvOptions());
    assertNull(config.paginator());
    assertNull(config.userName());
    assertNull(config.password());
    assertNull(config.credentialsProvider());
  }

  @Test
  public void testBuilder() {
    Map<String, String> headers = new HashMap<>();
    headers.put("Authorization", "Bearer token");

    HttpApiConfig.HttpApiConfigBuilder builder = HttpApiConfig.builder()
        .url("http://example.com")
        .method("GET")
        .postBody("testBody")
        .postParameterLocation(HttpApiConfig.POST_BODY_POST_LOCATION)
        .headers(headers)
        .params(Arrays.asList("param1", "param2"))
        .dataPath("/data/path")
        .authType("none")
        .inputType("json")
        .limitQueryParam("limit")
        .errorOn400(true)
        .jsonOptions(null)
        .xmlOptions(null)
        .csvOptions(null)
        .paginator(null)
        .requireTail(true)
        .verifySSLCert(true)
        .caseSensitiveFilters(true);

    HttpApiConfig config = builder.build();

    assertEquals("http://example.com", config.url());
    assertEquals("GET", config.method());
    assertEquals("testBody", config.postBody());
    assertEquals("POST_BODY", config.getPostParameterLocation());
    assertEquals(headers, config.headers());
    assertEquals(Arrays.asList("param1", "param2"), config.params());
    assertEquals("/data/path", config.dataPath());
    assertEquals("none", config.authType());
    assertEquals("json", config.inputType());
    assertEquals("limit", config.limitQueryParam());
    assertTrue(config.errorOn400());
    assertNull(config.jsonOptions());
    assertNull(config.xmlOptions());
    assertNull(config.csvOptions());
    assertNull(config.paginator());
    assertTrue(config.verifySSLCert());
    assertTrue(config.requireTail());
    assertTrue(config.caseSensitiveFilters());
  }

  @Test
  public void testUserExceptionOnNoURL() {
    HttpApiConfig config = HttpApiConfig.builder().url("http://example.com").build();

    assertEquals("http://example.com", config.url());
    assertEquals("GET", config.method());
    assertTrue(config.verifySSLCert());
    assertTrue(config.requireTail());
    assertEquals(HttpApiConfig.DEFAULT_INPUT_FORMAT, config.inputType());
    assertEquals("QUERY_STRING", config.getPostParameterLocation());
    assertEquals("none", config.authType());

    assertNull(config.postBody());
    assertNull(config.headers());
    assertNull(config.params());
    assertNull(config.dataPath());
    assertNull(config.jsonOptions());
    assertNull(config.xmlOptions());
    assertNull(config.csvOptions());
    assertNull(config.paginator());
    assertNull(config.userName());
    assertNull(config.password());
    assertNull(config.credentialsProvider());
  }

  @Test
  public void testInvalidHttpMethod() {
    String invalidMethod = "INVALID";

    assertThrows(UserException.class, () -> {
      HttpApiConfig.builder()
          .method(invalidMethod)
          .build();
    });
  }

  @Test
  public void testErrorOnEmptyURL() {

    assertThrows(UserException.class, () -> {
      HttpApiConfig.builder()
          .url(null)
          .build();
    });

    assertThrows(UserException.class, () -> {
      HttpApiConfig.builder()
          .url("")
          .build();
    });
  }

  @Test
  public void testJSONSerialization() throws JsonProcessingException {
    HttpApiConfig.HttpApiConfigBuilder builder = HttpApiConfig.builder()
        .url("http://example.com")
        .method("GET")
        .postBody("testBody")
        .postParameterLocation(HttpApiConfig.POST_BODY_POST_LOCATION)
        .headers(EXAMPLE_HEADERS)
        .params(Arrays.asList("param1", "param2"))
        .dataPath("/data/path")
        .authType("none")
        .inputType("json")
        .limitQueryParam("limit")
        .errorOn400(true)
        .jsonOptions(null)
        .xmlOptions(null)
        .csvOptions(null)
        .paginator(null)
        .requireTail(true)
        .verifySSLCert(true)
        .caseSensitiveFilters(true);

    HttpApiConfig config = builder.build();
    String json = objectMapper.writeValueAsString(config);

    assertEquals(EXAMPLE_HTTP_API_CONFIG_JSON, json);
  }

  @Test
  public void testJSONDeserialization() throws JsonProcessingException {
    HttpApiConfig config = objectMapper.readValue(EXAMPLE_HTTP_API_CONFIG_JSON,
        HttpApiConfig.class);

    assertEquals("http://example.com", config.url());
    assertEquals("GET", config.method());
    assertEquals("testBody", config.postBody());
    assertEquals("POST_BODY", config.getPostParameterLocation());
    assertEquals(EXAMPLE_HEADERS, config.headers());
    assertEquals(Arrays.asList("param1", "param2"), config.params());
    assertEquals("/data/path", config.dataPath());
    assertEquals("none", config.authType());
    assertEquals("json", config.inputType());
    assertEquals("limit", config.limitQueryParam());
    assertTrue(config.errorOn400());
    assertNull(config.jsonOptions());
    assertNull(config.xmlOptions());
    assertNull(config.csvOptions());
    assertNull(config.paginator());
    assertTrue(config.verifySSLCert());
    assertTrue(config.requireTail());
    assertTrue(config.caseSensitiveFilters());
  }
}
