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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Provider for OpenAI-compatible chat APIs fronted by an Apigee-style enterprise gateway
 * that authenticates with OAuth2 client-credentials and mTLS. The chat API itself is
 * OpenAI-compatible, so this reuses the entire request-building and SSE-parsing path of
 * {@link OpenAiCompatibleProvider} and only overrides authentication:
 *
 * <ol>
 *   <li>Fetch a bearer token from the auth URL via OAuth2 client-credentials — Basic auth
 *       with {@code base64(consumerKey:consumerSecret)}, body {@code grant_type=client_credentials}.
 *   <li>Cache the token until shortly before its {@code expires_in}.
 *   <li>Attach the headers defined by {@code gatewayHeaders} (see {@link #DEFAULT_GATEWAY_HEADERS}).
 * </ol>
 *
 * <p>Header names are fully configurable via {@code gatewayHeaders}, a map of header name to a
 * value template. Templates may contain the placeholders {@code {token}} (the fetched bearer
 * token), {@code {uuid}} (a fresh UUID per request), {@code {timestamp}} (a local microsecond
 * ISO-8601 timestamp), {@code {apiKey}}, {@code {clientId}}, {@code {usecaseId}} and
 * {@code {model}}; anything else is sent literally. See {@link #DEFAULT_GATEWAY_HEADERS} for
 * the mapping used when none is configured.
 *
 * <p>mTLS (the client {@code .pem}) is handled by {@link HttpClientFactory} via
 * {@code clientCertPath}, so it applies to both the token call and the endpoint call.
 */
public class OAuth2GatewayProvider extends OpenAiCompatibleProvider {

  private static final Logger logger = LoggerFactory.getLogger(OAuth2GatewayProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final MediaType FORM = MediaType.parse("application/x-www-form-urlencoded");

  // Matches Python's datetime.now().isoformat(): local time, microsecond precision, no zone.
  // ponytail: local time to mirror the reference client; switch to UTC here if the gateway
  // rejects the date as stale.
  private static final DateTimeFormatter REQUEST_DATE =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");

  // Refresh a bit before the stated expiry to absorb clock skew and request latency.
  private static final long EXPIRY_MARGIN_MILLIS = 60_000L;

  /**
   * Default header mapping used when {@code gatewayHeaders} is not configured. Keys are
   * header names; values are templates resolved by {@link #resolveTemplate}.
   */
  static final Map<String, String> DEFAULT_GATEWAY_HEADERS;

  static {
    Map<String, String> defaults = new LinkedHashMap<>();
    defaults.put("Authorization", "Bearer {token}");
    defaults.put("client-id", "{clientId}");
    defaults.put("usecase-id", "{usecaseId}");
    defaults.put("api-key", "{apiKey}");
    defaults.put("x-request-id", "{uuid}");
    defaults.put("x-correlation-id", "{uuid}");
    defaults.put("request-date", "{timestamp}");
    DEFAULT_GATEWAY_HEADERS = Collections.unmodifiableMap(defaults);
  }

  private String cachedToken;
  private String cachedFor;
  private long cachedExpiryMillis;

  @Override
  public String getId() {
    return "oauth2";
  }

  @Override
  public String getDisplayName() {
    return "OAuth2 Gateway";
  }

  @Override
  public ValidationResult validateConfig(LlmConfig config) {
    String missing = firstMissingField(config);
    if (missing != null) {
      return ValidationResult.error(missing + " is required");
    }
    // probe() drives the real flow: token fetch -> endpoint call, so a failure tells the
    // user which leg broke (auth vs. mTLS vs. endpoint) via the ValidationResult details.
    return probe(config);
  }

  @Override
  protected void decorateRequest(Request.Builder builder, LlmConfig config) throws Exception {
    Map<String, String> headers = config.getGatewayHeaders();
    if (headers == null || headers.isEmpty()) {
      headers = DEFAULT_GATEWAY_HEADERS;
    }
    // Fetch the token only if some header actually references it.
    String token = null;
    for (Map.Entry<String, String> header : headers.entrySet()) {
      String template = header.getValue();
      if (template == null) {
        continue;
      }
      if (token == null && template.contains("{token}")) {
        token = getAccessToken(config);
      }
      builder.addHeader(header.getKey(), resolveTemplate(template, config, token));
    }
  }

  /** Substitute the supported placeholders in a header value template. */
  private String resolveTemplate(String template, LlmConfig config, String token) {
    return template
        .replace("{token}", orEmpty(token))
        .replace("{uuid}", UUID.randomUUID().toString())
        .replace("{timestamp}", LocalDateTime.now().format(REQUEST_DATE))
        .replace("{apiKey}", orEmpty(config.getApiKey()))
        .replace("{clientId}", orEmpty(config.getClientId()))
        .replace("{usecaseId}", orEmpty(config.getUsecaseId()))
        .replace("{model}", orEmpty(config.getModel()));
  }

  private static String orEmpty(String value) {
    return value == null ? "" : value;
  }

  /**
   * Fetch and cache an OAuth2 client-credentials bearer token. Synchronized because the
   * singleton provider serves concurrent chat requests; contention only occurs on refresh.
   */
  private synchronized String getAccessToken(LlmConfig config) throws Exception {
    long now = System.currentTimeMillis();
    if (cachedToken != null
        && config.getConsumerKey().equals(cachedFor)
        && now < cachedExpiryMillis - EXPIRY_MARGIN_MILLIS) {
      return cachedToken;
    }

    String basic = Base64.getEncoder().encodeToString(
        (config.getConsumerKey() + ":" + config.getConsumerSecret())
            .getBytes(StandardCharsets.UTF_8));

    Request request = new Request.Builder()
        .url(config.getAuthUrl())
        .post(RequestBody.create("grant_type=client_credentials", FORM))
        .addHeader("Authorization", "Basic " + basic)
        .addHeader("Content-Type", "application/x-www-form-urlencoded")
        .build();

    try (Response response = HttpClientFactory.createClient(config).newCall(request).execute()) {
      String body = response.body() != null ? response.body().string() : "";
      if (!response.isSuccessful()) {
        throw new IllegalStateException("Token request to " + config.getAuthUrl()
            + " failed: HTTP " + response.code() + " " + body);
      }
      JsonNode json = MAPPER.readTree(body);
      JsonNode token = json.get("access_token");
      if (token == null || token.asText().isEmpty()) {
        throw new IllegalStateException("Token response from " + config.getAuthUrl()
            + " had no access_token field: " + body);
      }
      // expires_in may be a number or a numeric string; asLong handles both, else default 1h.
      long expiresInSeconds = json.has("expires_in") ? json.get("expires_in").asLong(3600L) : 3600L;
      cachedToken = token.asText();
      cachedFor = config.getConsumerKey();
      cachedExpiryMillis = now + expiresInSeconds * 1000L;
      logger.debug("Fetched OAuth2 gateway token (expires_in={}s)", expiresInSeconds);
      return cachedToken;
    }
  }

  private String firstMissingField(LlmConfig config) {
    if (isBlank(config.getApiEndpoint())) {
      return "Endpoint URL";
    }
    if (isBlank(config.getModel())) {
      return "Model";
    }
    if (isBlank(config.getAuthUrl())) {
      return "Auth URL";
    }
    if (isBlank(config.getConsumerKey())) {
      return "Consumer Key";
    }
    if (isBlank(config.getConsumerSecret())) {
      return "Consumer Secret";
    }
    // clientId/usecaseId/apiKey are only required if the gateway-header mapping references
    // them, so they are not validated here — the default mapping uses them, but a custom
    // mapping need not. A blank value simply renders as an empty header.
    return null;
  }

  private static boolean isBlank(String value) {
    return value == null || value.isEmpty();
  }
}
