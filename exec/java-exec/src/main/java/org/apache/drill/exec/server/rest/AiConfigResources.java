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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.server.rest.ai.LlmConfig;
import org.apache.drill.exec.server.rest.ai.LlmProvider;
import org.apache.drill.exec.server.rest.ai.LlmProviderRegistry;
import org.apache.drill.exec.server.rest.ai.ValidationResult;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.work.WorkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * REST resource for managing Prospector AI configuration.
 * Admin-only access. API keys are stored securely and redacted in GET responses.
 */
@Path("/api/v1/ai/config")
@Tag(name = "Prospector Configuration", description = "Admin configuration for Prospector AI")
@RolesAllowed(DrillUserPrincipal.ADMIN_ROLE)
public class AiConfigResources {

  private static final Logger logger = LoggerFactory.getLogger(AiConfigResources.class);
  private static final String CONFIG_STORE_NAME = "drill.sqllab.ai_config";
  private static final String CONFIG_KEY = "default";

  @Inject
  WorkManager workManager;

  @Inject
  PersistentStoreProvider storeProvider;

  private static volatile PersistentStore<LlmConfig> cachedStore;

  // ==================== Response Models ====================

  public static class ConfigResponse {
    @JsonProperty
    public String provider;

    @JsonProperty
    public String apiEndpoint;

    @JsonProperty
    public boolean apiKeySet;

    @JsonProperty
    public String model;

    @JsonProperty
    public int maxTokens;

    @JsonProperty
    public double temperature;

    @JsonProperty
    public boolean enabled;

    @JsonProperty
    public String systemPrompt;

    @JsonProperty
    public boolean sendDataToAi;

    @JsonProperty
    public int maxToolRounds;

    // Network Configuration
    @JsonProperty
    public Map<String, String> customHeaders;

    @JsonProperty
    public String proxyUrl;

    @JsonProperty
    public String proxyUsername;

    @JsonProperty
    public boolean proxyPasswordSet;

    @JsonProperty
    public Integer connectTimeoutSeconds;

    @JsonProperty
    public Integer readTimeoutSeconds;

    @JsonProperty
    public Integer writeTimeoutSeconds;

    // SSL/TLS Configuration
    @JsonProperty
    public String keystorePath;

    @JsonProperty
    public boolean keystorePasswordSet;

    @JsonProperty
    public String keystoreType;

    @JsonProperty
    public String truststorePath;

    @JsonProperty
    public boolean truststorePasswordSet;

    @JsonProperty
    public String truststoreType;

    @JsonProperty
    public Boolean verifySSL;

    // Additional Request Parameters
    @JsonProperty
    public Map<String, Object> additionalParameters;

    // Custom API Format
    @JsonProperty
    public String requestTemplate;

    @JsonProperty
    public String responseMapping;

    // OAuth2 gateway. consumerSecret is a secret, so only its presence is returned.
    @JsonProperty
    public String authUrl;

    @JsonProperty
    public String consumerKey;

    @JsonProperty
    public boolean consumerSecretSet;

    @JsonProperty
    public String clientId;

    @JsonProperty
    public String usecaseId;

    @JsonProperty
    public String clientCertPath;

    @JsonProperty
    public Map<String, String> gatewayHeaders;

    public ConfigResponse() {
    }

    public ConfigResponse(LlmConfig config) {
      this.provider = config.getProvider();
      this.apiEndpoint = config.getApiEndpoint();
      this.apiKeySet = config.getApiKey() != null && !config.getApiKey().isEmpty();
      this.model = config.getModel();
      this.maxTokens = config.getMaxTokens();
      this.temperature = config.getTemperature();
      this.enabled = config.isEnabled();
      this.systemPrompt = config.getSystemPrompt();
      this.sendDataToAi = config.isSendDataToAi();
      this.maxToolRounds = config.getMaxToolRounds();

      // Network Configuration
      this.customHeaders = config.getCustomHeaders();
      this.proxyUrl = config.getProxyUrl();
      this.proxyUsername = config.getProxyUsername();
      this.proxyPasswordSet = config.getProxyPassword() != null && !config.getProxyPassword().isEmpty();
      this.connectTimeoutSeconds = config.getConnectTimeoutSeconds();
      this.readTimeoutSeconds = config.getReadTimeoutSeconds();
      this.writeTimeoutSeconds = config.getWriteTimeoutSeconds();

      // SSL/TLS Configuration
      this.keystorePath = config.getKeystorePath();
      this.keystorePasswordSet = config.getKeystorePassword() != null && !config.getKeystorePassword().isEmpty();
      this.keystoreType = config.getKeystoreType();
      this.truststorePath = config.getTruststorePath();
      this.truststorePasswordSet = config.getTruststorePassword() != null && !config.getTruststorePassword().isEmpty();
      this.truststoreType = config.getTruststoreType();
      this.verifySSL = config.getVerifySSL();

      // Additional Request Parameters
      this.additionalParameters = config.getAdditionalParameters();

      // Custom API Format
      this.requestTemplate = config.getRequestTemplate();
      this.responseMapping = config.getResponseMapping();

      // OAuth2 gateway
      this.authUrl = config.getAuthUrl();
      this.consumerKey = config.getConsumerKey();
      this.consumerSecretSet = config.getConsumerSecret() != null && !config.getConsumerSecret().isEmpty();
      this.clientId = config.getClientId();
      this.usecaseId = config.getUsecaseId();
      this.clientCertPath = config.getClientCertPath();
      this.gatewayHeaders = config.getGatewayHeaders();
    }
  }

  public static class UpdateConfigRequest {
    @JsonProperty
    public String provider;

    @JsonProperty
    public String apiEndpoint;

    @JsonProperty
    public String apiKey;

    @JsonProperty
    public String model;

    @JsonProperty
    public Integer maxTokens;

    @JsonProperty
    public Double temperature;

    @JsonProperty
    public Boolean enabled;

    @JsonProperty
    public String systemPrompt;

    @JsonProperty
    public Boolean sendDataToAi;

    @JsonProperty
    public Integer maxToolRounds;

    // Network Configuration
    @JsonProperty
    public Map<String, String> customHeaders;

    @JsonProperty
    public String proxyUrl;

    @JsonProperty
    public String proxyUsername;

    @JsonProperty
    public String proxyPassword;

    @JsonProperty
    public Integer connectTimeoutSeconds;

    @JsonProperty
    public Integer readTimeoutSeconds;

    @JsonProperty
    public Integer writeTimeoutSeconds;

    // SSL/TLS Configuration
    @JsonProperty
    public String keystorePath;

    @JsonProperty
    public String keystorePassword;

    @JsonProperty
    public String keystoreType;

    @JsonProperty
    public String truststorePath;

    @JsonProperty
    public String truststorePassword;

    @JsonProperty
    public String truststoreType;

    @JsonProperty
    public Boolean verifySSL;

    // Additional Request Parameters
    @JsonProperty
    public Map<String, Object> additionalParameters;

    // Custom API Format
    @JsonProperty
    public String requestTemplate;

    @JsonProperty
    public String responseMapping;

    // OAuth2 gateway
    @JsonProperty
    public String authUrl;

    @JsonProperty
    public String consumerKey;

    @JsonProperty
    public String consumerSecret;

    @JsonProperty
    public String clientId;

    @JsonProperty
    public String usecaseId;

    @JsonProperty
    public String clientCertPath;

    @JsonProperty
    public Map<String, String> gatewayHeaders;

    public UpdateConfigRequest() {
    }

    @JsonCreator
    public UpdateConfigRequest(
        @JsonProperty("provider") String provider,
        @JsonProperty("apiEndpoint") String apiEndpoint,
        @JsonProperty("apiKey") String apiKey,
        @JsonProperty("model") String model,
        @JsonProperty("maxTokens") Integer maxTokens,
        @JsonProperty("temperature") Double temperature,
        @JsonProperty("enabled") Boolean enabled,
        @JsonProperty("systemPrompt") String systemPrompt,
        @JsonProperty("sendDataToAi") Boolean sendDataToAi,
        @JsonProperty("maxToolRounds") Integer maxToolRounds,
        @JsonProperty("customHeaders") Map<String, String> customHeaders,
        @JsonProperty("proxyUrl") String proxyUrl,
        @JsonProperty("proxyUsername") String proxyUsername,
        @JsonProperty("proxyPassword") String proxyPassword,
        @JsonProperty("connectTimeoutSeconds") Integer connectTimeoutSeconds,
        @JsonProperty("readTimeoutSeconds") Integer readTimeoutSeconds,
        @JsonProperty("writeTimeoutSeconds") Integer writeTimeoutSeconds,
        @JsonProperty("keystorePath") String keystorePath,
        @JsonProperty("keystorePassword") String keystorePassword,
        @JsonProperty("keystoreType") String keystoreType,
        @JsonProperty("truststorePath") String truststorePath,
        @JsonProperty("truststorePassword") String truststorePassword,
        @JsonProperty("truststoreType") String truststoreType,
        @JsonProperty("verifySSL") Boolean verifySSL,
        @JsonProperty("additionalParameters") Map<String, Object> additionalParameters,
        @JsonProperty("requestTemplate") String requestTemplate,
        @JsonProperty("responseMapping") String responseMapping) {
      this.provider = provider;
      this.apiEndpoint = apiEndpoint;
      this.apiKey = apiKey;
      this.model = model;
      this.maxTokens = maxTokens;
      this.temperature = temperature;
      this.enabled = enabled;
      this.systemPrompt = systemPrompt;
      this.sendDataToAi = sendDataToAi;
      this.maxToolRounds = maxToolRounds;
      this.customHeaders = customHeaders;
      this.proxyUrl = proxyUrl;
      this.proxyUsername = proxyUsername;
      this.proxyPassword = proxyPassword;
      this.connectTimeoutSeconds = connectTimeoutSeconds;
      this.readTimeoutSeconds = readTimeoutSeconds;
      this.writeTimeoutSeconds = writeTimeoutSeconds;
      this.keystorePath = keystorePath;
      this.keystorePassword = keystorePassword;
      this.keystoreType = keystoreType;
      this.truststorePath = truststorePath;
      this.truststorePassword = truststorePassword;
      this.truststoreType = truststoreType;
      this.verifySSL = verifySSL;
      this.additionalParameters = additionalParameters;
      this.requestTemplate = requestTemplate;
      this.responseMapping = responseMapping;
    }
  }

  public static class ProviderInfo {
    @JsonProperty
    public String id;

    @JsonProperty
    public String displayName;

    public ProviderInfo(String id, String displayName) {
      this.id = id;
      this.displayName = displayName;
    }
  }

  public static class ProvidersResponse {
    @JsonProperty
    public List<ProviderInfo> providers;

    public ProvidersResponse(List<ProviderInfo> providers) {
      this.providers = providers;
    }
  }

  public static class MessageResponse {
    @JsonProperty
    public String message;

    public MessageResponse(String message) {
      this.message = message;
    }
  }

  // ==================== Endpoints ====================

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get AI configuration",
      description = "Returns AI configuration with API key redacted")
  public Response getConfig() {
    logger.debug("Getting AI configuration");
    try {
      PersistentStore<LlmConfig> store = getStore();
      LlmConfig config = store.get(CONFIG_KEY);

      if (config == null) {
        return Response.ok(new ConfigResponse(new LlmConfig())).build();
      }

      return Response.ok(new ConfigResponse(config)).build();
    } catch (Exception e) {
      logger.error("Error reading AI config", e);
      throw new DrillRuntimeException("Failed to read AI configuration: " + e.getMessage(), e);
    }
  }

  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Update AI configuration",
      description = "Updates AI configuration. Supports partial updates.")
  public Response updateConfig(UpdateConfigRequest request) {
    logger.debug("Updating AI configuration");

    try {
      PersistentStore<LlmConfig> store = getStore();
      LlmConfig existing = store.get(CONFIG_KEY);
      if (existing == null) {
        existing = new LlmConfig();
      }

      // Apply partial updates
      if (request.provider != null) {
        existing.setProvider(request.provider);
      }
      if (request.apiEndpoint != null) {
        existing.setApiEndpoint(request.apiEndpoint);
      }
      // Only update API key if provided and non-empty
      if (request.apiKey != null && !request.apiKey.isEmpty()) {
        existing.setApiKey(request.apiKey);
      }
      if (request.model != null) {
        existing.setModel(request.model);
      }
      if (request.maxTokens != null) {
        existing.setMaxTokens(request.maxTokens);
      }
      if (request.temperature != null) {
        existing.setTemperature(request.temperature);
      }
      if (request.enabled != null) {
        existing.setEnabled(request.enabled);
      }
      if (request.systemPrompt != null) {
        existing.setSystemPrompt(request.systemPrompt);
      }
      if (request.sendDataToAi != null) {
        existing.setSendDataToAi(request.sendDataToAi);
      }
      if (request.maxToolRounds != null) {
        existing.setMaxToolRounds(request.maxToolRounds);
      }

      // Network Configuration
      if (request.customHeaders != null) {
        existing.setCustomHeaders(request.customHeaders);
      }
      if (request.proxyUrl != null) {
        existing.setProxyUrl(request.proxyUrl);
      }
      if (request.proxyUsername != null) {
        existing.setProxyUsername(request.proxyUsername);
      }
      if (request.proxyPassword != null && !request.proxyPassword.isEmpty()) {
        existing.setProxyPassword(request.proxyPassword);
      }
      if (request.connectTimeoutSeconds != null) {
        existing.setConnectTimeoutSeconds(request.connectTimeoutSeconds);
      }
      if (request.readTimeoutSeconds != null) {
        existing.setReadTimeoutSeconds(request.readTimeoutSeconds);
      }
      if (request.writeTimeoutSeconds != null) {
        existing.setWriteTimeoutSeconds(request.writeTimeoutSeconds);
      }

      // SSL/TLS Configuration
      if (request.keystorePath != null) {
        existing.setKeystorePath(request.keystorePath);
      }
      if (request.keystorePassword != null && !request.keystorePassword.isEmpty()) {
        existing.setKeystorePassword(request.keystorePassword);
      }
      if (request.keystoreType != null) {
        existing.setKeystoreType(request.keystoreType);
      }
      if (request.truststorePath != null) {
        existing.setTruststorePath(request.truststorePath);
      }
      if (request.truststorePassword != null && !request.truststorePassword.isEmpty()) {
        existing.setTruststorePassword(request.truststorePassword);
      }
      if (request.truststoreType != null) {
        existing.setTruststoreType(request.truststoreType);
      }
      if (request.verifySSL != null) {
        existing.setVerifySSL(request.verifySSL);
      }

      // Additional Request Parameters
      if (request.additionalParameters != null) {
        existing.setAdditionalParameters(request.additionalParameters);
      }

      // Custom API Format
      if (request.requestTemplate != null) {
        existing.setRequestTemplate(request.requestTemplate);
      }
      if (request.responseMapping != null) {
        existing.setResponseMapping(request.responseMapping);
      }

      // OAuth2 gateway. consumerSecret only updates on a non-empty value so a
      // save from the UI (which sends it blank when unchanged) doesn't wipe the stored secret.
      if (request.authUrl != null) {
        existing.setAuthUrl(request.authUrl);
      }
      if (request.consumerKey != null) {
        existing.setConsumerKey(request.consumerKey);
      }
      if (request.consumerSecret != null && !request.consumerSecret.isEmpty()) {
        existing.setConsumerSecret(request.consumerSecret);
      }
      if (request.clientId != null) {
        existing.setClientId(request.clientId);
      }
      if (request.usecaseId != null) {
        existing.setUsecaseId(request.usecaseId);
      }
      if (request.clientCertPath != null) {
        existing.setClientCertPath(request.clientCertPath);
      }
      if (request.gatewayHeaders != null) {
        existing.setGatewayHeaders(request.gatewayHeaders);
      }

      store.put(CONFIG_KEY, existing);

      return Response.ok(new ConfigResponse(existing)).build();
    } catch (Exception e) {
      logger.error("Error updating AI config", e);
      throw new DrillRuntimeException("Failed to update AI configuration: " + e.getMessage(), e);
    }
  }

  @POST
  @Path("/test")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Test AI configuration",
      description = "Validates the configuration and tests connectivity to the LLM provider")
  public Response testConfig(UpdateConfigRequest request) {
    logger.debug("Testing AI configuration");

    try {
      // Build a config for testing
      PersistentStore<LlmConfig> store = getStore();
      LlmConfig existing = store.get(CONFIG_KEY);

      LlmConfig testConfig = new LlmConfig();
      testConfig.setProvider(request.provider != null ? request.provider
          : (existing != null ? existing.getProvider() : "openai"));
      testConfig.setApiEndpoint(request.apiEndpoint != null ? request.apiEndpoint
          : (existing != null ? existing.getApiEndpoint() : null));
      testConfig.setModel(request.model != null ? request.model
          : (existing != null ? existing.getModel() : null));
      testConfig.setMaxTokens(request.maxTokens != null ? request.maxTokens : 100);
      testConfig.setTemperature(request.temperature != null ? request.temperature : 0.7);

      // Use provided API key, or fall back to existing
      if (request.apiKey != null && !request.apiKey.isEmpty()) {
        testConfig.setApiKey(request.apiKey);
      } else if (existing != null) {
        testConfig.setApiKey(existing.getApiKey());
      }

      // Network Configuration
      if (request.customHeaders != null) {
        testConfig.setCustomHeaders(request.customHeaders);
      } else if (existing != null) {
        testConfig.setCustomHeaders(existing.getCustomHeaders());
      }
      if (request.proxyUrl != null) {
        testConfig.setProxyUrl(request.proxyUrl);
      } else if (existing != null) {
        testConfig.setProxyUrl(existing.getProxyUrl());
      }
      if (request.proxyUsername != null) {
        testConfig.setProxyUsername(request.proxyUsername);
      } else if (existing != null) {
        testConfig.setProxyUsername(existing.getProxyUsername());
      }
      if (request.proxyPassword != null && !request.proxyPassword.isEmpty()) {
        testConfig.setProxyPassword(request.proxyPassword);
      } else if (existing != null) {
        testConfig.setProxyPassword(existing.getProxyPassword());
      }
      if (request.connectTimeoutSeconds != null) {
        testConfig.setConnectTimeoutSeconds(request.connectTimeoutSeconds);
      } else if (existing != null) {
        testConfig.setConnectTimeoutSeconds(existing.getConnectTimeoutSeconds());
      }
      if (request.readTimeoutSeconds != null) {
        testConfig.setReadTimeoutSeconds(request.readTimeoutSeconds);
      } else if (existing != null) {
        testConfig.setReadTimeoutSeconds(existing.getReadTimeoutSeconds());
      }
      if (request.writeTimeoutSeconds != null) {
        testConfig.setWriteTimeoutSeconds(request.writeTimeoutSeconds);
      } else if (existing != null) {
        testConfig.setWriteTimeoutSeconds(existing.getWriteTimeoutSeconds());
      }

      // SSL/TLS Configuration
      if (request.keystorePath != null) {
        testConfig.setKeystorePath(request.keystorePath);
      } else if (existing != null) {
        testConfig.setKeystorePath(existing.getKeystorePath());
      }
      if (request.keystorePassword != null && !request.keystorePassword.isEmpty()) {
        testConfig.setKeystorePassword(request.keystorePassword);
      } else if (existing != null) {
        testConfig.setKeystorePassword(existing.getKeystorePassword());
      }
      if (request.keystoreType != null) {
        testConfig.setKeystoreType(request.keystoreType);
      } else if (existing != null) {
        testConfig.setKeystoreType(existing.getKeystoreType());
      }
      if (request.truststorePath != null) {
        testConfig.setTruststorePath(request.truststorePath);
      } else if (existing != null) {
        testConfig.setTruststorePath(existing.getTruststorePath());
      }
      if (request.truststorePassword != null && !request.truststorePassword.isEmpty()) {
        testConfig.setTruststorePassword(request.truststorePassword);
      } else if (existing != null) {
        testConfig.setTruststorePassword(existing.getTruststorePassword());
      }
      if (request.truststoreType != null) {
        testConfig.setTruststoreType(request.truststoreType);
      } else if (existing != null) {
        testConfig.setTruststoreType(existing.getTruststoreType());
      }
      if (request.verifySSL != null) {
        testConfig.setVerifySSL(request.verifySSL);
      } else if (existing != null) {
        testConfig.setVerifySSL(existing.getVerifySSL());
      }

      // Additional Request Parameters
      if (request.additionalParameters != null) {
        testConfig.setAdditionalParameters(request.additionalParameters);
      } else if (existing != null) {
        testConfig.setAdditionalParameters(existing.getAdditionalParameters());
      }

      // Custom API Format
      if (request.requestTemplate != null) {
        testConfig.setRequestTemplate(request.requestTemplate);
      } else if (existing != null) {
        testConfig.setRequestTemplate(existing.getRequestTemplate());
      }
      if (request.responseMapping != null) {
        testConfig.setResponseMapping(request.responseMapping);
      } else if (existing != null) {
        testConfig.setResponseMapping(existing.getResponseMapping());
      }

      // OAuth2 gateway
      if (request.authUrl != null) {
        testConfig.setAuthUrl(request.authUrl);
      } else if (existing != null) {
        testConfig.setAuthUrl(existing.getAuthUrl());
      }
      if (request.consumerKey != null) {
        testConfig.setConsumerKey(request.consumerKey);
      } else if (existing != null) {
        testConfig.setConsumerKey(existing.getConsumerKey());
      }
      if (request.consumerSecret != null && !request.consumerSecret.isEmpty()) {
        testConfig.setConsumerSecret(request.consumerSecret);
      } else if (existing != null) {
        testConfig.setConsumerSecret(existing.getConsumerSecret());
      }
      if (request.clientId != null) {
        testConfig.setClientId(request.clientId);
      } else if (existing != null) {
        testConfig.setClientId(existing.getClientId());
      }
      if (request.usecaseId != null) {
        testConfig.setUsecaseId(request.usecaseId);
      } else if (existing != null) {
        testConfig.setUsecaseId(existing.getUsecaseId());
      }
      if (request.clientCertPath != null) {
        testConfig.setClientCertPath(request.clientCertPath);
      } else if (existing != null) {
        testConfig.setClientCertPath(existing.getClientCertPath());
      }
      if (request.gatewayHeaders != null) {
        testConfig.setGatewayHeaders(request.gatewayHeaders);
      } else if (existing != null) {
        testConfig.setGatewayHeaders(existing.getGatewayHeaders());
      }

      LlmProvider provider = LlmProviderRegistry.get(testConfig.getProvider());
      if (provider == null) {
        return Response.ok(ValidationResult.error("Unknown provider: " + testConfig.getProvider())).build();
      }

      ValidationResult result = provider.validateConfig(testConfig);
      return Response.ok(result).build();
    } catch (Exception e) {
      logger.error("Error testing AI config", e);
      return Response.ok(ValidationResult.error("Test failed: " + e.getMessage())).build();
    }
  }

  @GET
  @Path("/providers")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "List available LLM providers",
      description = "Returns the list of supported LLM providers")
  public ProvidersResponse getProviders() {
    List<ProviderInfo> providers = new ArrayList<>();
    for (LlmProvider provider : LlmProviderRegistry.getAll()) {
      providers.add(new ProviderInfo(provider.getId(), provider.getDisplayName()));
    }
    return new ProvidersResponse(providers);
  }

  // ==================== Helper Methods ====================

  private PersistentStore<LlmConfig> getStore() {
    if (cachedStore == null) {
      synchronized (AiConfigResources.class) {
        if (cachedStore == null) {
          try {
            cachedStore = storeProvider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    LlmConfig.class
                )
                .name(CONFIG_STORE_NAME)
                .build()
            );
          } catch (StoreException e) {
            throw new DrillRuntimeException("Failed to access AI config store", e);
          }
        }
      }
    }
    return cachedStore;
  }
}
