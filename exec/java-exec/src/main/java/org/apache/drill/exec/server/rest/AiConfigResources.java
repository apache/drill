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
        @JsonProperty("maxToolRounds") Integer maxToolRounds) {
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
