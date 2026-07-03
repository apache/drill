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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/**
 * Configuration POJO for the AI copilot LLM connection.
 * Stored via PersistentStore.
 */
public class LlmConfig {

  @JsonProperty
  private String provider;

  @JsonProperty
  private String apiEndpoint;

  @JsonProperty
  private String apiKey;

  @JsonProperty
  private String model;

  @JsonProperty
  private int maxTokens;

  @JsonProperty
  private double temperature;

  @JsonProperty
  private boolean enabled;

  @JsonProperty
  private String systemPrompt;

  @JsonProperty
  private boolean sendDataToAi;

  @JsonProperty
  private int maxToolRounds;

  // Network Configuration
  @JsonProperty
  private Map<String, String> customHeaders;

  @JsonProperty
  private String proxyUrl;

  @JsonProperty
  private String proxyUsername;

  @JsonProperty
  private String proxyPassword;

  @JsonProperty
  private Integer connectTimeoutSeconds;

  @JsonProperty
  private Integer readTimeoutSeconds;

  @JsonProperty
  private Integer writeTimeoutSeconds;

  // SSL/TLS Configuration
  @JsonProperty
  private String keystorePath;

  @JsonProperty
  private String keystorePassword;

  @JsonProperty
  private String keystoreType;

  @JsonProperty
  private String truststorePath;

  @JsonProperty
  private String truststorePassword;

  @JsonProperty
  private String truststoreType;

  @JsonProperty
  private Boolean verifySSL;

  // Additional Request Parameters
  @JsonProperty
  private Map<String, Object> additionalParameters;

  // Custom API Format
  @JsonProperty
  private String requestTemplate;

  @JsonProperty
  private String responseMapping;

  public LlmConfig() {
    this.provider = "openai";
    this.maxTokens = 4096;
    this.temperature = 0.7;
    this.enabled = false;
    this.sendDataToAi = true;
    this.maxToolRounds = 15;
    // Network configuration defaults to null (will use system defaults)
    this.verifySSL = true;
    // Custom headers and additional parameters default to null
  }

  @JsonCreator
  public LlmConfig(
      @JsonProperty("provider") String provider,
      @JsonProperty("apiEndpoint") String apiEndpoint,
      @JsonProperty("apiKey") String apiKey,
      @JsonProperty("model") String model,
      @JsonProperty("maxTokens") int maxTokens,
      @JsonProperty("temperature") double temperature,
      @JsonProperty("enabled") boolean enabled,
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
    this.sendDataToAi = sendDataToAi != null ? sendDataToAi : true;
    this.maxToolRounds = maxToolRounds != null ? maxToolRounds : 15;
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
    this.verifySSL = verifySSL != null ? verifySSL : true;
    this.additionalParameters = additionalParameters;
    this.requestTemplate = requestTemplate;
    this.responseMapping = responseMapping;
  }

  public String getProvider() {
    return provider;
  }

  public void setProvider(String provider) {
    this.provider = provider;
  }

  public String getApiEndpoint() {
    return apiEndpoint;
  }

  public void setApiEndpoint(String apiEndpoint) {
    this.apiEndpoint = apiEndpoint;
  }

  public String getApiKey() {
    return apiKey;
  }

  public void setApiKey(String apiKey) {
    this.apiKey = apiKey;
  }

  public String getModel() {
    return model;
  }

  public void setModel(String model) {
    this.model = model;
  }

  public int getMaxTokens() {
    return maxTokens;
  }

  public void setMaxTokens(int maxTokens) {
    this.maxTokens = maxTokens;
  }

  public double getTemperature() {
    return temperature;
  }

  public void setTemperature(double temperature) {
    this.temperature = temperature;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public String getSystemPrompt() {
    return systemPrompt;
  }

  public void setSystemPrompt(String systemPrompt) {
    this.systemPrompt = systemPrompt;
  }

  public boolean isSendDataToAi() {
    return sendDataToAi;
  }

  public void setSendDataToAi(boolean sendDataToAi) {
    this.sendDataToAi = sendDataToAi;
  }

  public int getMaxToolRounds() {
    return maxToolRounds > 0 ? maxToolRounds : 15;
  }

  public void setMaxToolRounds(int maxToolRounds) {
    this.maxToolRounds = maxToolRounds;
  }

  // Network Configuration Getters/Setters
  public Map<String, String> getCustomHeaders() {
    return customHeaders;
  }

  public void setCustomHeaders(Map<String, String> customHeaders) {
    this.customHeaders = customHeaders;
  }

  public String getProxyUrl() {
    return proxyUrl;
  }

  public void setProxyUrl(String proxyUrl) {
    this.proxyUrl = proxyUrl;
  }

  public String getProxyUsername() {
    return proxyUsername;
  }

  public void setProxyUsername(String proxyUsername) {
    this.proxyUsername = proxyUsername;
  }

  public String getProxyPassword() {
    return proxyPassword;
  }

  public void setProxyPassword(String proxyPassword) {
    this.proxyPassword = proxyPassword;
  }

  public Integer getConnectTimeoutSeconds() {
    return connectTimeoutSeconds;
  }

  public void setConnectTimeoutSeconds(Integer connectTimeoutSeconds) {
    this.connectTimeoutSeconds = connectTimeoutSeconds;
  }

  public Integer getReadTimeoutSeconds() {
    return readTimeoutSeconds;
  }

  public void setReadTimeoutSeconds(Integer readTimeoutSeconds) {
    this.readTimeoutSeconds = readTimeoutSeconds;
  }

  public Integer getWriteTimeoutSeconds() {
    return writeTimeoutSeconds;
  }

  public void setWriteTimeoutSeconds(Integer writeTimeoutSeconds) {
    this.writeTimeoutSeconds = writeTimeoutSeconds;
  }

  // SSL/TLS Configuration Getters/Setters
  public String getKeystorePath() {
    return keystorePath;
  }

  public void setKeystorePath(String keystorePath) {
    this.keystorePath = keystorePath;
  }

  public String getKeystorePassword() {
    return keystorePassword;
  }

  public void setKeystorePassword(String keystorePassword) {
    this.keystorePassword = keystorePassword;
  }

  public String getKeystoreType() {
    return keystoreType;
  }

  public void setKeystoreType(String keystoreType) {
    this.keystoreType = keystoreType;
  }

  public String getTruststorePath() {
    return truststorePath;
  }

  public void setTruststorePath(String truststorePath) {
    this.truststorePath = truststorePath;
  }

  public String getTruststorePassword() {
    return truststorePassword;
  }

  public void setTruststorePassword(String truststorePassword) {
    this.truststorePassword = truststorePassword;
  }

  public String getTruststoreType() {
    return truststoreType;
  }

  public void setTruststoreType(String truststoreType) {
    this.truststoreType = truststoreType;
  }

  public Boolean getVerifySSL() {
    return verifySSL;
  }

  public void setVerifySSL(Boolean verifySSL) {
    this.verifySSL = verifySSL;
  }

  // Additional Request Parameters Getters/Setters
  public Map<String, Object> getAdditionalParameters() {
    return additionalParameters;
  }

  public void setAdditionalParameters(Map<String, Object> additionalParameters) {
    this.additionalParameters = additionalParameters;
  }

  // Custom API Format Getters/Setters
  public String getRequestTemplate() {
    return requestTemplate;
  }

  public void setRequestTemplate(String requestTemplate) {
    this.requestTemplate = requestTemplate;
  }

  public String getResponseMapping() {
    return responseMapping;
  }

  public void setResponseMapping(String responseMapping) {
    this.responseMapping = responseMapping;
  }
}
