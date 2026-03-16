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

  public LlmConfig() {
    this.provider = "openai";
    this.maxTokens = 4096;
    this.temperature = 0.7;
    this.enabled = false;
    this.sendDataToAi = true;
    this.maxToolRounds = 15;
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
      @JsonProperty("maxToolRounds") Integer maxToolRounds) {
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
}
