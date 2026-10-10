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

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Per-model pricing entry used to estimate cost in the analytics dashboard.
 * Prices are expressed per million tokens.
 */
public class AiPricing {

  @JsonProperty
  private String provider;

  @JsonProperty
  private String model;

  @JsonProperty
  private double inputPricePerMTokens;

  @JsonProperty
  private double outputPricePerMTokens;

  @JsonProperty
  private String currency = "USD";

  @JsonProperty
  private long updatedAt;

  public AiPricing() {
  }

  public String getProvider() {
    return provider;
  }

  public void setProvider(String provider) {
    this.provider = provider;
  }

  public String getModel() {
    return model;
  }

  public void setModel(String model) {
    this.model = model;
  }

  public double getInputPricePerMTokens() {
    return inputPricePerMTokens;
  }

  public void setInputPricePerMTokens(double inputPricePerMTokens) {
    this.inputPricePerMTokens = inputPricePerMTokens;
  }

  public double getOutputPricePerMTokens() {
    return outputPricePerMTokens;
  }

  public void setOutputPricePerMTokens(double outputPricePerMTokens) {
    this.outputPricePerMTokens = outputPricePerMTokens;
  }

  public String getCurrency() {
    return currency;
  }

  public void setCurrency(String currency) {
    this.currency = currency;
  }

  public long getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(long updatedAt) {
    this.updatedAt = updatedAt;
  }

  /** Build the storage key for a given provider+model pair. */
  public static String key(String provider, String model) {
    return (provider == null ? "" : provider) + "|" + (model == null ? "" : model);
  }
}
