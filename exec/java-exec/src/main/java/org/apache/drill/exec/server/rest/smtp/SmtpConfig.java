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
package org.apache.drill.exec.server.rest.smtp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration POJO for SMTP email settings.
 * Stored via PersistentStore.
 */
public class SmtpConfig {

  @JsonProperty
  private String host;

  @JsonProperty
  private int port;

  @JsonProperty
  private String username;

  @JsonProperty
  private String password;

  @JsonProperty
  private String fromAddress;

  @JsonProperty
  private String fromName;

  @JsonProperty
  private String encryption;

  @JsonProperty
  private boolean enabled;

  public SmtpConfig() {
    this.host = "";
    this.port = 587;
    this.username = "";
    this.password = "";
    this.fromAddress = "";
    this.fromName = "Apache Drill";
    this.encryption = "starttls";
    this.enabled = false;
  }

  @JsonCreator
  public SmtpConfig(
      @JsonProperty("host") String host,
      @JsonProperty("port") int port,
      @JsonProperty("username") String username,
      @JsonProperty("password") String password,
      @JsonProperty("fromAddress") String fromAddress,
      @JsonProperty("fromName") String fromName,
      @JsonProperty("encryption") String encryption,
      @JsonProperty("enabled") boolean enabled) {
    this.host = host;
    this.port = port;
    this.username = username;
    this.password = password;
    this.fromAddress = fromAddress;
    this.fromName = fromName != null ? fromName : "Apache Drill";
    this.encryption = encryption != null ? encryption : "starttls";
    this.enabled = enabled;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getFromAddress() {
    return fromAddress;
  }

  public void setFromAddress(String fromAddress) {
    this.fromAddress = fromAddress;
  }

  public String getFromName() {
    return fromName;
  }

  public void setFromName(String fromName) {
    this.fromName = fromName;
  }

  public String getEncryption() {
    return encryption;
  }

  public void setEncryption(String encryption) {
    this.encryption = encryption;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }
}
