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
package org.apache.drill.common.logical.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.drill.common.PlanStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Implementation of {@link CredentialsProvider} that holds credentials provided by user.
 * <p>
 * Its constructor accepts a map with credential names as keys and values as corresponding credential values.
 */
public class PlainCredentialsProvider implements CredentialsProvider {
  private static final Logger logger = LoggerFactory.getLogger(PlainCredentialsProvider.class);
  public static final CredentialsProvider EMPTY_CREDENTIALS_PROVIDER =
    new PlainCredentialsProvider(Collections.emptyMap());

  private final Map<String, String> credentials;
  private final Map<String, Map<String, String>> userCredentials;

  public PlainCredentialsProvider(Map<String, String> credentials) {
   this(credentials, new HashMap<>());
  }

  @JsonCreator
  public PlainCredentialsProvider(
    @JsonProperty("credentials") Map<String, String> credentials,
    @JsonProperty("userCredentials") Map<String, Map<String, String>> userCredentials
  ) {
    this.credentials = Optional.ofNullable(credentials).orElse(new HashMap<>());
    this.userCredentials = Optional.ofNullable(userCredentials).orElse(new HashMap<>());
  }

  @JsonIgnore
  public PlainCredentialsProvider(String username, Map<String, String> credentials) {
    this.credentials = new HashMap<>();
    this.userCredentials = new HashMap<>();
    userCredentials.put(username,credentials);
  }

  @Override
  @JsonIgnore(false)
  @JsonProperty("credentials") public Map<String, String> getCredentials() {
    return credentials;
  }

  @JsonProperty("userCredentials")
  @JsonInclude(Include.NON_NULL)
  public Map<String, Map<String, String>> getUserCredentials() {
    return userCredentials;
  }

  /**
   * Returns the credentials for a given query user.  If that user does not have credentials,
   * the function will add an entry for that user with keys username, password which are both null.
   * @param queryUser A String of the currently logged in user
   * @return A Map of the active user's credentials
   */
  @Override
  public Map<String, String> getCredentials(String queryUser) {
    assert queryUser != null;
    logger.debug("Getting credentials for query user {}", queryUser);

    return userCredentials.getOrDefault(queryUser, new HashMap<>());
  }

  @Override
  public void setUserCredentials(String username, String password, String queryUser) {
    assert queryUser != null;
    logger.debug("Setting credentials for query user {}", queryUser);

    Map<String, String> creds = userCredentials.computeIfAbsent(
      queryUser,
      c -> new HashMap<String, String>()
    );
    creds.put("username", username);
    creds.put("password", password);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PlainCredentialsProvider that = (PlainCredentialsProvider) o;
    return Objects.equals(credentials, that.credentials) &&
      Objects.equals(userCredentials, that.userCredentials);
  }

  @Override
  public int hashCode() {
    return Objects.hash(credentials, userCredentials);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("credentials", credentials)
      .field("userCredentials", userCredentials)
      .toString();
  }
}
