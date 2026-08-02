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
package org.apache.drill.exec.store.accumulo;

import java.util.Base64;
import java.util.Objects;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken.AuthenticationTokenSerializer;
import org.apache.accumulo.core.client.security.tokens.DelegationToken;
import org.apache.drill.common.PlanStringBuilder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Serializable wrapper for Accumulo delegation tokens.
 *
 * <p>This class enables delegation tokens to be passed across Drill's distributed
 * execution pipeline via JSON serialization. The token is stored as a Base64-encoded
 * string for safe transport through JSON.</p>
 *
 * <p>Usage flow:</p>
 * <ol>
 *   <li>Service client obtains a delegation token for a user</li>
 *   <li>Token is wrapped in DelegationTokenInfo and attached to AccumuloGroupScan</li>
 *   <li>Token is serialized to JSON for distributed planning</li>
 *   <li>At execution time, token is deserialized and used to create a client</li>
 * </ol>
 */
public class DelegationTokenInfo {

  /**
   * The username this delegation token was created for.
   */
  private final String userName;

  /**
   * Base64-encoded serialized delegation token.
   * Using Base64 string instead of raw byte[] for safer JSON serialization.
   */
  private final String serializedToken;

  /**
   * The fully qualified class name of the token implementation.
   * Needed for deserialization.
   */
  private final String tokenClassName;

  /**
   * Time when this delegation token was created (epoch millis).
   * Used for cache eviction and token refresh decisions.
   */
  private final long creationTime;

  @JsonCreator
  public DelegationTokenInfo(
      @JsonProperty("userName") String userName,
      @JsonProperty("serializedToken") String serializedToken,
      @JsonProperty("tokenClassName") String tokenClassName,
      @JsonProperty("creationTime") long creationTime) {
    this.userName = userName;
    this.serializedToken = serializedToken;
    this.tokenClassName = tokenClassName;
    this.creationTime = creationTime;
  }

  /**
   * Creates a DelegationTokenInfo from an Accumulo DelegationToken.
   *
   * @param userName the user this token is for
   * @param token the Accumulo delegation token
   * @return a new DelegationTokenInfo
   */
  public static DelegationTokenInfo fromDelegationToken(String userName, DelegationToken token) {
    byte[] tokenBytes = AuthenticationTokenSerializer.serialize(token);
    String serialized = Base64.getEncoder().encodeToString(tokenBytes);
    String className = token.getClass().getName();
    return new DelegationTokenInfo(userName, serialized, className, System.currentTimeMillis());
  }

  /**
   * Converts this wrapper back to an Accumulo AuthenticationToken.
   *
   * <p>Note: This returns an AuthenticationToken (the parent interface) rather than
   * DelegationToken because the deserialization uses the stored class name.</p>
   *
   * @return the deserialized AuthenticationToken
   */
  @SuppressWarnings("unchecked")
  @JsonIgnore
  public AuthenticationToken toAuthenticationToken() {
    byte[] tokenBytes = Base64.getDecoder().decode(serializedToken);
    try {
      Class<? extends AuthenticationToken> tokenClass =
          (Class<? extends AuthenticationToken>) Class.forName(tokenClassName);
      return AuthenticationTokenSerializer.deserialize(tokenClass, tokenBytes);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to load token class: " + tokenClassName, e);
    }
  }

  @JsonProperty("userName")
  public String getUserName() {
    return userName;
  }

  @JsonProperty("serializedToken")
  public String getSerializedToken() {
    return serializedToken;
  }

  @JsonProperty("tokenClassName")
  public String getTokenClassName() {
    return tokenClassName;
  }

  @JsonProperty("creationTime")
  public long getCreationTime() {
    return creationTime;
  }

  /**
   * Returns the age of this token in milliseconds.
   */
  @JsonIgnore
  public long getAgeMillis() {
    return System.currentTimeMillis() - creationTime;
  }

  /**
   * Checks if this token is older than the specified age.
   *
   * @param maxAgeMillis maximum acceptable age in milliseconds
   * @return true if the token is older than maxAgeMillis
   */
  @JsonIgnore
  public boolean isOlderThan(long maxAgeMillis) {
    return getAgeMillis() > maxAgeMillis;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DelegationTokenInfo that = (DelegationTokenInfo) o;
    return creationTime == that.creationTime
        && Objects.equals(userName, that.userName)
        && Objects.equals(serializedToken, that.serializedToken)
        && Objects.equals(tokenClassName, that.tokenClassName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(userName, serializedToken, tokenClassName, creationTime);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
        .field("userName", userName)
        .field("tokenClassName", tokenClassName)
        .field("creationTime", creationTime)
        .field("tokenLength", serializedToken != null ? serializedToken.length() : 0)
        .toString();
  }
}
