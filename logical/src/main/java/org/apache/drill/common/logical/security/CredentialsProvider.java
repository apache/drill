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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.drill.common.exceptions.UserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Provider of authentication credentials.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
  property = "credentialsProviderType",
  defaultImpl = PlainCredentialsProvider.class)
public interface CredentialsProvider {
  Logger logger = LoggerFactory.getLogger(CredentialsProvider.class);
  /**
   * Returns map with authentication credentials. Key is the credential name, for example {@code "username"}
   * and map value is corresponding credential value.
   */
  @JsonIgnore
  Map<String, String> getCredentials();

  /**
   * This method returns the credentials associated with a specific user.
   * @param username The logged in username
   * @return A Map of the logged in user's credentials.
   */
  @JsonIgnore
  default Map<String, String> getUserCredentials(String username) {
    throw UserException.unsupportedError()
      .message("%s does not support per-user credentials.", getClass())
      .build(logger);
  }

  @JsonIgnore
  default void setUserCredentials(String username, String password, String queryUser) {
    throw UserException.unsupportedError()
      .message("%s does not support per-user credentials.", getClass())
      .build(logger);
  }
}
