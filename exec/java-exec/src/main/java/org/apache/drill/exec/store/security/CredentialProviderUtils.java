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
package org.apache.drill.exec.store.security;

import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;

public class CredentialProviderUtils {

  /**
   * Returns specified {@code CredentialsProvider credentialsProvider}
   * if it is not null or builds and returns {@link PlainCredentialsProvider}
   * with specified {@code USERNAME} and {@code PASSWORD}.
   */
  public static CredentialsProvider getCredentialsProvider(
      String username, String password,
      CredentialsProvider credentialsProvider) {
    if (credentialsProvider != null) {
      return credentialsProvider;
    }
    ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();
    if (username != null) {
      mapBuilder.put(UsernamePasswordCredentials.USERNAME, username);
    }
    if (password != null) {
      mapBuilder.put(UsernamePasswordCredentials.PASSWORD, password);
    }
    return new PlainCredentialsProvider(mapBuilder.build());
  }
}
