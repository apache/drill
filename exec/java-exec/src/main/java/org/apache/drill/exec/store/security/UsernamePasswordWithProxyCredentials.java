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
import org.apache.drill.exec.store.security.oauth.OAuthTokenCredentials;

import java.util.HashMap;
import java.util.Map;

public class UsernamePasswordWithProxyCredentials extends UsernamePasswordCredentials {
  private final String proxyUsername;
  private final String proxyPassword;

  public UsernamePasswordWithProxyCredentials(CredentialsProvider credentialsProvider) {
    super(credentialsProvider);
    if (credentialsProvider == null || credentialsProvider.getCredentials() == null) {
      this.proxyUsername = null;
      this.proxyPassword = null;
    } else {
      Map<String, String> credentials = credentialsProvider.getCredentials() == null ? new HashMap<>() : credentialsProvider.getCredentials();
      this.proxyUsername = credentials.get(OAuthTokenCredentials.PROXY_USERNAME);
      this.proxyPassword = credentials.get(OAuthTokenCredentials.PROXY_PASSWORD);
    }
  }

  public String getProxyUsername() {
    return proxyUsername;
  }

  public String getProxyPassword() {
    return proxyPassword;
  }
}
