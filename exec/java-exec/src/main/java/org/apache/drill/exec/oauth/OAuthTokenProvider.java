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
package org.apache.drill.exec.oauth;

import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.exec.server.DrillbitContext;

/**
 * Class for managing oauth tokens.  Storage plugins will have to manage obtaining the plugins, but
 * these classes handle the storage of access and refresh tokens.
 */
public class OAuthTokenProvider implements AutoCloseable {
  private static final String STORAGE_REGISTRY_PATH = "oauth_tokens";

  private final DrillbitContext context;

  private PersistentTokenRegistry oauthTokenRegistry;

  public OAuthTokenProvider(DrillbitContext context) {
    this.context = context;
  }

  public TokenRegistry getOauthTokenRegistry(String username) {
    if (oauthTokenRegistry == null) {
      initRemoteRegistries(username);
    }
    return oauthTokenRegistry;
  }

  private synchronized void initRemoteRegistries(String username) {
    // Add the username to the path if present
    String finalpath;
    if (StringUtils.isNotEmpty(username)) {
      finalpath = STORAGE_REGISTRY_PATH + "/" + username;
    } else {
      finalpath = STORAGE_REGISTRY_PATH;
    }

    if (oauthTokenRegistry == null) {
      oauthTokenRegistry = new PersistentTokenRegistry(context, finalpath);
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.closeSilently(oauthTokenRegistry);
  }
}
