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
package org.apache.drill.common.logical;

import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

public abstract class AbstractSecuredStoragePluginConfig extends StoragePluginConfig {

  protected final CredentialsProvider credentialsProvider;
  protected boolean inlineCredentials;
  public final AuthMode authMode;

  public AbstractSecuredStoragePluginConfig() {
    this(PlainCredentialsProvider.EMPTY_CREDENTIALS_PROVIDER,  true, AuthMode.SHARED_USER);
  }

  public AbstractSecuredStoragePluginConfig(CredentialsProvider credentialsProvider, boolean inlineCredentials) {
    this(credentialsProvider, inlineCredentials, AuthMode.SHARED_USER);
  }

  public AbstractSecuredStoragePluginConfig(
    CredentialsProvider credentialsProvider,
    boolean inlineCredentials,
    AuthMode authMode
  ) {
    this.credentialsProvider = credentialsProvider;
    this.inlineCredentials = inlineCredentials;
    this.authMode = authMode;
  }

  public abstract AbstractSecuredStoragePluginConfig updateCredentialProvider(CredentialsProvider credentialsProvider);

  public AuthMode getAuthMode() {
    return authMode;
  }

  @Override
  public boolean isEnabled() {
     /*
     This method overrides the isEnabled method of the StoragePlugin when per-user credentials are enabled.
     The issue that arises is that per-user credentials are enabled and a user_a has set up the creds, but user_b
     has not, some plugins will not initialize which could potentially destabilize Drill.  This function thus treats
     plugins as disabled when user impersonation is enabled AND the active user's credentials are null.

     This only applies to plugins which use the AbstractSecuredPluginConfig, IE: not file based plugins, or
     HBase and a few others.  This doesn't actually disable the plugin, but when a user w/o credentials tries to access
     the plugin, either in a query or via info_schema queries, the plugin will act as if it is disabled for that user.
     */

    String activeUser;
    String otherUser;
    try {
      // TODO Fix me...  We need to get the correct logged in user
      otherUser = UserGroupInformation.getCurrentUser().getShortUserName();
      activeUser = UserGroupInformation.getLoginUser().getShortUserName();
    } catch (IOException e) {
      activeUser = null;
    }

    /*if (authMode != AuthMode.USER_TRANSLATION || StringUtils.isEmpty(activeUser)) {
      return super.isEnabled();
    } else {
       /*
         If the credential provider isn't null, but the credentials are missing,
         disable the plugin.  Also disable it if the credential provider is null.

      if (credentialsProvider != null) {
        Map<String, String> credentials = credentialsProvider.getCredentials(activeUser);
        if (credentials.isEmpty() ||
          ! credentials.containsKey("username") ||
          ! credentials.containsKey("password") ||
          StringUtils.isEmpty(credentials.get("username")) ||
          StringUtils.isEmpty(credentials.get("password"))) {
          return false;
        }
      } else {
        // Case for null credential provider.
        return false;
      }
      return super.isEnabled();
    }*/
    return super.isEnabled();
  }


  public CredentialsProvider getCredentialsProvider() {
    if (inlineCredentials) {
      return null;
    }
    return credentialsProvider;
  }

  public enum AuthMode {
    /**
     * Connects with either the Drill process user or a shared user with stored credentials.
     * Default.  Unaffected by user impersonation in Drill.
     */
    SHARED_USER,
    /**
     * As above but, in combination with the storage, then impersonates the query user.
     * Only useful when Drill user impersonation is active.
     */
    USER_IMPERSONATION,
    /**
     * Connects with stored credentials looked up for (translated from) the query user.
     * Only useful when Drill user impersonation is active.
     */
    USER_TRANSLATION
  }
}
