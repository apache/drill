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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;

import java.io.IOException;
import java.util.Map;

public abstract class AbstractSecuredStoragePluginConfig extends StoragePluginConfig {

  protected final CredentialsProvider credentialsProvider;
  protected final Boolean userImpersonation;
  protected boolean directCredentials;

  public AbstractSecuredStoragePluginConfig() {
    this(PlainCredentialsProvider.EMPTY_CREDENTIALS_PROVIDER,  true);
  }

  public AbstractSecuredStoragePluginConfig(CredentialsProvider credentialsProvider, boolean directCredentials) {
    this.credentialsProvider = credentialsProvider;
    this.directCredentials = directCredentials;
    this.userImpersonation = false;
  }

  public AbstractSecuredStoragePluginConfig(CredentialsProvider credentialsProvider, boolean directCredentials, boolean userImpersonation) {
    this.directCredentials = directCredentials;
    if (directCredentials) {
      this.userImpersonation = false;
    } else {
      this.userImpersonation = userImpersonation;
    }
    // Recreate credential provider with per user credentials
    this.credentialsProvider = credentialsProvider;
  }

  public CredentialsProvider getCredentialsProvider() {
    if (directCredentials) {
      return null;
    }
    return credentialsProvider;
  }

  public boolean getUserImpersonation() {
    return userImpersonation != null && userImpersonation;
  }

  // TODO Implement me
  public boolean hasValidCredentials(String activeUser) {
    return false;
  }

  public abstract AbstractSecuredStoragePluginConfig updateCredentialProvider(CredentialsProvider credentialsProvider);

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

    if (!userImpersonation || StringUtils.isEmpty(activeUser)) {
      return super.isEnabled();
    } else {
      /*
        If the credential provider isn't null, but the credentials are missing,
        disable the plugin.  Also disable it if the credential provider is null.
       */
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
    }
  }
}
