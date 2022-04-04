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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractSecuredStoragePluginConfig extends StoragePluginConfig {

  private static final Logger logger = LoggerFactory.getLogger(AbstractSecuredStoragePluginConfig.class);
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
    logger.debug("Enabled status");
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
