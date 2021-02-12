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

public abstract class AbstractSecuredStoragePluginConfig extends StoragePluginConfig {

  protected final CredentialsProvider credentialsProvider;
  protected boolean directCredentials;

  public AbstractSecuredStoragePluginConfig(CredentialsProvider credentialsProvider, boolean directCredentials) {
    this.credentialsProvider = credentialsProvider;
    this.directCredentials = directCredentials;
  }

  public AbstractSecuredStoragePluginConfig() {
    this.credentialsProvider = PlainCredentialsProvider.EMPTY_CREDENTIALS_PROVIDER;
    this.directCredentials = true;
  }

  public CredentialsProvider getCredentialsProvider() {
    if (directCredentials) {
      return null;
    }
    return credentialsProvider;
  }
}
