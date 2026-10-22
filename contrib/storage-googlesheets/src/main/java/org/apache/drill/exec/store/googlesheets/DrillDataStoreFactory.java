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

package org.apache.drill.exec.store.googlesheets;

import com.google.api.client.util.store.AbstractDataStoreFactory;
import com.google.api.client.util.store.DataStore;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.exec.oauth.OAuthTokenProvider;

import java.io.Serializable;

public class DrillDataStoreFactory extends AbstractDataStoreFactory {
  private final OAuthTokenProvider tokenProvider;
  private final String pluginName;
  private final String registryIdentity;

  /**
   * @param registryIdentity the identity whose Drill token registry backs these
   *     data stores: the query user for a {@code USER_TRANSLATION} plugin, or
   *     null for the shared registry. This is deliberately separate from the
   *     Google-side data store id passed to {@link #createDataStore(String)},
   *     which must be a non-null string and is not a Drill identity.
   */
  public DrillDataStoreFactory(OAuthTokenProvider tokenProvider, String pluginName,
                               String registryIdentity) {
    this.tokenProvider = tokenProvider;
    this.pluginName = pluginName;
    this.registryIdentity = registryIdentity;
  }

  public String getRegistryIdentity() {
    return registryIdentity;
  }

  @Override
  protected <V extends Serializable> DataStore<V> createDataStore(String id) {
    return new DrillDataStore<>(tokenProvider, pluginName, id, this);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("tokenProvider", tokenProvider)
      .field("pluginName", pluginName)
      .field("registryIdentity", registryIdentity)
      .toString();
  }
}
