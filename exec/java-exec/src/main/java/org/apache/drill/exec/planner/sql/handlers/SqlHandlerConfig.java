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
package org.apache.drill.exec.planner.sql.handlers;

import java.util.Collection;
import java.util.Map.Entry;

import org.apache.calcite.tools.RuleSet;
import org.apache.drill.common.logical.AbstractSecuredStoragePluginConfig;
import org.apache.drill.common.logical.AbstractSecuredStoragePluginConfig.AuthMode;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.PlannerPhase;
import org.apache.drill.exec.planner.sql.conversion.SqlConverter;
import org.apache.drill.exec.store.StoragePlugin;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

public class SqlHandlerConfig {

  private final QueryContext context;
  private final SqlConverter converter;

  public SqlHandlerConfig(QueryContext context, SqlConverter converter) {
    super();
    this.context = context;
    this.converter = converter;
  }

  public QueryContext getContext() {
    return context;
  }

  public RuleSet getRules(PlannerPhase phase) {
    Collection<StoragePlugin> plugins = Lists.newArrayList();
    for (Entry<String, StoragePlugin> k : context.getStorage()) {
      StoragePlugin plugin = k.getValue();
      if (verifyPlugin(plugin)) {
        plugins.add(plugin);
      } else {
        // Remove plugins with invalid credentials
        plugins.remove(plugin);
      }
    }
    return phase.getRules(context, plugins);
  }

  private boolean verifyPlugin(StoragePlugin plugin) {
    // First see if the plugin uses the AbstractSecuredPluginConfig or not
    StoragePluginConfig rawConfig = plugin.getJdbcStorageConfig();
    if (! (rawConfig instanceof AbstractSecuredStoragePluginConfig)) {
      return true;
    }

    // Next, we need to see whether user translation is activated.  If not, we're ok
    AbstractSecuredStoragePluginConfig securedConfig = (AbstractSecuredStoragePluginConfig) rawConfig;
    if (securedConfig.getAuthMode() != AuthMode.USER_TRANSLATION) {
      return true;
    }

    // At this point, we know that user translation is on and we need to verify that the plugin in question
    // has valid credentials, and by that, we simply mean that they are not null.
    CredentialsProvider provider = securedConfig.getCredentialsProvider();
    if (provider == null) {
      // In this case, if there is no credential provider, the plugin is not configured properly and should be ignored.
      return false;
    } else {
      return provider.hasValidUsername(context.getQueryUserName()) && provider.hasValidPassword(context.getQueryUserName());
    }
  }

  public SqlConverter getConverter() {
    return converter;
  }
}
