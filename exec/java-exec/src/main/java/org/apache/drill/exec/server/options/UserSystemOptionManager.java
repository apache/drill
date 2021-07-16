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
package org.apache.drill.exec.server.options;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.LogicalPlanPersistence;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.drill.exec.store.sys.PersistentStore.USER_PSTORE_ROOT_NAME;

/**
 * It is the same as {@link SystemOptionManager}, but created for every {@link UserSession} in case
 * {@link org.apache.drill.exec.ExecConstants#SEPARATE_WORKSPACE} enabled
 */
public class UserSystemOptionManager extends SystemOptionManager {
  private static final Logger logger = LoggerFactory.getLogger(UserSystemOptionManager.class);

  public UserSystemOptionManager(SystemOptionManager sysManager, LogicalPlanPersistence lpPersistence, PersistentStoreProvider provider,
                                 DrillConfig bootConfig, CaseInsensitiveMap<OptionDefinition> definitions,
                                 UserSession session) {
    super(lpPersistence, provider, bootConfig, definitions, String.join("/", USER_PSTORE_ROOT_NAME,
            session.getCredentials().getUserName(), OPTIONS_PSTORE_NAME));
    init(sysManager);
  }

  public void init(SystemOptionManager sysManager) {
    super.init();
    for (final Map.Entry<String, PersistedOptionValue> entry : Lists.newArrayList(sysManager.options.getAll())) {
      final String name = entry.getKey();
      final OptionDefinition optionDefinition = getOptionDefinition(name);
      final PersistedOptionValue persistedOptionValue = entry.getValue();
      final OptionValue optionValue = persistedOptionValue
              .toOptionValue(optionDefinition, OptionValue.OptionScope.SYSTEM);
      options.put(optionDefinition.getValidator().getOptionName().toLowerCase(), optionValue.toPersisted());
    }
  }
}
