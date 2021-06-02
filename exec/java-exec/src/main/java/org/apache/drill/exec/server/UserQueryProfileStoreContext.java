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
package org.apache.drill.exec.server;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;

/**
 * It is the same as {@link UserQueryProfileStoreContext}, but created for every {@link UserSession} in case
 * {@link org.apache.drill.exec.ExecConstants#SEPARATE_WORKSPACE} enabled
 */
// todo: do we need it?
public class UserQueryProfileStoreContext extends QueryProfileStoreContext {

  private static final String PROFILES = "profiles";
  private static final String RUNNING = "running";
//  public static final String OPTIONS_PSTORE_NAME = "options";
//  public static final String SYS_OPTIONS_PSTORE_NAME = "sys." + OPTIONS_PSTORE_NAME;

  public UserQueryProfileStoreContext(DrillConfig config, PersistentStoreProvider storeProvider,
                                      ClusterCoordinator coordinator) {
    super(config, storeProvider, coordinator);
  }
}
