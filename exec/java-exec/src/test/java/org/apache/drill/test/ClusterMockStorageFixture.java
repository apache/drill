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
package org.apache.drill.test;

import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.StoragePluginRegistryImpl;
import org.apache.drill.exec.store.mock.MockBreakageStorage;
import org.apache.drill.exec.store.mock.MockStorageEngineConfig;

public class ClusterMockStorageFixture extends ClusterFixture {
  ClusterMockStorageFixture(ClusterFixtureBuilder builder) {
    super(builder);

  }

  /**
   * This should be called after bits are started
   * @param name nthe mock storage name we are going to create
   */
  public void insertMockStorage(String name, boolean breakRegisterSchema) {
    for (Drillbit bit : drillbits()) {

      // Bit name and registration.
      final StoragePluginRegistry pluginRegistry = bit.getContext().getStorage();
      MockStorageEngineConfig config = MockStorageEngineConfig.INSTANCE;
      @SuppressWarnings("resource")
      MockBreakageStorage plugin = new MockBreakageStorage(
          MockStorageEngineConfig.INSTANCE, bit.getContext(), name);
      config.setEnabled(true);
      ((StoragePluginRegistryImpl) pluginRegistry).addPluginToPersistentStoreIfAbsent(name, config, plugin);

      plugin.setBreakRegister(breakRegisterSchema);
    }
  }

}
