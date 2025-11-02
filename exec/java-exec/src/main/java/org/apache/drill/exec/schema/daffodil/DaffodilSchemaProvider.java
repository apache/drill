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
package org.apache.drill.exec.schema.daffodil;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;

/**
 * Class for managing Daffodil schemata.  Schemata will be obtained via CREATE DAFFODIL SCHEMA queries.
 */
public class DaffodilSchemaProvider implements AutoCloseable {

  private RemoteDaffodilSchemaRegistry remoteDaffodilSchemaRegistry;

  public DaffodilSchemaProvider(DrillbitContext context) {
    this(context.getConfig(), context.getStoreProvider(), context.getClusterCoordinator());
  }

  public DaffodilSchemaProvider(DrillConfig config, ScanResult classpathScan) {
    // This constructor is incomplete - needs StoreProvider and ClusterCoordinator
  }

  public DaffodilSchemaProvider(DrillConfig config, PersistentStoreProvider storeProvider, ClusterCoordinator coordinator) {
    this.remoteDaffodilSchemaRegistry = new RemoteDaffodilSchemaRegistry();
    this.remoteDaffodilSchemaRegistry.init(config, storeProvider, coordinator);
  }

  public RemoteDaffodilSchemaRegistry getRemoteDaffodilSchemaRegistry() {
    return remoteDaffodilSchemaRegistry;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.closeSilently(remoteDaffodilSchemaRegistry);
  }
}
