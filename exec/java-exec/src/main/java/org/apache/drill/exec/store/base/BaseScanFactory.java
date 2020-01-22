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
package org.apache.drill.exec.store.base;

import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.metastore.MetadataProviderManager;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework.ScanFrameworkBuilder;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.SessionOptionManager;

/**
 * Creates the multiple classes needed to plan and execute a scan. Centralizes
 * most implementation-specific object creation in one location to avoid needing
 * to implement many methods just for type-specific creation.
 *
 * @param <PLUGIN> the storage plugin class
 * @param <SPEC> the scan specification class
 * @param <GROUP> the group scan class
 * @param <SUB> the sub scan class
 */
public abstract class BaseScanFactory<
      PLUGIN extends BaseStoragePlugin<?>,
      SPEC,
      GROUP extends BaseGroupScan,
      SUB extends BaseSubScan> {

  @SuppressWarnings("unchecked")
  protected BaseGroupScan newGroupScanShim(BaseStoragePlugin<?> plugin,
      String userName, Object scanSpec,
      SessionOptionManager sessionOptions, MetadataProviderManager metadataProviderManager) {
    return newGroupScan(
        (PLUGIN) plugin, userName, (SPEC) scanSpec,
        sessionOptions, metadataProviderManager);
  }

  @SuppressWarnings("unchecked")
  protected ScanFrameworkBuilder scanBuilderShim(BaseStoragePlugin<?> plugin,
      OptionManager options, BaseSubScan subScan) {
    return scanBuilder((PLUGIN) plugin, options, (SUB) subScan);
  }

  @SuppressWarnings("unchecked")
  protected GROUP groupWithColumnsShim(BaseGroupScan group, List<SchemaPath> columns) {
    return groupWithColumns((GROUP) group, columns);
  }

  @SuppressWarnings("unchecked")
  protected GROUP copyGroupShim(BaseGroupScan group) {
    return copyGroup((GROUP) group);
  }

  public abstract GROUP newGroupScan(PLUGIN storagePlugin, String userName, SPEC scanSpec,
      SessionOptionManager sessionOptions,
      MetadataProviderManager metadataProviderManager);

  public abstract GROUP groupWithColumns(GROUP group, List<SchemaPath> columns);

  public GROUP copyGroup(GROUP group) {
    // Default implementation works for immutable group scans
    return group;
  }

  public abstract ScanFrameworkBuilder scanBuilder(PLUGIN storagePlugin, OptionManager options, SUB subScan);
}
