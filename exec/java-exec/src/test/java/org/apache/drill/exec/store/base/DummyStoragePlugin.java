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
import java.util.Set;

import org.apache.drill.common.exceptions.ChildErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.metastore.MetadataProviderManager;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework.ReaderFactory;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework.ScanFrameworkBuilder;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.planner.PlannerPhase;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.SessionOptionManager;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.base.filter.RelOp;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * "Test mule" for the base storage plugin and the filter push down
 * framework.
 */

public class DummyStoragePlugin
  extends BaseStoragePlugin<DummyStoragePluginConfig> {

  private static class DummyScanFactory extends
        BaseScanFactory<DummyStoragePlugin, DummyScanSpec, DummyGroupScan, DummySubScan> {

    @Override
    public DummyGroupScan newGroupScan(DummyStoragePlugin storagePlugin,
        String userName, DummyScanSpec scanSpec,
        SessionOptionManager sessionOptions,
        MetadataProviderManager metadataProviderManager) {

      // Force user name to "dummy" so golden and actual test files are stable
      return new DummyGroupScan(storagePlugin, "dummy", scanSpec);
    }

    @Override
    public DummyGroupScan groupWithColumns(DummyGroupScan group,
        List<SchemaPath> columns) {
      return new DummyGroupScan(group, columns);
    }

    @Override
    public ScanFrameworkBuilder scanBuilder(DummyStoragePlugin storagePlugin,
        OptionManager options, DummySubScan subScan) {
      ScanFrameworkBuilder builder = new ScanFrameworkBuilder();
      storagePlugin.initFramework(builder, subScan);
      ReaderFactory readerFactory = new DummyReaderFactory(storagePlugin.config(), subScan);
      builder.setReaderFactory(readerFactory);
      builder.setContext(
        new ChildErrorContext(builder.errorContext()) {
          @Override
          public void addContext(UserException.Builder builder) {
            builder.addContext("Table:", subScan.scanSpec().tableName());
          }
        });
      return builder;
    }
  }

  public DummyStoragePlugin(DummyStoragePluginConfig config,
      DrillbitContext context, String name) {
    super(context, config, name, buildOptions(config));
    schemaFactory = new DummySchemaFactory(this);
  }

  private static StoragePluginOptions buildOptions(DummyStoragePluginConfig config) {
    StoragePluginOptions options = new StoragePluginOptions();
    options.supportsRead = true;
    options.supportsProjectPushDown = config.enableProjectPushDown();
    options.nullType = Types.optional(MinorType.VARCHAR);
    options.scanSpecType = new TypeReference<DummyScanSpec>() { };
    options.scanFactory = new DummyScanFactory();
    return options;
  }

  @Override
  public Set<? extends StoragePluginOptimizerRule> getOptimizerRules(OptimizerRulesContext optimizerContext, PlannerPhase phase) {

    // Push-down planning is done at the logical phase so it can
    // influence parallelization in the physical phase. Note that many
    // existing plugins perform filter push-down at the physical
    // phase.

    if (phase.isFilterPushDownPhase() && config.enableFilterPushDown()) {
      return DummyFilterPushDownListener.rulesFor(optimizerContext, config);
    }
    return ImmutableSet.of();
  }

  private static class DummyReaderFactory implements ReaderFactory {

    private final DummyStoragePluginConfig config;
    private final DummySubScan subScan;
    private final int readerCount;
    private int readerIndex;

    public DummyReaderFactory(DummyStoragePluginConfig config, DummySubScan subScan) {
      this.config = config;
      this.subScan = subScan;
      List<List<RelOp>> filters = subScan.filters();
      readerCount = filters == null || filters.isEmpty() ? 1 : filters.size();
    }

    @Override
    public void bind(ManagedScanFramework framework) { }

    @Override
    public ManagedReader<? extends SchemaNegotiator> next() {
      if (readerIndex >= readerCount) {
        return null;
      }
      List<RelOp> filters;
      if (subScan.filters() == null) {
        filters = null;
      } else {
        filters = subScan.filters().get(readerIndex);
      }
      readerIndex++;
      return new DummyBatchReader(config, subScan.columns(), filters);
    }
  }
}
