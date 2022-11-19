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
package org.apache.drill.exec.store.delta.format;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.metastore.MetadataProviderManager;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.AbstractWriter;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.PlannerPhase;
import org.apache.drill.exec.planner.common.DrillStatsTable;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.schema.SchemaProvider;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.PluginRulesProviderImpl;
import org.apache.drill.exec.store.StoragePluginRulesSupplier;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FormatMatcher;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.delta.DeltaGroupScan;
import org.apache.drill.exec.store.delta.plan.DeltaPluginImplementor;
import org.apache.drill.exec.store.parquet.ParquetReaderConfig;
import org.apache.drill.exec.store.plan.rel.PluginRel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class DeltaFormatPlugin implements FormatPlugin {

  private static final String DELTA_CONVENTION_PREFIX = "DELTA.";

  /**
   * Generator for format id values. Formats with the same name may be defined
   * in multiple storage plugins, so using the unique id within the convention name
   * to ensure the rule names will be unique for different plugin instances.
   */
  private static final AtomicInteger NEXT_ID = new AtomicInteger(0);

  private final FileSystemConfig storageConfig;

  private final DeltaFormatPluginConfig config;

  private final Configuration fsConf;

  private final DrillbitContext context;

  private final String name;

  private final DeltaFormatMatcher matcher;

  private final StoragePluginRulesSupplier storagePluginRulesSupplier;

  public DeltaFormatPlugin(
    String name,
    DrillbitContext context,
    Configuration fsConf,
    FileSystemConfig storageConfig,
    DeltaFormatPluginConfig config) {
    this.storageConfig = storageConfig;
    this.config = config;
    this.fsConf = fsConf;
    this.context = context;
    this.name = name;
    this.matcher = new DeltaFormatMatcher(this);
    this.storagePluginRulesSupplier = storagePluginRulesSupplier(name + NEXT_ID.getAndIncrement());
  }

  private static StoragePluginRulesSupplier storagePluginRulesSupplier(String name) {
    Convention convention = new Convention.Impl(DELTA_CONVENTION_PREFIX + name, PluginRel.class);
    return StoragePluginRulesSupplier.builder()
      .rulesProvider(new PluginRulesProviderImpl(convention, DeltaPluginImplementor::new))
      .supportsFilterPushdown(true)
      .supportsProjectPushdown(true)
      .supportsLimitPushdown(true)
      .convention(convention)
      .build();
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public boolean supportsWrite() {
    return false;
  }

  @Override
  public boolean supportsAutoPartitioning() {
    return false;
  }

  @Override
  public FormatMatcher getMatcher() {
    return matcher;
  }

  @Override
  public AbstractWriter getWriter(PhysicalOperator child, String location, List<String> partitionColumns) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<? extends RelOptRule> getOptimizerRules(PlannerPhase phase) {
    switch (phase) {
      case PHYSICAL:
      case LOGICAL:
        return storagePluginRulesSupplier.getOptimizerRules();
      case LOGICAL_PRUNE_AND_JOIN:
      case LOGICAL_PRUNE:
      case PARTITION_PRUNING:
      case JOIN_PLANNING:
      default:
        return Collections.emptySet();
    }
  }

  @Override
  public AbstractGroupScan getGroupScan(String userName, FileSelection selection, List<SchemaPath> columns) throws IOException {
    return getGroupScan(userName, selection, columns, (OptionManager) null);
  }

  @Override
  public AbstractGroupScan getGroupScan(String userName, FileSelection selection, List<SchemaPath> columns, OptionManager options) throws IOException {
    return getGroupScan(userName, selection, columns, options, null);
  }

  @Override
  public AbstractGroupScan getGroupScan(String userName, FileSelection selection,
    List<SchemaPath> columns, OptionManager options, MetadataProviderManager metadataProviderManager) throws IOException {
    ParquetReaderConfig readerConfig = ParquetReaderConfig.builder()
      .withConf(fsConf)
      .withOptions(options)
      .build();
    return DeltaGroupScan.builder()
      .userName(userName)
      .formatPlugin(this)
      .readerConfig(readerConfig)
      .path(selection.selectionRoot.toUri().getPath())
      .columns(columns)
      .limit(-1)
      .build();
  }

  @Override
  public AbstractGroupScan getGroupScan(String userName, FileSelection selection,
    List<SchemaPath> columns, MetadataProviderManager metadataProviderManager) throws IOException {
    SchemaProvider schemaProvider = metadataProviderManager.getSchemaProvider();
    TupleMetadata schema = schemaProvider != null
      ? schemaProvider.read().getSchema()
      : null;
    return DeltaGroupScan.builder()
      .userName(userName)
      .formatPlugin(this)
      .readerConfig(ParquetReaderConfig.builder().withConf(fsConf).build())
      .schema(schema)
      .path(selection.selectionRoot.toUri().getPath())
      .columns(columns)
      .limit(-1)
      .build();
  }

  @Override
  public boolean supportsStatistics() {
    return false;
  }

  @Override
  public DrillStatsTable.TableStatistics readStatistics(FileSystem fs, Path statsTablePath) {
    throw new UnsupportedOperationException("unimplemented");
  }

  @Override
  public void writeStatistics(DrillStatsTable.TableStatistics statistics, FileSystem fs, Path statsTablePath) {
    throw new UnsupportedOperationException("unimplemented");
  }

  @Override
  public DeltaFormatPluginConfig getConfig() {
    return config;
  }

  @Override
  public FileSystemConfig getStorageConfig() {
    return storageConfig;
  }

  @Override
  public Configuration getFsConf() {
    return fsConf;
  }

  @Override
  public DrillbitContext getContext() {
    return context;
  }

  @Override
  public String getName() {
    return name;
  }

  public Convention getConvention() {
    return storagePluginRulesSupplier.convention();
  }

}
