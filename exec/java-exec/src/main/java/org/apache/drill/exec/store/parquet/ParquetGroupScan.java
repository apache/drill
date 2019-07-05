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
package org.apache.drill.exec.store.parquet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.metastore.FileSystemMetadataProviderManager;
import org.apache.drill.exec.metastore.MetadataProviderManager;
import org.apache.drill.exec.metastore.ParquetTableMetadataProvider;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.metastore.metadata.LocationProvider;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.ColumnExplorer;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

@JsonTypeName("parquet-scan")
public class ParquetGroupScan extends AbstractParquetGroupScan {

  private final ParquetFormatPlugin formatPlugin;
  private final ParquetFormatConfig formatConfig;

  private boolean usedMetadataCache; // false by default
  // may change when filter push down / partition pruning is applied
  private Path selectionRoot;
  private Path cacheFileRoot;

  @JsonCreator
  public ParquetGroupScan(@JacksonInject StoragePluginRegistry engineRegistry,
                          @JsonProperty("userName") String userName,
                          @JsonProperty("entries") List<ReadEntryWithPath> entries,
                          @JsonProperty("storage") StoragePluginConfig storageConfig,
                          @JsonProperty("format") FormatPluginConfig formatConfig,
                          @JsonProperty("columns") List<SchemaPath> columns,
                          @JsonProperty("selectionRoot") Path selectionRoot,
                          @JsonProperty("cacheFileRoot") Path cacheFileRoot,
                          @JsonProperty("readerConfig") ParquetReaderConfig readerConfig,
                          @JsonProperty("filter") LogicalExpression filter,
                          @JsonProperty("schema") TupleMetadata schema) throws IOException, ExecutionSetupException {
    super(ImpersonationUtil.resolveUserName(userName), columns, entries, readerConfig, filter);
    Preconditions.checkNotNull(storageConfig);
    Preconditions.checkNotNull(formatConfig);

    this.cacheFileRoot = cacheFileRoot;
    this.formatPlugin =
        Preconditions.checkNotNull((ParquetFormatPlugin) engineRegistry.getFormatPlugin(storageConfig, formatConfig));
    this.formatConfig = this.formatPlugin.getConfig();
    DrillFileSystem fs =
        ImpersonationUtil.createFileSystem(ImpersonationUtil.resolveUserName(userName), formatPlugin.getFsConf());

    // used metadata provider manager which takes metadata from file system to reduce calls into metastore when plan is submitted
    ParquetFileTableMetadataProviderBuilder builder =
        (ParquetFileTableMetadataProviderBuilder) new FileSystemMetadataProviderManager().builder(MetadataProviderManager.MetadataProviderKind.PARQUET_TABLE);

    this.metadataProvider = builder.withEntries(this.entries)
        .withSelectionRoot(selectionRoot)
        .withCacheFileRoot(cacheFileRoot)
        .withReaderConfig(readerConfig)
        .withFileSystem(fs)
        .withCorrectCorruptedDates(this.formatConfig.areCorruptDatesAutoCorrected())
        .withSchema(schema)
        .build();

    ParquetTableMetadataProvider metadataProvider = (ParquetTableMetadataProvider) this.metadataProvider;
    this.selectionRoot = metadataProvider.getSelectionRoot();
    this.usedMetadataCache = metadataProvider.isUsedMetadataCache();
    this.fileSet = metadataProvider.getFileSet();

    init();
  }

  public ParquetGroupScan(String userName,
                          FileSelection selection,
                          ParquetFormatPlugin formatPlugin,
                          List<SchemaPath> columns,
                          ParquetReaderConfig readerConfig,
                          MetadataProviderManager metadataProviderManager) throws IOException {
    this(userName, selection, formatPlugin, columns, readerConfig, ValueExpressions.BooleanExpression.TRUE, metadataProviderManager);
  }

  public ParquetGroupScan(String userName,
                          FileSelection selection,
                          ParquetFormatPlugin formatPlugin,
                          List<SchemaPath> columns,
                          ParquetReaderConfig readerConfig,
                          LogicalExpression filter,
                          MetadataProviderManager metadataProviderManager) throws IOException {
    super(userName, columns, new ArrayList<>(), readerConfig, filter);

    this.formatPlugin = formatPlugin;
    this.formatConfig = formatPlugin.getConfig();
    this.cacheFileRoot = selection.getCacheFileRoot();

    DrillFileSystem fs = ImpersonationUtil.createFileSystem(ImpersonationUtil.resolveUserName(userName), formatPlugin.getFsConf());
    if (metadataProviderManager == null) {
      // use file system metadata provider without specified schema and statistics
      metadataProviderManager = new FileSystemMetadataProviderManager();
    }

    ParquetFileTableMetadataProviderBuilder builder =
        (ParquetFileTableMetadataProviderBuilder) metadataProviderManager.builder(MetadataProviderManager.MetadataProviderKind.PARQUET_TABLE);

    this.metadataProvider = builder
        .withSelection(selection)
        .withReaderConfig(readerConfig)
        .withFileSystem(fs)
        .withCorrectCorruptedDates(formatConfig.areCorruptDatesAutoCorrected())
        .build();

    ParquetTableMetadataProvider parquetTableMetadataProvider = (ParquetTableMetadataProvider) this.metadataProvider;
    this.usedMetadataCache = parquetTableMetadataProvider.isUsedMetadataCache();
    this.selectionRoot = parquetTableMetadataProvider.getSelectionRoot();
    this.entries = parquetTableMetadataProvider.getEntries();
    this.fileSet = parquetTableMetadataProvider.getFileSet();

    init();
  }

  /**
   * Copy constructor for shallow partial cloning
   * @param that old groupScan
   */
  private ParquetGroupScan(ParquetGroupScan that) {
    this(that, null);
  }

  /**
   * Copy constructor for shallow partial cloning with new {@link FileSelection}
   * @param that old groupScan
   * @param selection new selection
   */
  private ParquetGroupScan(ParquetGroupScan that, FileSelection selection) {
    super(that);
    this.formatConfig = that.formatConfig;
    this.formatPlugin = that.formatPlugin;
    this.selectionRoot = that.selectionRoot;
    this.cacheFileRoot = selection == null ? that.cacheFileRoot : selection.getCacheFileRoot();
    this.usedMetadataCache = that.usedMetadataCache;
  }

  // getters for serialization / deserialization start
  @JsonProperty("format")
  public ParquetFormatConfig getFormatConfig() {
    return formatConfig;
  }

  @JsonProperty("storage")
  public StoragePluginConfig getEngineConfig() {
    return formatPlugin.getStorageConfig();
  }

  @JsonProperty
  public Path getSelectionRoot() {
    return selectionRoot;
  }

  @JsonProperty
  public Path getCacheFileRoot() {
    return cacheFileRoot;
  }

  @JsonProperty
  @JsonIgnore(value = false)
  @Override
  public TupleMetadata getSchema() {
    return super.getSchema();
  }
  // getters for serialization / deserialization end

  @Override
  public ParquetRowGroupScan getSpecificScan(int minorFragmentId) {
    return new ParquetRowGroupScan(getUserName(), formatPlugin, getReadEntries(minorFragmentId), columns, readerConfig, selectionRoot, filter,
      tableMetadata == null ? null : tableMetadata.getSchema());
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new ParquetGroupScan(this);
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    ParquetGroupScan newScan = new ParquetGroupScan(this);
    newScan.columns = columns;
    return newScan;
  }

  @Override
  public ParquetGroupScan clone(FileSelection selection) throws IOException {
    ParquetGroupScan newScan = new ParquetGroupScan(this, selection);
    newScan.modifyFileSelection(selection);
    newScan.init();
    return newScan;
  }

  private List<ReadEntryWithPath> entries() {
    return entries;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("ParquetGroupScan [");
    builder.append("entries=").append(entries());
    builder.append(", selectionRoot=").append(selectionRoot);
    // TODO: solve whether print entries when no pruning is done or list of files
    //  and the actual number instead of root and 1 file...
    builder.append(", numFiles=").append(getEntries().size());
    builder.append(", numRowGroups=").append(getRowGroupsMetadata().size());
    builder.append(", usedMetadataFile=").append(usedMetadataCache);

    String filterString = getFilterString();
    if (!filterString.isEmpty()) {
      builder.append(", filter=").append(filterString);
    }

    if (usedMetadataCache) {
      // For EXPLAIN, remove the URI prefix from cacheFileRoot.  If cacheFileRoot is null, we
      // would have read the cache file from selectionRoot
      String cacheFileRootString = (cacheFileRoot == null) ?
          Path.getPathWithoutSchemeAndAuthority(selectionRoot).toString() :
          Path.getPathWithoutSchemeAndAuthority(cacheFileRoot).toString();
      builder.append(", cacheFileRoot=").append(cacheFileRootString);
    }

    builder.append(", columns=").append(columns);
    builder.append("]");

    return builder.toString();
  }

  // overridden protected methods block start
  @Override
  protected AbstractParquetGroupScan cloneWithFileSelection(Collection<Path> filePaths) throws IOException {
    FileSelection newSelection = new FileSelection(null, new ArrayList<>(filePaths), getSelectionRoot(), cacheFileRoot, false);
    return clone(newSelection);
  }

  @Override
  protected RowGroupScanFilterer getFilterer() {
    return new ParquetGroupScanFilterer(this);
  }

  @Override
  protected Collection<DrillbitEndpoint> getDrillbits() {
    return formatPlugin.getContext().getBits();
  }

  @Override
  protected boolean supportsFileImplicitColumns() {
    return selectionRoot != null;
  }

  @Override
  protected List<String> getPartitionValues(LocationProvider locationProvider) {
    return ColumnExplorer.listPartitionValues(locationProvider.getPath(), selectionRoot, false);
  }

  // overridden protected methods block end

  /**
   * Implementation of RowGroupScanFilterer which uses {@link ParquetGroupScan} as source and
   * builds {@link ParquetGroupScan} instance with filtered metadata.
   */
  private class ParquetGroupScanFilterer extends RowGroupScanFilterer<ParquetGroupScanFilterer> {

    ParquetGroupScanFilterer(ParquetGroupScan source) {
      super(source);
    }

    @Override
    protected AbstractParquetGroupScan getNewScan() {
      return new ParquetGroupScan((ParquetGroupScan) source);
    }

    @Override
    protected ParquetGroupScanFilterer self() {
      return this;
    }
  }
}
