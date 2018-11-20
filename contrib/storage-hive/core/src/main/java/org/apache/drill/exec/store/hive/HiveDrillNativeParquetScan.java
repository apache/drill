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
package org.apache.drill.exec.store.hive;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.exec.store.parquet.ParquetReaderConfig;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
import org.apache.drill.exec.store.hive.HiveMetadataProvider.LogicalInputSplit;
import org.apache.drill.exec.store.parquet.AbstractParquetGroupScan;
import org.apache.drill.exec.store.parquet.RowGroupReadEntry;
import org.apache.drill.exec.store.parquet.metadata.Metadata;
import org.apache.drill.exec.store.parquet.RowGroupInfo;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.io.parquet.ProjectionPusher;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@JsonTypeName("hive-drill-native-parquet-scan")
public class HiveDrillNativeParquetScan extends AbstractParquetGroupScan {

  private final HiveStoragePlugin hiveStoragePlugin;
  private final HivePartitionHolder hivePartitionHolder;
  private final Map<String, String> confProperties;

  @JsonCreator
  public HiveDrillNativeParquetScan(@JacksonInject StoragePluginRegistry engineRegistry,
                                    @JsonProperty("userName") String userName,
                                    @JsonProperty("hiveStoragePluginConfig") HiveStoragePluginConfig hiveStoragePluginConfig,
                                    @JsonProperty("columns") List<SchemaPath> columns,
                                    @JsonProperty("entries") List<ReadEntryWithPath> entries,
                                    @JsonProperty("hivePartitionHolder") HivePartitionHolder hivePartitionHolder,
                                    @JsonProperty("confProperties") Map<String, String> confProperties,
                                    @JsonProperty("readerConfig") ParquetReaderConfig readerConfig,
                                    @JsonProperty("filter") LogicalExpression filter) throws IOException, ExecutionSetupException {
    super(ImpersonationUtil.resolveUserName(userName), columns, entries, readerConfig, filter);
    this.hiveStoragePlugin = (HiveStoragePlugin) engineRegistry.getPlugin(hiveStoragePluginConfig);
    this.hivePartitionHolder = hivePartitionHolder;
    this.confProperties = confProperties;

    init();
  }

  public HiveDrillNativeParquetScan(String userName,
                                    List<SchemaPath> columns,
                                    HiveStoragePlugin hiveStoragePlugin,
                                    List<LogicalInputSplit> logicalInputSplits,
                                    Map<String, String> confProperties,
                                    ParquetReaderConfig readerConfig) throws IOException {
    this(userName, columns, hiveStoragePlugin, logicalInputSplits, confProperties, readerConfig, ValueExpressions.BooleanExpression.TRUE);
  }

  public HiveDrillNativeParquetScan(String userName,
                                    List<SchemaPath> columns,
                                    HiveStoragePlugin hiveStoragePlugin,
                                    List<LogicalInputSplit> logicalInputSplits,
                                    Map<String, String> confProperties,
                                    ParquetReaderConfig readerConfig,
                                    LogicalExpression filter) throws IOException {
    super(userName, columns, new ArrayList<>(), readerConfig, filter);

    this.hiveStoragePlugin = hiveStoragePlugin;
    this.hivePartitionHolder = new HivePartitionHolder();
    this.confProperties = confProperties;

    for (LogicalInputSplit logicalInputSplit : logicalInputSplits) {
      Iterator<InputSplit> iterator = logicalInputSplit.getInputSplits().iterator();
      // logical input split contains list of splits by files
      // we need to read path of only one to get file path
      assert iterator.hasNext();
      InputSplit split = iterator.next();
      assert split instanceof FileSplit;
      FileSplit fileSplit = (FileSplit) split;
      Path finalPath = fileSplit.getPath();
      String pathString = Path.getPathWithoutSchemeAndAuthority(finalPath).toString();
      entries.add(new ReadEntryWithPath(pathString));

      // store partition values per path
      Partition partition = logicalInputSplit.getPartition();
      if (partition != null) {
        hivePartitionHolder.add(pathString, partition.getValues());
      }
    }

    init();
  }

  private HiveDrillNativeParquetScan(HiveDrillNativeParquetScan that) {
    super(that);
    this.hiveStoragePlugin = that.hiveStoragePlugin;
    this.hivePartitionHolder = that.hivePartitionHolder;
    this.confProperties = that.confProperties;
  }

  @JsonProperty
  public HiveStoragePluginConfig getHiveStoragePluginConfig() {
    return hiveStoragePlugin.getConfig();
  }

  @JsonProperty
  public HivePartitionHolder getHivePartitionHolder() {
    return hivePartitionHolder;
  }

  @JsonProperty
  public Map<String, String> getConfProperties() {
    return confProperties;
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) {
    List<RowGroupReadEntry> readEntries = getReadEntries(minorFragmentId);
    HivePartitionHolder subPartitionHolder = new HivePartitionHolder();
    for (RowGroupReadEntry readEntry : readEntries) {
      List<String> values = hivePartitionHolder.get(readEntry.getPath());
      subPartitionHolder.add(readEntry.getPath(), values);
    }
    return new HiveDrillNativeParquetRowGroupScan(getUserName(), hiveStoragePlugin, readEntries, columns, subPartitionHolder,
      confProperties, readerConfig, filter);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new HiveDrillNativeParquetScan(this);
  }

  @Override
  public HiveDrillNativeParquetScan clone(FileSelection selection) throws IOException {
    HiveDrillNativeParquetScan newScan = new HiveDrillNativeParquetScan(this);
    newScan.modifyFileSelection(selection);
    newScan.init();
    return newScan;
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    HiveDrillNativeParquetScan newScan = new HiveDrillNativeParquetScan(this);
    newScan.columns = columns;
    return newScan;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("HiveDrillNativeParquetScan [");
    builder.append("entries=").append(entries);
    builder.append(", numFiles=").append(getEntries().size());
    builder.append(", numRowGroups=").append(rowGroupInfos.size());

    String filterString = getFilterString();
    if (!filterString.isEmpty()) {
      builder.append(", filter=").append(filterString);
    }

    builder.append(", columns=").append(columns);
    builder.append("]");

    return builder.toString();
  }

  @Override
  protected void initInternal() throws IOException {
    Map<FileStatus, FileSystem> fileStatusConfMap = new LinkedHashMap<>();
    for (ReadEntryWithPath entry : entries) {
      Path path = new Path(entry.getPath());
      Configuration conf = new ProjectionPusher().pushProjectionsAndFilters(
          new JobConf(hiveStoragePlugin.getHiveConf()),
          path.getParent());
      FileSystem fs = path.getFileSystem(conf);
      fileStatusConfMap.put(fs.getFileStatus(Path.getPathWithoutSchemeAndAuthority(path)), fs);
    }
    parquetTableMetadata = Metadata.getParquetTableMetadata(fileStatusConfMap, readerConfig);
  }

  @Override
  protected Collection<CoordinationProtos.DrillbitEndpoint> getDrillbits() {
    return hiveStoragePlugin.getContext().getBits();
  }

  @Override
  protected AbstractParquetGroupScan cloneWithFileSelection(Collection<String> filePaths) throws IOException {
    FileSelection newSelection = new FileSelection(null, new ArrayList<>(filePaths), null, null, false);
    return clone(newSelection);
  }

  @Override
  protected boolean supportsFileImplicitColumns() {
    return false;
  }

  @Override
  protected List<String> getPartitionValues(RowGroupInfo rowGroupInfo) {
    return hivePartitionHolder.get(rowGroupInfo.getPath());
  }

}
