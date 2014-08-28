/**
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
package org.apache.drill.exec.store.dfs.easy;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.schedule.AffinityCreator;
import org.apache.drill.exec.store.schedule.AssignmentCreator;
import org.apache.drill.exec.store.schedule.BlockMapBuilder;
import org.apache.drill.exec.store.schedule.CompleteFileWork;
import org.apache.drill.exec.store.schedule.CompleteFileWork.FileWorkImpl;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

@JsonTypeName("fs-scan")
public class EasyGroupScan extends AbstractGroupScan{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EasyGroupScan.class);

  private final FileSelection selection;
  private final EasyFormatPlugin<?> formatPlugin;
  private final int maxWidth;
  private List<SchemaPath> columns;

  private ListMultimap<Integer, CompleteFileWork> mappings;
  private List<CompleteFileWork> chunks;
  private List<EndpointAffinity> endpointAffinities;
  private String selectionRoot;

  @JsonCreator
  public EasyGroupScan(
      @JsonProperty("files") List<String> files, //
      @JsonProperty("storage") StoragePluginConfig storageConfig, //
      @JsonProperty("format") FormatPluginConfig formatConfig, //
      @JacksonInject StoragePluginRegistry engineRegistry, //
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("selectionRoot") String selectionRoot
      ) throws IOException, ExecutionSetupException {
        this(new FileSelection(files, true),
            (EasyFormatPlugin<?>)engineRegistry.getFormatPlugin(storageConfig, formatConfig),
            columns,
            selectionRoot);
  }

  public EasyGroupScan(FileSelection selection, EasyFormatPlugin<?> formatPlugin, String selectionRoot)
      throws IOException {
    this(selection, formatPlugin, ALL_COLUMNS, selectionRoot);
  }

  public EasyGroupScan(
      FileSelection selection, //
      EasyFormatPlugin<?> formatPlugin, //
      List<SchemaPath> columns,
      String selectionRoot
      ) throws IOException{
    this.selection = Preconditions.checkNotNull(selection);
    this.formatPlugin = Preconditions.checkNotNull(formatPlugin, "Unable to load format plugin for provided format config.");
    this.columns = columns == null || columns.size() == 0? ALL_COLUMNS : columns;
    this.selectionRoot = selectionRoot;
    BlockMapBuilder b = new BlockMapBuilder(formatPlugin.getFileSystem().getUnderlying(), formatPlugin.getContext().getBits());
    this.chunks = b.generateFileWork(selection.getFileStatusList(formatPlugin.getFileSystem()), formatPlugin.isBlockSplittable());
    this.maxWidth = chunks.size();
    this.endpointAffinities = AffinityCreator.getAffinityMap(chunks);
  }

  private EasyGroupScan(EasyGroupScan that) {
    Preconditions.checkNotNull(that, "Unable to clone: source is null.");
    selection = that.selection;
    formatPlugin = that.formatPlugin;
    columns = that.columns;
    selectionRoot = that.selectionRoot;
    chunks = that.chunks;
    endpointAffinities = that.endpointAffinities;
    maxWidth = that.maxWidth;
    mappings = that.mappings;
  }

  public String getSelectionRoot() {
    return selectionRoot;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return maxWidth;
  }


  @Override
  public ScanStats getScanStats() {
    long data =0;
    for(CompleteFileWork work : chunks){
      data += work.getTotalBytes();
    }

    long estRowCount = data/1024;
    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, estRowCount, 1, data);
  }

  @JsonProperty("files")
  public List<String> getFiles() {
    return selection.getAsFiles();
  }

  @JsonProperty("columns")
  public List<SchemaPath> getColumns(){
    return columns;
  }


  @JsonIgnore
  public FileSelection getFileSelection(){
    return selection;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    assert children == null || children.isEmpty();
    return new EasyGroupScan(this);
  }


  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    assert chunks != null && chunks.size() > 0;
    if (endpointAffinities == null) {
        logger.debug("chunks: {}", chunks.size());
        endpointAffinities = AffinityCreator.getAffinityMap(chunks);
    }
    return endpointAffinities;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> incomingEndpoints) {
    mappings = AssignmentCreator.getMappings(incomingEndpoints, chunks);
  }

  @Override
  public EasySubScan getSpecificScan(int minorFragmentId) {
    assert minorFragmentId < mappings.size() : String.format(
        "Mappings length [%d] should be longer than minor fragment id [%d] but it isn't.", mappings.size(),
        minorFragmentId);

    List<CompleteFileWork> filesForMinor = mappings.get(minorFragmentId);

    Preconditions.checkArgument(!filesForMinor.isEmpty(),
        String.format("MinorFragmentId %d has no read entries assigned", minorFragmentId));

    return new EasySubScan(convert(filesForMinor), formatPlugin, columns, selectionRoot);
  }

  private List<FileWorkImpl> convert(List<CompleteFileWork> list){
    List<FileWorkImpl> newList = Lists.newArrayList();
    for(CompleteFileWork f : list){
      newList.add(f.getAsFileWork());
    }
    return newList;
  }

  @JsonProperty("storage")
  public StoragePluginConfig getStorageConfig(){
    return formatPlugin.getStorageConfig();
  }

  @JsonProperty("format")
  public FormatPluginConfig getFormatConfig(){
    return formatPlugin.getConfig();
  }

  @Override
  public String toString() {
    return "EasyGroupScan [selectionRoot=" + selectionRoot + ", columns = " + columns + "]";
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    if(!formatPlugin.supportsPushDown()) throw new IllegalStateException(String.format("%s doesn't support pushdown.", this.getClass().getSimpleName()));
    EasyGroupScan newScan = new EasyGroupScan(this);
    newScan.columns = columns;
    return newScan;
  }

  @JsonIgnore
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return formatPlugin.supportsPushDown();
  }

}
