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
import java.util.Collections;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.schedule.AffinityCreator;
import org.apache.drill.exec.store.schedule.AssignmentCreator;
import org.apache.drill.exec.store.schedule.BlockMapBuilder;
import org.apache.drill.exec.store.schedule.CompleteFileWork;
import org.apache.drill.exec.store.schedule.CompleteFileWork.FileWorkImpl;

import com.beust.jcommander.internal.Lists;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ListMultimap;

@JsonTypeName("fs-scan")
public class EasyGroupScan extends AbstractGroupScan{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EasyGroupScan.class);

  private final FileSelection selection;
  private final EasyFormatPlugin<?> formatPlugin;
  private final FieldReference ref;
  private final int maxWidth;
  private final List<SchemaPath> columns;
  
  private ListMultimap<Integer, CompleteFileWork> mappings;
  private List<CompleteFileWork> chunks;
  private List<EndpointAffinity> endpointAffinities;

  @JsonCreator
  public EasyGroupScan(
      @JsonProperty("files") List<String> files, //
      @JsonProperty("storage") StoragePluginConfig storageConfig, //
      @JsonProperty("format") FormatPluginConfig formatConfig, //
      @JacksonInject StoragePluginRegistry engineRegistry, // 
      @JsonProperty("ref") FieldReference ref, //
      @JsonProperty("columns") List<SchemaPath> columns
      ) throws IOException, ExecutionSetupException {
    
    this.formatPlugin = (EasyFormatPlugin<?>) engineRegistry.getFormatPlugin(storageConfig, formatConfig);
    Preconditions.checkNotNull(formatPlugin, "Unable to load format plugin for provided format config.");
    this.selection = new FileSelection(files, true);
    this.maxWidth = selection.getFileStatusList(formatPlugin.getFileSystem()).size();
    this.ref = ref;
    this.columns = columns;
  }
  
  public EasyGroupScan(
      FileSelection selection, //
      EasyFormatPlugin<?> formatPlugin, // 
      FieldReference ref, //
      List<SchemaPath> columns
      ) throws IOException{
    this.selection = selection;
    this.maxWidth = selection.getFileStatusList(formatPlugin.getFileSystem()).size();
    this.formatPlugin = formatPlugin;
    this.ref = ref;
    this.columns = columns;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return maxWidth;
  }

  @Override
  public OperatorCost getCost() {
    return new OperatorCost(1,1,1,1);
  }

  @Override
  public Size getSize() {
    return new Size(1024,1024);
  }

  @JsonProperty("files")
  public List<String> getFiles() {
    return selection.getAsFiles();
  }
  
  @JsonIgnore
  public FileSelection getFileSelection(){
    return selection;
  }
  
  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    assert children == null || children.isEmpty();
    return this;
  }

  @JsonProperty("columns")
  public List<SchemaPath> getColumns(){
    return columns;
  }
  
  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    if (this.endpointAffinities == null) {
      try{
      BlockMapBuilder b = new BlockMapBuilder(formatPlugin.getFileSystem().getUnderlying(), formatPlugin.getContext().getBits());
      this.chunks = b.generateFileWork(selection.getFileStatusList(formatPlugin.getFileSystem()), formatPlugin.isBlockSplittable());
      this.endpointAffinities = AffinityCreator.getAffinityMap(chunks);
      }catch(IOException e){
        logger.warn("Failure determining endpoint affinity.", e);
        this.endpointAffinities = Collections.emptyList();
      }
    }
    return this.endpointAffinities;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> incomingEndpoints) {
    this.mappings = AssignmentCreator.getMappings(incomingEndpoints, chunks);
  }

  @Override
  public EasySubScan getSpecificScan(int minorFragmentId) {
    assert minorFragmentId < mappings.size() : String.format(
        "Mappings length [%d] should be longer than minor fragment id [%d] but it isn't.", mappings.size(),
        minorFragmentId);

    List<CompleteFileWork> filesForMinor = mappings.get(minorFragmentId);

    Preconditions.checkArgument(!filesForMinor.isEmpty(),
        String.format("MinorFragmentId %d has no read entries assigned", minorFragmentId));

    return new EasySubScan(convert(filesForMinor), formatPlugin, ref, columns);
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
}
