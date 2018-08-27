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
package org.apache.drill.exec.store.dfs.easy;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractFileGroupScan;
import org.apache.drill.exec.physical.base.FileGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.schedule.AffinityCreator;
import org.apache.drill.exec.store.schedule.AssignmentCreator;
import org.apache.drill.exec.store.schedule.BlockMapBuilder;
import org.apache.drill.exec.store.schedule.CompleteFileWork;
import org.apache.drill.exec.store.schedule.CompleteFileWork.FileWorkImpl;
import org.apache.drill.exec.util.ImpersonationUtil;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Iterators;
import org.apache.drill.shaded.guava.com.google.common.collect.ListMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

@JsonTypeName("fs-scan")
public class EasyGroupScan extends AbstractFileGroupScan {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EasyGroupScan.class);

  private FileSelection selection;
  private final EasyFormatPlugin<?> formatPlugin;
  private int maxWidth;
  private List<SchemaPath> columns;

  private ListMultimap<Integer, CompleteFileWork> mappings;
  private List<CompleteFileWork> chunks;
  private List<EndpointAffinity> endpointAffinities;
  private String selectionRoot;

  @JsonCreator
  public EasyGroupScan(
      @JsonProperty("userName") String userName,
      @JsonProperty("files") List<String> files, //
      @JsonProperty("storage") StoragePluginConfig storageConfig, //
      @JsonProperty("format") FormatPluginConfig formatConfig, //
      @JacksonInject StoragePluginRegistry engineRegistry, //
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("selectionRoot") String selectionRoot
      ) throws IOException, ExecutionSetupException {
        this(ImpersonationUtil.resolveUserName(userName),
            FileSelection.create(null, files, selectionRoot),
            (EasyFormatPlugin<?>)engineRegistry.getFormatPlugin(storageConfig, formatConfig),
            columns,
            selectionRoot);
  }

  public EasyGroupScan(String userName, FileSelection selection, EasyFormatPlugin<?> formatPlugin, String selectionRoot)
      throws IOException {
    this(userName, selection, formatPlugin, ALL_COLUMNS, selectionRoot);
  }

  public EasyGroupScan(
      String userName,
      FileSelection selection, //
      EasyFormatPlugin<?> formatPlugin, //
      List<SchemaPath> columns,
      String selectionRoot
      ) throws IOException{
    super(userName);
    this.selection = Preconditions.checkNotNull(selection);
    this.formatPlugin = Preconditions.checkNotNull(formatPlugin, "Unable to load format plugin for provided format config.");
    this.columns = columns == null ? ALL_COLUMNS : columns;
    this.selectionRoot = selectionRoot;
    initFromSelection(selection, formatPlugin);
  }

  @JsonIgnore
  public Iterable<CompleteFileWork> getWorkIterable() {
    return new Iterable<CompleteFileWork>() {
      @Override
      public Iterator<CompleteFileWork> iterator() {
        return Iterators.unmodifiableIterator(chunks.iterator());
      }
    };
  }

  private EasyGroupScan(final EasyGroupScan that) {
    super(that.getUserName());
    selection = that.selection;
    formatPlugin = that.formatPlugin;
    columns = that.columns;
    selectionRoot = that.selectionRoot;
    chunks = that.chunks;
    endpointAffinities = that.endpointAffinities;
    maxWidth = that.maxWidth;
    mappings = that.mappings;
  }

  private void initFromSelection(FileSelection selection, EasyFormatPlugin<?> formatPlugin) throws IOException {
    @SuppressWarnings("resource")
    final DrillFileSystem dfs = ImpersonationUtil.createFileSystem(getUserName(), formatPlugin.getFsConf());
    this.selection = selection;
    BlockMapBuilder b = new BlockMapBuilder(dfs, formatPlugin.getContext().getBits());
    this.chunks = b.generateFileWork(selection.getStatuses(dfs), formatPlugin.isBlockSplittable());
    this.maxWidth = chunks.size();
    this.endpointAffinities = AffinityCreator.getAffinityMap(chunks);
  }

  public String getSelectionRoot() {
    return selectionRoot;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return maxWidth;
  }


  @Override
  public ScanStats getScanStats(final PlannerSettings settings) {
    return formatPlugin.getScanStats(settings, this);
  }

  @Override
  public boolean hasFiles() {
    return true;
  }

  @JsonProperty("files")
  @Override
  public List<String> getFiles() {
    return selection.getFiles();
  }

  @JsonProperty("columns")
  public List<SchemaPath> getColumns() {
    return columns;
  }


  @JsonIgnore
  public FileSelection getFileSelection() {
    return selection;
  }

  @Override
  public void modifyFileSelection(FileSelection selection) {
    this.selection = selection;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    assert children == null || children.isEmpty();
    return new EasyGroupScan(this);
  }


  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
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

  private void createMappings(List<EndpointAffinity> affinities) {
    List<DrillbitEndpoint> endpoints = Lists.newArrayList();
    for (EndpointAffinity e : affinities) {
      endpoints.add(e.getEndpoint());
    }
    this.applyAssignments(endpoints);
  }

  @Override
  public EasySubScan getSpecificScan(int minorFragmentId) {
    if (mappings == null) {
      createMappings(this.endpointAffinities);
    }
    assert minorFragmentId < mappings.size() : String.format(
        "Mappings length [%d] should be longer than minor fragment id [%d] but it isn't.", mappings.size(),
        minorFragmentId);

    List<CompleteFileWork> filesForMinor = mappings.get(minorFragmentId);

    Preconditions.checkArgument(!filesForMinor.isEmpty(),
        String.format("MinorFragmentId %d has no read entries assigned", minorFragmentId));

    EasySubScan subScan = new EasySubScan(getUserName(), convert(filesForMinor), formatPlugin, columns, selectionRoot);
    subScan.setOperatorId(this.getOperatorId());
    return subScan;
  }

  private List<FileWorkImpl> convert(List<CompleteFileWork> list) {
    List<FileWorkImpl> newList = Lists.newArrayList();
    for (CompleteFileWork f : list) {
      newList.add(f.getAsFileWork());
    }
    return newList;
  }

  @JsonProperty("storage")
  public StoragePluginConfig getStorageConfig() {
    return formatPlugin.getStorageConfig();
  }

  @JsonProperty("format")
  public FormatPluginConfig getFormatConfig() {
    return formatPlugin.getConfig();
  }

  @Override
  public String toString() {
    final String pattern = "EasyGroupScan [selectionRoot=%s, numFiles=%s, columns=%s, files=%s]";
    return String.format(pattern, selectionRoot, getFiles().size(), columns, getFiles());
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    if (!formatPlugin.supportsPushDown()) {
      throw new IllegalStateException(String.format("%s doesn't support pushdown.", this.getClass().getSimpleName()));
    }
    EasyGroupScan newScan = new EasyGroupScan(this);
    newScan.columns = columns;
    return newScan;
  }

  @Override
  public FileGroupScan clone(FileSelection selection) throws IOException {
    EasyGroupScan newScan = new EasyGroupScan(this);
    newScan.initFromSelection(selection, formatPlugin);
    newScan.mappings = null; /* the mapping will be created later when we get specific scan
                                since the end-point affinities are not known at this time */
    return newScan;
  }

  @Override
  @JsonIgnore
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return formatPlugin.supportsPushDown();
  }

}
