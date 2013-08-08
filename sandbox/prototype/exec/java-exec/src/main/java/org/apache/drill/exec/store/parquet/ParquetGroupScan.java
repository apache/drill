/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.store.parquet;

import java.io.IOException;
import java.util.*;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.exception.SetupException;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.ReadEntryFromHDFS;
import org.apache.drill.exec.physical.ReadEntryWithPath;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.physical.config.MockGroupScanPOP;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.google.common.base.Preconditions;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.StorageEngineRegistry;
import org.apache.drill.exec.store.AffinityCalculator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.org.codehaus.jackson.annotate.JsonCreator;


@JsonTypeName("parquet-scan")
public class ParquetGroupScan extends AbstractGroupScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetGroupScan.class);

  private LinkedList<ParquetRowGroupScan.RowGroupReadEntry>[] mappings;
  private List<RowGroupInfo> rowGroupInfos;

  public List<ReadEntryWithPath> getEntries() {
    return entries;
  }

  public ParquetStorageEngineConfig getEngineConfig() {
    return this.engineConfig;
  }

  private List<ReadEntryWithPath> entries;
  @JsonIgnore
  private long totalBytes;
  private Collection<DrillbitEndpoint> availableEndpoints;
  private ParquetStorageEngine storageEngine;
  private StorageEngineRegistry engineRegistry;
  private ParquetStorageEngineConfig engineConfig;
  private FileSystem fs;
  private String fileName;
  private List<EndpointAffinity> endpointAffinities;

  @JsonCreator
  public ParquetGroupScan(@JsonProperty("entries") List<ReadEntryWithPath> entries,
                          @JsonProperty("storageengine") ParquetStorageEngineConfig storageEngineConfig,
                          @JacksonInject StorageEngineRegistry engineRegistry
                           )throws SetupException,IOException {
    engineRegistry.init(DrillConfig.create());
    this.storageEngine = (ParquetStorageEngine) engineRegistry.getEngine(storageEngineConfig);
    this.availableEndpoints = storageEngine.getContext().getBits();
    this.fs = storageEngine.getFileSystem();
    this.engineConfig = storageEngineConfig;
    this.engineRegistry = engineRegistry;
    this.entries = entries;
    readFooter();
    this.fileName = rowGroupInfos.get(0).getPath();
    calculateEndpointBytes();
  }

  public ParquetGroupScan(ArrayList<ReadEntryWithPath> entries,
                          ParquetStorageEngine storageEngine) throws IOException {
    this.storageEngine = storageEngine;
    this.availableEndpoints = storageEngine.getContext().getBits();
    this.fs = storageEngine.getFileSystem();
    this.entries = entries;
    readFooter();
    this.fileName = rowGroupInfos.get(0).getPath();
    calculateEndpointBytes();
  }

  private void readFooter() throws IOException {
    long tA = System.nanoTime();
    rowGroupInfos = new ArrayList();
    long start = 0, length = 0;
    ColumnChunkMetaData columnChunkMetaData;
    for (ReadEntryWithPath readEntryWithPath : entries){
      Path path = new Path(readEntryWithPath.getPath());

      ParquetMetadata footer = ParquetFileReader.readFooter(this.storageEngine.getHadoopConfig(), path);
      readEntryWithPath.getPath();

      int i = 0;
      for (BlockMetaData rowGroup : footer.getBlocks()){
        // need to grab block information from HDFS
        columnChunkMetaData = rowGroup.getColumns().iterator().next();
        start = columnChunkMetaData.getFirstDataPageOffset();
        // this field is not being populated correctly, but the column chunks know their sizes, just summing them for now
        //end = start + rowGroup.getTotalByteSize();
        length = 0;
        for (ColumnChunkMetaData col : rowGroup.getColumns()){
          length += col.getTotalSize();
        }
        rowGroupInfos.add(new ParquetGroupScan.RowGroupInfo(readEntryWithPath.getPath(), start, length, i));
        logger.debug("rowGroupInfo path: {} start: {} length {}", readEntryWithPath.getPath(), start, length);
        i++;
      }
    }
    long tB = System.nanoTime();
    logger.debug("Took {} ms to get row group infos", (float)(tB - tA) / 1E6);
  }

  private void calculateEndpointBytes() {
    long tA = System.nanoTime();
    AffinityCalculator ac = new AffinityCalculator(fileName, fs, availableEndpoints);
    for (RowGroupInfo e : rowGroupInfos) {
      ac.setEndpointBytes(e);
      totalBytes += e.getLength();
    }
    long tB = System.nanoTime();
    logger.debug("Took {} ms to calculate EndpointBytes", (float)(tB - tA) / 1E6);
  }
/*
  public LinkedList<RowGroupInfo> getRowGroups() {
    return rowGroups;
  }

  public void setRowGroups(LinkedList<RowGroupInfo> rowGroups) {
    this.rowGroups = rowGroups;
  }

  public static class ParquetFileReadEntry {

    String path;

    public ParquetFileReadEntry(@JsonProperty String path){
      this.path = path;
    }
  }
  */

  @JsonIgnore
  public FileSystem getFileSystem() {
    return this.fs;
  }

  public static class RowGroupInfo extends ReadEntryFromHDFS {

    private HashMap<DrillbitEndpoint,Long> endpointBytes;
    private long maxBytes;
    private int rowGroupIndex;

    @JsonCreator
    public RowGroupInfo(@JsonProperty("path") String path, @JsonProperty("start") long start,
                        @JsonProperty("length") long length, @JsonProperty("rowGroupIndex") int rowGroupIndex) {
      super(path, start, length);
      this.rowGroupIndex = rowGroupIndex;
    }

    @Override
    public OperatorCost getCost() {
      return new OperatorCost(1, 2, 1, 1);
    }

    @Override
    public Size getSize() {
      // TODO - these values are wrong, I cannot know these until after I read a file
      return new Size(10, 10);
    }

    public HashMap<DrillbitEndpoint,Long> getEndpointBytes() {
      return endpointBytes;
    }

    public void setEndpointBytes(HashMap<DrillbitEndpoint,Long> endpointBytes) {
      this.endpointBytes = endpointBytes;
    }

    public void setMaxBytes(long bytes) {
      this.maxBytes = bytes;
    }

    public long getMaxBytes() {
      return maxBytes;
    }

    public ParquetRowGroupScan.RowGroupReadEntry getRowGroupReadEntry() {
      return new ParquetRowGroupScan.RowGroupReadEntry(this.getPath(), this.getStart(), this.getLength(), this.rowGroupIndex);
    }

    public int getRowGroupIndex() {
      return this.rowGroupIndex;
    }
  }

  private class ParquetReadEntryComparator implements Comparator<RowGroupInfo> {
    public int compare(RowGroupInfo e1, RowGroupInfo e2) {
      if (e1.getMaxBytes() == e2.getMaxBytes()) return 0;
      return (e1.getMaxBytes() > e2.getMaxBytes()) ? 1 : -1;
    }
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    long tA = System.nanoTime();
    if (this.endpointAffinities == null) {
      HashMap<DrillbitEndpoint, Float> affinities = new HashMap<>();
      for (RowGroupInfo entry : rowGroupInfos) {
        for (DrillbitEndpoint d : entry.getEndpointBytes().keySet()) {
          long bytes = entry.getEndpointBytes().get(d);
          float affinity = (float)bytes / (float)totalBytes;
          logger.error("RowGroup: {} Endpoint: {} Bytes: {}", entry.getRowGroupIndex(), d.getAddress(), bytes);
          if (affinities.keySet().contains(d)) {
            affinities.put(d, affinities.get(d) + affinity);
          } else {
            affinities.put(d, affinity);
          }
        }
      }
      List<EndpointAffinity> affinityList = new LinkedList<>();
      for (DrillbitEndpoint d : affinities.keySet()) {
        logger.debug("Endpoint {} has affinity {}", d.getAddress(), affinities.get(d).floatValue());
        affinityList.add(new EndpointAffinity(d,affinities.get(d).floatValue()));
      }
      this.endpointAffinities = affinityList;
    }
    long tB = System.nanoTime();
    logger.debug("Took {} ms to get operator affinity", (float)(tB - tA) / 1E6);
    return this.endpointAffinities;
  }




  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) {
    long tA = System.nanoTime();
    Preconditions.checkArgument(endpoints.size() <= rowGroupInfos.size());

    int i = 0;
    for (DrillbitEndpoint endpoint : endpoints) {
      logger.debug("Endpoint index {}, endpoint host: {}", i++, endpoint.getAddress());
    }

    Collections.sort(rowGroupInfos, new ParquetReadEntryComparator());
    mappings = new LinkedList[endpoints.size()];
    LinkedList<RowGroupInfo> unassigned = scanAndAssign(endpoints, rowGroupInfos, 100, true, false);
    LinkedList<RowGroupInfo> unassigned2 = scanAndAssign(endpoints, unassigned, 50, true, false);
    LinkedList<RowGroupInfo> unassigned3 = scanAndAssign(endpoints, unassigned2, 25, true, false);
    LinkedList<RowGroupInfo> unassigned4 = scanAndAssign(endpoints, unassigned3, 0, false, false);
    LinkedList<RowGroupInfo> unassigned5 = scanAndAssign(endpoints, unassigned4, 0, false, true);
    assert unassigned5.size() == 0 : String.format("All readEntries should be assigned by now, but some are still unassigned");
    long tB = System.nanoTime();
    logger.debug("Took {} ms to apply assignments ", (float)(tB - tA) / 1E6);
  }

  private LinkedList<RowGroupInfo> scanAndAssign (List<DrillbitEndpoint> endpoints, List<RowGroupInfo> rowGroups, int requiredPercentage, boolean mustContain, boolean assignAll) {
    Collections.sort(rowGroupInfos, new ParquetReadEntryComparator());
    LinkedList<RowGroupInfo> unassigned = new LinkedList<>();

    int maxEntries = (int) (rowGroupInfos.size() / endpoints.size() * 1.5);

    if (maxEntries < 1) maxEntries = 1;

    int i =0;
    for(RowGroupInfo e : rowGroups) {
      boolean assigned = false;
      for (int j = i; j < i + endpoints.size(); j++) {
        DrillbitEndpoint currentEndpoint = endpoints.get(j%endpoints.size());
        if (assignAll ||
                (e.getEndpointBytes().size() > 0 &&
                (e.getEndpointBytes().containsKey(currentEndpoint) || !mustContain) &&
                (mappings[j%endpoints.size()] == null || mappings[j%endpoints.size()].size() < maxEntries) &&
                e.getEndpointBytes().get(currentEndpoint) >= e.getMaxBytes() * requiredPercentage / 100)) {
          LinkedList<ParquetRowGroupScan.RowGroupReadEntry> entries = mappings[j%endpoints.size()];
          if(entries == null){
            entries = new LinkedList<ParquetRowGroupScan.RowGroupReadEntry>();
            mappings[j%endpoints.size()] = entries;
          }
          entries.add(e.getRowGroupReadEntry());
          logger.debug("Assigned rowGroup ( {} , {} ) to endpoint {}", e.getPath(), e.getStart(), currentEndpoint.getAddress());
          assigned = true;
          break;
        }
      }
      if (!assigned) unassigned.add(e);
      i++;
    }
    return unassigned;
  }

  @Override
  public ParquetRowGroupScan getSpecificScan(int minorFragmentId) {
    assert minorFragmentId < mappings.length : String.format("Mappings length [%d] should be longer than minor fragment id [%d] but it isn't.", mappings.length, minorFragmentId);
    for (ParquetRowGroupScan.RowGroupReadEntry rg : mappings[minorFragmentId]) {
      logger.debug("minorFragmentId: {} Path: {} RowGroupIndex: {}",minorFragmentId, rg.getPath(),rg.getRowGroupIndex());
    }
    try {
      return new ParquetRowGroupScan(storageEngine, engineConfig, mappings[minorFragmentId]);
    } catch (SetupException e) {
      e.printStackTrace(); // TODO - fix this
    }
    return null;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return rowGroupInfos.size();
  }

  @Override
  public OperatorCost getCost() {
    return new OperatorCost(1,1,1,1);
  }

  @Override
  public Size getSize() {
    // TODO - this is wrong, need to populate correctly
    return new Size(10,10);
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    //TODO return copy of self
    return this;
  }

}
