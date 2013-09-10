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
package org.apache.drill.exec.store.parquet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Histogram;
import com.yammer.metrics.MetricRegistry;
import com.yammer.metrics.Timer;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.ReadEntryFromHDFS;
import org.apache.drill.exec.physical.ReadEntryWithPath;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.AffinityCalculator;
import org.apache.drill.exec.store.StorageEngineRegistry;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import parquet.hadoop.Footer;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.org.codehaus.jackson.annotate.JsonCreator;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;


@JsonTypeName("parquet-scan")
public class ParquetGroupScan extends AbstractGroupScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetGroupScan.class);
  static final MetricRegistry metrics = DrillMetrics.getInstance();
  static final String READ_FOOTER_TIMER = MetricRegistry.name(ParquetGroupScan.class, "readFooter");
  static final String ENDPOINT_BYTES_TIMER = MetricRegistry.name(ParquetGroupScan.class, "endpointBytes");
  static final String ASSIGNMENT_TIMER = MetricRegistry.name(ParquetGroupScan.class, "applyAssignments");
  static final String ASSIGNMENT_AFFINITY_HIST = MetricRegistry.name(ParquetGroupScan.class, "assignmentAffinity");
  final Histogram assignmentAffinityStats = metrics.histogram(ASSIGNMENT_AFFINITY_HIST);

  private ArrayListMultimap<Integer, ParquetRowGroupScan.RowGroupReadEntry> mappings;
  private List<RowGroupInfo> rowGroupInfos;
  private Stopwatch watch = new Stopwatch();

  public List<ReadEntryWithPath> getEntries() {
    return entries;
  }

  @JsonProperty("storageengine")
  public ParquetStorageEngineConfig getEngineConfig() {
    return this.engineConfig;
  }

  private List<ReadEntryWithPath> entries;
  private long totalBytes;
  private Collection<DrillbitEndpoint> availableEndpoints;
  private ParquetStorageEngine storageEngine;
  private ParquetStorageEngineConfig engineConfig;
  private FileSystem fs;
  private final FieldReference ref;
  private List<EndpointAffinity> endpointAffinities;

  @JsonCreator
  public ParquetGroupScan(@JsonProperty("entries") List<ReadEntryWithPath> entries,
                          @JsonProperty("storageengine") ParquetStorageEngineConfig storageEngineConfig,
                          @JacksonInject StorageEngineRegistry engineRegistry,
                          @JsonProperty("ref") FieldReference ref
                           )throws IOException, ExecutionSetupException {
    engineRegistry.init(DrillConfig.create());
    this.storageEngine = (ParquetStorageEngine) engineRegistry.getEngine(storageEngineConfig);
    this.availableEndpoints = storageEngine.getContext().getBits();
    this.fs = storageEngine.getFileSystem();
    this.engineConfig = storageEngineConfig;
    this.entries = entries;
    this.ref = ref;
    readFooter();
    calculateEndpointBytes();
  }

  public ParquetGroupScan(ArrayList<ReadEntryWithPath> entries,
                          ParquetStorageEngine storageEngine, FieldReference ref) throws IOException {
    this.storageEngine = storageEngine;
    this.engineConfig = storageEngine.getEngineConfig();
    this.availableEndpoints = storageEngine.getContext().getBits();
    this.fs = storageEngine.getFileSystem();
    this.entries = entries;
    this.ref = ref;
    readFooter();
    calculateEndpointBytes();
  }

  private void readFooter() throws IOException {
    watch.reset();
    watch.start();
    Timer.Context tContext = metrics.timer(READ_FOOTER_TIMER).time();
    rowGroupInfos = new ArrayList();
    long start = 0, length = 0;
    ColumnChunkMetaData columnChunkMetaData;
    for (ReadEntryWithPath readEntryWithPath : entries){
      Path path = new Path(readEntryWithPath.getPath());
      List<Footer> footers = ParquetFileReader.readFooters(this.storageEngine.getHadoopConfig(), path);
      if (footers.size() == 0) {
        logger.warn("No footers found");
      }
//      readEntryWithPath.getPath();

      for (Footer footer : footers) {
        int index = 0;
        ParquetMetadata metadata = footer.getParquetMetadata();
        for (BlockMetaData rowGroup : metadata.getBlocks()){
          // need to grab block information from HDFS
          columnChunkMetaData = rowGroup.getColumns().iterator().next();
          start = columnChunkMetaData.getFirstDataPageOffset();
          // this field is not being populated correctly, but the column chunks know their sizes, just summing them for now
          //end = start + rowGroup.getTotalByteSize();
          length = 0;
          for (ColumnChunkMetaData col : rowGroup.getColumns()){
            length += col.getTotalSize();
          }
          String filePath = footer.getFile().toUri().getPath();
          rowGroupInfos.add(new ParquetGroupScan.RowGroupInfo(filePath, start, length, index));
          logger.debug("rowGroupInfo path: {} start: {} length {}", filePath, start, length);
          index++;
        }
      }
    }
    Preconditions.checkState(!rowGroupInfos.isEmpty(), "No row groups found");
    tContext.stop();
    watch.stop();
    logger.debug("Took {} ms to get row group infos", watch.elapsed(TimeUnit.MILLISECONDS));
  }

  private void calculateEndpointBytes() {
    Timer.Context tContext = metrics.timer(ENDPOINT_BYTES_TIMER).time();
    watch.reset();
    watch.start();
    AffinityCalculator ac = new AffinityCalculator(fs, availableEndpoints);
    for (RowGroupInfo e : rowGroupInfos) {
      ac.setEndpointBytes(e);
      totalBytes += e.getLength();
    }
    watch.stop();
    tContext.stop();
    logger.debug("Took {} ms to calculate EndpointBytes", watch.elapsed(TimeUnit.MILLISECONDS));
  }

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

  /**
   *Calculates the affinity each endpoint has for this scan, by adding up the affinity each endpoint has for each
   * rowGroup
   * @return a list of EndpointAffinity objects
   */
  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    watch.reset();
    watch.start();
    if (this.endpointAffinities == null) {
      HashMap<DrillbitEndpoint, Float> affinities = new HashMap<>();
      for (RowGroupInfo entry : rowGroupInfos) {
        for (DrillbitEndpoint d : entry.getEndpointBytes().keySet()) {
          long bytes = entry.getEndpointBytes().get(d);
          float affinity = (float)bytes / (float)totalBytes;
          logger.debug("RowGroup: {} Endpoint: {} Bytes: {}", entry.getRowGroupIndex(), d.getAddress(), bytes);
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
    watch.stop();
    logger.debug("Took {} ms to get operator affinity", watch.elapsed(TimeUnit.MILLISECONDS));
    return this.endpointAffinities;
  }


  static final double[] ASSIGNMENT_CUTOFFS = {0.99, 0.50, 0.25, 0.01};

  /**
   *
   * @param incomingEndpoints
   */
  @Override
  public void applyAssignments(List<DrillbitEndpoint> incomingEndpoints) {
    watch.reset();
    watch.start();
    final Timer.Context tcontext = metrics.timer(ASSIGNMENT_TIMER).time();
    Preconditions.checkArgument(incomingEndpoints.size() <= rowGroupInfos.size(), String.format("Incoming endpoints %d " +
            "is greater than number of row groups %d", incomingEndpoints.size(), rowGroupInfos.size()));
    mappings = ArrayListMultimap.create();
    ArrayList rowGroupList = new ArrayList(rowGroupInfos);
    List<DrillbitEndpoint> endpointLinkedlist = Lists.newLinkedList(incomingEndpoints);
    for(double cutoff : ASSIGNMENT_CUTOFFS ){
      scanAndAssign(mappings, endpointLinkedlist, rowGroupList, cutoff, false);
    }
    scanAndAssign(mappings, endpointLinkedlist, rowGroupList, 0.0, true);
    tcontext.stop();
    watch.stop();
    logger.debug("Took {} ms to apply assignments", watch.elapsed(TimeUnit.MILLISECONDS));
    Preconditions.checkState(rowGroupList.isEmpty(), "All readEntries should be assigned by now, but some are still unassigned");
    Preconditions.checkState(!rowGroupInfos.isEmpty());
  }

  public int fragmentPointer = 0;

  /**
   *
   * @param endpointAssignments the mapping between fragment/endpoint and rowGroup
   * @param endpoints the list of drillbits, ordered by the corresponding fragment
   * @param rowGroups the list of rowGroups to assign
   * @param requiredPercentage the percentage of max bytes required to make an assignment
   * @param assignAll if true, will assign even if no affinity
   */
  private void scanAndAssign (Multimap<Integer, ParquetRowGroupScan.RowGroupReadEntry> endpointAssignments, List<DrillbitEndpoint> endpoints, List<RowGroupInfo> rowGroups, double requiredPercentage, boolean assignAll) {
    Collections.sort(rowGroups, new ParquetReadEntryComparator());
    final boolean requireAffinity = requiredPercentage > 0;
    int maxAssignments = (int) (rowGroups.size() / endpoints.size());

    if (maxAssignments < 1) maxAssignments = 1;

    for(Iterator<RowGroupInfo> iter = rowGroups.iterator(); iter.hasNext();){
      RowGroupInfo rowGroupInfo = iter.next();
      for (int i = 0; i < endpoints.size(); i++) {
        int minorFragmentId = (fragmentPointer + i) % endpoints.size();
        DrillbitEndpoint currentEndpoint = endpoints.get(minorFragmentId);
        Map<DrillbitEndpoint, Long> bytesPerEndpoint = rowGroupInfo.getEndpointBytes();
        boolean haveAffinity = bytesPerEndpoint.containsKey(currentEndpoint) ;

        if (assignAll ||
                (!bytesPerEndpoint.isEmpty() &&
                        (!requireAffinity || haveAffinity) &&
                        (!endpointAssignments.containsKey(minorFragmentId) || endpointAssignments.get(minorFragmentId).size() < maxAssignments) &&
                        bytesPerEndpoint.get(currentEndpoint) >= rowGroupInfo.getMaxBytes() * requiredPercentage)) {

          endpointAssignments.put(minorFragmentId, rowGroupInfo.getRowGroupReadEntry());
          logger.debug("Assigned rowGroup {} to minorFragmentId {} endpoint {}", rowGroupInfo.getRowGroupIndex(), minorFragmentId, endpoints.get(minorFragmentId).getAddress());
          assignmentAffinityStats.update(bytesPerEndpoint.get(currentEndpoint) / rowGroupInfo.getLength());
          iter.remove();
          fragmentPointer = (minorFragmentId + 1) % endpoints.size();
          break;
        }
      }

    }
  }

  @Override
  public ParquetRowGroupScan getSpecificScan(int minorFragmentId) {
    assert minorFragmentId < mappings.size() : String.format("Mappings length [%d] should be longer than minor fragment id [%d] but it isn't.", mappings.size(), minorFragmentId);
    for (ParquetRowGroupScan.RowGroupReadEntry rg : mappings.get(minorFragmentId)) {
      logger.debug("minorFragmentId: {} Path: {} RowGroupIndex: {}",minorFragmentId, rg.getPath(),rg.getRowGroupIndex());
    }
    Preconditions.checkArgument(!mappings.get(minorFragmentId).isEmpty(), String.format("MinorFragmentId %d has no read entries assigned", minorFragmentId));
    return new ParquetRowGroupScan(storageEngine, engineConfig, mappings.get(minorFragmentId), ref);
  }

  
  public FieldReference getRef() {
    return ref;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return rowGroupInfos.size();
  }

  @Override
  public OperatorCost getCost() {
    //TODO Figure out how to properly calculate cost
    return new OperatorCost(1,rowGroupInfos.size(),1,1);
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
