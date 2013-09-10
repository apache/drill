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
package org.apache.drill.exec.store;


import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import com.yammer.metrics.*;
import com.yammer.metrics.Timer;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.store.parquet.ParquetGroupScan;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class AffinityCalculator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AffinityCalculator.class);
  static final MetricRegistry metrics = DrillMetrics.getInstance();
  static final String BLOCK_MAP_BUILDER_TIMER = MetricRegistry.name(AffinityCalculator.class, "blockMapBuilderTimer");


  HashMap<String,ImmutableRangeMap<Long,BlockLocation>> blockMapMap = new HashMap<>();
  FileSystem fs;
  String fileName;
  Collection<DrillbitEndpoint> endpoints;
  HashMap<String,DrillbitEndpoint> endPointMap;

  public AffinityCalculator(FileSystem fs, Collection<DrillbitEndpoint> endpoints) {
    this.fs = fs;
    this.endpoints = endpoints;
    buildEndpointMap();
  }

  /**
   * Builds a mapping of block locations to file byte range
   */
  private void buildBlockMap(String fileName) {
    final Timer.Context context = metrics.timer(BLOCK_MAP_BUILDER_TIMER).time();
    BlockLocation[] blocks;
    ImmutableRangeMap<Long,BlockLocation> blockMap;
    try {
      FileStatus file = fs.getFileStatus(new Path(fileName));
      blocks = fs.getFileBlockLocations(file, 0 , file.getLen());
    } catch (IOException ioe) { throw new RuntimeException(ioe); }
    ImmutableRangeMap.Builder<Long, BlockLocation> blockMapBuilder = new ImmutableRangeMap.Builder<Long,BlockLocation>();
    for (BlockLocation block : blocks) {
      long start = block.getOffset();
      long end = start + block.getLength();
      Range<Long> range = Range.closedOpen(start, end);
      blockMapBuilder = blockMapBuilder.put(range, block);
    }
    blockMap = blockMapBuilder.build();
    blockMapMap.put(fileName, blockMap);
    context.stop();
  }
  /**
   * For a given RowGroup, calculate how many bytes are available on each on drillbit endpoint
   *
   * @param rowGroup the RowGroup to calculate endpoint bytes for
   */
  public void setEndpointBytes(ParquetGroupScan.RowGroupInfo rowGroup) {
    Stopwatch watch = new Stopwatch();
    watch.start();
    String fileName = rowGroup.getPath();
    if (!blockMapMap.containsKey(fileName)) {
      buildBlockMap(fileName);
    }

    ImmutableRangeMap<Long,BlockLocation> blockMap = blockMapMap.get(fileName);
    HashMap<String,Long> hostMap = new HashMap<>();
    HashMap<DrillbitEndpoint,Long> endpointByteMap = new HashMap();
    long start = rowGroup.getStart();
    long end = start + rowGroup.getLength();
    Range<Long> rowGroupRange = Range.closedOpen(start, end);

    // Find submap of ranges that intersect with the rowGroup
    ImmutableRangeMap<Long,BlockLocation> subRangeMap = blockMap.subRangeMap(rowGroupRange);

    // Iterate through each block in this submap and get the host for the block location
    for (Map.Entry<Range<Long>,BlockLocation> block : subRangeMap.asMapOfRanges().entrySet()) {
      String[] hosts;
      Range<Long> blockRange = block.getKey();
      try {
        hosts = block.getValue().getHosts();
      } catch (IOException ioe) {
        throw new RuntimeException("Failed to get hosts for block location", ioe);
      }
      Range<Long> intersection = rowGroupRange.intersection(blockRange);
      long bytes = intersection.upperEndpoint() - intersection.lowerEndpoint();

      // For each host in the current block location, add the intersecting bytes to the corresponding endpoint
      for (String host : hosts) {
        DrillbitEndpoint endpoint = getDrillBitEndpoint(host);
        if (endpointByteMap.containsKey(endpoint)) {
          endpointByteMap.put(endpoint, endpointByteMap.get(endpoint) + bytes);
        } else {
          if (endpoint != null ) endpointByteMap.put(endpoint, bytes);
        }
      }
    }

    rowGroup.setEndpointBytes(endpointByteMap);
    rowGroup.setMaxBytes(endpointByteMap.size() > 0 ? Collections.max(endpointByteMap.values()) : 0);
    logger.debug("Row group ({},{}) max bytes {}", rowGroup.getPath(), rowGroup.getStart(), rowGroup.getMaxBytes());
    watch.stop();
    logger.debug("Took {} ms to set endpoint bytes", watch.elapsed(TimeUnit.MILLISECONDS));
  }

  private DrillbitEndpoint getDrillBitEndpoint(String hostName) {
    return endPointMap.get(hostName);
  }

  /**
   * Builds a mapping of drillbit endpoints to hostnames
   */
  private void buildEndpointMap() {
    Stopwatch watch = new Stopwatch();
    watch.start();
    endPointMap = new HashMap<String, DrillbitEndpoint>();
    for (DrillbitEndpoint d : endpoints) {
      String hostName = d.getAddress();
      endPointMap.put(hostName, d);
    }
    watch.stop();
    logger.debug("Took {} ms to build endpoint map", watch.elapsed(TimeUnit.MILLISECONDS));
  }
}
