package org.apache.drill.exec.store;


import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import org.apache.drill.exec.store.parquet.ParquetGroupScan;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.*;

public class AffinityCalculator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AffinityCalculator.class);


  BlockLocation[] blocks;
  ImmutableRangeMap<Long,BlockLocation> blockMap;
  FileSystem fs;
  String fileName;
  Collection<DrillbitEndpoint> endpoints;
  HashMap<String,DrillbitEndpoint> endPointMap;

  public AffinityCalculator(String fileName, FileSystem fs, Collection<DrillbitEndpoint> endpoints) {
    this.fs = fs;
    this.fileName = fileName;
    this.endpoints = endpoints;
    buildBlockMap();
    buildEndpointMap();
  }

  private void buildBlockMap() {
    try {
      FileStatus file = fs.getFileStatus(new Path(fileName));
      long tC = System.nanoTime();
      blocks = fs.getFileBlockLocations(file, 0 , file.getLen());
      long tD = System.nanoTime();
      logger.debug("Block locations: {}", blocks);
      logger.debug("Took {} ms to get Block locations", (float)(tD - tC) / 1e6);
    } catch (IOException ioe) { throw new RuntimeException(ioe); }
    long tA = System.nanoTime();
    ImmutableRangeMap.Builder<Long, BlockLocation> blockMapBuilder = new ImmutableRangeMap.Builder<Long,BlockLocation>();
    for (BlockLocation block : blocks) {
      long start = block.getOffset();
      long end = start + block.getLength();
      Range<Long> range = Range.closedOpen(start, end);
      blockMapBuilder = blockMapBuilder.put(range, block);
    }
    blockMap = blockMapBuilder.build();
    long tB = System.nanoTime();
    logger.debug("Took {} ms to build block map", (float)(tB - tA) / 1e6);
  }
  /**
   *
   * @param entry
   */
  public void setEndpointBytes(ParquetGroupScan.RowGroupInfo entry) {
    long tA = System.nanoTime();
    HashMap<String,Long> hostMap = new HashMap<>();
    long start = entry.getStart();
    long end = start + entry.getLength();
    Range<Long> entryRange = Range.closedOpen(start, end);
    ImmutableRangeMap<Long,BlockLocation> subRangeMap = blockMap.subRangeMap(entryRange);
    for (Map.Entry<Range<Long>,BlockLocation> e : subRangeMap.asMapOfRanges().entrySet()) {
      String[] hosts = null;
      Range<Long> blockRange = e.getKey();
      try {
        hosts = e.getValue().getHosts();
      } catch (IOException ioe) { /*TODO Handle this exception */}
      Range<Long> intersection = entryRange.intersection(blockRange);
      long bytes = intersection.upperEndpoint() - intersection.lowerEndpoint();
      for (String host : hosts) {
        if (hostMap.containsKey(host)) {
          hostMap.put(host, hostMap.get(host) + bytes);
        } else {
          hostMap.put(host, bytes);
        }
      }
    }
    HashMap<DrillbitEndpoint,Long> ebs = new HashMap();
    try {
      for (Map.Entry<String,Long> hostEntry : hostMap.entrySet()) {
        String host = hostEntry.getKey();
        Long bytes = hostEntry.getValue();
        DrillbitEndpoint d = getDrillBitEndpoint(host);
        if (d != null ) ebs.put(d, bytes);
      }
    } catch (NullPointerException n) {}
    entry.setEndpointBytes(ebs);
    long tB = System.nanoTime();
    logger.debug("Took {} ms to set endpoint bytes", (float)(tB - tA) / 1e6);
  }

  private DrillbitEndpoint getDrillBitEndpoint(String hostName) {
    return endPointMap.get(hostName);
  }

  private void buildEndpointMap() {
    long tA = System.nanoTime();
    endPointMap = new HashMap<String, DrillbitEndpoint>();
    for (DrillbitEndpoint d : endpoints) {
      String hostName = d.getAddress();
      endPointMap.put(hostName, d);
    }
    long tB = System.nanoTime();
    logger.debug("Took {} ms to build endpoint map", (float)(tB - tA) / 1e6);
  }
}
