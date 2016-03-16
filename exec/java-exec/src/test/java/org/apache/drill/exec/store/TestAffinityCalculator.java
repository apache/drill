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

import java.util.LinkedList;

import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.store.parquet.ParquetGroupScan;
import org.apache.hadoop.fs.BlockLocation;
import org.junit.Test;

import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;

public class TestAffinityCalculator extends ExecTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestAffinityCalculator.class);

  String port = "1234";
  final String path = "path";

  public BlockLocation[] buildBlockLocations(String[] hosts, long blockSize) {
    String[] names = new String[hosts.length];

    for (int i = 0; i < hosts.length; i++) {
      hosts[i] = "host" + i;
      names[i] = "host:" + port;
    }

    BlockLocation[] blockLocations = new BlockLocation[3];
    blockLocations[0] = new BlockLocation(new String[]{names[0], names[1], names[2]}, new String[]{hosts[0], hosts[1], hosts[2]}, 0, blockSize);
    blockLocations[1] = new BlockLocation(new String[]{names[0], names[2], names[3]}, new String[]{hosts[0], hosts[2], hosts[3]}, blockSize, blockSize);
    blockLocations[2] = new BlockLocation(new String[]{names[0], names[1], names[3]}, new String[]{hosts[0], hosts[1], hosts[3]}, blockSize*2, blockSize);

    return blockLocations;
  }

  public BlockLocation[] buildBlockLocations2(String[] hosts, long blockSize) {
    String[] names = new String[hosts.length];

    for (int i = 0; i < hosts.length; i++) {
      hosts[i] = "host" + i;
      names[i] = "host:" + port;
    }

    BlockLocation[] blockLocations = new BlockLocation[4];
    blockLocations[0] = new BlockLocation(new String[]{names[0]}, new String[]{hosts[0]}, 0, blockSize);
    blockLocations[1] = new BlockLocation(new String[]{names[1]}, new String[]{hosts[1]}, blockSize, blockSize);
    blockLocations[3] = new BlockLocation(new String[]{names[3]}, new String[]{hosts[3]}, blockSize*2, blockSize);
    blockLocations[2] = new BlockLocation(new String[]{names[2]}, new String[]{hosts[2]}, blockSize*3, blockSize);

    return blockLocations;
  }
  public void buildRowGroups(LinkedList<ParquetGroupScan.RowGroupInfo> rowGroups, int numberOfBlocks, long blockSize, int numberOfRowGroups) {
    long rowGroupSize = numberOfBlocks * blockSize / numberOfRowGroups;

    rowGroups.clear();

    for (int i = 0; i < numberOfRowGroups; i++) {
      // buildRowGroups method seems not be used at all.  Pass -1 as rowCount.
      // Maybe remove this method completely ?
      rowGroups.add(new ParquetGroupScan.RowGroupInfo(path, (long)i*rowGroupSize, (long)rowGroupSize, i, -1));
    }
  }

  public LinkedList<CoordinationProtos.DrillbitEndpoint> buildEndpoints(int numberOfEndpoints) {
    LinkedList<CoordinationProtos.DrillbitEndpoint> endPoints = new LinkedList<>();

    for (int i = 0; i < numberOfEndpoints; i++) {
      endPoints.add(CoordinationProtos.DrillbitEndpoint.newBuilder().setAddress("host" + i).build());
    }
    return endPoints;
  }

//  @Test
//  public void testSetEndpointBytes(@Injectable final FileSystem fs, @Injectable final FileStatus file) throws Throwable{
//    final long blockSize = 256*1024*1024;
//    LinkedList<ParquetGroupScan.RowGroupInfo> rowGroups = new LinkedList<>();
//    int numberOfHosts = 4;
//    int numberOfBlocks = 3;
//    String port = "1234";
//    String[] hosts = new String[numberOfHosts];
//
//    final BlockLocation[] blockLocations = buildBlockLocations(hosts, blockSize);
//    final LinkedList<CoordinationProtos.DrillbitEndpoint> endPoints = buildEndpoints(numberOfHosts);
//    buildRowGroups(rowGroups, numberOfBlocks, blockSize, 3);
//
//    new NonStrictExpectations() {{
//      fs.getFileBlockLocations(file, 0, 3*blockSize); result = blockLocations;
//      fs.getFileStatus(new Path(path)); result = file;
//      file.getLen(); result = 3*blockSize;
//    }};
//
//
//    BlockMapBuilder ac = new BlockMapBuilder(fs, endPoints);
//    for (ParquetGroupScan.RowGroupInfo rowGroup : rowGroups) {
//      ac.setEndpointBytes(rowGroup);
//    }
//    ParquetGroupScan.RowGroupInfo rg = rowGroups.get(0);
//    Long b = rg.getEndpointBytes().get(endPoints.get(0));
//    assertEquals(blockSize,b.longValue());
//    b = rg.getEndpointBytes().get(endPoints.get(3));
//    assertNull(b);
//
//    buildRowGroups(rowGroups, numberOfBlocks, blockSize, 2);
//
//    ac = new BlockMapBuilder(fs, endPoints);
//    for (ParquetGroupScan.RowGroupInfo rowGroup : rowGroups) {
//      ac.setEndpointBytes(rowGroup);
//    }
//    rg = rowGroups.get(0);
//    b = rg.getEndpointBytes().get(endPoints.get(0));
//    assertEquals(blockSize*3/2,b.longValue());
//    b = rg.getEndpointBytes().get(endPoints.get(3));
//    assertEquals(blockSize / 2, b.longValue());
//
//    buildRowGroups(rowGroups, numberOfBlocks, blockSize, 6);
//
//    ac = new BlockMapBuilder(fs, endPoints);
//    for (ParquetGroupScan.RowGroupInfo rowGroup : rowGroups) {
//      ac.setEndpointBytes(rowGroup);
//    }
//    rg = rowGroups.get(0);
//    b = rg.getEndpointBytes().get(endPoints.get(0));
//    assertEquals(blockSize/2,b.longValue());
//    b = rg.getEndpointBytes().get(endPoints.get(3));
//    assertNull(b);
//  }

  @Test
  public void testBuildRangeMap() {
    BlockLocation[] blocks = buildBlockLocations(new String[4], 256*1024*1024);
    long tA = System.nanoTime();
    ImmutableRangeMap.Builder<Long, BlockLocation> blockMapBuilder = new ImmutableRangeMap.Builder<Long,BlockLocation>();
    for (BlockLocation block : blocks) {
      long start = block.getOffset();
      long end = start + block.getLength();
      Range<Long> range = Range.closedOpen(start, end);
      blockMapBuilder = blockMapBuilder.put(range, block);
    }
    ImmutableRangeMap<Long,BlockLocation> map = blockMapBuilder.build();
    long tB = System.nanoTime();
    System.out.println(String.format("Took %f ms to build range map", (tB - tA) / 1e6));
  }
  /*
  @Test
  public void testApplyAssignments(@Injectable final DrillbitContext context, @Injectable final ParquetStorageEngine engine,
                                   @Injectable final FileSystem fs, @Injectable final FileStatus file) throws IOException {

    final long blockSize = 256*1024*1024;
    LinkedList<ParquetGroupScan.RowGroupInfo> rowGroups = new LinkedList<>();
    int numberOfHosts = 4;
    int numberOfBlocks = 4;
    String port = "1234";
    String[] hosts = new String[numberOfHosts];

    final BlockLocation[] blockLocations = buildBlockLocations2(hosts, blockSize);
    final LinkedList<CoordinationProtos.DrillbitEndpoint> endPoints = buildEndpoints(numberOfHosts);

    new NonStrictExpectations() {{
      engine.getFileSystem(); result = fs;
      engine.getContext(); result = context;
      context.getBits(); result = endPoints;
      fs.getFileBlockLocations(file, 0, 3*blockSize); result = blockLocations;
      fs.getFileStatus(new Path(path)); result = file;
      file.getLen(); result = 3*blockSize;
    }};

    buildRowGroups(rowGroups, numberOfBlocks, blockSize, 4);
    ParquetGroupScan scan = new ParquetGroupScan(rowGroups, engine);

    List<EndpointAffinity> affinities = scan.getOperatorAffinity();

    for (EndpointAffinity affinity : affinities) {
      CoordinationProtos.DrillbitEndpoint db = affinity.getEndpoint();
      assertEquals((float)0.25, affinity.getAffinity(), .01);
    }

    scan.applyAssignments(endPoints);

    for (int i = 0; i < endPoints.size(); i++) {
      List<ParquetRowGroupScan.RowGroupReadEntry> rowGroupReadEntries = scan.getSpecificScan(i).getRowGroupReadEntries();
      assertEquals(1, rowGroupReadEntries.size());
      switch(i) {
        case 0: assertEquals(0,rowGroupReadEntries.get(0).getRowGroupIndex());
          break;
        case 1: assertEquals(1,rowGroupReadEntries.get(0).getRowGroupIndex());
          break;
        case 2: assertEquals(3,rowGroupReadEntries.get(0).getRowGroupIndex());
          break;
        case 3: assertEquals(2,rowGroupReadEntries.get(0).getRowGroupIndex());
          break;
      }
    }

    scan.applyAssignments(endPoints.subList(2,4));

    List<ParquetRowGroupScan.RowGroupReadEntry> rowGroupReadEntries = scan.getSpecificScan(0).getRowGroupReadEntries();
    assertEquals(2, rowGroupReadEntries.size());
    assertEquals(3,rowGroupReadEntries.get(0).getRowGroupIndex());

    rowGroupReadEntries = scan.getSpecificScan(1).getRowGroupReadEntries();
    assertEquals(2, rowGroupReadEntries.size());
    assertEquals(2,rowGroupReadEntries.get(0).getRowGroupIndex());

    LinkedList<CoordinationProtos.DrillbitEndpoint> dupList = new LinkedList<>();
    dupList.add(endPoints.get(0));
    dupList.add(endPoints.get(0));
    scan.applyAssignments(dupList);

    rowGroupReadEntries = scan.getSpecificScan(0).getRowGroupReadEntries();
    assertEquals(2, rowGroupReadEntries.size());
    rowGroupReadEntries = scan.getSpecificScan(1).getRowGroupReadEntries();
    assertEquals(2, rowGroupReadEntries.size());
  }
  */

}
