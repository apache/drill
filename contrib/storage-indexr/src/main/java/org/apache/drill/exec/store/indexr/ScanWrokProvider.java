/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.indexr;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.cursors.ObjectLongCursor;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.schedule.EndpointByteMap;
import org.apache.drill.exec.store.schedule.EndpointByteMapImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.indexr.segment.InfoSegment;
import io.indexr.segment.RSValue;
import io.indexr.segment.SegmentFd;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.helper.RangeWork;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.rc.RCOperator;
import io.indexr.segment.rt.RTSGroupInfo;
import io.indexr.server.HybridTable;
import io.indexr.server.TablePool;

public class ScanWrokProvider {
  private static final Logger logger = LoggerFactory.getLogger(ScanWrokProvider.class);
  private static final double RT_COST_RATE = 3.5;
  private static final int DEFAULT_PACK_SPLIT_STEP = 16;
  private static final Random RANDOM = new Random();

  private static final Cache<CacheKey, Works> workCache = CacheBuilder.newBuilder()
      .initialCapacity(1024)
      .expireAfterAccess(5, TimeUnit.MINUTES)
      .maximumSize(4096)
      .build();
  private static final Cache<CacheKey, Stat> statCache = CacheBuilder.newBuilder()
      .initialCapacity(1024)
      .expireAfterAccess(5, TimeUnit.MINUTES)
      .maximumSize(4096)
      .build();

  private static class CacheKey {
    private final String scanId;
    private final IndexRScanSpec scanSpec;
    private final long limitScanRows;

    public CacheKey(String scanId, IndexRScanSpec scanSpec, long limitScanRows) {
      this.scanId = scanId;
      this.scanSpec = scanSpec;
      this.limitScanRows = limitScanRows;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CacheKey cacheKey = (CacheKey) o;

      if (scanId != null ? !scanId.equals(cacheKey.scanId) : cacheKey.scanId != null) return false;
      if (limitScanRows != cacheKey.limitScanRows) return false;
      return scanSpec != null ? scanSpec.equals(cacheKey.scanSpec) : cacheKey.scanSpec == null;
    }

    @Override
    public int hashCode() {
      int result = scanId != null ? scanId.hashCode() : 0;
      result = 31 * result + (scanSpec != null ? scanSpec.hashCode() : 0);
      result = 31 * result + (int) (limitScanRows ^ (limitScanRows >>> 32));
      return result;
    }
  }

  public static class Stat {
    public final long scanRowCount;
    public final int minPw;
    public final int maxPw;

    public Stat(long scanRowCount, int minPw, int maxPw) {
      this.scanRowCount = scanRowCount;
      this.minPw = minPw;
      this.maxPw = maxPw;
    }
  }

  public static class Works {
    public final int minPw;
    public final int maxPw;
    public final List<ScanCompleteWork> historyWorks;
    public final Map<DrillbitEndpoint, List<ScanCompleteWork>> realtimeWorks;
    public final List<EndpointAffinity> endpointAffinities;

    public Works(int minPw,
                 int maxPw,
                 List<ScanCompleteWork> historyWorks,
                 Map<DrillbitEndpoint, List<ScanCompleteWork>> realtimeWorks,
                 List<EndpointAffinity> endpointAffinities) {
      this.minPw = minPw;
      this.maxPw = maxPw;
      this.historyWorks = historyWorks;
      this.realtimeWorks = realtimeWorks;
      this.endpointAffinities = endpointAffinities;
    }
  }

  public static class FragmentAssignment {
    int fragmentCount;
    int fragmentIndex;
    List<RangeWork> endpointWorks;

    public FragmentAssignment(int fragmentCount, int fragmentIndex, List<RangeWork> endpointWorks) {
      this.fragmentCount = fragmentCount;
      this.fragmentIndex = fragmentIndex;
      this.endpointWorks = endpointWorks;
    }

    @Override
    public String toString() {
      return "FragmentAssignment{" +
          "fragmentCount=" + fragmentCount +
          ", fragmentIndex=" + fragmentIndex +
          ", endpointWorks=" + endpointWorks +
          '}';
    }
  }


  public static Stat getStat(IndexRStoragePlugin plugin,
                             IndexRScanSpec scanSpec,
                             String scanId,
                             long limitScanRows,
                             List<SchemaPath> columns) {
    CacheKey key = new CacheKey(scanId, scanSpec, limitScanRows);
    try {
      return statCache.get(key, () -> calStat(plugin, scanSpec, scanId, limitScanRows, columns));
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private static Stat calStat(IndexRStoragePlugin plugin,
                              IndexRScanSpec scanSpec,
                              String scanId,
                              long limitScanRows,
                              List<SchemaPath> columns) throws IOException {
    TablePool tablePool = plugin.indexRNode().getTablePool();
    HybridTable table = tablePool.get(scanSpec.getTableName());
    SegmentSchema schema = table.schema().schema;

    List<SegmentFd> allSegments = table.segmentPool().all();

    RCOperator rsFilter = scanSpec.getRSFilter();
    long totalRowCount = 0;
    long passRowCount = 0;
    int passPackCount = 0;
    int passSegmentCount = 0;
    Set<String> realtimeNodes = new HashSet<>();
    for (SegmentFd fd : allSegments) {
      InfoSegment infoSegment = fd.info();
      totalRowCount += infoSegment.rowCount();
      if (rsFilter != null) {
        rsFilter.materialize(infoSegment.schema().getColumns());
      }
      if (rsFilter == null || rsFilter.roughCheckOnColumn(infoSegment) != RSValue.None) {
        if (infoSegment.isRealtime()) {
          passPackCount += DataPack.rowCountToPackCount(infoSegment.rowCount());
          RTSGroupInfo rtsg = (RTSGroupInfo) infoSegment;
          realtimeNodes.add(rtsg.host());
        } else {
          passPackCount += infoSegment.packCount();
        }
        passRowCount += infoSegment.rowCount();
        passSegmentCount++;
      } else {
        logger.debug("rs filter ignore segment {}", infoSegment.name());
      }
      if (passRowCount >= limitScanRows) {
        break;
      }
    }

    long statScanRowCount = passRowCount;
    if (rsFilter != null
        && passRowCount == totalRowCount
        && passRowCount > 0) {
      // We definitely want the scan node with rcFilter to win in query plan competition,
      // so that we trick the planner with less scan rows.
      // The rsFilter is good, even if it looks useless in this level, i.e. roughCheckOnColumn.
      // It could make different in next level, i.e. roughCheckOnPack.
      statScanRowCount = passRowCount - 1;
    }
    statScanRowCount = Math.min(statScanRowCount, limitScanRows);

    int minPw = Math.max(1, realtimeNodes.size());

    // estimated maxPw.
    int packGroup = passPackCount / DEFAULT_PACK_SPLIT_STEP;
    int maxPw;
    if (packGroup > passSegmentCount) {
      maxPw = (int) ((packGroup - passSegmentCount) * 0.45) + passSegmentCount;
    } else {
      maxPw = passSegmentCount;
    }

    // Hack!!!
    // Drill HardAffinityFragmentParallelizer will slice the scan rows to decide the maxPw,
    // we also need to do it here.
    maxPw = Math.min((int) Math.ceil((double) statScanRowCount / ExecConstants.SLICE_TARGET_DEFAULT), maxPw);
    maxPw = Math.max(minPw, maxPw);

    // Add one width to advoid some error from drill.
    maxPw++;

    logger.debug("=============== calStat totalRowCount:{}, passRowCount:{}, passPackCount:{}, statScanRowCount:{}, maxPw: {}",
        totalRowCount, passRowCount, passPackCount, statScanRowCount, maxPw);

    return new Stat(statScanRowCount, minPw, maxPw);
  }

  public static Works getScanWorks(boolean must,
                                   IndexRStoragePlugin plugin,
                                   IndexRScanSpec scanSpec,
                                   String scanId,
                                   long limitScanRows,
                                   List<SchemaPath> columns) {
    CacheKey key = new CacheKey(scanId, scanSpec, limitScanRows);
    try {
      return must
          ? workCache.get(key, () -> calScanWorks(plugin, scanSpec, scanId, limitScanRows, columns))
          : workCache.getIfPresent(key);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private static Works calScanWorks(IndexRStoragePlugin plugin,
                                    IndexRScanSpec scanSpec,
                                    String scanId,
                                    long limitScanRows,
                                    List<SchemaPath> columns) throws IOException {
    TablePool tablePool = plugin.indexRNode().getTablePool();
    HybridTable table = tablePool.get(scanSpec.getTableName());
    SegmentSchema schema = table.schema().schema;
    List<SegmentFd> allSegments = table.segmentPool().all();

    RCOperator rsFilter = scanSpec.getRSFilter();

    long totalRowCount = 0;
    long passRowCount = 0;
    int passSegmentCount = 0;

    List<InfoSegment> usedSegments = new ArrayList<>(Math.max(allSegments.size() / 2, 100));
    for (SegmentFd fd : allSegments) {
      InfoSegment infoSegment = fd.info();
      totalRowCount += infoSegment.rowCount();
      if (rsFilter != null) {
        rsFilter.materialize(infoSegment.schema().getColumns());
      }
      if (rsFilter == null || rsFilter.roughCheckOnColumn(infoSegment) != RSValue.None) {
        usedSegments.add(infoSegment);
        passRowCount += infoSegment.rowCount();
        passSegmentCount++;
      } else {
        logger.debug("rs filter ignore segment {}", infoSegment.name());
      }

      if (passRowCount >= limitScanRows) {
        break;
      }
    }

    if (logger.isInfoEnabled()) {
      double passRate = totalRowCount == 0 ? 0.0 : ((double) passRowCount) / totalRowCount;
      passRate = Math.min(passRate, 1.0);
      logger.info("=============== calScanWorks limitScanRows: {}, Pass rate: {}, scan row: {}",
          limitScanRows,
          String.format("%.2f%%", (float) (passRate * 100)),
          passRowCount);
    }

    Map<String, DrillbitEndpoint> hostEndpointMap = new HashMap<>();
    List<DrillbitEndpoint> endpointList = new ArrayList<>();
    for (DrillbitEndpoint endpoint : plugin.context().getBits()) {
      hostEndpointMap.put(endpoint.getAddress(), endpoint);
      endpointList.add(endpoint);
    }

    int packSplitStep = (int) Math.min(DEFAULT_PACK_SPLIT_STEP, Math.max(limitScanRows / DataPack.MAX_COUNT, 1));

    double hisByteCostPerRow = DrillIndexRTable.byteCostPerRow(table, columns, true);
    double rtByteCostPerRow = hisByteCostPerRow * RT_COST_RATE;

    List<ScanCompleteWork> historyWorks = new ArrayList<>(1024);
    Map<DrillbitEndpoint, List<ScanCompleteWork>> realtimeWorks = new HashMap<>();
    ObjectLongHashMap<DrillbitEndpoint> affinities = new ObjectLongHashMap<>();

    long totalScanRowCount = 0;
    int totalScanPackCount = 0;

    for (InfoSegment segment : usedSegments) {
      List<String> hosts = table.segmentLocality().getHosts(segment.name(), segment.isRealtime());
      if (segment.isRealtime()) {
        // Realtime segments.
        assert hosts.size() == 1;
        long bytes = (long) (rtByteCostPerRow * segment.rowCount());
        DrillbitEndpoint endpoint = hostEndpointMap.get(hosts.get(0));
        if (endpoint == null) {
          // Looks like this endpoint is down, the realtime segment on it cannot reach right now, let's move on.
          continue;
        }

        affinities.putOrAdd(endpoint, bytes, bytes);

        EndpointByteMap endpointByteMap = new EndpointByteMapImpl();
        endpointByteMap.add(endpoint, bytes);

        List<ScanCompleteWork> rtWorks = realtimeWorks.get(endpoint);
        if (rtWorks == null) {
          rtWorks = new ArrayList<>();
          realtimeWorks.put(endpoint, rtWorks);
        }
        rtWorks.add(new ScanCompleteWork(segment.name(), -1, 0, bytes, endpointByteMap));

        totalScanRowCount += segment.rowCount();
        totalScanPackCount += DataPack.rowCountToPackCount(segment.rowCount());

        if (totalScanRowCount >= limitScanRows) {
          break;
        }
      } else {
        // Historical segments.
        int segPackCount = segment.packCount();
        long segRowCount = segment.rowCount();
        int startPackId = 0;
        while (startPackId < segPackCount) {
          int endPackId = Math.min(startPackId + packSplitStep, segPackCount);
          int scanPackCount = endPackId - startPackId;

          long rowCount;
          if (endPackId < segPackCount) {
            rowCount = scanPackCount * DataPack.MAX_COUNT;
          } else {
            assert endPackId == segPackCount;
            // We guarantee that all packs are full (DataPack.MAX_COUNT) except the last one.
            rowCount = (scanPackCount - 1) * DataPack.MAX_COUNT + DataPack.packRowCount(segRowCount, endPackId - 1);
          }

          long bytes = (long) (hisByteCostPerRow * rowCount);
          EndpointByteMap endpointByteMap = new EndpointByteMapImpl();
          boolean assigned = false;
          for (String host : hosts) {
            DrillbitEndpoint endpoint = hostEndpointMap.get(host);
            // endpoint may not found in this host, could be shutdown or not installed.
            if (endpoint != null) {
              endpointByteMap.add(endpoint, bytes);
              affinities.putOrAdd(endpoint, bytes, bytes);
              assigned = true;
            }
          }
          if (!assigned) {
            // Randomly pick a volunteer to take it.
            DrillbitEndpoint endpoint = endpointList.get(RANDOM.nextInt(endpointList.size()));
            endpointByteMap.add(endpoint, bytes);
            affinities.putOrAdd(endpoint, bytes, bytes);
          }
          historyWorks.add(new ScanCompleteWork(segment.name(), startPackId, endPackId, bytes, endpointByteMap));

          startPackId = endPackId;

          totalScanRowCount += rowCount;
          totalScanPackCount += scanPackCount;

          if (totalScanRowCount >= limitScanRows) {
            break;
          }
        }
      }
    }


    List<EndpointAffinity> endpointAffinities = new ArrayList<>(affinities.size());
    for (ObjectLongCursor<DrillbitEndpoint> cursor : affinities) {
      endpointAffinities.add(new EndpointAffinity(cursor.key, cursor.value));
    }
    if (endpointAffinities.size() == 0) {
      endpointAffinities.add(new EndpointAffinity(plugin.context().getEndpoint(), 0));
    }
    endpointAffinities.sort(new RTSFirstDesc(realtimeWorks));

    int minPw = Math.max(1, realtimeWorks.size());

    // TODO fix the algorithm of maxPw

    int packGroup = totalScanPackCount / DEFAULT_PACK_SPLIT_STEP;
    int maxPw;
    if (packGroup > passSegmentCount) {
      maxPw = (int) ((packGroup - passSegmentCount) * 0.45) + passSegmentCount;
    } else {
      maxPw = passSegmentCount;
    }

    // Hack!!!
    // Drill HardAffinityFragmentParallelizer will slice the scan rows to decide the maxPw,
    // we also need to do it here.
    maxPw = Math.min((int) Math.ceil((double) totalScanRowCount / ExecConstants.SLICE_TARGET_DEFAULT), maxPw);
    maxPw = Math.max(maxPw, minPw);

    if (maxPw < endpointAffinities.size()) {
      endpointAffinities = endpointAffinities.subList(0, maxPw);
    }
    for (EndpointAffinity ea : endpointAffinities) {
      ea.setAssignmentRequired();
    }

    // Add one width to advoid some error from drill.
    maxPw++;

    logger.debug("=============== calScanWorks minPw: {}, maxPw: {}, historyWorks: {}, realtimeWorks({}): {}, endpointAffinities: {}",
        minPw, maxPw, historyWorks, realtimeWorks.size(), realtimeWorks, endpointAffinities);

    return new Works(minPw, maxPw, historyWorks, realtimeWorks, endpointAffinities);
  }

  private static int colCount(HybridTable table, List<SchemaPath> columns) {
    int colCount = 0;
    if (columns == null) {
      colCount = 20;
    } else if (AbstractRecordReader.isStarQuery(columns)) {
      colCount = table.schema().schema.columns.size();
    } else {
      colCount = columns.size();
    }
    return colCount;
  }

  private static class RTSFirstDesc implements Comparator<EndpointAffinity> {

    final Map<DrillbitEndpoint, List<ScanCompleteWork>> isRealtime;

    public RTSFirstDesc(Map<DrillbitEndpoint, List<ScanCompleteWork>> isRealtime) {
      this.isRealtime = isRealtime;
    }

    @Override
    public int compare(EndpointAffinity o1, EndpointAffinity o2) {
      boolean rt1 = isRealtime.containsKey(o1.getEndpoint());
      boolean rt2 = isRealtime.containsKey(o2.getEndpoint());
      if (rt1 && !rt2) {
        return -1;
      } else if (!rt1 && rt2) {
        return 1;
      }
      return Double.compare(o2.getAffinity(), o1.getAffinity());
    }
  }
}
