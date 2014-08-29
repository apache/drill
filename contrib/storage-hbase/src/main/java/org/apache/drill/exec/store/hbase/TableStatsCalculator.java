/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.hbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.drill.common.config.DrillConfig;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HServerLoad.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Computes size of each region for given table.
 */
public class TableStatsCalculator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TableStatsCalculator.class);

  private static final String DRILL_EXEC_HBASE_SCAN_SAMPLE_ROWS_COUNT = "drill.exec.hbase.scan.samplerows.count";

  private static final String DRILL_EXEC_HBASE_SCAN_SIZECALCULATOR_ENABLED = "drill.exec.hbase.scan.sizecalculator.enabled";

  private static final int DEFAULT_SAMPLE_SIZE = 100;

  /**
   * Maps each region to its size in bytes.
   */
  private Map<byte[], Long> sizeMap = null;

  private int avgRowSizeInBytes;

  private int colsPerRow;

  /**
   * Computes size of each region for table.
   *
   * @param table
   * @param hbaseScanSpec 
   * @param drillConfig 
   * @throws IOException
   */
  public TableStatsCalculator(HTable table, HBaseScanSpec hbaseScanSpec, DrillConfig config) throws IOException {
    HBaseAdmin admin = new HBaseAdmin(table.getConfiguration());
    try {
      int rowsToSample = rowsToSample(config);
      if (rowsToSample > 0) {
        Scan scan = new Scan(hbaseScanSpec.getStartRow(), hbaseScanSpec.getStopRow());
        scan.setCaching(rowsToSample < DEFAULT_SAMPLE_SIZE ? rowsToSample : DEFAULT_SAMPLE_SIZE);
        scan.setMaxVersions(1);
        ResultScanner scanner = table.getScanner(scan);
        int rowSizeSum = 0, numColumnsSum = 0, rowCount = 0;
        for (; rowCount < rowsToSample; ++rowCount) {
          Result row = scanner.next();
          if (row == null) {
            break;
          }
          numColumnsSum += row.size();
          rowSizeSum += row.getBytes().getLength();
        }
        avgRowSizeInBytes = rowSizeSum/rowCount;
        colsPerRow = numColumnsSum/rowCount;
        scanner.close();
      }

      if (!enabled(config)) {
        logger.info("Region size calculation disabled.");
        return;
      }

      logger.info("Calculating region sizes for table \"" + new String(table.getTableName()) + "\".");

      //get regions for table
      Set<HRegionInfo> tableRegionInfos = table.getRegionLocations().keySet();
      Set<byte[]> tableRegions = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
      for (HRegionInfo regionInfo : tableRegionInfos) {
        tableRegions.add(regionInfo.getRegionName());
      }

      ClusterStatus clusterStatus = null;
      try {
        clusterStatus = admin.getClusterStatus();
      } catch (Exception e) {
        logger.debug(e.getMessage());
      } finally {
        if (clusterStatus == null) {
          return;
        }
      }

      sizeMap = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);

      Collection<ServerName> servers = clusterStatus.getServers();
      //iterate all cluster regions, filter regions from our table and compute their size
      for (ServerName serverName : servers) {
        HServerLoad serverLoad = clusterStatus.getLoad(serverName);

        for (RegionLoad regionLoad : serverLoad.getRegionsLoad().values()) {
          byte[] regionId = regionLoad.getName();

          if (tableRegions.contains(regionId)) {
            long regionSizeMB = regionLoad.getMemStoreSizeMB() + regionLoad.getStorefileSizeMB();
            sizeMap.put(regionId, (regionSizeMB > 0 ? regionSizeMB : 1) * (1024*1024));
            if (logger.isDebugEnabled()) {
              logger.debug("Region " + regionLoad.getNameAsString() + " has size " + regionSizeMB + "MB");
            }
          }
        }
      }
      logger.debug("Region sizes calculated");
    } finally {
      admin.close();
    }

  }

  private boolean enabled(DrillConfig config) {
    return config.hasPath(DRILL_EXEC_HBASE_SCAN_SIZECALCULATOR_ENABLED)
        ? config.getBoolean(DRILL_EXEC_HBASE_SCAN_SIZECALCULATOR_ENABLED) : true;
  }

  private int rowsToSample(DrillConfig config) {
    return config.hasPath(DRILL_EXEC_HBASE_SCAN_SAMPLE_ROWS_COUNT)
        ? config.getInt(DRILL_EXEC_HBASE_SCAN_SAMPLE_ROWS_COUNT) : DEFAULT_SAMPLE_SIZE;
  }

  /**
   * Returns size of given region in bytes. Returns 0 if region was not found.
   */
  public long getRegionSizeInBytes(byte[] regionId) {
    if (sizeMap == null) {
      return avgRowSizeInBytes*1024*1024; // 1 million rows
    } else {
      Long size = sizeMap.get(regionId);
      if (size == null) {
        logger.debug("Unknown region:" + Arrays.toString(regionId));
        return 0;
      } else {
        return size;
      }
    }
  }

  public int getAvgRowSizeInBytes() {
    return avgRowSizeInBytes;
  }

  public int getColsPerRow() {
    return colsPerRow;
  }

}
