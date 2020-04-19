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

package org.apache.drill.exec.store.elasticsearch;

import java.util.Map;
import java.util.TreeMap;

import org.elasticsearch.hadoop.rest.PartitionDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Computes size of each region for given table.
 */
public class TableStatsCalculator {
  private static final Logger logger = LoggerFactory.getLogger(TableStatsCalculator.class);

  public static final long DEFAULT_ROW_COUNT = 1024L * 1024L;

  /**
   * Maps each region to its size in bytes.
   */
  private Map<PartitionDefinition, Long> sizeMap;

  private int avgRowSizeInBytes = 1;

  /**
   * Computes size of each region for table.
   */

  public TableStatsCalculator(ElasticSearchPluginConfig storageConfig) {
    sizeMap = new TreeMap<>();
  }

  /**
   * Returns size of given region in bytes. Returns 0 if region was not found.
   */
  public long getRegionSizeInBytes(PartitionDefinition part) {
    if (sizeMap == null) {
      return (long) avgRowSizeInBytes * DEFAULT_ROW_COUNT; // 1 million rows
    } else {
      Long size = sizeMap.get(part);
      if (size == null) {
        logger.debug("Unknown region:" + part);
        return 0;
      } else {
        return size;
      }
    }
  }
}