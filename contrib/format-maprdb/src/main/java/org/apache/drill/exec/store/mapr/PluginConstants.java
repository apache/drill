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
package org.apache.drill.exec.store.mapr;

import static org.ojai.DocumentConstants.ID_KEY;
import org.apache.drill.common.expression.SchemaPath;
import com.mapr.db.DBConstants;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.planner.cost.PluginCost.CheckValid;

public class PluginConstants {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PluginConstants.class);

  public final static CheckValid alwaysValid = new CheckValid<Integer>() {
    @Override
    public boolean isValid(Integer parameter) {
      return true;
    }
  };

  public final static CheckValid isNonNegative = new CheckValid<Integer>() {
    @Override
    public boolean isValid(Integer paramValue) {
      if (paramValue > 0 && paramValue <= Integer.MAX_VALUE) {
        return true;
      } else {
        logger.warn("Setting default value as the supplied parameter value is less than/equals to 0");
        return false;
      }
    }
  };

  public static final String SSD = "SSD";
  public static final String HDD = "HDD";
  public static final SchemaPath ID_SCHEMA_PATH = SchemaPath.getSimplePath(ID_KEY);

  public static final SchemaPath DOCUMENT_SCHEMA_PATH = SchemaPath.getSimplePath(DBConstants.DOCUMENT_FIELD);

  public static final int JSON_TABLE_NUM_TABLETS_PER_INDEX_DEFAULT = 32;

  public static final int JSON_TABLE_SCAN_SIZE_MB_MIN = 32;
  public static final int JSON_TABLE_SCAN_SIZE_MB_MAX = 8192;

  public static final String JSON_TABLE_SCAN_SIZE_MB = "format-maprdb.json.scanSizeMB";
  public static final int JSON_TABLE_SCAN_SIZE_MB_DEFAULT = 128;

  public static final String JSON_TABLE_RESTRICTED_SCAN_SIZE_MB = "format-maprdb.json.restrictedScanSizeMB";
  public static final int JSON_TABLE_RESTRICTED_SCAN_SIZE_MB_DEFAULT = 4096;

  public static final String JSON_TABLE_USE_NUM_REGIONS_FOR_DISTRIBUTION_PLANNING = "format-maprdb.json.useNumRegionsForDistribution";
  public static final boolean JSON_TABLE_USE_NUM_REGIONS_FOR_DISTRIBUTION_PLANNING_DEFAULT = false;

  public static final String JSON_TABLE_BLOCK_SIZE = "format-maprdb.json.pluginCost.blockSize";
  public static final int JSON_TABLE_BLOCK_SIZE_DEFAULT = 8192;

  public static final String JSON_TABLE_MEDIA_TYPE = "format-maprdb.json.mediaType";
  public static final String JSON_TABLE_MEDIA_TYPE_DEFAULT = SSD;

  public static final String JSON_TABLE_SSD_BLOCK_SEQ_READ_COST = "format-maprdb.json.pluginCost.ssdBlockSequentialReadCost";
  public static final int JSON_TABLE_SSD_BLOCK_SEQ_READ_COST_DEFAULT = 32 * DrillCostBase.BASE_CPU_COST * JSON_TABLE_BLOCK_SIZE_DEFAULT;

  // for SSD random and sequential costs are the same
  public static final String JSON_TABLE_SSD_BLOCK_RANDOM_READ_COST = "format-maprdb.json.pluginCost.ssdBlockRandomReadCost";
  public static final int JSON_TABLE_SSD_BLOCK_RANDOM_READ_COST_DEFAULT = JSON_TABLE_SSD_BLOCK_SEQ_READ_COST_DEFAULT;

  public static final String JSON_TABLE_AVERGE_COLUMN_SIZE = "format-maprdb.json.pluginCost.averageColumnSize";
  public static final int JSON_TABLE_AVERGE_COLUMN_SIZE_DEFAULT = 10;

  public static final int TABLE_BLOCK_SIZE_DEFAULT = 8192;
  public static final int TABLE_SSD_BLOCK_SEQ_READ_COST_DEFAULT = 32 * DrillCostBase.BASE_CPU_COST * TABLE_BLOCK_SIZE_DEFAULT;
  public static final int TABLE_SSD_BLOCK_RANDOM_READ_COST_DEFAULT = TABLE_SSD_BLOCK_SEQ_READ_COST_DEFAULT;
  public static final int TABLE_AVERGE_COLUMN_SIZE_DEFAULT = 10;
  public static final String JSON_TABLE_HDD_BLOCK_SEQ_READ_COST = "format-maprdb.json.pluginCost.hddBlockSequentialReadCost";
  public static final int JSON_TABLE_HDD_BLOCK_SEQ_READ_COST_DEFAULT = 6 * JSON_TABLE_SSD_BLOCK_SEQ_READ_COST_DEFAULT;

  public static final String JSON_TABLE_HDD_BLOCK_RANDOM_READ_COST = "format-maprdb.json.pluginCost.hddBlockRandomReadCost";
  public static final int JSON_TABLE_HDD_BLOCK_RANDOM_READ_COST_DEFAULT = 1000 * JSON_TABLE_HDD_BLOCK_SEQ_READ_COST_DEFAULT;
}
