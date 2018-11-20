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
package org.apache.drill.exec.planner.sql.logical;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.hive.HiveMetadataProvider;
import org.apache.drill.exec.store.hive.HiveReadEntry;
import org.apache.drill.exec.store.hive.HiveScan;
import org.apache.drill.exec.store.mapr.db.MapRDBFormatPlugin;
import org.apache.drill.exec.store.mapr.db.MapRDBFormatPluginConfig;
import org.apache.drill.exec.store.mapr.db.json.JsonScanSpec;
import org.apache.drill.exec.store.mapr.db.json.JsonTableGroupScan;
import org.ojai.DocumentConstants;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.drill.exec.store.hive.HiveUtilities.nativeReadersRuleMatches;

/**
 * Convert Hive scan to use Drill's native MapR-DB reader instead of Hive's MapR-DB JSON Handler.
 */
public class ConvertHiveMapRDBJsonScanToDrillMapRDBJsonScan extends StoragePluginOptimizerRule {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ConvertHiveMapRDBJsonScanToDrillMapRDBJsonScan.class);

  public static final ConvertHiveMapRDBJsonScanToDrillMapRDBJsonScan INSTANCE =
      new ConvertHiveMapRDBJsonScanToDrillMapRDBJsonScan();

  /**
   * The constants from org.apache.hadoop.hive.maprdb.json.conf.MapRDBConstants
   */
  private static final String MAPRDB_PFX = "maprdb.";
  private static final String MAPRDB_TABLE_NAME = MAPRDB_PFX + "table.name";
  private static final String ID_KEY = DocumentConstants.ID_KEY;
  private static final String MAPRDB_COLUMN_ID = MAPRDB_PFX + "column.id";

  private ConvertHiveMapRDBJsonScanToDrillMapRDBJsonScan() {
    super(RelOptHelper.any(DrillScanRel.class), "ConvertHiveScanToHiveDrillNativeScan:MapR-DB");
  }

  /**
   * {@see org.apache.drill.exec.store.hive.HiveUtilities#nativeReadersRuleMatches}
   */
  @Override
  public boolean matches(RelOptRuleCall call) {
    try {
      return nativeReadersRuleMatches(call,
          Class.forName("org.apache.hadoop.hive.maprdb.json.input.HiveMapRDBJsonInputFormat"));
    } catch (ClassNotFoundException e) {
      throw UserException.resourceError(e)
          .message("Current Drill build is not designed for working with Hive MapR-DB tables. " +
              "Please disable \"%s\" option", ExecConstants.HIVE_OPTIMIZE_MAPRDB_JSON_SCAN_WITH_NATIVE_READER)
          .build(logger);
    }
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    try {
      DrillScanRel hiveScanRel = call.rel(0);

      HiveScan hiveScan = (HiveScan) hiveScanRel.getGroupScan();
      HiveReadEntry hiveReadEntry = hiveScan.getHiveReadEntry();
      HiveMetadataProvider hiveMetadataProvider = new HiveMetadataProvider(hiveScan.getUserName(), hiveReadEntry,
          hiveScan.getHiveConf());
      if (hiveMetadataProvider.getInputSplits(hiveReadEntry).isEmpty()) {
        // table is empty, use original scan
        return;
      }

      if (hiveScan.getHiveReadEntry().getTable().isSetPartitionKeys()) {
        logger.warn("Hive MapR-DB JSON Handler doesn't support table partitioning. Consider recreating table without " +
            "partitions");
      }

      DrillScanRel nativeScanRel = createNativeScanRel(hiveScanRel);
      call.transformTo(nativeScanRel);

      /*
        Drill native scan should take precedence over Hive since it's more efficient and faster.
        Hive does not always give correct costing (i.e. for external tables Hive does not have number of rows
        and we calculate them approximately). On the contrary, Drill calculates number of rows exactly
        and thus Hive Scan can be chosen instead of Drill native scan because costings allegedly lower for Hive.
        To ensure Drill MapR-DB Json scan will be chosen, reduce Hive scan importance to 0.
       */
      call.getPlanner().setImportance(hiveScanRel, 0.0);
    } catch (final Exception e) {
      logger.warn("Failed to convert HiveScan to JsonScanSpec", e);
    }
  }

  /**
   * Helper method which creates a DrillScanRel with native Drill HiveScan.
   */
  private DrillScanRel createNativeScanRel(final DrillScanRel hiveScanRel) throws Exception {
    RelDataTypeFactory typeFactory = hiveScanRel.getCluster().getTypeFactory();
    HiveScan hiveScan = (HiveScan) hiveScanRel.getGroupScan();
    Map<String, String> parameters = hiveScan.getHiveReadEntry().getHiveTableWrapper().getParameters();

    JsonScanSpec scanSpec = new JsonScanSpec(parameters.get(MAPRDB_TABLE_NAME), null, null);
    List<SchemaPath> hiveScanCols = hiveScanRel.getColumns().stream()
        .map(colNameSchemaPath -> replaceOverriddenSchemaPath(parameters, colNameSchemaPath))
        .collect(Collectors.toList());
    JsonTableGroupScan nativeMapRDBScan =
        new JsonTableGroupScan(
            hiveScan.getUserName(),
            hiveScan.getStoragePlugin(),
            // TODO: We should use Hive format plugins here, once it will be implemented. DRILL-6621
            (MapRDBFormatPlugin) hiveScan.getStoragePlugin().getFormatPlugin(new MapRDBFormatPluginConfig()),
            scanSpec,
            hiveScanCols
        );

    List<String> nativeScanColNames = hiveScanRel.getRowType().getFieldList().stream()
        .map(field -> replaceOverriddenColumnId(parameters, field.getName()))
        .collect(Collectors.toList());
    List<RelDataType> nativeScanColTypes = hiveScanRel.getRowType().getFieldList().stream()
        .map(RelDataTypeField::getType)
        .collect(Collectors.toList());
    RelDataType nativeScanRowType = typeFactory.createStructType(nativeScanColTypes, nativeScanColNames);

    return new DrillScanRel(
        hiveScanRel.getCluster(),
        hiveScanRel.getTraitSet(),
        hiveScanRel.getTable(),
        nativeMapRDBScan,
        nativeScanRowType,
        hiveScanCols);
  }

  /**
   * Hive maps column id "_id" with custom user column id name. Replace it for {@link DrillScanRel}
   *
   * @param parameters Hive table properties
   * @param colName Hive column name
   * @return original column name, null if colName is absent
   */
  private String replaceOverriddenColumnId(Map<String, String> parameters, String colName) {
    return colName != null && colName.equals(parameters.get(MAPRDB_COLUMN_ID)) ? ID_KEY : colName;
  }

  /**
   * The same as above, but for {@link SchemaPath} object
   *
   * @param parameters Hive table properties
   * @param colNameSchemaPath SchemaPath with Hive column name
   * @return SchemaPath with original column name
   */
  private SchemaPath replaceOverriddenSchemaPath(Map<String, String> parameters, SchemaPath colNameSchemaPath) {
    String hiveColumnName = colNameSchemaPath.getRootSegmentPath();
    return hiveColumnName != null && hiveColumnName.equals(parameters.get(MAPRDB_COLUMN_ID))
        ? SchemaPath.getSimplePath(ID_KEY) : colNameSchemaPath;
  }
}
