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
package org.apache.drill.exec.planner.sql.logical;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.sql.DrillSqlOperator;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.hive.HiveDrillNativeParquetScan;
import org.apache.drill.exec.store.hive.HiveReadEntry;
import org.apache.drill.exec.store.hive.HiveScan;
import org.apache.drill.exec.store.hive.HiveTable.HivePartition;
import org.apache.drill.exec.store.hive.HiveUtilities;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Convert Hive scan to use Drill's native parquet reader instead of Hive's native reader. It also adds a
 * project to convert/cast the output of Drill's native parquet reader to match the expected output of Hive's
 * native reader.
 */
public class ConvertHiveParquetScanToDrillParquetScan extends StoragePluginOptimizerRule {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ConvertHiveParquetScanToDrillParquetScan.class);

  public static final ConvertHiveParquetScanToDrillParquetScan INSTANCE = new ConvertHiveParquetScanToDrillParquetScan();

  private static final DrillSqlOperator INT96_TO_TIMESTAMP =
      new DrillSqlOperator("convert_fromTIMESTAMP_IMPALA", 1, true);

  private static final DrillSqlOperator RTRIM = new DrillSqlOperator("RTRIM", 1, true);

  private ConvertHiveParquetScanToDrillParquetScan() {
    super(RelOptHelper.any(DrillScanRel.class), "ConvertHiveScanToHiveDrillNativeScan:Parquet");
  }

  /**
   * Rule is matched when all of the following match:
   * 1) GroupScan in given DrillScalRel is an {@link HiveScan}
   * 2) {@link HiveScan} is not already rewritten using Drill's native readers
   * 3) InputFormat in Hive table metadata and all partitions metadata contains the same value
   *    {@link MapredParquetInputFormat}
   * 4) No error occurred while checking for the above conditions. An error is logged as warning.
   *
   * @param call
   * @return True if the rule can be applied. False otherwise
   */
  @Override
  public boolean matches(RelOptRuleCall call) {
    final DrillScanRel scanRel = (DrillScanRel) call.rel(0);

    if (!(scanRel.getGroupScan() instanceof HiveScan) || ((HiveScan) scanRel.getGroupScan()).isNativeReader()) {
      return false;
    }

    final HiveScan hiveScan = (HiveScan) scanRel.getGroupScan();
    final HiveConf hiveConf = hiveScan.getHiveConf();
    final Table hiveTable = hiveScan.hiveReadEntry.getTable();

    final Class<? extends InputFormat<?,?>> tableInputFormat =
        getInputFormatFromSD(MetaStoreUtils.getTableMetadata(hiveTable), hiveScan.hiveReadEntry, hiveTable.getSd(),
            hiveConf);
    if (tableInputFormat == null || !tableInputFormat.equals(MapredParquetInputFormat.class)) {
      return false;
    }

    final List<HivePartition> partitions = hiveScan.hiveReadEntry.getHivePartitionWrappers();
    if (partitions == null) {
      return true;
    }

    final List<FieldSchema> tableSchema = hiveTable.getSd().getCols();
    // Make sure all partitions have the same input format as the table input format
    for (HivePartition partition : partitions) {
      final StorageDescriptor partitionSD = partition.getPartition().getSd();
      Class<? extends InputFormat<?, ?>> inputFormat = getInputFormatFromSD(
          HiveUtilities.getPartitionMetadata(partition.getPartition(), hiveTable), hiveScan.hiveReadEntry, partitionSD,
          hiveConf);
      if (inputFormat == null || !inputFormat.equals(tableInputFormat)) {
        return false;
      }

      // Make sure the schema of the table and schema of the partition matches. If not return false. Schema changes
      // between table and partition can happen when table schema is altered using ALTER statements after some
      // partitions are already created. Currently native reader conversion doesn't handle schema changes between
      // partition and table. Hive has extensive list of convert methods to convert from one type to rest of the
      // possible types. Drill doesn't have the similar set of methods yet.
      if (!partitionSD.getCols().equals(tableSchema)) {
        logger.debug("Partitions schema is different from table schema. Currently native reader conversion can't " +
            "handle schema difference between partitions and table");
        return false;
      }
    }

    return true;
  }

  /**
   * Get the input format from given {@link StorageDescriptor}
   * @param properties
   * @param hiveReadEntry
   * @param sd
   * @return {@link InputFormat} class or null if a failure has occurred. Failure is logged as warning.
   */
  private Class<? extends InputFormat<?, ?>> getInputFormatFromSD(final Properties properties,
      final HiveReadEntry hiveReadEntry, final StorageDescriptor sd, final HiveConf hiveConf) {
    final Table hiveTable = hiveReadEntry.getTable();
    try {
      final String inputFormatName = sd.getInputFormat();
      if (!Strings.isNullOrEmpty(inputFormatName)) {
        return (Class<? extends InputFormat<?, ?>>) Class.forName(inputFormatName);
      }

      final JobConf job = new JobConf(hiveConf);
      HiveUtilities.addConfToJob(job, properties);
      return HiveUtilities.getInputFormatClass(job, sd, hiveTable);
    } catch (final Exception e) {
      logger.warn("Failed to get InputFormat class from Hive table '{}.{}'. StorageDescriptor [{}]",
          hiveTable.getDbName(), hiveTable.getTableName(), sd.toString(), e);
      return null;
    }
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    try {
      final DrillScanRel hiveScanRel = (DrillScanRel) call.rel(0);
      final HiveScan hiveScan = (HiveScan) hiveScanRel.getGroupScan();

      final PlannerSettings settings = PrelUtil.getPlannerSettings(call.getPlanner());
      final String partitionColumnLabel = settings.getFsPartitionColumnLabel();

      final Table hiveTable = hiveScan.hiveReadEntry.getTable();
      checkForUnsupportedDataTypes(hiveTable);

      final Map<String, String> partitionColMapping =
          getPartitionColMapping(hiveTable, partitionColumnLabel);

      final DrillScanRel nativeScanRel = createNativeScanRel(partitionColMapping, hiveScanRel);
      if(hiveScanRel.getRowType().getFieldCount() == 0) {
        call.transformTo(nativeScanRel);
      } else {
        final DrillProjectRel projectRel = createProjectRel(hiveScanRel, partitionColMapping, nativeScanRel);
        call.transformTo(projectRel);
      }
    } catch (final Exception e) {
      logger.warn("Failed to convert HiveScan to HiveDrillNativeParquetScan", e);
    }
  }

  /**
   * Create mapping of Hive partition column to directory column mapping.
   */
  private Map<String, String> getPartitionColMapping(final Table hiveTable, final String partitionColumnLabel) {
    final Map<String, String> partitionColMapping = Maps.newHashMap();
    int i = 0;
    for (FieldSchema col : hiveTable.getPartitionKeys()) {
      partitionColMapping.put(col.getName(), partitionColumnLabel+i);
      i++;
    }

    return partitionColMapping;
  }

  /**
   * Helper method which creates a DrillScalRel with native HiveScan.
   */
  private DrillScanRel createNativeScanRel(final Map<String, String> partitionColMapping,
      final DrillScanRel hiveScanRel) throws Exception{

    final RelDataTypeFactory typeFactory = hiveScanRel.getCluster().getTypeFactory();
    final RelDataType varCharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

    final List<String> nativeScanColNames = Lists.newArrayList();
    final List<RelDataType> nativeScanColTypes = Lists.newArrayList();
    for (RelDataTypeField field : hiveScanRel.getRowType().getFieldList()) {
      final String dirColName = partitionColMapping.get(field.getName());
      if (dirColName != null) { // partition column
        nativeScanColNames.add(dirColName);
        nativeScanColTypes.add(varCharType);
      } else {
        nativeScanColNames.add(field.getName());
        nativeScanColTypes.add(field.getType());
      }
    }

    final RelDataType nativeScanRowType = typeFactory.createStructType(nativeScanColTypes, nativeScanColNames);

    // Create the list of projected columns set in HiveScan. The order of this list may not be same as the order of
    // columns in HiveScan row type. Note: If the HiveScan.getColumn() contains a '*', we just need to add it as it is,
    // unlike above where we expanded the '*'. HiveScan and related (subscan) can handle '*'.
    final List<SchemaPath> nativeScanCols = Lists.newArrayList();
    for(SchemaPath colName : hiveScanRel.getColumns()) {
      final String partitionCol = partitionColMapping.get(colName.getAsUnescapedPath());
      if (partitionCol != null) {
        nativeScanCols.add(SchemaPath.getSimplePath(partitionCol));
      } else {
        nativeScanCols.add(colName);
      }
    }

    final HiveScan hiveScan = (HiveScan) hiveScanRel.getGroupScan();
    final HiveDrillNativeParquetScan nativeHiveScan =
        new HiveDrillNativeParquetScan(
            hiveScan.getUserName(),
            hiveScan.hiveReadEntry,
            hiveScan.storagePlugin,
            nativeScanCols,
            null);

    return new DrillScanRel(
        hiveScanRel.getCluster(),
        hiveScanRel.getTraitSet(),
        hiveScanRel.getTable(),
        nativeHiveScan,
        nativeScanRowType,
        nativeScanCols);
  }

  /**
   * Create a project that converts the native scan output to expected output of Hive scan.
   */
  private DrillProjectRel createProjectRel(final DrillScanRel hiveScanRel,
      final Map<String, String> partitionColMapping, final DrillScanRel nativeScanRel) {

    final List<RexNode> rexNodes = Lists.newArrayList();
    final RexBuilder rb = hiveScanRel.getCluster().getRexBuilder();
    final RelDataType hiveScanRowType = hiveScanRel.getRowType();

    for (String colName : hiveScanRowType.getFieldNames()) {
      final String dirColName = partitionColMapping.get(colName);
      if (dirColName != null) {
        rexNodes.add(createPartitionColumnCast(hiveScanRel, nativeScanRel, colName, dirColName, rb));
      } else {
        rexNodes.add(createColumnFormatConversion(hiveScanRel, nativeScanRel, colName, rb));
      }
    }

    return DrillProjectRel.create(
        hiveScanRel.getCluster(), hiveScanRel.getTraitSet(), nativeScanRel, rexNodes,
        hiveScanRowType /* project rowtype and HiveScanRel rowtype should be the same */);
  }

  /**
   * Apply any data format conversion expressions.
   */
  private RexNode createColumnFormatConversion(final DrillScanRel hiveScanRel, final DrillScanRel nativeScanRel,
      final String colName, final RexBuilder rb) {

    final RelDataType outputType = hiveScanRel.getRowType().getField(colName, false, false).getType();
    final RelDataTypeField inputField = nativeScanRel.getRowType().getField(colName, false, false);
    final RexInputRef inputRef = rb.makeInputRef(inputField.getType(), inputField.getIndex());

    if (outputType.getSqlTypeName() == SqlTypeName.TIMESTAMP) {
      // TIMESTAMP is stored as INT96 by Hive in ParquetFormat. Use convert_fromTIMESTAMP_IMPALA UDF to convert
      // INT96 format data to TIMESTAMP
      return rb.makeCall(INT96_TO_TIMESTAMP, inputRef);
    }

    return inputRef;
  }

  /**
   * Create a cast for partition column. Partition column is output as "VARCHAR" in native parquet reader. Cast it
   * appropriate type according the partition type in HiveScan.
   */
  private RexNode createPartitionColumnCast(final DrillScanRel hiveScanRel, final DrillScanRel nativeScanRel,
      final String outputColName, final String dirColName, final RexBuilder rb) {

    final RelDataType outputType = hiveScanRel.getRowType().getField(outputColName, false, false).getType();
    final RelDataTypeField inputField = nativeScanRel.getRowType().getField(dirColName, false, false);
    final RexInputRef inputRef =
        rb.makeInputRef(rb.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), inputField.getIndex());
    if (outputType.getSqlTypeName() == SqlTypeName.CHAR) {
      return rb.makeCall(RTRIM, inputRef);
    }

    return rb.makeCast(outputType, inputRef);
  }

  private void checkForUnsupportedDataTypes(final Table hiveTable) {
    for(FieldSchema hiveField : hiveTable.getSd().getCols()) {
      final Category category = TypeInfoUtils.getTypeInfoFromTypeString(hiveField.getType()).getCategory();
      if (category == Category.MAP ||
          category == Category.STRUCT ||
          category == Category.UNION ||
          category == Category.LIST) {
        HiveUtilities.throwUnsupportedHiveDataTypeError(category.toString());
      }
    }
  }
}
