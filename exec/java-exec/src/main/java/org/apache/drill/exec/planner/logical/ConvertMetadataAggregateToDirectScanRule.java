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
package org.apache.drill.exec.planner.logical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.IsPredicate;
import org.apache.drill.exec.metastore.analyze.AnalyzeColumnUtils;
import org.apache.drill.exec.metastore.analyze.MetadataAggregateContext;
import org.apache.drill.exec.metastore.analyze.MetastoreAnalyzeConstants;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.store.ColumnExplorer;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.drill.exec.store.direct.DirectGroupScan;
import org.apache.drill.exec.store.parquet.ParquetGroupScan;
import org.apache.drill.exec.store.pojo.DynamicPojoRecordReader;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.exec.util.Utilities;
import org.apache.drill.metastore.metadata.MetadataType;
import org.apache.drill.metastore.metadata.RowGroupMetadata;
import org.apache.drill.metastore.statistics.ColumnStatistics;
import org.apache.drill.metastore.statistics.ColumnStatisticsKind;
import org.apache.drill.metastore.statistics.ExactStatisticsConstants;
import org.apache.drill.metastore.statistics.StatisticsKind;
import org.apache.drill.metastore.statistics.TableStatisticsKind;
import org.apache.drill.shaded.guava.com.google.common.collect.Multimap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Rule which converts
 *
 * <pre>
 *   MetadataAggRel(metadataLevel=ROW_GROUP)
 *   \
 *   DrillScanRel
 * </pre>
 * <p/>
 * plan into
 * <pre>
 *   DrillDirectScanRel
 * </pre>
 * where {@link DrillDirectScanRel} is populated with row group metadata.
 * <p/>
 * For the case when aggregate level is not ROW_GROUP, resulting plan will be the following:
 *
 * <pre>
 *   MetadataAggRel(metadataLevel=FILE (or another non-ROW_GROUP value), createNewAggregations=false)
 *   \
 *   DrillDirectScanRel
 * </pre>
 */
public class ConvertMetadataAggregateToDirectScanRule extends RelOptRule {
  public static final ConvertMetadataAggregateToDirectScanRule INSTANCE =
      new ConvertMetadataAggregateToDirectScanRule();

  private static final Logger logger = LoggerFactory.getLogger(ConvertMetadataAggregateToDirectScanRule.class);

  public ConvertMetadataAggregateToDirectScanRule() {
    super(
        RelOptHelper.some(MetadataAggRel.class, RelOptHelper.any(DrillScanRel.class)),
        DrillRelFactories.LOGICAL_BUILDER, "ConvertMetadataAggregateToDirectScanRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    MetadataAggRel agg = call.rel(0);
    DrillScanRel scan = call.rel(1);

    GroupScan oldGrpScan = scan.getGroupScan();
    PlannerSettings settings = PrelUtil.getPlannerSettings(call.getPlanner());

    // Only apply the rule for parquet group scan and for the case when required column metadata is present
    if (!(oldGrpScan instanceof ParquetGroupScan)
        || (oldGrpScan.getTableMetadata().getInterestingColumns() != null
          && !oldGrpScan.getTableMetadata().getInterestingColumns().containsAll(agg.getContext().interestingColumns()))) {
      return;
    }

    try {
      DirectGroupScan directScan = buildDirectScan(agg.getContext().interestingColumns(), scan, settings);
      if (directScan == null) {
        logger.warn("Unable to use parquet metadata for ANALYZE since some required metadata is absent within parquet metadata");
        return;
      }

      RelNode converted = new DrillDirectScanRel(scan.getCluster(), scan.getTraitSet().plus(DrillRel.DRILL_LOGICAL),
          directScan, scan.getRowType());
      if (agg.getContext().metadataLevel() != MetadataType.ROW_GROUP) {
        MetadataAggregateContext updatedContext = agg.getContext().toBuilder()
            .createNewAggregations(false)
            .build();
        converted = new MetadataAggRel(agg.getCluster(), agg.getTraitSet(), converted, updatedContext);
      }

      call.transformTo(converted);
    } catch (Exception e) {
      logger.warn("Unable to use parquet metadata for ANALYZE due to exception {}", e.getMessage(), e);
    }
  }

  private DirectGroupScan buildDirectScan(List<SchemaPath> interestingColumns, DrillScanRel scan, PlannerSettings settings) throws IOException {
    DrillTable drillTable = Utilities.getDrillTable(scan.getTable());

    Map<String, Class<?>> schema = new HashMap<>();
    // map with index of specific field within record
    Map<String, Integer> fieldIndexes = new HashMap<>();
    int index = 0;

    // populates schema and fieldIndexes to be used when adding record values
    FormatSelection selection = (FormatSelection) drillTable.getSelection();
    // adds partition columns to the schema
    for (String partitionColumnName : ColumnExplorer.getPartitionColumnNames(selection.getSelection(), settings.getOptions())) {
      schema.put(partitionColumnName, String.class);
      fieldIndexes.put(partitionColumnName, index++);
    }

    String fqn = settings.getOptions().getOption(ExecConstants.IMPLICIT_FQN_COLUMN_LABEL).string_val;
    String rgi = settings.getOptions().getOption(ExecConstants.IMPLICIT_ROW_GROUP_INDEX_COLUMN_LABEL).string_val;
    String rgs = settings.getOptions().getOption(ExecConstants.IMPLICIT_ROW_GROUP_START_COLUMN_LABEL).string_val;
    String rgl = settings.getOptions().getOption(ExecConstants.IMPLICIT_ROW_GROUP_LENGTH_COLUMN_LABEL).string_val;
    String lmt = settings.getOptions().getOption(ExecConstants.IMPLICIT_LAST_MODIFIED_TIME_COLUMN_LABEL).string_val;

    // adds internal implicit columns to the schema
    fieldIndexes.put(fqn, index++);
    fieldIndexes.put(rgi, index++);
    fieldIndexes.put(rgs, index++);
    fieldIndexes.put(rgl, index++);
    fieldIndexes.put(lmt, index++);
    fieldIndexes.put(MetastoreAnalyzeConstants.SCHEMA_FIELD, index);

    schema.put(MetastoreAnalyzeConstants.SCHEMA_FIELD, String.class);
    schema.put(fqn, String.class);
    schema.put(rgi, String.class);
    schema.put(rgs, String.class);
    schema.put(rgl, String.class);
    schema.put(lmt, String.class);

    return populateRecords(interestingColumns, schema, fieldIndexes, scan, settings);
  }

  /**
   * Populates records list with row group metadata.
   */
  private DirectGroupScan populateRecords(Collection<SchemaPath> interestingColumns, Map<String, Class<?>> schema,
      Map<String, Integer> fieldIndexes, DrillScanRel scan, PlannerSettings settings) throws IOException {
    ParquetGroupScan parquetGroupScan = (ParquetGroupScan) scan.getGroupScan();
    DrillTable drillTable = Utilities.getDrillTable(scan.getTable());

    int index = fieldIndexes.size();
    Multimap<Path, RowGroupMetadata> rowGroupsMetadataMap = parquetGroupScan.getMetadataProvider().getRowGroupsMetadataMap();

    List<List<Object>> records = new ArrayList<>();
    FormatSelection selection = (FormatSelection) drillTable.getSelection();
    List<String> partitionColumnNames = ColumnExplorer.getPartitionColumnNames(selection.getSelection(), settings.getOptions());

    String fqn = settings.getOptions().getOption(ExecConstants.IMPLICIT_FQN_COLUMN_LABEL).string_val;
    String rgi = settings.getOptions().getOption(ExecConstants.IMPLICIT_ROW_GROUP_INDEX_COLUMN_LABEL).string_val;
    String rgs = settings.getOptions().getOption(ExecConstants.IMPLICIT_ROW_GROUP_START_COLUMN_LABEL).string_val;
    String rgl = settings.getOptions().getOption(ExecConstants.IMPLICIT_ROW_GROUP_LENGTH_COLUMN_LABEL).string_val;
    String lmt = settings.getOptions().getOption(ExecConstants.IMPLICIT_LAST_MODIFIED_TIME_COLUMN_LABEL).string_val;

    FileSystem rawFs = selection.getSelection().getSelectionRoot().getFileSystem(new Configuration());
    DrillFileSystem fileSystem =
        ImpersonationUtil.createFileSystem(ImpersonationUtil.getProcessUserName(), rawFs.getConf());

    for (Map.Entry<Path, RowGroupMetadata> rgEntry : rowGroupsMetadataMap.entries()) {
      Path path = rgEntry.getKey();
      RowGroupMetadata rowGroupMetadata = rgEntry.getValue();
      List<String> partitionValues = ColumnExplorer.listPartitionValues(path, selection.getSelection().getSelectionRoot(), false);
      List<Object> record = new ArrayList<>();
      for (int i = 0; i < partitionValues.size(); i++) {
        String partitionColumnName = partitionColumnNames.get(i);
        record.add(fieldIndexes.get(partitionColumnName), partitionValues.get(i));
      }

      record.add(fieldIndexes.get(fqn), ColumnExplorer.ImplicitFileColumns.FQN.getValue(path));
      record.add(fieldIndexes.get(rgi), rowGroupMetadata.getRowGroupIndex());

      if (interestingColumns == null) {
        interestingColumns = rowGroupMetadata.getColumnsStatistics().keySet();
      }

      // populates record list with row group column metadata
      for (SchemaPath schemaPath : interestingColumns) {
        ColumnStatistics columnStatistics = rowGroupMetadata.getColumnsStatistics().get(schemaPath);
        if (IsPredicate.isNullOrEmpty(columnStatistics)) {
          logger.warn("Statistics for {} column wasn't found within {} row group.", schemaPath, path);
          return null;
        }
        for (StatisticsKind statisticsKind : AnalyzeColumnUtils.COLUMN_STATISTICS_FUNCTIONS.keySet()) {
          Object statsValue;
          if (statisticsKind.getName().equalsIgnoreCase(TableStatisticsKind.ROW_COUNT.getName())) {
            statsValue = TableStatisticsKind.ROW_COUNT.getValue(rowGroupMetadata);
          } else if (statisticsKind.getName().equalsIgnoreCase(ColumnStatisticsKind.NON_NULL_COUNT.getName())) {
            statsValue = TableStatisticsKind.ROW_COUNT.getValue(rowGroupMetadata) - ColumnStatisticsKind.NULLS_COUNT.getFrom(columnStatistics);
          } else {
            statsValue = columnStatistics.get(statisticsKind);
          }
          String columnStatisticsFieldName = AnalyzeColumnUtils.getColumnStatisticsFieldName(schemaPath.getRootSegmentPath(), statisticsKind);
          fieldIndexes.putIfAbsent(columnStatisticsFieldName, index++);
          record.add(fieldIndexes.get(columnStatisticsFieldName), statsValue);
          if (statsValue != null) {
            schema.putIfAbsent(
                columnStatisticsFieldName,
                statsValue.getClass());
          }
        }
      }

      // populates record list with row group metadata
      for (StatisticsKind<?> statisticsKind : AnalyzeColumnUtils.META_STATISTICS_FUNCTIONS.keySet()) {
        String metadataStatisticsFieldName = AnalyzeColumnUtils.getMetadataStatisticsFieldName(statisticsKind);
        fieldIndexes.putIfAbsent(metadataStatisticsFieldName, index++);
        record.add(fieldIndexes.get(metadataStatisticsFieldName), rowGroupMetadata.getStatistic(statisticsKind));
      }

      // populates record list internal columns
      record.add(fieldIndexes.get(MetastoreAnalyzeConstants.SCHEMA_FIELD), rowGroupMetadata.getSchema().jsonString());
      record.add(fieldIndexes.get(rgs), Long.toString(rowGroupMetadata.getStatistic(() -> ExactStatisticsConstants.START)));
      record.add(fieldIndexes.get(rgl), Long.toString(rowGroupMetadata.getStatistic(() -> ExactStatisticsConstants.LENGTH)));
      record.add(fieldIndexes.get(lmt), String.valueOf(fileSystem.getFileStatus(path).getModificationTime()));

      records.add(record);
    }

    LinkedHashMap<String, Class<?>> orderedSchema = new LinkedHashMap<>();

    // DynamicPojoRecordReader requires LinkedHashMap with fields order
    // which corresponds to the value position in record list.
    // Sorts fieldIndexes by index and puts field names in required order to orderedSchema
    fieldIndexes.entrySet().stream()
        .sorted(Comparator.comparingInt(Map.Entry::getValue))
        .forEach(entry -> orderedSchema.put(entry.getKey(), schema.get(entry.getKey())));

    DynamicPojoRecordReader<?> reader = new DynamicPojoRecordReader<>(orderedSchema, records);

    ScanStats scanStats = new ScanStats(ScanStats.GroupScanProperty.EXACT_ROW_COUNT, records.size(), 1, schema.size());

    return new DirectGroupScan(reader, scanStats);
  }
}
