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
package org.apache.drill.exec.physical.impl.metadata;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.metastore.analyze.MetastoreAnalyzeConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.MetadataAggPOP;
import org.apache.drill.exec.physical.impl.aggregate.StreamingAggBatch;
import org.apache.drill.exec.physical.impl.aggregate.StreamingAggregator;
import org.apache.drill.exec.metastore.analyze.AnalyzeColumnUtils;
import org.apache.drill.exec.planner.types.DrillRelDataTypeSystem;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

/**
 * Operator which adds aggregate calls for all incoming columns to calculate required metadata and produces aggregations.
 * If aggregation is performed on top of another aggregation, required aggregate calls for merging metadata will be added.
 */
public class MetadataAggBatch extends StreamingAggBatch {

  private List<NamedExpression> valueExpressions;

  public MetadataAggBatch(MetadataAggPOP popConfig, RecordBatch incoming, FragmentContext context) throws OutOfMemoryException {
    super(popConfig, incoming, context);
  }

  @Override
  protected StreamingAggregator createAggregatorInternal()
      throws SchemaChangeException, ClassTransformationException, IOException {
    valueExpressions = new ArrayList<>();
    MetadataAggPOP popConfig = (MetadataAggPOP) this.popConfig;

    List<SchemaPath> excludedColumns = popConfig.getContext().excludedColumns();

    BatchSchema schema = incoming.getSchema();
    // Iterates through input expressions and adds aggregate calls for table fields
    // to collect required statistics (MIN, MAX, COUNT, etc.) or aggregate calls to merge incoming metadata
    getUnflattenedFileds(Lists.newArrayList(schema), null)
        .forEach((fieldName, fieldRef) -> addColumnAggregateCalls(fieldRef, fieldName));

    List<LogicalExpression> fieldsList = new ArrayList<>();
    StreamSupport.stream(schema.spliterator(), false)
        .map(MaterializedField::getName)
        .filter(field -> !excludedColumns.contains(FieldReference.getWithQuotedRef(field)))
        .forEach(filedName -> {
          // adds string literal with field name to the list
          fieldsList.add(ValueExpressions.getChar(filedName,
              DrillRelDataTypeSystem.DRILL_REL_DATATYPE_SYSTEM.getDefaultPrecision(SqlTypeName.VARCHAR)));
          // adds field reference to the list
          fieldsList.add(FieldReference.getWithQuotedRef(filedName));
        });

    if (popConfig.getContext().createNewAggregations()) {
      addMetadataAggregateCalls();
      // infer schema from incoming data
      addSchemaCall(fieldsList);
      addNewMetadataAggregations();
    } else {
      addCollectListCall(fieldsList);
      addMergeSchemaCall();
    }

    for (SchemaPath excludedColumn : excludedColumns) {
      if (excludedColumn.equals(SchemaPath.getSimplePath(context.getOptions().getString(ExecConstants.IMPLICIT_ROW_GROUP_START_COLUMN_LABEL)))
          || excludedColumn.equals(SchemaPath.getSimplePath(context.getOptions().getString(ExecConstants.IMPLICIT_ROW_GROUP_LENGTH_COLUMN_LABEL)))) {
        LogicalExpression lastModifiedTime = new FunctionCall("any_value",
            Collections.singletonList(
                FieldReference.getWithQuotedRef(excludedColumn.getRootSegmentPath())),
            ExpressionPosition.UNKNOWN);

        valueExpressions.add(new NamedExpression(lastModifiedTime,
            FieldReference.getWithQuotedRef(excludedColumn.getRootSegmentPath())));
      }
    }

    addMaxLastModifiedCall();

    return super.createAggregatorInternal();
  }

  /**
   * Adds {@code max(`lastModifiedTime`)} function call to the value expressions list.
   */
  private void addMaxLastModifiedCall() {
    String lastModifiedColumn = context.getOptions().getString(ExecConstants.IMPLICIT_LAST_MODIFIED_TIME_COLUMN_LABEL);
    LogicalExpression lastModifiedTime = new FunctionCall("max",
        Collections.singletonList(
            FieldReference.getWithQuotedRef(lastModifiedColumn)),
        ExpressionPosition.UNKNOWN);

    valueExpressions.add(new NamedExpression(lastModifiedTime,
        FieldReference.getWithQuotedRef(lastModifiedColumn)));
  }

  /**
   * Adds {@code collect_list()}} function call with specified fields list
   * and excluded columns as arguments of this function to collect all these fields into repeated map list.
   *
   * @param fieldList list of the function arguments
   */
  private void addCollectListCall(List<LogicalExpression> fieldList) {
    ArrayList<LogicalExpression> collectListArguments = new ArrayList<>(fieldList);
    MetadataAggPOP popConfig = (MetadataAggPOP) this.popConfig;
    List<SchemaPath> excludedColumns = popConfig.getContext().excludedColumns();
    // populate columns which weren't included in the schema, but should be collected to the COLLECTED_MAP_FIELD
    for (SchemaPath logicalExpressions : excludedColumns) {
      // adds string literal with field name to the list
      collectListArguments.add(ValueExpressions.getChar(logicalExpressions.getRootSegmentPath(),
          DrillRelDataTypeSystem.DRILL_REL_DATATYPE_SYSTEM.getDefaultPrecision(SqlTypeName.VARCHAR)));
      // adds field reference to the list
      collectListArguments.add(FieldReference.getWithQuotedRef(logicalExpressions.getRootSegmentPath()));
    }

    LogicalExpression collectList = new FunctionCall("collect_list",
        collectListArguments, ExpressionPosition.UNKNOWN);

    valueExpressions.add(
        new NamedExpression(collectList, FieldReference.getWithQuotedRef(MetastoreAnalyzeConstants.COLLECTED_MAP_FIELD)));
  }

  /**
   * Adds {@code merge_schema(`schema`)}} function call to obtain schema common for underlying metadata parts.
   */
  private void addMergeSchemaCall() {
    LogicalExpression schemaExpr = new FunctionCall("merge_schema",
        Collections.singletonList(FieldReference.getWithQuotedRef(MetastoreAnalyzeConstants.SCHEMA_FIELD)),
        ExpressionPosition.UNKNOWN);

    valueExpressions.add(new NamedExpression(schemaExpr, FieldReference.getWithQuotedRef(MetastoreAnalyzeConstants.SCHEMA_FIELD)));
  }

  /**
   * Adds {@code collect_to_list_varchar(`fqn`)} call to collect file paths into the list.
   */
  private void addNewMetadataAggregations() {
    LogicalExpression locationsExpr = new FunctionCall("collect_to_list_varchar",
        Collections.singletonList(SchemaPath.getSimplePath(context.getOptions().getString(ExecConstants.IMPLICIT_FQN_COLUMN_LABEL))),
        ExpressionPosition.UNKNOWN);

    valueExpressions.add(new NamedExpression(locationsExpr, FieldReference.getWithQuotedRef(MetastoreAnalyzeConstants.LOCATIONS_FIELD)));
  }

  /**
   * Adds a call to {@code schema()}} function with specified fields list
   * as arguments of this function to obtain their schema.
   *
   * @param fieldsList list of the function arguments
   */
  private void addSchemaCall(List<LogicalExpression> fieldsList) {
    LogicalExpression schemaExpr = new FunctionCall("schema",
        fieldsList, ExpressionPosition.UNKNOWN);

    valueExpressions.add(new NamedExpression(schemaExpr, FieldReference.getWithQuotedRef(MetastoreAnalyzeConstants.SCHEMA_FIELD)));
  }

  /**
   * Adds aggregate calls to calculate non-column metadata.
   */
  private void addMetadataAggregateCalls() {
    AnalyzeColumnUtils.META_STATISTICS_FUNCTIONS.forEach((statisticsKind, sqlKind) -> {
      LogicalExpression call = new FunctionCall(sqlKind.name(),
          Collections.singletonList(ValueExpressions.getBigInt(1)), ExpressionPosition.UNKNOWN);
      valueExpressions.add(
          new NamedExpression(call,
              FieldReference.getWithQuotedRef(AnalyzeColumnUtils.getMetadataStatisticsFieldName(statisticsKind))));
    });
  }

  /**
   * Returns map with field names as keys and field references as values. For the case when field is map,
   * fully qualified child names will be present in this map.
   * For example, for (a{b, c}, d) fields list will be returned map with a.b, a.c and d keys.
   *
   * @param fields       list of top-level fields to unflatten if required
   * @param parentFields list of parent name segments
   * @return map with field names as keys and field references as values
   */
  private Map<String, FieldReference> getUnflattenedFileds(Collection<MaterializedField> fields, List<String> parentFields) {
    Map<String, FieldReference> fieldNameRefMap = new HashMap<>();
    for (MaterializedField field : fields) {
      // statistics collecting is not supported for array types
      if (field.getType().getMode() != TypeProtos.DataMode.REPEATED) {
        MetadataAggPOP popConfig = (MetadataAggPOP) this.popConfig;
        List<SchemaPath> excludedColumns = popConfig.getContext().excludedColumns();
        // excludedColumns are applied for root fields only
        if (parentFields != null || !excludedColumns.contains(SchemaPath.getSimplePath(field.getName()))) {
          List<String> currentPath;
          if (parentFields == null) {
            currentPath = Collections.singletonList(field.getName());
          } else {
            currentPath = new ArrayList<>(parentFields);
            currentPath.add(field.getName());
          }
          if (field.getType().getMinorType() == TypeProtos.MinorType.MAP && popConfig.getContext().createNewAggregations()) {
            fieldNameRefMap.putAll(getUnflattenedFileds(field.getChildren(), currentPath));
          } else {
            SchemaPath schemaPath = SchemaPath.getCompoundPath(currentPath.toArray(new String[0]));
            // adds backticks for popConfig.createNewAggregations() to ensure that field will be parsed correctly
            String name = popConfig.getContext().createNewAggregations() ? schemaPath.toExpr() : schemaPath.getRootSegmentPath();
            fieldNameRefMap.put(name, new FieldReference(schemaPath));
          }
        }
      }
    }

    return fieldNameRefMap;
  }

  /**
   * Adds aggregate calls to calculate column metadata either from column data itself
   * or from previously calculated metadata.
   *
   * @param fieldRef  field reference
   * @param fieldName field name
   */
  private void addColumnAggregateCalls(FieldReference fieldRef, String fieldName) {
    MetadataAggPOP popConfig = (MetadataAggPOP) this.popConfig;
    List<SchemaPath> interestingColumns = popConfig.getContext().interestingColumns();
    if (popConfig.getContext().createNewAggregations()) {
      if (interestingColumns == null || interestingColumns.contains(fieldRef)) {
        // collect statistics for all or only interesting columns if they are specified
        AnalyzeColumnUtils.COLUMN_STATISTICS_FUNCTIONS.forEach((statisticsKind, sqlKind) -> {
          LogicalExpression call = new FunctionCall(sqlKind.name(),
              Collections.singletonList(fieldRef), ExpressionPosition.UNKNOWN);
          valueExpressions.add(
              new NamedExpression(call,
                  FieldReference.getWithQuotedRef(AnalyzeColumnUtils.getColumnStatisticsFieldName(fieldName, statisticsKind))));
        });
      }
    } else if (AnalyzeColumnUtils.isColumnStatisticsField(fieldName)
        || AnalyzeColumnUtils.isMetadataStatisticsField(fieldName)) {
      SqlKind function = AnalyzeColumnUtils.COLUMN_STATISTICS_FUNCTIONS.get(
          AnalyzeColumnUtils.getStatisticsKind(fieldName));
      if (function == SqlKind.COUNT) {
        // for the case when aggregation was done, call SUM function for the results of COUNT aggregate call
        function = SqlKind.SUM;
      }
      LogicalExpression functionCall = new FunctionCall(function.name(),
          Collections.singletonList(fieldRef), ExpressionPosition.UNKNOWN);
      valueExpressions.add(new NamedExpression(functionCall, fieldRef));
    }
  }

  @Override
  protected List<NamedExpression> getValueExpressions() {
    return valueExpressions;
  }
}
