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
package org.apache.drill.exec.planner.sql.handlers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillScreenRel;
import org.apache.drill.exec.planner.logical.DrillWriterRel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.ProjectAllowDupPrel;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.WriterPrel;
import org.apache.drill.exec.planner.physical.visitor.BasePrelVisitor;
import org.apache.drill.exec.planner.sql.DrillSqlOperator;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.planner.sql.parser.SqlCreateTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.util.Pointer;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.drill.exec.work.foreman.SqlUnsupportedException;

public class CreateTableHandler extends DefaultSqlHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CreateTableHandler.class);

  public CreateTableHandler(SqlHandlerConfig config, Pointer<String> textPlan) {
    super(config, textPlan);
  }

  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException, ForemanSetupException {
    SqlCreateTable sqlCreateTable = unwrap(sqlNode, SqlCreateTable.class);
    final String newTblName = sqlCreateTable.getName();

    final ConvertedRelNode convertedRelNode = validateAndConvert(sqlCreateTable.getQuery());
    final RelDataType validatedRowType = convertedRelNode.getValidatedRowType();
    final RelNode queryRelNode = convertedRelNode.getConvertedNode();


    final RelNode newTblRelNode =
        SqlHandlerUtil.resolveNewTableRel(false, sqlCreateTable.getFieldNames(), validatedRowType, queryRelNode);


    final AbstractSchema drillSchema =
        SchemaUtilites.resolveToMutableDrillSchema(context.getNewDefaultSchema(), sqlCreateTable.getSchemaPath());
    final String schemaPath = drillSchema.getFullSchemaName();

    if (SqlHandlerUtil.getTableFromSchema(drillSchema, newTblName) != null) {
      throw UserException.validationError()
          .message("A table or view with given name [%s] already exists in schema [%s]", newTblName, schemaPath)
          .build(logger);
    }

    final RelNode newTblRelNodeWithPCol = SqlHandlerUtil.qualifyPartitionCol(newTblRelNode, sqlCreateTable.getPartitionColumns());

    log("Optiq Logical", newTblRelNodeWithPCol, logger);

    // Convert the query to Drill Logical plan and insert a writer operator on top.
    DrillRel drel = convertToDrel(newTblRelNodeWithPCol, drillSchema, newTblName, sqlCreateTable.getPartitionColumns(), newTblRelNode.getRowType());
    log("Drill Logical", drel, logger);
    Prel prel = convertToPrel(drel, newTblRelNode.getRowType(), sqlCreateTable.getPartitionColumns());
    log("Drill Physical", prel, logger);
    PhysicalOperator pop = convertToPop(prel);
    PhysicalPlan plan = convertToPlan(pop);
    log("Drill Plan", plan, logger);

    return plan;
  }

  private DrillRel convertToDrel(RelNode relNode, AbstractSchema schema, String tableName, List<String> partitionColumns, RelDataType queryRowType)
      throws RelConversionException, SqlUnsupportedException {

    final DrillRel convertedRelNode = convertToDrel(relNode);

    DrillWriterRel writerRel = new DrillWriterRel(convertedRelNode.getCluster(), convertedRelNode.getTraitSet(),
        convertedRelNode, schema.createNewTable(tableName, partitionColumns));
    return new DrillScreenRel(writerRel.getCluster(), writerRel.getTraitSet(), writerRel);
  }

  private Prel convertToPrel(RelNode drel, RelDataType inputRowType, List<String> partitionColumns)
      throws RelConversionException, SqlUnsupportedException {
    Prel prel = convertToPrel(drel);

    prel = prel.accept(new ProjectForWriterVisitor(inputRowType, partitionColumns), null);

    return prel;
  }

  /**
   * A PrelVisitor which will insert a project under Writer.
   *
   * For CTAS : create table t1 partition by (con_A) select * from T1;
   *   A Project with Item expr will be inserted, in addition to *.  We need insert another Project to remove
   *   this additional expression.
   *
   * In addition, to make execution's implementation easier,  a special field is added to Project :
   *     PARTITION_COLUMN_IDENTIFIER = newPartitionValue(Partition_colA)
   *                                    || newPartitionValue(Partition_colB)
   *                                    || ...
   *                                    || newPartitionValue(Partition_colN).
   */
  private class ProjectForWriterVisitor extends BasePrelVisitor<Prel, Void, RuntimeException> {

    private final RelDataType queryRowType;
    private final List<String> partitionColumns;

    ProjectForWriterVisitor(RelDataType queryRowType, List<String> partitionColumns) {
      this.queryRowType = queryRowType;
      this.partitionColumns = partitionColumns;
    }

    @Override
    public Prel visitPrel(Prel prel, Void value) throws RuntimeException {
      List<RelNode> children = Lists.newArrayList();
      for(Prel child : prel){
        child = child.accept(this, null);
        children.add(child);
      }

      return (Prel) prel.copy(prel.getTraitSet(), children);

    }

    @Override
    public Prel visitWriter(WriterPrel prel, Void value) throws RuntimeException {

      final Prel child = ((Prel)prel.getInput()).accept(this, null);

      final RelDataType childRowType = child.getRowType();

      final RelOptCluster cluster = prel.getCluster();

      final List<RexNode> exprs = Lists.newArrayListWithExpectedSize(queryRowType.getFieldCount() + 1);
      final List<String> fieldnames = new ArrayList<String>(queryRowType.getFieldNames());

      for (final RelDataTypeField field : queryRowType.getFieldList()) {
        exprs.add(RexInputRef.of(field.getIndex(), queryRowType));
      }

      // No partition columns.
      if (partitionColumns.size() == 0) {
        final ProjectPrel projectUnderWriter = new ProjectAllowDupPrel(cluster,
            cluster.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL), child, exprs, queryRowType);

        return (Prel) prel.copy(projectUnderWriter.getTraitSet(),
            Collections.singletonList( (RelNode) projectUnderWriter));
      } else {
        // find list of partiiton columns.
        final List<RexNode> partitionColumnExprs = Lists.newArrayListWithExpectedSize(partitionColumns.size());
        for (final String colName : partitionColumns) {
          final RelDataTypeField field = childRowType.getField(colName, false, false);

          if (field == null) {
            throw UserException.validationError()
                .message("Partition column %s is not in the SELECT list of CTAS!", colName)
                .build(logger);
          }

          partitionColumnExprs.add(RexInputRef.of(field.getIndex(), childRowType));
        }

        // Add partition column comparator to Project's field name list.
        fieldnames.add(WriterPrel.PARTITION_COMPARATOR_FIELD);

        // Add partition column comparator to Project's expression list.
        final RexNode partionColComp = createPartitionColComparator(prel.getCluster().getRexBuilder(), partitionColumnExprs);
        exprs.add(partionColComp);


        final RelDataType rowTypeWithPCComp = RexUtil.createStructType(cluster.getTypeFactory(), exprs, fieldnames);

        final ProjectPrel projectUnderWriter = new ProjectAllowDupPrel(cluster,
            cluster.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL), child, exprs, rowTypeWithPCComp);

        return (Prel) prel.copy(projectUnderWriter.getTraitSet(),
            Collections.singletonList( (RelNode) projectUnderWriter));
      }
    }

  }

  private RexNode createPartitionColComparator(final RexBuilder rexBuilder, List<RexNode> inputs) {
    final DrillSqlOperator op = new DrillSqlOperator(WriterPrel.PARTITION_COMPARATOR_FUNC, 1, true);

    final List<RexNode> compFuncs = Lists.newArrayListWithExpectedSize(inputs.size());

    for (final RexNode input : inputs) {
      compFuncs.add(rexBuilder.makeCall(op, ImmutableList.of(input)));
    }

    return RexUtil.composeDisjunction(rexBuilder, compFuncs, false);
  }

}