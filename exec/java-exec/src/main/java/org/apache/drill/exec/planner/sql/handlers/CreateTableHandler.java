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
import java.util.List;

import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.tools.Planner;
import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.ValidationException;

import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillScreenRel;
import org.apache.drill.exec.planner.logical.DrillStoreRel;
import org.apache.drill.exec.planner.logical.DrillWriterRel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.DrillSqlWorker;
import org.apache.drill.exec.planner.sql.parser.SqlCreateTable;
import org.apache.drill.exec.planner.types.DrillFixedRelDataTypeImpl;
import org.apache.drill.exec.store.AbstractSchema;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.hep.HepPlanner;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.SqlNode;

public class CreateTableHandler extends DefaultSqlHandler {

  public CreateTableHandler(SqlHandlerConfig config) {
    super(config);
  }

  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException {
    SqlCreateTable sqlCreateTable = unwrap(sqlNode, SqlCreateTable.class);

    try {
      // Convert the query in CTAS statement into a RelNode
      SqlNode validatedQuery = validateNode(sqlCreateTable.getQuery());
      RelNode relQuery = convertToRel(validatedQuery);

      List<String> tblFiledNames = sqlCreateTable.getFieldNames();
      RelDataType queryRowType = relQuery.getRowType();

      if (tblFiledNames.size() > 0) {
        // Field count should match.
        if (tblFiledNames.size() != queryRowType.getFieldCount()) {
          return DirectPlan.createDirectPlan(context, false,
              "Table's field list and the table's query field list have different counts.");
        }

        // CTAS's query field list shouldn't have "*" when table's field list is specified.
        for (String field : queryRowType.getFieldNames()) {
          if (field.equals("*")) {
            return DirectPlan.createDirectPlan(context, false,
                "Table's query field list has a '*', which is invalid when table's field list is specified.");
          }
        }
      }

      // if the CTAS statement has table fields lists (ex. below), add a project rel to rename the query fields.
      // Ex. CREATE TABLE tblname(col1, medianOfCol2, avgOfCol3) AS
      //        SELECT col1, median(col2), avg(col3) FROM sourcetbl GROUP BY col1 ;
      if (tblFiledNames.size() > 0) {
        // create rowtype to which the select rel needs to be casted.
        RelDataType rowType = new DrillFixedRelDataTypeImpl(planner.getTypeFactory(), tblFiledNames);

        relQuery = RelOptUtil.createCastRel(relQuery, rowType, true);
      }

      SchemaPlus schema = findSchema(context.getRootSchema(), context.getNewDefaultSchema(),
          sqlCreateTable.getSchemaPath());

      AbstractSchema drillSchema = getDrillSchema(schema);

      if (!drillSchema.isMutable()) {
        return DirectPlan.createDirectPlan(context, false, String.format("Current schema '%s' is not a mutable schema. " +
            "Can't create tables in this schema.", drillSchema.getFullSchemaName()));
      }

      String newTblName = sqlCreateTable.getName();
      if (schema.getTable(newTblName) != null) {
        return DirectPlan.createDirectPlan(context, false, String.format("Table '%s' already exists.", newTblName));
      }

      log("Optiq Logical", relQuery);

      // Convert the query to Drill Logical plan and insert a writer operator on top.
      DrillRel drel = convertToDrel(relQuery, drillSchema, newTblName);
      log("Drill Logical", drel);
      Prel prel = convertToPrel(drel);
      log("Drill Physical", prel);
      PhysicalOperator pop = convertToPop(prel);
      PhysicalPlan plan = convertToPlan(pop);
      log("Drill Plan", plan);

      return plan;
    } catch(Exception e) {
      logger.error("Failed to create table '{}'", sqlCreateTable.getName(), e);
      return DirectPlan.createDirectPlan(context, false, String.format("Error: %s", e.getMessage()));
    }
  }

  private DrillRel convertToDrel(RelNode relNode, AbstractSchema schema, String tableName) throws RelConversionException {
    RelNode convertedRelNode = planner.transform(DrillSqlWorker.LOGICAL_RULES,
        relNode.getTraitSet().plus(DrillRel.DRILL_LOGICAL), relNode);

    if (convertedRelNode instanceof DrillStoreRel) {
      throw new UnsupportedOperationException();
    }

    DrillWriterRel writerRel = new DrillWriterRel(convertedRelNode.getCluster(), convertedRelNode.getTraitSet(),
        convertedRelNode, schema.createNewTable(tableName));
    return new DrillScreenRel(writerRel.getCluster(), writerRel.getTraitSet(), writerRel);
  }

}
