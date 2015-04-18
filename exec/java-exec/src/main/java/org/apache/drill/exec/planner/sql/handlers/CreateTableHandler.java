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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillScreenRel;
import org.apache.drill.exec.planner.logical.DrillStoreRel;
import org.apache.drill.exec.planner.logical.DrillWriterRel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.sql.DrillSqlWorker;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.planner.sql.parser.SqlCreateTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.util.Pointer;
import org.apache.drill.exec.work.foreman.ForemanSetupException;

public class CreateTableHandler extends DefaultSqlHandler {
  public CreateTableHandler(SqlHandlerConfig config, Pointer<String> textPlan) {
    super(config, textPlan);
  }

  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException, ForemanSetupException {
    SqlCreateTable sqlCreateTable = unwrap(sqlNode, SqlCreateTable.class);

    final String newTblName = sqlCreateTable.getName();
    final RelNode newTblRelNode =
        SqlHandlerUtil.resolveNewTableRel(false, planner, sqlCreateTable.getFieldNames(), sqlCreateTable.getQuery());

    final AbstractSchema drillSchema =
        SchemaUtilites.resolveToMutableDrillSchema(context.getNewDefaultSchema(), sqlCreateTable.getSchemaPath());
    final String schemaPath = drillSchema.getFullSchemaName();

    if (SqlHandlerUtil.getTableFromSchema(drillSchema, newTblName) != null) {
      throw UserException.validationError()
          .message("A table or view with given name [%s] already exists in schema [%s]", newTblName, schemaPath)
          .build();
    }

    log("Optiq Logical", newTblRelNode);

    // Convert the query to Drill Logical plan and insert a writer operator on top.
    DrillRel drel = convertToDrel(newTblRelNode, drillSchema, newTblName);
    log("Drill Logical", drel);
    Prel prel = convertToPrel(drel);
    log("Drill Physical", prel);
    PhysicalOperator pop = convertToPop(prel);
    PhysicalPlan plan = convertToPlan(pop);
    log("Drill Plan", plan);

    return plan;
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
