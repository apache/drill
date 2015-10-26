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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.PlanProperties.Generator.ResultMode;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.logical.DrillImplementor;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.explain.PrelSequencer;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;

public class ExplainHandler extends DefaultSqlHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExplainHandler.class);

  private ResultMode mode;
  private SqlExplainLevel level = SqlExplainLevel.ALL_ATTRIBUTES;
  public ExplainHandler(SqlHandlerConfig config) {
    super(config);
  }

  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException, ForemanSetupException {
    final ConvertedRelNode convertedRelNode = validateAndConvert(sqlNode);
    final RelDataType validatedRowType = convertedRelNode.getValidatedRowType();
    final RelNode queryRelNode = convertedRelNode.getConvertedNode();

    log("Optiq Logical", queryRelNode, logger);
    DrillRel drel = convertToDrel(queryRelNode, validatedRowType);
    log("Drill Logical", drel, logger);

    if (mode == ResultMode.LOGICAL) {
      LogicalExplain logicalResult = new LogicalExplain(drel, level, context);
      return DirectPlan.createDirectPlan(context, logicalResult);
    }

    Prel prel = convertToPrel(drel);
    log("Drill Physical", prel, logger);
    PhysicalOperator pop = convertToPop(prel);
    PhysicalPlan plan = convertToPlan(pop);
    log("Drill Plan", plan, logger);
    PhysicalExplain physicalResult = new PhysicalExplain(prel, plan, level, context);
    return DirectPlan.createDirectPlan(context, physicalResult);
  }

  @Override
  public SqlNode rewrite(SqlNode sqlNode) throws RelConversionException, ForemanSetupException {
    SqlExplain node = unwrap(sqlNode, SqlExplain.class);
    SqlLiteral op = node.operand(2);
    SqlExplain.Depth depth = (SqlExplain.Depth) op.getValue();
    if (node.getDetailLevel() != null) {
      level = node.getDetailLevel();
    }
    switch (depth) {
    case LOGICAL:
      mode = ResultMode.LOGICAL;
      break;
    case PHYSICAL:
      mode = ResultMode.PHYSICAL;
      break;
    default:
      throw new UnsupportedOperationException("Unknown depth " + depth);
    }

    return node.operand(0);
  }


  public static class LogicalExplain{
    public String text;
    public String json;

    public LogicalExplain(RelNode node, SqlExplainLevel level, QueryContext context) {
      this.text = RelOptUtil.toString(node, level);
      DrillImplementor implementor = new DrillImplementor(new DrillParseContext(context.getPlannerSettings()), ResultMode.LOGICAL);
      implementor.go( (DrillRel) node);
      LogicalPlan plan = implementor.getPlan();
      this.json = plan.unparse(context.getLpPersistence());
    }
  }

  public static class PhysicalExplain{
    public String text;
    public String json;

    public PhysicalExplain(RelNode node, PhysicalPlan plan, SqlExplainLevel level, QueryContext context) {
      this.text = PrelSequencer.printWithIds((Prel) node, level);
      this.json = plan.unparse(context.getLpPersistence().getMapper().writer());
    }
  }

}
