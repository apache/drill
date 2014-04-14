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

import net.hydromatic.optiq.tools.Planner;
import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.ValidationException;

import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.PlanProperties.Generator.ResultMode;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.logical.DrillImplementor;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.sql.SqlExplain;
import org.eigenbase.sql.SqlExplainLevel;
import org.eigenbase.sql.SqlLiteral;
import org.eigenbase.sql.SqlNode;

public class ExplainHandler extends DefaultSqlHandler{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExplainHandler.class);

  private ResultMode mode;

  public ExplainHandler(Planner planner, QueryContext context) {
    super(planner, context);
  }

  @Override
  public PhysicalPlan getPlan(SqlNode node) throws ValidationException, RelConversionException, IOException {
    SqlExplain explain = unwrap(node, SqlExplain.class);
    SqlNode sqlNode = rewrite(explain);
    SqlNode validated = validateNode(sqlNode);
    RelNode rel = convertToRel(validated);
    DrillRel drel = convertToDrel(rel);

    if(mode == ResultMode.LOGICAL){
      LogicalExplain logicalResult = new LogicalExplain(drel);
      return DirectPlan.createDirectPlan(context, logicalResult);
    }

    Prel prel = convertToPrel(drel);
    PhysicalOperator pop = convertToPop(prel);
    PhysicalPlan plan = convertToPlan(pop);
    PhysicalExplain physicalResult = new PhysicalExplain(prel, plan);
    return DirectPlan.createDirectPlan(context, physicalResult);
  }

  private SqlNode rewrite(SqlExplain node) {
    SqlLiteral op = (SqlLiteral) node.operand(2);
    SqlExplain.Depth depth = (SqlExplain.Depth) op.getValue();

    switch(depth){
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


  public class LogicalExplain{
    public String text;
    public String json;

    public LogicalExplain(RelNode node){
      this.text = RelOptUtil.toString(node, SqlExplainLevel.DIGEST_ATTRIBUTES);
      DrillImplementor implementor = new DrillImplementor(new DrillParseContext(), ResultMode.LOGICAL);
      implementor.go( (DrillRel) node);
      LogicalPlan plan = implementor.getPlan();
      this.json = plan.unparse(context.getConfig());
    }
  }

  public class PhysicalExplain{
    public String text;
    public String json;

    public PhysicalExplain(RelNode node, PhysicalPlan plan){
      this.text = RelOptUtil.toString(node, SqlExplainLevel.ALL_ATTRIBUTES);
      this.json = plan.unparse(context.getConfig().getMapper().writer());
    }
  }



}
