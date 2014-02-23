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
package org.apache.drill.exec.planner.sql;

import net.hydromatic.optiq.jdbc.ConnectionConfig;
import net.hydromatic.optiq.tools.Frameworks;
import net.hydromatic.optiq.tools.Planner;
import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.RuleSet;
import net.hydromatic.optiq.tools.ValidationException;

import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.PlanProperties.Generator.ResultMode;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.common.BaseScreenRel;
import org.apache.drill.exec.planner.common.DrillStoreRel;
import org.apache.drill.exec.planner.logical.DrillImplementor;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillRuleSets;
import org.apache.drill.exec.planner.logical.DrillScreenRel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.store.StoragePluginRegistry.DrillSchemaFactory;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.sql.SqlExplain;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlLiteral;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParseException;

public class DrillSqlWorker {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSqlWorker.class);

  private final Planner planner;
  private final static RuleSet[] RULES = new RuleSet[]{DrillRuleSets.DRILL_BASIC_RULES, DrillRuleSets.DRILL_PHYSICAL_MEM};
  private final static int LOGICAL_RULES = 0;
  private final static int PHYSICAL_MEM_RULES = 1;
  
  public DrillSqlWorker(DrillSchemaFactory schemaFactory) throws Exception {
    this.planner = Frameworks.getPlanner(ConnectionConfig.Lex.MYSQL, schemaFactory, SqlStdOperatorTable.instance(), new RuleSet[]{DrillRuleSets.DRILL_BASIC_RULES});
  }
  
  private class RelResult{
    final ResultMode mode;
    final RelNode node;
    public RelResult(ResultMode mode, RelNode node) {
      super();
      this.mode = mode;
      this.node = node;
    }
  }
  
  private RelResult getRel(String sql) throws SqlParseException, ValidationException, RelConversionException{
    SqlNode sqlNode = planner.parse(sql);

    ResultMode resultMode = ResultMode.EXEC;
    if(sqlNode.getKind() == SqlKind.EXPLAIN){
      SqlExplain explain = (SqlExplain) sqlNode;
      sqlNode = explain.operands[0];
      SqlLiteral op = (SqlLiteral) explain.operands[2];
      SqlExplain.Depth depth = (SqlExplain.Depth) op.getValue();
      switch(depth){
      case Logical:
        resultMode = ResultMode.LOGICAL;
        break;
      case Physical:
        resultMode = ResultMode.PHYSICAL;
        break;
      default:
      }
    }
    
    SqlNode validatedNode = planner.validate(sqlNode);
    RelNode relNode = planner.convert(validatedNode);
    return new RelResult(resultMode, relNode);
  }
  
  
  
  public LogicalPlan getLogicalPlan(String sql) throws SqlParseException, ValidationException, RelConversionException{
    RelResult result = getRel(sql);
    RelNode convertedRelNode = planner.transform(LOGICAL_RULES, planner.getEmptyTraitSet().plus(DrillRel.DRILL_LOGICAL), result.node);
    if(convertedRelNode instanceof DrillStoreRel){
      throw new UnsupportedOperationException();
    }else{
      convertedRelNode = new DrillScreenRel(convertedRelNode.getCluster(), convertedRelNode.getTraitSet(), convertedRelNode);
    }
    DrillImplementor implementor = new DrillImplementor(new DrillParseContext(), result.mode);
    implementor.go( (DrillRel) convertedRelNode);
    planner.close();
    planner.reset();
    return implementor.getPlan();
    
  }

  
  public PhysicalPlan getPhysicalPlan(String sql) throws SqlParseException, ValidationException, RelConversionException{
    RelResult result = getRel(sql);
    RelTraitSet traits = planner.getEmptyTraitSet().plus(Prel.DRILL_PHYSICAL);
    RelNode transformed = planner.transform(PHYSICAL_MEM_RULES, traits, result.node);
    planner.close();
    planner.reset();
    return null;
  }
  
}
