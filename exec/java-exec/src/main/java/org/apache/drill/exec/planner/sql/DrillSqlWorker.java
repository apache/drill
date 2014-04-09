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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.hydromatic.optiq.config.Lex;
import net.hydromatic.optiq.tools.Frameworks;
import net.hydromatic.optiq.tools.Planner;
import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.RuleSet;
import net.hydromatic.optiq.tools.ValidationException;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.PlanProperties.Generator.ResultMode;
import org.apache.drill.exec.client.QuerySubmitter;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.logical.DrillImplementor;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillRuleSets;
import org.apache.drill.exec.planner.logical.DrillScreenRel;
import org.apache.drill.exec.planner.logical.DrillStoreRel;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait;
import org.apache.drill.exec.planner.physical.DrillDistributionTraitDef;
import org.apache.drill.exec.planner.physical.PhysicalPlanCreator;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.store.StoragePluginRegistry.DrillSchemaFactory;
import org.eigenbase.rel.RelCollationTraitDef;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.ConventionTraitDef;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelTraitDef;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.sql.SqlExplain;
import org.eigenbase.sql.SqlExplainLevel;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlLiteral;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.parser.SqlParseException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.hive12.common.base.Preconditions;

public class DrillSqlWorker {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSqlWorker.class);

  private final Planner planner;
  private final static RuleSet[] RULES = new RuleSet[]{DrillRuleSets.DRILL_BASIC_RULES, DrillRuleSets.DRILL_PHYSICAL_MEM};
  private final static int LOGICAL_RULES = 0;
  private final static int PHYSICAL_MEM_RULES = 1;
  
  public DrillSqlWorker(DrillSchemaFactory schemaFactory, FunctionImplementationRegistry registry) throws Exception {
    final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();
    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(DrillDistributionTraitDef.INSTANCE);    
    traitDefs.add(RelCollationTraitDef.INSTANCE);
    
    DrillOperatorTable table = new DrillOperatorTable(registry);
    DrillParserFactory factory = new DrillParserFactory(table);
    this.planner = Frameworks.getPlanner(Lex.MYSQL, factory, schemaFactory, table, traitDefs, RULES);
//    this.planner = Frameworks.getPlanner(Lex.MYSQL, SqlParserImpl.FACTORY, schemaFactory, SqlStdOperatorTable.instance(), traitDefs, RULES);
  }
  
  public class RelResult{
    final ResultMode mode;
    final RelNode node;
    public RelResult(ResultMode mode, RelNode node) {
      super();
      this.mode = mode;
      this.node = node;
    }
    
    public ResultMode getMode() {
      return this.mode;
    }
  }

  /*
   * Given a SQL string, return the logical DrillRel tree, plus mode (execute, or EXPLAIN mode).  
   */
  public RelResult getLogicalRel(String sql) throws SqlParseException, ValidationException, RelConversionException{
    if(logger.isDebugEnabled()) {
      logger.debug("SQL : " + sql);
    }

    // Call optiq to parse the SQL string. 
    SqlNode sqlNode = planner.parse(sql);  
    ResultMode resultMode = ResultMode.EXEC;
    
    //Process EXPLAIN
    if(sqlNode.getKind() == SqlKind.EXPLAIN){
      SqlExplain explain = (SqlExplain) sqlNode;
      sqlNode = explain.operand(0);
      SqlExplain.Depth depth = (SqlExplain.Depth) explain.getDepth();
      switch(depth){
      case LOGICAL:
        resultMode = ResultMode.LOGICAL;
        break;
      case PHYSICAL:
        resultMode = ResultMode.PHYSICAL;
        break;
      default:
      }
    }
    
    // Call optiq to validate SqlNode tree and convert it to RelNode tree. 
    SqlNode validatedNode = planner.validate(sqlNode);
    RelNode relNode = planner.convert(validatedNode);
    
    //Debug
    if(logger.isDebugEnabled()) {
      logger.debug("RelNode tree : " + RelOptUtil.toString(relNode, SqlExplainLevel.ALL_ATTRIBUTES));
    }
    
    // Call optiq to transform RelNode into Drill Logical RelNode tree. 
    RelNode convertedRelNode = planner.transform(LOGICAL_RULES, relNode.getTraitSet().plus(DrillRel.DRILL_LOGICAL), relNode);
    if(convertedRelNode instanceof DrillStoreRel){
      throw new UnsupportedOperationException();
    }else{
      convertedRelNode = new DrillScreenRel(convertedRelNode.getCluster(), convertedRelNode.getTraitSet(), convertedRelNode);
    }
    
    //Debug
    if(logger.isDebugEnabled()) {
      logger.debug("Drill LogicalRel tree : " + RelOptUtil.toString(convertedRelNode, SqlExplainLevel.ALL_ATTRIBUTES));
    }
    
    return new RelResult(resultMode, convertedRelNode);
  }
  
  /*
   * Given a Drill LogicalRel tree, return Drill Logical Plan. 
   * @param relResult :  RelResult whose node is the root of Drill logicalrel tree.
   */
  public LogicalPlan getLogicalPlan(RelResult relResult) throws SqlParseException, ValidationException, RelConversionException{
    RelNode logicalRelRoot = relResult.node;
    
    Preconditions.checkArgument(logicalRelRoot.getConvention() == DrillRel.DRILL_LOGICAL);
    
    DrillImplementor implementor = new DrillImplementor(new DrillParseContext(), relResult.mode);
    implementor.go( (DrillRel) logicalRelRoot);
    planner.close();
    planner.reset();
    return implementor.getPlan();    
  }

  /*
   * Given a SQL string, return the Drill logical plan.
   */
  public LogicalPlan getLogicalPlan(String sql) throws SqlParseException, ValidationException, RelConversionException{
    RelResult result = getLogicalRel(sql);

    RelNode convertedRelNode = planner.transform(LOGICAL_RULES, result.node.getTraitSet().plus(DrillRel.DRILL_LOGICAL), result.node);
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

  /*
   * Given a Drill LogicalRel tree, return Drill Physical plan.
   * @param relResult : RelResult whose node is the root of Drill logicalrel tree.
   * @param qcontext  : QueryContext used by PhysicalPlanCreator. 
   */
  public PhysicalPlan getPhysicalPlan(RelResult relResult, QueryContext qcontext) throws SqlParseException, ValidationException, RelConversionException, IOException {
    RelNode logicalRelRoot = relResult.node;
    
    Preconditions.checkArgument(logicalRelRoot.getConvention() == DrillRel.DRILL_LOGICAL);
    
    RelTraitSet traits = logicalRelRoot.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.SINGLETON);    
    Prel phyRelNode = (Prel) planner.transform(PHYSICAL_MEM_RULES, traits, logicalRelRoot);
    
    //Debug
    if(logger.isDebugEnabled()) {     
      String msg = RelOptUtil.toString(phyRelNode, SqlExplainLevel.ALL_ATTRIBUTES);
      logger.debug("Drill PhysicalRel tree: " + msg);
    }
    
    PhysicalPlanCreator pplanCreator = new PhysicalPlanCreator(qcontext);
    PhysicalPlan plan = pplanCreator.build(phyRelNode, true /* rebuild */);
        
    planner.close();
    planner.reset();
    return plan;
  }
  
  /*
   * Given a SQL string, return Drill physical plan. 
   */
  public PhysicalPlan getPhysicalPlan(String sql, QueryContext qcontext) throws SqlParseException, ValidationException, RelConversionException, IOException {
    RelResult result = getLogicalRel(sql);

    RelTraitSet traits = result.node.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.SINGLETON);    
    Prel phyRelNode = (Prel) planner.transform(PHYSICAL_MEM_RULES, traits, result.node);
    
    //Debug.
    if(logger.isDebugEnabled()) {     
      logger.debug("SQL : " + sql);
      String msg = RelOptUtil.toString(phyRelNode, SqlExplainLevel.ALL_ATTRIBUTES);
      logger.debug("Drill PhysicalRel tree: " + msg);      
    }
        
    PhysicalPlanCreator pplanCreator = new PhysicalPlanCreator(qcontext);
    PhysicalPlan plan = pplanCreator.build(phyRelNode, true /* rebuild */);
        
    planner.close();
    planner.reset();
    return plan;

  }
 
}
