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
import net.hydromatic.optiq.tools.FrameworkConfig;
import net.hydromatic.optiq.tools.Frameworks;
import net.hydromatic.optiq.tools.Planner;
import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.RuleSet;
import net.hydromatic.optiq.tools.ValidationException;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.planner.logical.DrillConstExecutor;
import org.apache.drill.exec.planner.logical.DrillRuleSets;
import org.apache.drill.exec.planner.physical.DrillDistributionTraitDef;
import org.apache.drill.exec.planner.sql.handlers.AbstractSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.DefaultSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.ExplainHandler;
import org.apache.drill.exec.planner.sql.handlers.SetOptionHandler;
import org.apache.drill.exec.planner.sql.handlers.SqlHandlerConfig;
import org.apache.drill.exec.planner.sql.parser.DrillSqlCall;
import org.apache.drill.exec.planner.sql.parser.SqlCreateTable;
import org.apache.drill.exec.planner.sql.parser.impl.DrillParserWithCompoundIdConverter;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.testing.ExecutionControlsInjector;
import org.apache.drill.exec.util.Pointer;
import org.apache.drill.exec.work.foreman.ForemanException;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.eigenbase.rel.RelCollationTraitDef;
import org.eigenbase.rel.rules.ReduceExpressionsRule;
import org.eigenbase.rel.rules.WindowedAggSplitterRule;
import org.eigenbase.relopt.ConventionTraitDef;
import org.eigenbase.relopt.RelOptCostFactory;
import org.eigenbase.relopt.RelTraitDef;
import org.eigenbase.relopt.hep.HepPlanner;
import org.eigenbase.relopt.hep.HepProgramBuilder;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.parser.SqlParseException;
import org.eigenbase.sql.parser.SqlParser;

public class DrillSqlWorker {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSqlWorker.class);
  private final static ExecutionControlsInjector injector = ExecutionControlsInjector.getInjector(DrillSqlWorker.class);

  private final Planner planner;
  private final HepPlanner hepPlanner;
  public final static int LOGICAL_RULES = 0;
  public final static int PHYSICAL_MEM_RULES = 1;
  private final QueryContext context;

  public DrillSqlWorker(QueryContext context) {
    final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(DrillDistributionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);
    this.context = context;
    RelOptCostFactory costFactory = (context.getPlannerSettings().useDefaultCosting()) ?
        null : new DrillCostBase.DrillCostFactory() ;
    int idMaxLength = (int)context.getPlannerSettings().getIdentifierMaxLength();

    FrameworkConfig config = Frameworks.newConfigBuilder() //
        .parserConfig(new SqlParser.ParserConfigImpl(Lex.MYSQL, idMaxLength)) //
        .parserFactory(DrillParserWithCompoundIdConverter.FACTORY) //
        .defaultSchema(context.getNewDefaultSchema()) //
        .operatorTable(context.getDrillOperatorTable()) //
        .traitDefs(traitDefs) //
        .convertletTable(new DrillConvertletTable()) //
        .context(context.getPlannerSettings()) //
        .ruleSets(getRules(context)) //
        .costFactory(costFactory) //
        .executor(new DrillConstExecutor(context.getFunctionRegistry(), context))
        .build();
    this.planner = Frameworks.getPlanner(config);
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);
    builder.addRuleClass(WindowedAggSplitterRule.class);
    this.hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(ReduceExpressionsRule.CALC_INSTANCE);
    hepPlanner.addRule(WindowedAggSplitterRule.PROJECT);
  }

  private RuleSet[] getRules(QueryContext context) {
    StoragePluginRegistry storagePluginRegistry = context.getStorage();
    RuleSet drillLogicalRules = DrillRuleSets.mergedRuleSets(
        DrillRuleSets.getDrillBasicRules(context),
        DrillRuleSets.getDrillUserConfigurableLogicalRules(context));
    RuleSet drillPhysicalMem = DrillRuleSets.mergedRuleSets(
        DrillRuleSets.getPhysicalRules(context),
        storagePluginRegistry.getStoragePluginRuleSet());
    RuleSet[] allRules = new RuleSet[] {drillLogicalRules, drillPhysicalMem};
    return allRules;
  }

  public PhysicalPlan getPlan(String sql) throws SqlParseException, ValidationException, ForemanSetupException{
    return getPlan(sql, null);
  }

  public PhysicalPlan getPlan(String sql, Pointer<String> textPlan) throws ForemanSetupException {
    SqlNode sqlNode;
    try {
      injector.injectChecked(context.getExecutionControls(), "sql-parsing", ForemanSetupException.class);
      sqlNode = planner.parse(sql);
    } catch (SqlParseException e) {
      throw new QueryInputException("Failure parsing SQL. " + e.getMessage(), e);
    }

    AbstractSqlHandler handler;
    SqlHandlerConfig config = new SqlHandlerConfig(hepPlanner, planner, context);

    // TODO: make this use path scanning or something similar.
    switch(sqlNode.getKind()){
    case EXPLAIN:
      handler = new ExplainHandler(config);
      break;
    case SET_OPTION:
      handler = new SetOptionHandler(context);
      break;
    case OTHER:
      if(sqlNode instanceof SqlCreateTable) {
        handler = ((DrillSqlCall)sqlNode).getSqlHandler(config, textPlan);
        break;
      }

      if (sqlNode instanceof DrillSqlCall) {
        handler = ((DrillSqlCall)sqlNode).getSqlHandler(config);
        break;
      }
      // fallthrough
    default:
      handler = new DefaultSqlHandler(config, textPlan);
    }

    try {
      return handler.getPlan(sqlNode);
    } catch(ValidationException e) {
      String errorMessage = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
      throw UserException.parseError(e).message(errorMessage).build();
    } catch (IOException | RelConversionException e) {
      throw new QueryInputException("Failure handling SQL.", e);
    }
  }
}
