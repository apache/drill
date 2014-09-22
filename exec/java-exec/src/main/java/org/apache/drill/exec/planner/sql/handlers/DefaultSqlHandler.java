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
import java.util.Collection;
import java.util.List;

import net.hydromatic.optiq.tools.Planner;
import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.ValidationException;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.logical.PlanProperties;
import org.apache.drill.common.logical.PlanProperties.Generator.ResultMode;
import org.apache.drill.common.logical.PlanProperties.PlanPropertiesBuilder;
import org.apache.drill.common.logical.PlanProperties.PlanType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.AbstractPhysicalVisitor;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillScreenRel;
import org.apache.drill.exec.planner.logical.DrillStoreRel;
import org.apache.drill.exec.planner.logical.RewriteProjectRel;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait;
import org.apache.drill.exec.planner.physical.PhysicalPlanCreator;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.explain.PrelSequencer;
import org.apache.drill.exec.planner.physical.visitor.ComplexToJsonPrelVisitor;
import org.apache.drill.exec.planner.physical.visitor.ExcessiveExchangeIdentifier;
import org.apache.drill.exec.planner.physical.visitor.FinalColumnReorderer;
import org.apache.drill.exec.planner.physical.visitor.JoinPrelRenameVisitor;
import org.apache.drill.exec.planner.physical.visitor.MemoryEstimationVisitor;
import org.apache.drill.exec.planner.physical.visitor.ProducerConsumerPrelVisitor;
import org.apache.drill.exec.planner.physical.visitor.RelUniqifier;
import org.apache.drill.exec.planner.physical.visitor.SelectionVectorPrelVisitor;
import org.apache.drill.exec.planner.physical.visitor.StarColumnConverter;
import org.apache.drill.exec.planner.sql.DrillSqlWorker;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.util.Pointer;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.relopt.hep.HepPlanner;
import org.eigenbase.sql.SqlExplainLevel;
import org.eigenbase.sql.SqlNode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class DefaultSqlHandler extends AbstractSqlHandler {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DefaultSqlHandler.class);

  protected final SqlHandlerConfig config;
  protected final QueryContext context;
  protected final HepPlanner hepPlanner;
  protected final Planner planner;
  private Pointer<String> textPlan;
  private final long targetSliceSize;

  public DefaultSqlHandler(SqlHandlerConfig config) {
    this(config, null);
  }

  public DefaultSqlHandler(SqlHandlerConfig config, Pointer<String> textPlan) {
    super();
    this.planner = config.getPlanner();
    this.context = config.getContext();
    this.hepPlanner = config.getHepPlanner();
    this.config = config;
    this.textPlan = textPlan;
    targetSliceSize = context.getOptions().getOption(ExecConstants.SLICE_TARGET).num_val;
  }

  protected void log(String name, RelNode node) {
    if (logger.isDebugEnabled()) {
      logger.debug(name + " : \n" + RelOptUtil.toString(node, SqlExplainLevel.ALL_ATTRIBUTES));
    }
  }

  protected void log(String name, Prel node) {
    String plan = PrelSequencer.printWithIds(node, SqlExplainLevel.ALL_ATTRIBUTES);;
    if(textPlan != null){
      textPlan.value = plan;
    }

    if (logger.isDebugEnabled()) {
      logger.debug(name + " : \n" + plan);
    }
  }

  protected void log(String name, PhysicalPlan plan) throws JsonProcessingException {
    if (logger.isDebugEnabled()) {
      String planText = plan.unparse(context.getConfig().getMapper().writer());
      logger.debug(name + " : \n" + planText);
    }
  }

  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException {

    SqlNode rewrittenSqlNode = rewrite(sqlNode);
    SqlNode validated = validateNode(rewrittenSqlNode);
    RelNode rel = convertToRel(validated);

    /* Traverse the tree and replace the convert_from, convert_to function to actual implementations
     * Eg: convert_from(EXPR, 'JSON') be converted to convert_fromjson(EXPR);
     * TODO: Ideally all function rewrites would move here instead of DrillOptiq
     */
    rel = rel.accept(new RewriteProjectRel(planner.getTypeFactory(), context.getDrillOperatorTable()));
    log("Optiq Logical", rel);
    DrillRel drel = convertToDrel(rel);
    log("Drill Logical", drel);
    Prel prel = convertToPrel(drel);
    log("Drill Physical", prel);
    PhysicalOperator pop = convertToPop(prel);
    PhysicalPlan plan = convertToPlan(pop);
    log("Drill Plan", plan);
    return plan;
  }

  protected SqlNode validateNode(SqlNode sqlNode) throws ValidationException, RelConversionException {
    return planner.validate(sqlNode);
  }

  protected RelNode convertToRel(SqlNode node) throws RelConversionException {
    RelNode convertedNode = planner.convert(node);
    hepPlanner.setRoot(convertedNode);
    return hepPlanner.findBestExp();
  }

  protected DrillRel convertToDrel(RelNode relNode) throws RelConversionException {
    RelNode convertedRelNode = planner.transform(DrillSqlWorker.LOGICAL_RULES,
        relNode.getTraitSet().plus(DrillRel.DRILL_LOGICAL), relNode);
    if (convertedRelNode instanceof DrillStoreRel) {
      throw new UnsupportedOperationException();
    } else {
      return new DrillScreenRel(convertedRelNode.getCluster(), convertedRelNode.getTraitSet(), convertedRelNode);
    }
  }

  protected Prel convertToPrel(RelNode drel) throws RelConversionException {
    Preconditions.checkArgument(drel.getConvention() == DrillRel.DRILL_LOGICAL);
    RelTraitSet traits = drel.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.SINGLETON);
    Prel phyRelNode = (Prel) planner.transform(DrillSqlWorker.PHYSICAL_MEM_RULES, traits, drel);
    OptionManager queryOptions = context.getOptions();

    if (context.getPlannerSettings().isMemoryEstimationEnabled()
      && !MemoryEstimationVisitor.enoughMemory(phyRelNode, queryOptions, context.getActiveEndpoints().size())) {
      log("Not enough memory for this plan", phyRelNode);
      logger.debug("Re-planning without hash operations.");

      queryOptions.setOption(OptionValue.createBoolean(OptionValue.OptionType.QUERY, PlannerSettings.HASHJOIN.getOptionName(), false));
      queryOptions.setOption(OptionValue.createBoolean(OptionValue.OptionType.QUERY, PlannerSettings.HASHAGG.getOptionName(), false));

      phyRelNode = (Prel) planner.transform(DrillSqlWorker.PHYSICAL_MEM_RULES, traits, drel);
    }

    /*  The order of the following transformation is important */

    /*
     * 0.) For select * from join query, we need insert project on top of scan and a top project just
     * under screen operator. The project on top of scan will rename from * to T1*, while the top project
     * will rename T1* to *, before it output the final result. Only the top project will allow
     * duplicate columns, since user could "explicitly" ask for duplicate columns ( select *, col, *).
     * The rest of projects will remove the duplicate column when we generate POP in json format.
     */
    phyRelNode = StarColumnConverter.insertRenameProject(phyRelNode, phyRelNode.getRowType());

    /*
     * 1.)
     * Join might cause naming conflicts from its left and right child.
     * In such case, we have to insert Project to rename the conflicting names.
     */
    phyRelNode = JoinPrelRenameVisitor.insertRenameProject(phyRelNode);

    /*
     * 2.)
     * Since our operators work via names rather than indices, we have to make to reorder any
     * output before we return data to the user as we may have accidentally shuffled things.
     * This adds a trivial project to reorder columns prior to output.
     */
    phyRelNode = FinalColumnReorderer.addFinalColumnOrdering(phyRelNode);

    /*
     * 3.)
     * If two fragments are both estimated to be parallelization one, remove the exchange
     * separating them
     */
    phyRelNode = ExcessiveExchangeIdentifier.removeExcessiveEchanges(phyRelNode, targetSliceSize);


    /* 4.)
     * Next, we add any required selection vector removers given the supported encodings of each
     * operator. This will ultimately move to a new trait but we're managing here for now to avoid
     * introducing new issues in planning before the next release
     */
    phyRelNode = SelectionVectorPrelVisitor.addSelectionRemoversWhereNecessary(phyRelNode);

    /* 5.)
     * Add ProducerConsumer after each scan if the option is set
     * Use the configured queueSize
     */
    if (context.getOptions().getOption(PlannerSettings.PRODUCER_CONSUMER.getOptionName()).bool_val) {
      long queueSize = context.getOptions().getOption(PlannerSettings.PRODUCER_CONSUMER_QUEUE_SIZE.getOptionName()).num_val;
      phyRelNode = ProducerConsumerPrelVisitor.addProducerConsumerToScans(phyRelNode, (int) queueSize);
    }


    /* 6.)
     * if the client does not support complex types (Map, Repeated)
     * insert a project which which would convert
     */
    if (!context.getSession().isSupportComplexTypes()) {
      logger.debug("Client does not support complex types, add ComplexToJson operator.");
      phyRelNode = ComplexToJsonPrelVisitor.addComplexToJsonPrel(phyRelNode);
    }

    /* 7.)
     * Finally, Make sure that the no rels are repeats.
     * This could happen in the case of querying the same table twice as Optiq may canonicalize these.
     */
    phyRelNode = RelUniqifier.uniqifyGraph(phyRelNode);

    return phyRelNode;
  }

  protected PhysicalOperator convertToPop(Prel prel) throws IOException {
    PhysicalPlanCreator creator = new PhysicalPlanCreator(context, PrelSequencer.getIdMap(prel));
    PhysicalOperator op = prel.getPhysicalOperator(creator);
    return op;
  }

  protected PhysicalPlan convertToPlan(PhysicalOperator op) {
    PlanPropertiesBuilder propsBuilder = PlanProperties.builder();
    propsBuilder.type(PlanType.APACHE_DRILL_PHYSICAL);
    propsBuilder.version(1);
    propsBuilder.options(new JSONOptions(context.getOptions().getOptionList()));
    propsBuilder.resultMode(ResultMode.EXEC);
    propsBuilder.generator(this.getClass().getSimpleName(), "");
    return new PhysicalPlan(propsBuilder.build(), getPops(op));
  }

  public static List<PhysicalOperator> getPops(PhysicalOperator root) {
    List<PhysicalOperator> ops = Lists.newArrayList();
    PopCollector c = new PopCollector();
    root.accept(c, ops);
    return ops;
  }

  private static class PopCollector extends
      AbstractPhysicalVisitor<Void, Collection<PhysicalOperator>, RuntimeException> {

    @Override
    public Void visitOp(PhysicalOperator op, Collection<PhysicalOperator> collection) throws RuntimeException {
      collection.add(op);
      for (PhysicalOperator o : op) {
        o.accept(this, collection);
      }
      return null;
    }

  }

  /**
   * Rewrite the parse tree. Used before validating the parse tree. Useful if a particular statement needs to converted
   * into another statement.
   *
   * @param node
   * @return Rewritten sql parse tree
   * @throws RelConversionException
   */
  public SqlNode rewrite(SqlNode node) throws RelConversionException {
    return node;
  }
}
