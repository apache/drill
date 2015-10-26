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
import java.util.Collection;
import java.util.List;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rules.JoinToMultiJoinRule;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.TypedSqlNode;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
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
import org.apache.drill.exec.physical.impl.join.JoinUtils;
import org.apache.drill.exec.planner.common.DrillRelOptUtil;
import org.apache.drill.exec.planner.cost.DrillDefaultRelMetadataProvider;
import org.apache.drill.exec.planner.logical.DrillJoinRel;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillRelFactories;
import org.apache.drill.exec.planner.logical.DrillScreenRel;
import org.apache.drill.exec.planner.logical.DrillStoreRel;
import org.apache.drill.exec.planner.logical.PreProcessLogicalRel;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait;
import org.apache.drill.exec.planner.physical.PhysicalPlanCreator;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.explain.PrelSequencer;
import org.apache.drill.exec.planner.physical.visitor.ComplexToJsonPrelVisitor;
import org.apache.drill.exec.planner.physical.visitor.ExcessiveExchangeIdentifier;
import org.apache.drill.exec.planner.physical.visitor.FinalColumnReorderer;
import org.apache.drill.exec.planner.physical.visitor.InsertLocalExchangeVisitor;
import org.apache.drill.exec.planner.physical.visitor.JoinPrelRenameVisitor;
import org.apache.drill.exec.planner.physical.visitor.MemoryEstimationVisitor;
import org.apache.drill.exec.planner.physical.visitor.RelUniqifier;
import org.apache.drill.exec.planner.physical.visitor.RewriteProjectToFlatten;
import org.apache.drill.exec.planner.physical.visitor.SelectionVectorPrelVisitor;
import org.apache.drill.exec.planner.physical.visitor.SplitUpComplexExpressions;
import org.apache.drill.exec.planner.physical.visitor.StarColumnConverter;
import org.apache.drill.exec.planner.physical.visitor.SwapHashJoinVisitor;
import org.apache.drill.exec.planner.sql.DrillSqlWorker;
import org.apache.drill.exec.planner.sql.parser.UnsupportedOperatorsVisitor;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.util.Pointer;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.drill.exec.work.foreman.SqlUnsupportedException;
import org.apache.drill.exec.work.foreman.UnsupportedRelOperatorException;
import org.slf4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class DefaultSqlHandler extends AbstractSqlHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DefaultSqlHandler.class);

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

  protected static void log(final String name, final RelNode node, final Logger logger) {
    if (logger.isDebugEnabled()) {
      logger.debug(name + " : \n" + RelOptUtil.toString(node, SqlExplainLevel.ALL_ATTRIBUTES));
    }
  }

  protected void log(final String name, final Prel node, final Logger logger) {
    String plan = PrelSequencer.printWithIds(node, SqlExplainLevel.ALL_ATTRIBUTES);
    if(textPlan != null){
      textPlan.value = plan;
    }

    if (logger.isDebugEnabled()) {
      logger.debug(name + " : \n" + plan);
    }
  }

  protected void log(final String name, final PhysicalPlan plan, final Logger logger) throws JsonProcessingException {
    if (logger.isDebugEnabled()) {
      String planText = plan.unparse(context.getLpPersistence().getMapper().writer());
      logger.debug(name + " : \n" + planText);
    }
  }


  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException, ForemanSetupException {
    final ConvertedRelNode convertedRelNode = validateAndConvert(sqlNode);
    final RelDataType validatedRowType = convertedRelNode.getValidatedRowType();
    final RelNode queryRelNode = convertedRelNode.getConvertedNode();

    log("Optiq Logical", queryRelNode, logger);
    DrillRel drel = convertToDrel(queryRelNode, validatedRowType);

    log("Drill Logical", drel, logger);
    Prel prel = convertToPrel(drel);
    log("Drill Physical", prel, logger);
    PhysicalOperator pop = convertToPop(prel);
    PhysicalPlan plan = convertToPlan(pop);
    log("Drill Plan", plan, logger);
    return plan;
  }


  /**
   * Rewrite the parse tree. Used before validating the parse tree. Useful if a particular statement needs to converted
   * into another statement.
   *
   * @param node
   * @return Rewritten sql parse tree
   * @throws RelConversionException
   */
  protected SqlNode rewrite(SqlNode node) throws RelConversionException, ForemanSetupException {
    return node;
  }

  protected ConvertedRelNode validateAndConvert(SqlNode sqlNode) throws ForemanSetupException, RelConversionException, ValidationException {
    final SqlNode rewrittenSqlNode = rewrite(sqlNode);
    final TypedSqlNode validatedTypedSqlNode = validateNode(rewrittenSqlNode);
    final SqlNode validated = validatedTypedSqlNode.getSqlNode();

    RelNode rel = convertToRel(validated);
    rel = preprocessNode(rel);

    return new ConvertedRelNode(rel, validatedTypedSqlNode.getType());
  }

  /**
   *  Given a relNode tree for SELECT statement, convert to Drill Logical RelNode tree.
   * @param relNode
   * @return
   * @throws SqlUnsupportedException
   * @throws RelConversionException
   */
  protected DrillRel convertToDrel(RelNode relNode) throws SqlUnsupportedException, RelConversionException {
    try {
      final DrillRel convertedRelNode;

      if (! context.getPlannerSettings().isHepJoinOptEnabled()) {
        convertedRelNode = (DrillRel) logicalPlanningVolcano(relNode);
      } else {
        convertedRelNode = (DrillRel) logicalPlanningVolcanoAndLopt(relNode);
      }

      if (convertedRelNode instanceof DrillStoreRel) {
        throw new UnsupportedOperationException();
      } else {

        // If the query contains a limit 0 clause, disable distributed mode since it is overkill for determining schema.
        if (FindLimit0Visitor.containsLimit0(convertedRelNode)) {
          context.getPlannerSettings().forceSingleMode();
        }

        return convertedRelNode;
      }
    } catch (RelOptPlanner.CannotPlanException ex) {
      logger.error(ex.getMessage());

      if(JoinUtils.checkCartesianJoin(relNode, new ArrayList<Integer>(), new ArrayList<Integer>())) {
        throw new UnsupportedRelOperatorException("This query cannot be planned possibly due to either a cartesian join or an inequality join");
      } else {
        throw ex;
      }
    }
  }

  /**
   * Return Drill Logical RelNode tree for a SELECT statement, when it is executed / explained directly.
   *
   * @param relNode : root RelNode corresponds to Calcite Logical RelNode.
   * @param validatedRowType : the rowType for the final field names. A rename project may be placed on top of the root.
   * @return
   * @throws RelConversionException
   * @throws SqlUnsupportedException
   */
  protected DrillRel convertToDrel(RelNode relNode, RelDataType validatedRowType) throws RelConversionException, SqlUnsupportedException {
    final DrillRel convertedRelNode = convertToDrel(relNode);

    // Put a non-trivial topProject to ensure the final output field name is preserved, when necessary.
    DrillRel topPreservedNameProj = addRenamedProject(convertedRelNode, validatedRowType);
    return new DrillScreenRel(topPreservedNameProj.getCluster(), topPreservedNameProj.getTraitSet(),
        topPreservedNameProj);
  }

  /**
   * A shuttle designed to finalize all RelNodes.
   */
  private static class PrelFinalizer extends RelShuttleImpl {

    @Override
    public RelNode visit(RelNode other) {
      if (other instanceof PrelFinalizable) {
        return ((PrelFinalizable) other).finalizeRel();
      } else {
        return super.visit(other);
      }
    }

  }

  protected Prel convertToPrel(RelNode drel) throws RelConversionException, SqlUnsupportedException {
    Preconditions.checkArgument(drel.getConvention() == DrillRel.DRILL_LOGICAL);
    RelTraitSet traits = drel.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.SINGLETON);
    Prel phyRelNode;
    try {
      final RelNode relNode = planner.transform(DrillSqlWorker.PHYSICAL_MEM_RULES, traits, drel);
      phyRelNode = (Prel) relNode.accept(new PrelFinalizer());
    } catch (RelOptPlanner.CannotPlanException ex) {
      logger.error(ex.getMessage());

      if(JoinUtils.checkCartesianJoin(drel, new ArrayList<Integer>(), new ArrayList<Integer>())) {
        throw new UnsupportedRelOperatorException("This query cannot be planned possibly due to either a cartesian join or an inequality join");
      } else {
        throw ex;
      }
    }

    OptionManager queryOptions = context.getOptions();

    if (context.getPlannerSettings().isMemoryEstimationEnabled()
      && !MemoryEstimationVisitor.enoughMemory(phyRelNode, queryOptions, context.getActiveEndpoints().size())) {
      log("Not enough memory for this plan", phyRelNode, logger);
      logger.debug("Re-planning without hash operations.");

      queryOptions.setOption(OptionValue.createBoolean(OptionValue.OptionType.QUERY, PlannerSettings.HASHJOIN.getOptionName(), false));
      queryOptions.setOption(OptionValue.createBoolean(OptionValue.OptionType.QUERY, PlannerSettings.HASHAGG.getOptionName(), false));

      try {
        final RelNode relNode = planner.transform(DrillSqlWorker.PHYSICAL_MEM_RULES, traits, drel);
        phyRelNode = (Prel) relNode.accept(new PrelFinalizer());
      } catch (RelOptPlanner.CannotPlanException ex) {
        logger.error(ex.getMessage());

        if(JoinUtils.checkCartesianJoin(drel, new ArrayList<Integer>(), new ArrayList<Integer>())) {
          throw new UnsupportedRelOperatorException("This query cannot be planned possibly due to either a cartesian join or an inequality join");
        } else {
          throw ex;
        }
      }
    }

    /*  The order of the following transformation is important */

    /*
     * 0.) For select * from join query, we need insert project on top of scan and a top project just
     * under screen operator. The project on top of scan will rename from * to T1*, while the top project
     * will rename T1* to *, before it output the final result. Only the top project will allow
     * duplicate columns, since user could "explicitly" ask for duplicate columns ( select *, col, *).
     * The rest of projects will remove the duplicate column when we generate POP in json format.
     */
    phyRelNode = StarColumnConverter.insertRenameProject(phyRelNode);

    /*
     * 1.)
     * Join might cause naming conflicts from its left and right child.
     * In such case, we have to insert Project to rename the conflicting names.
     */
    phyRelNode = JoinPrelRenameVisitor.insertRenameProject(phyRelNode);

    /*
     * 1.1) Swap left / right for INNER hash join, if left's row count is < (1 + margin) right's row count.
     * We want to have smaller dataset on the right side, since hash table builds on right side.
     */
    if (context.getPlannerSettings().isHashJoinSwapEnabled()) {
      phyRelNode = SwapHashJoinVisitor.swapHashJoin(phyRelNode, new Double(context.getPlannerSettings().getHashJoinSwapMarginFactor()));
    }

    /*
     * 1.2) Break up all expressions with complex outputs into their own project operations
     */
    phyRelNode = phyRelNode.accept(new SplitUpComplexExpressions(planner.getTypeFactory(), context.getDrillOperatorTable(), context.getPlannerSettings().functionImplementationRegistry), null);

    /*
     * 1.3) Projections that contain reference to flatten are rewritten as Flatten operators followed by Project
     */
    phyRelNode = phyRelNode.accept(new RewriteProjectToFlatten(planner.getTypeFactory(), context.getDrillOperatorTable()), null);

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
     * Add ProducerConsumer after each scan if the option is set
     * Use the configured queueSize
     */
    /* DRILL-1617 Disabling ProducerConsumer as it produces incorrect results
    if (context.getOptions().getOption(PlannerSettings.PRODUCER_CONSUMER.getOptionName()).bool_val) {
      long queueSize = context.getOptions().getOption(PlannerSettings.PRODUCER_CONSUMER_QUEUE_SIZE.getOptionName()).num_val;
      phyRelNode = ProducerConsumerPrelVisitor.addProducerConsumerToScans(phyRelNode, (int) queueSize);
    }
    */


    /* 5.)
     * if the client does not support complex types (Map, Repeated)
     * insert a project which which would convert
     */
    if (!context.getSession().isSupportComplexTypes()) {
      logger.debug("Client does not support complex types, add ComplexToJson operator.");
      phyRelNode = ComplexToJsonPrelVisitor.addComplexToJsonPrel(phyRelNode);
    }


    /* 6.)
     * Insert LocalExchange (mux and/or demux) nodes
     */
    phyRelNode = InsertLocalExchangeVisitor.insertLocalExchanges(phyRelNode, queryOptions);


    /* 7.)
     * Next, we add any required selection vector removers given the supported encodings of each
     * operator. This will ultimately move to a new trait but we're managing here for now to avoid
     * introducing new issues in planning before the next release
     */
    phyRelNode = SelectionVectorPrelVisitor.addSelectionRemoversWhereNecessary(phyRelNode);

    /* 8.)
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

  private TypedSqlNode validateNode(SqlNode sqlNode) throws ValidationException, RelConversionException, ForemanSetupException {
    TypedSqlNode typedSqlNode = planner.validateAndGetType(sqlNode);

    SqlNode sqlNodeValidated = typedSqlNode.getSqlNode();

    // Check if the unsupported functionality is used
    UnsupportedOperatorsVisitor visitor = UnsupportedOperatorsVisitor.createVisitor(context);
    try {
      sqlNodeValidated.accept(visitor);
    } catch (UnsupportedOperationException ex) {
      // If the exception due to the unsupported functionalities
      visitor.convertException();

      // If it is not, let this exception move forward to higher logic
      throw ex;
    }

    return typedSqlNode;
  }

  private RelNode convertToRel(SqlNode node) throws RelConversionException {
    final RelNode convertedNode = planner.convert(node);

    final RelMetadataProvider provider = convertedNode.getCluster().getMetadataProvider();

    // Register RelMetadataProvider with HepPlanner.
    final List<RelMetadataProvider> list = Lists.newArrayList(provider);
    hepPlanner.registerMetadataProviders(list);
    final RelMetadataProvider cachingMetaDataProvider = new CachingRelMetadataProvider(ChainedRelMetadataProvider.of(list), hepPlanner);
    convertedNode.accept(new MetaDataProviderModifier(cachingMetaDataProvider));

    // HepPlanner is specifically used for Window Function planning only.
    hepPlanner.setRoot(convertedNode);
    RelNode rel = hepPlanner.findBestExp();

    rel.accept(new MetaDataProviderModifier(provider));
    return rel;
  }

  private RelNode preprocessNode(RelNode rel) throws SqlUnsupportedException {
    /*
     * Traverse the tree to do the following pre-processing tasks: 1. replace the convert_from, convert_to function to
     * actual implementations Eg: convert_from(EXPR, 'JSON') be converted to convert_fromjson(EXPR); TODO: Ideally all
     * function rewrites would move here instead of DrillOptiq.
     *
     * 2. see where the tree contains unsupported functions; throw SqlUnsupportedException if there is any.
     */

    PreProcessLogicalRel visitor = PreProcessLogicalRel.createVisitor(planner.getTypeFactory(),
        context.getDrillOperatorTable());
    try {
      rel = rel.accept(visitor);
    } catch (UnsupportedOperationException ex) {
      visitor.convertException();
      throw ex;
    }

    return rel;
  }

  protected DrillRel addRenamedProject(DrillRel rel, RelDataType validatedRowType) {
    RelDataType t = rel.getRowType();

    RexBuilder b = rel.getCluster().getRexBuilder();
    List<RexNode> projections = Lists.newArrayList();
    int projectCount = t.getFieldList().size();

    for (int i =0; i < projectCount; i++) {
      projections.add(b.makeInputRef(rel, i));
    }

    final List<String> fieldNames2 = SqlValidatorUtil.uniquify(validatedRowType.getFieldNames(), SqlValidatorUtil.F_SUGGESTER2);

    RelDataType newRowType = RexUtil.createStructType(rel.getCluster().getTypeFactory(), projections, fieldNames2);

    DrillProjectRel topProj = DrillProjectRel.create(rel.getCluster(), rel.getTraitSet(), rel, projections, newRowType);

    // Add a final non-trivial Project to get the validatedRowType, if child is not project.
    if (rel instanceof Project && DrillRelOptUtil.isTrivialProject(topProj, true)) {
      return rel;
    } else{
      return topProj;
    }
  }


  private RelNode logicalPlanningVolcano(RelNode relNode) throws RelConversionException, SqlUnsupportedException {
    return planner.transform(DrillSqlWorker.LOGICAL_RULES, relNode.getTraitSet().plus(DrillRel.DRILL_LOGICAL), relNode);
  }

  /**
   * Do logical planning using both VolcanoPlanner and LOPT HepPlanner.
   * @param relNode
   * @return
   * @throws RelConversionException
   * @throws SqlUnsupportedException
   */
  private RelNode logicalPlanningVolcanoAndLopt(RelNode relNode) throws RelConversionException, SqlUnsupportedException {

    final RelNode convertedRelNode = planner.transform(DrillSqlWorker.LOGICAL_CONVERT_RULES, relNode.getTraitSet().plus(DrillRel.DRILL_LOGICAL), relNode);
    log("VolCalciteRel", convertedRelNode, logger);

    final RelNode loptNode = getLoptJoinOrderTree(
        convertedRelNode,
        DrillJoinRel.class,
        DrillRelFactories.DRILL_LOGICAL_JOIN_FACTORY,
        DrillRelFactories.DRILL_LOGICAL_FILTER_FACTORY,
        DrillRelFactories.DRILL_LOGICAL_PROJECT_FACTORY);

    log("HepCalciteRel", loptNode, logger);

    return loptNode;
  }


  /**
   * Appy Join Order Optimizations using Hep Planner.
   */
  private RelNode getLoptJoinOrderTree(RelNode root,
                                      Class<? extends Join> joinClass,
                                             RelFactories.JoinFactory joinFactory,
                                             RelFactories.FilterFactory filterFactory,
                                             RelFactories.ProjectFactory projectFactory) {
    final HepProgramBuilder hepPgmBldr = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(new JoinToMultiJoinRule(joinClass))
        .addRuleInstance(new LoptOptimizeJoinRule(joinFactory, projectFactory, filterFactory))
        .addRuleInstance(ProjectRemoveRule.INSTANCE);
        // .addRuleInstance(new ProjectMergeRule(true, projectFactory));

        // .addRuleInstance(DrillMergeProjectRule.getInstance(true, projectFactory, this.context.getFunctionRegistry()));


    final HepProgram hepPgm = hepPgmBldr.build();
    final HepPlanner hepPlanner = new HepPlanner(hepPgm);

    final List<RelMetadataProvider> list = Lists.newArrayList();
    list.add(DrillDefaultRelMetadataProvider.INSTANCE);
    hepPlanner.registerMetadataProviders(list);
    final RelMetadataProvider cachingMetaDataProvider = new CachingRelMetadataProvider(ChainedRelMetadataProvider.of(list), hepPlanner);

    // Modify RelMetaProvider for every RelNode in the SQL operator Rel tree.
    root.accept(new MetaDataProviderModifier(cachingMetaDataProvider));

    hepPlanner.setRoot(root);

    RelNode calciteOptimizedPlan = hepPlanner.findBestExp();

    return calciteOptimizedPlan;
  }


  public static class MetaDataProviderModifier extends RelShuttleImpl {
    private final RelMetadataProvider metadataProvider;

    public MetaDataProviderModifier(RelMetadataProvider metadataProvider) {
      this.metadataProvider = metadataProvider;
    }

    @Override
    public RelNode visit(TableScan scan) {
      scan.getCluster().setMetadataProvider(metadataProvider);
      return super.visit(scan);
    }

    @Override
    public RelNode visit(TableFunctionScan scan) {
      scan.getCluster().setMetadataProvider(metadataProvider);
      return super.visit(scan);
    }

    @Override
    public RelNode visit(LogicalValues values) {
      values.getCluster().setMetadataProvider(metadataProvider);
      return super.visit(values);
    }

    @Override
    protected RelNode visitChild(RelNode parent, int i, RelNode child) {
      child.accept(this);
      parent.getCluster().setMetadataProvider(metadataProvider);
      return parent;
    }
  }

  protected class ConvertedRelNode {
    private final RelNode relNode;
    private final RelDataType validatedRowType;

    public ConvertedRelNode(RelNode relNode, RelDataType validatedRowType) {
      this.relNode = relNode;
      this.validatedRowType = validatedRowType;
    }

    public RelNode getConvertedNode() {
      return this.relNode;
    }

    public RelDataType getValidatedRowType() {
      return this.validatedRowType;
    }
  }


}
