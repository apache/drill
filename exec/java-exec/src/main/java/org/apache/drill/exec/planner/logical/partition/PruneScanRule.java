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
package org.apache.drill.exec.planner.logical.partition;

import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.BitSets;

import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.fn.interpreter.InterpreterEvaluator;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.FileGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.FileSystemPartitionDescriptor;
import org.apache.drill.exec.planner.PartitionDescriptor;
import org.apache.drill.exec.planner.PartitionLocation;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.drill.exec.vector.ValueVector;

public abstract class PruneScanRule extends StoragePluginOptimizerRule {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PruneScanRule.class);

  final OptimizerRulesContext optimizerContext;

  public PruneScanRule(RelOptRuleOperand operand, String id, OptimizerRulesContext optimizerContext) {
    super(operand, id);
    this.optimizerContext = optimizerContext;
  }

  public static final RelOptRule getFilterOnProject(OptimizerRulesContext optimizerRulesContext) {
    return new PruneScanRule(
        RelOptHelper.some(DrillFilterRel.class, RelOptHelper.some(DrillProjectRel.class, RelOptHelper.any(DrillScanRel.class))),
        "PruneScanRule:Filter_On_Project",
        optimizerRulesContext) {

      @Override
      public PartitionDescriptor getPartitionDescriptor(PlannerSettings settings, DrillScanRel scanRel) {
        return new FileSystemPartitionDescriptor(settings, scanRel);
      }

      @Override
      public boolean matches(RelOptRuleCall call) {
        final DrillScanRel scan = (DrillScanRel) call.rel(2);
        GroupScan groupScan = scan.getGroupScan();
        // this rule is applicable only for dfs based partition pruning
        return groupScan instanceof FileGroupScan && groupScan.supportsPartitionFilterPushdown();
      }

      @Override
      public void onMatch(RelOptRuleCall call) {
        final DrillFilterRel filterRel = (DrillFilterRel) call.rel(0);
        final DrillProjectRel projectRel = (DrillProjectRel) call.rel(1);
        final DrillScanRel scanRel = (DrillScanRel) call.rel(2);
        doOnMatch(call, filterRel, projectRel, scanRel);
      }
    };
  }

  public static final RelOptRule getFilterOnScan(OptimizerRulesContext optimizerRulesContext) {
    return new PruneScanRule(
        RelOptHelper.some(DrillFilterRel.class, RelOptHelper.any(DrillScanRel.class)),
        "PruneScanRule:Filter_On_Scan", optimizerRulesContext) {

      @Override
      public PartitionDescriptor getPartitionDescriptor(PlannerSettings settings, DrillScanRel scanRel) {
        return new FileSystemPartitionDescriptor(settings, scanRel);
      }

      @Override
      public boolean matches(RelOptRuleCall call) {
        final DrillScanRel scan = (DrillScanRel) call.rel(1);
        GroupScan groupScan = scan.getGroupScan();
        // this rule is applicable only for dfs based partition pruning
        return groupScan instanceof FileGroupScan && groupScan.supportsPartitionFilterPushdown();
      }

      @Override
      public void onMatch(RelOptRuleCall call) {
        final DrillFilterRel filterRel = (DrillFilterRel) call.rel(0);
        final DrillScanRel scanRel = (DrillScanRel) call.rel(1);
        doOnMatch(call, filterRel, null, scanRel);
      }
    };
  }

  protected void doOnMatch(RelOptRuleCall call, DrillFilterRel filterRel, DrillProjectRel projectRel, DrillScanRel scanRel) {
    final PlannerSettings settings = PrelUtil.getPlannerSettings(call.getPlanner());
    PartitionDescriptor descriptor = getPartitionDescriptor(settings, scanRel);
    final BufferAllocator allocator = optimizerContext.getAllocator();


    RexNode condition = null;
    if (projectRel == null) {
      condition = filterRel.getCondition();
    } else {
      // get the filter as if it were below the projection.
      condition = RelOptUtil.pushFilterPastProject(filterRel.getCondition(), projectRel);
    }

    RewriteAsBinaryOperators visitor = new RewriteAsBinaryOperators(true, filterRel.getCluster().getRexBuilder());
    condition = condition.accept(visitor);

    Map<Integer, String> fieldNameMap = Maps.newHashMap();
    List<String> fieldNames = scanRel.getRowType().getFieldNames();
    BitSet columnBitset = new BitSet();
    BitSet partitionColumnBitSet = new BitSet();

    {
      int relColIndex = 0;
      for (String field : fieldNames) {
        final Integer partitionIndex = descriptor.getIdIfValid(field);
        if (partitionIndex != null) {
          fieldNameMap.put(partitionIndex, field);
          partitionColumnBitSet.set(partitionIndex);
          columnBitset.set(relColIndex);
        }
        relColIndex++;
      }
    }

    if (partitionColumnBitSet.isEmpty()) {
      return;
    }

    FindPartitionConditions c = new FindPartitionConditions(columnBitset, filterRel.getCluster().getRexBuilder());
    c.analyze(condition);
    RexNode pruneCondition = c.getFinalCondition();

    if (pruneCondition == null) {
      return;
    }


    // set up the partitions
    final GroupScan groupScan = scanRel.getGroupScan();
    List<PartitionLocation> partitions = descriptor.getPartitions();

    if (partitions.size() > Character.MAX_VALUE) {
      return;
    }

    final NullableBitVector output = new NullableBitVector(MaterializedField.create("", Types.optional(MinorType.BIT)), allocator);
    final VectorContainer container = new VectorContainer();

    try {
      final ValueVector[] vectors = new ValueVector[descriptor.getMaxHierarchyLevel()];
      for (int partitionColumnIndex : BitSets.toIter(partitionColumnBitSet)) {
        SchemaPath column = SchemaPath.getSimplePath(fieldNameMap.get(partitionColumnIndex));
        MajorType type = descriptor.getVectorType(column, settings);
        MaterializedField field = MaterializedField.create(column, type);
        ValueVector v = TypeHelper.getNewVector(field, allocator);
        v.allocateNew();
        vectors[partitionColumnIndex] = v;
        container.add(v);
      }

      // populate partition vectors.
      descriptor.populatePartitionVectors(vectors, partitions, partitionColumnBitSet, fieldNameMap);

      // materialize the expression
      logger.debug("Attempting to prune {}", pruneCondition);
      final LogicalExpression expr = DrillOptiq.toDrill(new DrillParseContext(settings), scanRel, pruneCondition);
      final ErrorCollectorImpl errors = new ErrorCollectorImpl();

      LogicalExpression materializedExpr = ExpressionTreeMaterializer.materialize(expr, container, errors, optimizerContext.getFunctionRegistry());
      // Make sure pruneCondition's materialized expression is always of BitType, so that
      // it's same as the type of output vector.
      if (materializedExpr.getMajorType().getMode() == TypeProtos.DataMode.REQUIRED) {
        materializedExpr = ExpressionTreeMaterializer.convertToNullableType(
            materializedExpr,
            materializedExpr.getMajorType().getMinorType(),
            optimizerContext.getFunctionRegistry(),
            errors);
      }

      if (errors.getErrorCount() != 0) {
        logger.warn("Failure while materializing expression [{}].  Errors: {}", expr, errors);
      }

      output.allocateNew(partitions.size());
      InterpreterEvaluator.evaluate(partitions.size(), optimizerContext, container, output, materializedExpr);
      int record = 0;

      List<String> newFiles = Lists.newArrayList();
      for(PartitionLocation part: partitions){
        if(!output.getAccessor().isNull(record) && output.getAccessor().get(record) == 1){
          newFiles.add(part.getEntirePartitionLocation());
        }
        record++;
      }

      boolean canDropFilter = true;

      if (newFiles.isEmpty()) {
        newFiles.add(partitions.get(0).getEntirePartitionLocation());
        canDropFilter = false;
      }

      if (newFiles.size() == partitions.size()) {
        return;
      }

      logger.debug("Pruned {} => {}", partitions.size(), newFiles.size());


      List<RexNode> conjuncts = RelOptUtil.conjunctions(condition);
      List<RexNode> pruneConjuncts = RelOptUtil.conjunctions(pruneCondition);
      conjuncts.removeAll(pruneConjuncts);
      RexNode newCondition = RexUtil.composeConjunction(filterRel.getCluster().getRexBuilder(), conjuncts, false);

      RewriteCombineBinaryOperators reverseVisitor = new RewriteCombineBinaryOperators(true, filterRel.getCluster().getRexBuilder());

      condition = condition.accept(reverseVisitor);
      pruneCondition = pruneCondition.accept(reverseVisitor);

      final DrillScanRel newScanRel =
          new DrillScanRel(scanRel.getCluster(),
              scanRel.getTraitSet().plus(DrillRel.DRILL_LOGICAL),
              scanRel.getTable(),
              descriptor.createNewGroupScan(newFiles),
              scanRel.getRowType(),
              scanRel.getColumns());

      RelNode inputRel = newScanRel;

      if (projectRel != null) {
        inputRel = projectRel.copy(projectRel.getTraitSet(), Collections.singletonList(inputRel));
      }

      if (newCondition.isAlwaysTrue() && canDropFilter) {
        call.transformTo(inputRel);
      } else {
        final RelNode newFilter = filterRel.copy(filterRel.getTraitSet(), Collections.singletonList(inputRel));
        call.transformTo(newFilter);
      }

    } catch (Exception e) {
      logger.warn("Exception while trying to prune partition.", e);
    } finally {
      container.clear();
      if (output != null) {
        output.clear();
      }
    }
  }

  protected OptimizerRulesContext getOptimizerRulesContext() {
    return optimizerContext;
  }

  public abstract PartitionDescriptor getPartitionDescriptor(PlannerSettings settings, DrillScanRel scanRel);
}
