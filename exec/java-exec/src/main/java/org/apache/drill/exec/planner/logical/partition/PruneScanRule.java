/*
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
import java.util.concurrent.TimeUnit;

import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
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
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DrillTranslatableTable;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.drill.exec.store.dfs.MetadataContext;
import org.apache.drill.exec.store.dfs.MetadataContext.PruneStatus;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;

import org.apache.drill.exec.vector.ValueVector;


public abstract class PruneScanRule extends StoragePluginOptimizerRule {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PruneScanRule.class);

  final OptimizerRulesContext optimizerContext;

  public PruneScanRule(RelOptRuleOperand operand, String id, OptimizerRulesContext optimizerContext) {
    super(operand, id);
    this.optimizerContext = optimizerContext;
  }

  private static class DirPruneScanFilterOnProjectRule extends PruneScanRule {
    public DirPruneScanFilterOnProjectRule(OptimizerRulesContext optimizerRulesContext) {
      super(RelOptHelper.some(Filter.class, RelOptHelper.some(Project.class, RelOptHelper.any(TableScan.class))), "DirPruneScanRule:Filter_On_Project", optimizerRulesContext);
    }

    @Override
    public PartitionDescriptor getPartitionDescriptor(PlannerSettings settings, TableScan scanRel) {
      return new FileSystemPartitionDescriptor(settings, scanRel);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      final TableScan scan = call.rel(2);
      return isQualifiedDirPruning(scan);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Filter filterRel = call.rel(0);
      final Project projectRel = call.rel(1);
      final TableScan scanRel = call.rel(2);
      doOnMatch(call, filterRel, projectRel, scanRel);
    }
  }

  private static class DirPruneScanFilterOnScanRule extends PruneScanRule {
    public DirPruneScanFilterOnScanRule(OptimizerRulesContext optimizerRulesContext) {
      super(RelOptHelper.some(Filter.class, RelOptHelper.any(TableScan.class)), "DirPruneScanRule:Filter_On_Scan", optimizerRulesContext);
    }

    @Override
    public PartitionDescriptor getPartitionDescriptor(PlannerSettings settings, TableScan scanRel) {
      return new FileSystemPartitionDescriptor(settings, scanRel);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      final TableScan scan = call.rel(1);
      return isQualifiedDirPruning(scan);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Filter filterRel = call.rel(0);
      final TableScan scanRel = call.rel(1);
      doOnMatch(call, filterRel, null, scanRel);
    }
  }

  public static RelOptRule getDirFilterOnProject(OptimizerRulesContext optimizerRulesContext) {
    return new DirPruneScanFilterOnProjectRule(optimizerRulesContext);
  }

  public static RelOptRule getDirFilterOnScan(OptimizerRulesContext optimizerRulesContext) {
    return new DirPruneScanFilterOnScanRule(optimizerRulesContext);
  }

  protected void doOnMatch(RelOptRuleCall call, Filter filterRel, Project projectRel, TableScan scanRel) {

    final String pruningClassName = getClass().getName();
    logger.debug("Beginning partition pruning, pruning class: {}", pruningClassName);
    Stopwatch totalPruningTime = logger.isDebugEnabled() ? Stopwatch.createStarted() : null;

    final PlannerSettings settings = PrelUtil.getPlannerSettings(call.getPlanner());
    PartitionDescriptor descriptor = getPartitionDescriptor(settings, scanRel);
    final BufferAllocator allocator = optimizerContext.getAllocator();

    final Object selection = getDrillTable(scanRel).getSelection();
    MetadataContext metaContext = null;
    if (selection instanceof FormatSelection) {
         metaContext = ((FormatSelection)selection).getSelection().getMetaContext();
    }

    RexNode condition;
    if (projectRel == null) {
      condition = filterRel.getCondition();
    } else {
      // get the filter as if it were below the projection.
      condition = RelOptUtil.pushPastProject(filterRel.getCondition(), projectRel);
    }

    RewriteAsBinaryOperators visitor = new RewriteAsBinaryOperators(true, filterRel.getCluster().getRexBuilder());
    condition = condition.accept(visitor);

    Map<Integer, String> fieldNameMap = Maps.newHashMap();
    List<String> fieldNames = scanRel.getRowType().getFieldNames();
    BitSet columnBitset = new BitSet();
    BitSet partitionColumnBitSet = new BitSet();
    Map<Integer, Integer> partitionMap = Maps.newHashMap();

    int relColIndex = 0;
    for (String field : fieldNames) {
      final Integer partitionIndex = descriptor.getIdIfValid(field);
      if (partitionIndex != null) {
        fieldNameMap.put(partitionIndex, field);
        partitionColumnBitSet.set(partitionIndex);
        columnBitset.set(relColIndex);
        // mapping between the relColIndex and partitionIndex
        partitionMap.put(relColIndex, partitionIndex);
      }
      relColIndex++;
    }

    if (partitionColumnBitSet.isEmpty()) {
      if (totalPruningTime != null) {
        logger.debug("No partition columns are projected from the scan..continue. Total pruning elapsed time: {} ms",
            totalPruningTime.elapsed(TimeUnit.MILLISECONDS));
      }
      setPruneStatus(metaContext, PruneStatus.NOT_PRUNED);
      return;
    }

    // stop watch to track how long we spend in different phases of pruning
    // first track how long we spend building the filter tree
    Stopwatch miscTimer = logger.isDebugEnabled() ? Stopwatch.createStarted() : null;

    FindPartitionConditions c = new FindPartitionConditions(columnBitset, filterRel.getCluster().getRexBuilder());
    c.analyze(condition);
    RexNode pruneCondition = c.getFinalCondition();
    BitSet referencedDirsBitSet = c.getReferencedDirs();

    if (miscTimer != null) {
      logger.debug("Total elapsed time to build and analyze filter tree: {} ms", miscTimer.elapsed(TimeUnit.MILLISECONDS));
      miscTimer.reset();
    }

    if (pruneCondition == null) {
      if (totalPruningTime != null) {
        logger.debug("No conditions were found eligible for partition pruning. Total pruning elapsed time: {} ms",
            totalPruningTime.elapsed(TimeUnit.MILLISECONDS));
      }
      setPruneStatus(metaContext, PruneStatus.NOT_PRUNED);
      return;
    }

    // set up the partitions
    List<PartitionLocation> newPartitions = Lists.newArrayList();
    long numTotal = 0; // total number of partitions
    int batchIndex = 0;
    PartitionLocation firstLocation = null;
    LogicalExpression materializedExpr = null;
    String[] spInfo = null;
    int maxIndex = -1;
    BitSet matchBitSet = new BitSet();

    // Outer loop: iterate over a list of batches of PartitionLocations
    for (List<PartitionLocation> partitions : descriptor) {
      numTotal += partitions.size();
      logger.debug("Evaluating partition pruning for batch {}", batchIndex);
      if (batchIndex == 0) { // save the first location in case everything is pruned
        firstLocation = partitions.get(0);
      }
      final NullableBitVector output = new NullableBitVector(MaterializedField.create("", Types.optional(MinorType.BIT)), allocator);
      final VectorContainer container = new VectorContainer();

      try {
        final ValueVector[] vectors = new ValueVector[descriptor.getMaxHierarchyLevel()];
        for (int partitionColumnIndex : BitSets.toIter(partitionColumnBitSet)) {
          SchemaPath column = SchemaPath.getSimplePath(fieldNameMap.get(partitionColumnIndex));
          MajorType type = descriptor.getVectorType(column, settings);
          MaterializedField field = MaterializedField.create(column.getLastSegment().getNameSegment().getPath(), type);
          ValueVector v = TypeHelper.getNewVector(field, allocator);
          v.allocateNew();
          vectors[partitionColumnIndex] = v;
          container.add(v);
        }

        if (miscTimer != null) {
          // track how long we spend populating partition column vectors
          miscTimer.start();
        }

        // populate partition vectors.
        descriptor.populatePartitionVectors(vectors, partitions, partitionColumnBitSet, fieldNameMap);

        if (miscTimer != null) {
          logger.debug("Elapsed time to populate partitioning column vectors: {} ms within batchIndex: {}",
              miscTimer.elapsed(TimeUnit.MILLISECONDS), batchIndex);
          miscTimer.reset();
        }

        // materialize the expression; only need to do this once
        if (batchIndex == 0) {
          materializedExpr = materializePruneExpr(pruneCondition, settings, scanRel, container);
          if (materializedExpr == null) {
            // continue without partition pruning; no need to log anything here since
            // materializePruneExpr logs it already
            if (totalPruningTime != null) {
              logger.debug("Total pruning elapsed time: {} ms", totalPruningTime.elapsed(TimeUnit.MILLISECONDS));
            }
            setPruneStatus(metaContext, PruneStatus.NOT_PRUNED);
            return;
          }
        }

        output.allocateNew(partitions.size());

        if (miscTimer != null) {
          // start the timer to evaluate how long we spend in the interpreter evaluation
          miscTimer.start();
        }

        InterpreterEvaluator.evaluate(partitions.size(), optimizerContext, container, output, materializedExpr);

        if (miscTimer != null) {
          logger.debug("Elapsed time in interpreter evaluation: {} ms within batchIndex: {} with # of partitions : {}",
              miscTimer.elapsed(TimeUnit.MILLISECONDS), batchIndex, partitions.size());
          miscTimer.reset();
        }

        int recordCount = 0;
        int qualifiedCount = 0;

        if (descriptor.supportsMetadataCachePruning() &&
            partitions.get(0).isCompositePartition() /* apply single partition check only for composite partitions */) {
          // Inner loop: within each batch iterate over the PartitionLocations
          for (PartitionLocation part : partitions) {
            assert part.isCompositePartition();
            if(!output.getAccessor().isNull(recordCount) && output.getAccessor().get(recordCount) == 1) {
              newPartitions.add(part);
              // Rather than using the PartitionLocation, get the array of partition values for the directories that are
              // referenced by the filter since we are not interested in directory references in other parts of the query.
              Pair<String[], Integer> p = composePartition(referencedDirsBitSet, partitionMap, vectors, recordCount);
              String[] parts = p.getLeft();
              int tmpIndex = p.getRight();
              maxIndex = Math.max(maxIndex, tmpIndex);
              if (spInfo == null) { // initialization
                spInfo = parts;
                for (int j = 0; j <= tmpIndex; j++) {
                  if (parts[j] != null) {
                    matchBitSet.set(j);
                  }
                }
              } else {
                // compare the new partition with existing partition
                for (int j=0; j <= tmpIndex; j++) {
                  if (parts[j] == null || spInfo[j] == null) { // nulls don't match
                    matchBitSet.clear(j);
                  } else {
                    if (!parts[j].equals(spInfo[j])) {
                      matchBitSet.clear(j);
                    }
                  }
                }
              }
              qualifiedCount++;
            }
            recordCount++;
          }
        } else {
          // Inner loop: within each batch iterate over the PartitionLocations
          for(PartitionLocation part: partitions){
            if(!output.getAccessor().isNull(recordCount) && output.getAccessor().get(recordCount) == 1) {
              newPartitions.add(part);
              qualifiedCount++;
            }
            recordCount++;
          }
        }
        logger.debug("Within batch {}: total records: {}, qualified records: {}", batchIndex, recordCount, qualifiedCount);
        batchIndex++;
      } catch (Exception e) {
        logger.warn("Exception while trying to prune partition.", e);
        if (totalPruningTime != null) {
          logger.debug("Total pruning elapsed time: {} ms", totalPruningTime.elapsed(TimeUnit.MILLISECONDS));
        }

        setPruneStatus(metaContext, PruneStatus.NOT_PRUNED);
        return; // continue without partition pruning
      } finally {
        container.clear();
        if (output != null) {
          output.clear();
        }
      }
    }

    try {
      if (newPartitions.size() == numTotal) {
        logger.debug("No partitions were eligible for pruning");
        return;
      }

      // handle the case all partitions are filtered out.
      boolean canDropFilter = true;
      boolean wasAllPartitionsPruned = false;
      String cacheFileRoot = null;

      if (newPartitions.isEmpty()) {
        assert firstLocation != null;
        // Add the first non-composite partition location, since execution requires schema.
        // In such case, we should not drop filter.
        newPartitions.add(firstLocation.getPartitionLocationRecursive().get(0));
        canDropFilter = false;
        // NOTE: with DRILL-4530, the PruneScanRule may be called with only a list of
        // directories first and the non-composite partition location will still return
        // directories, not files.  So, additional processing is done depending on this flag
        wasAllPartitionsPruned = true;
        logger.debug("All {} partitions were pruned; added back a single partition to allow creating a schema", numTotal);

        // set the cacheFileRoot appropriately
        if (firstLocation.isCompositePartition()) {
          cacheFileRoot = descriptor.getBaseTableLocation() + firstLocation.getCompositePartitionPath();
        }
      }

      logger.debug("Pruned {} partitions down to {}", numTotal, newPartitions.size());

      List<RexNode> conjuncts = RelOptUtil.conjunctions(condition);
      List<RexNode> pruneConjuncts = RelOptUtil.conjunctions(pruneCondition);
      conjuncts.removeAll(pruneConjuncts);
      RexNode newCondition = RexUtil.composeConjunction(filterRel.getCluster().getRexBuilder(), conjuncts, false);

      RewriteCombineBinaryOperators reverseVisitor = new RewriteCombineBinaryOperators(true, filterRel.getCluster().getRexBuilder());

      condition = condition.accept(reverseVisitor);
      pruneCondition = pruneCondition.accept(reverseVisitor);

      if (descriptor.supportsMetadataCachePruning() && !wasAllPartitionsPruned) {
        // if metadata cache file could potentially be used, then assign a proper cacheFileRoot
        int index = -1;
        if (!matchBitSet.isEmpty()) {
          String path = "";
          index = matchBitSet.length() - 1;

          for (int j = 0; j < matchBitSet.length(); j++) {
            if (!matchBitSet.get(j)) {
              // stop at the first index with no match and use the immediate
              // previous index
              index = j-1;
              break;
            }
          }
          for (int j=0; j <= index; j++) {
            path += "/" + spInfo[j];
          }
          cacheFileRoot = descriptor.getBaseTableLocation() + path;
        }
        if (index != maxIndex) {
          // if multiple partitions are being selected, we should not drop the filter
          // since we are reading the cache file at a parent/ancestor level
          canDropFilter = false;
        }

      }

      RelNode inputRel = descriptor.supportsMetadataCachePruning() ?
          descriptor.createTableScan(newPartitions, cacheFileRoot, wasAllPartitionsPruned, metaContext) :
            descriptor.createTableScan(newPartitions, wasAllPartitionsPruned);

      if (projectRel != null) {
        inputRel = projectRel.copy(projectRel.getTraitSet(), Collections.singletonList(inputRel));
      }

      if (newCondition.isAlwaysTrue() && canDropFilter) {
        call.transformTo(inputRel);
      } else {
        final RelNode newFilter = filterRel.copy(filterRel.getTraitSet(), Collections.singletonList(inputRel));
        call.transformTo(newFilter);
      }

      setPruneStatus(metaContext, PruneStatus.PRUNED);

    } catch (Exception e) {
      logger.warn("Exception while using the pruned partitions.", e);
    } finally {
      if (totalPruningTime != null) {
        logger.debug("Total pruning elapsed time: {} ms", totalPruningTime.elapsed(TimeUnit.MILLISECONDS));
      }
    }
  }

  /** Compose the array of partition values for the directories that are referenced by filter:
   *  e.g suppose the dir hierarchy is year/quarter/month and the query is:
   *     SELECT * FROM T WHERE dir0=2015 AND dir1 = 'Q1',
   * then for 2015/Q1/Feb, this will have ['2015', 'Q1', null]
   * If the query filter condition is WHERE dir1 = 'Q2'  (i.e no dir0 condition) then the array will
   * have [null, 'Q2', null]
   */
  private Pair<String[], Integer> composePartition(BitSet referencedDirsBitSet,
      Map<Integer, Integer> partitionMap,
      ValueVector[] vectors,
      int recordCount) {
    String[] partition = new String[vectors.length];
    int maxIndex = -1;
    for (int referencedDirsIndex : BitSets.toIter(referencedDirsBitSet)) {
      int partitionColumnIndex = partitionMap.get(referencedDirsIndex);
      ValueVector vv = vectors[partitionColumnIndex];
      if (vv.getAccessor().getValueCount() > 0 &&
          vv.getAccessor().getObject(recordCount) != null) {
        String value = vv.getAccessor().getObject(recordCount).toString();
        partition[partitionColumnIndex] = value;
        maxIndex = Math.max(maxIndex, partitionColumnIndex);
      }
    }
    return Pair.of(partition, maxIndex);
  }

  protected LogicalExpression materializePruneExpr(RexNode pruneCondition,
      PlannerSettings settings,
      RelNode scanRel,
      VectorContainer container
      ) {
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
      return null;
    }
    return materializedExpr;
  }

  protected OptimizerRulesContext getOptimizerRulesContext() {
    return optimizerContext;
  }

  public abstract PartitionDescriptor getPartitionDescriptor(PlannerSettings settings, TableScan scanRel);

  private static DrillTable getDrillTable(final TableScan scan) {
    DrillTable drillTable;
    drillTable = scan.getTable().unwrap(DrillTable.class);
    if (drillTable == null) {
      drillTable = scan.getTable().unwrap(DrillTranslatableTable.class).getDrillTable();
    }
    return drillTable;
  }

  private static boolean isQualifiedDirPruning(final TableScan scan) {
    if (scan instanceof EnumerableTableScan) {
      final Object selection = getDrillTable(scan).getSelection();
      if (selection instanceof FormatSelection
          && ((FormatSelection)selection).supportDirPruning()) {
        return true;  // Do directory-based pruning in Calcite logical
      } else {
        return false; // Do not do directory-based pruning in Calcite logical
      }
    } else if (scan instanceof DrillScanRel) {
      final GroupScan groupScan = ((DrillScanRel) scan).getGroupScan();
      // this rule is applicable only for dfs based partition pruning in Drill Logical
      return groupScan instanceof FileGroupScan && groupScan.supportsPartitionFilterPushdown() && !((DrillScanRel)scan).partitionFilterPushdown();
    }
    return false;
  }

  private static void setPruneStatus(MetadataContext metaContext, PruneStatus pruneStatus) {
    if (metaContext != null) {
      metaContext.setPruneStatus(pruneStatus);
    }
  }

}
