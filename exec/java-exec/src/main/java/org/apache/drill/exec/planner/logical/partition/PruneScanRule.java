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

import java.util.ArrayList;
import java.util.BitSet;
 import java.util.Collections;
 import java.util.Iterator;
 import java.util.List;
 import java.util.Map;

 import org.apache.calcite.rex.RexUtil;
 import org.apache.calcite.util.BitSets;

 import org.apache.drill.common.expression.ErrorCollectorImpl;
 import org.apache.drill.common.expression.LogicalExpression;
 import org.apache.drill.common.expression.SchemaPath;
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
import org.apache.drill.exec.planner.ParquetPartitionDescriptor;
import org.apache.drill.exec.planner.PartitionDescriptor;
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
 import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.drill.exec.store.parquet.ParquetGroupScan;
import org.apache.drill.exec.vector.NullableBitVector;
 import org.apache.drill.exec.vector.NullableVarCharVector;
 import org.apache.calcite.rel.RelNode;
 import org.apache.calcite.plan.RelOptRule;
 import org.apache.calcite.plan.RelOptRuleCall;
 import org.apache.calcite.plan.RelOptRuleOperand;
 import org.apache.calcite.plan.RelOptUtil;
 import org.apache.calcite.rex.RexNode;

 import com.google.common.base.Charsets;
 import com.google.common.collect.Lists;
 import com.google.common.collect.Maps;
 import org.apache.drill.exec.vector.ValueVector;

public abstract class PruneScanRule extends RelOptRule {
   static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PruneScanRule.class);

   public static final RelOptRule getFilterOnProject(OptimizerRulesContext optimizerRulesContext){
       return new PruneScanRule(
           RelOptHelper.some(DrillFilterRel.class, RelOptHelper.some(DrillProjectRel.class, RelOptHelper.any(DrillScanRel.class))),
           "PruneScanRule:Filter_On_Project",
           optimizerRulesContext) {

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
       };

         @Override
         protected PartitionDescriptor getPartitionDescriptor(PlannerSettings settings, DrillScanRel scanRel) {
           return new FileSystemPartitionDescriptor(settings.getFsPartitionColumnLabel());
         }

         @Override
         protected void populatePartitionVectors(ValueVector[] vectors, List<PathPartition> partitions, BitSet partitionColumnBitSet, Map<Integer, String> fieldNameMap, GroupScan groupScan) {
           int record = 0;
           for(Iterator<PathPartition> iter = partitions.iterator(); iter.hasNext(); record++){
             final PathPartition partition = iter.next();
             for(int partitionColumnIndex : BitSets.toIter(partitionColumnBitSet)){
               if(partition.dirs[partitionColumnIndex] == null){
                 ((NullableVarCharVector) vectors[partitionColumnIndex]).getMutator().setNull(record);
               }else{
                 byte[] bytes = partition.dirs[partitionColumnIndex].getBytes(Charsets.UTF_8);
                 ((NullableVarCharVector) vectors[partitionColumnIndex]).getMutator().setSafe(record, bytes, 0, bytes.length);
               }
             }
           }

           for(ValueVector v : vectors){
             if(v == null){
               continue;
             }
             v.getMutator().setValueCount(partitions.size());
           }
         }

         @Override
         protected MajorType getVectorType(GroupScan groupScan, SchemaPath column) {
           return Types.optional(MinorType.VARCHAR);
         }

         @Override
         protected List<String> getFiles(DrillScanRel scanRel) {
           return ((FormatSelection)scanRel.getDrillTable().getSelection()).getAsFiles();
         }
       };
   }

   public static final RelOptRule getFilterOnScan(OptimizerRulesContext optimizerRulesContext){
     return new PruneScanRule(
           RelOptHelper.some(DrillFilterRel.class, RelOptHelper.any(DrillScanRel.class)),
           "PruneScanRule:Filter_On_Scan", optimizerRulesContext) {

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

       @Override
       protected PartitionDescriptor getPartitionDescriptor(PlannerSettings settings, DrillScanRel scanRel) {
         return new FileSystemPartitionDescriptor(settings.getFsPartitionColumnLabel());
       }

       @Override
       protected void populatePartitionVectors(ValueVector[] vectors, List<PathPartition> partitions, BitSet partitionColumnBitSet, Map<Integer, String> fieldNameMap, GroupScan groupScan) {
         int record = 0;
         for(Iterator<PathPartition> iter = partitions.iterator(); iter.hasNext(); record++){
           final PathPartition partition = iter.next();
           for(int partitionColumnIndex : BitSets.toIter(partitionColumnBitSet)){
             if(partition.dirs[partitionColumnIndex] == null){
               ((NullableVarCharVector) vectors[partitionColumnIndex]).getMutator().setNull(record);
             }else{
               byte[] bytes = partition.dirs[partitionColumnIndex].getBytes(Charsets.UTF_8);
               ((NullableVarCharVector) vectors[partitionColumnIndex]).getMutator().setSafe(record, bytes, 0, bytes.length);
             }
           }
         }

         for(ValueVector v : vectors){
           if(v == null){
             continue;
           }
           v.getMutator().setValueCount(partitions.size());
         }
       }

       @Override
        protected MajorType getVectorType(GroupScan groupScan, SchemaPath column) {
          return Types.optional(MinorType.VARCHAR);
        }

       @Override
       protected List<String> getFiles(DrillScanRel scanRel) {
         return ((FormatSelection)scanRel.getDrillTable().getSelection()).getAsFiles();
       }
     };
   }

  public static final RelOptRule getFilterOnProjectParquet(OptimizerRulesContext optimizerRulesContext){
    return new PruneScanRule(
        RelOptHelper.some(DrillFilterRel.class, RelOptHelper.some(DrillProjectRel.class, RelOptHelper.any(DrillScanRel.class))),
        "PruneScanRule:Filter_On_Project_Parquet",
        optimizerRulesContext) {

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
      };

      @Override
      protected PartitionDescriptor getPartitionDescriptor(PlannerSettings settings, DrillScanRel scanRel) {
        return new ParquetPartitionDescriptor(scanRel.getGroupScan().getPartitionColumns());
      }

      @Override
      protected void populatePartitionVectors(ValueVector[] vectors, List<PathPartition> partitions, BitSet partitionColumnBitSet, Map<Integer, String> fieldNameMap, GroupScan groupScan) {
        int record = 0;
        for(Iterator<PathPartition> iter = partitions.iterator(); iter.hasNext(); record++){
          final PathPartition partition = iter.next();
          for(int partitionColumnIndex : BitSets.toIter(partitionColumnBitSet)){
            SchemaPath column = SchemaPath.getSimplePath(fieldNameMap.get(partitionColumnIndex));
            ((ParquetGroupScan)groupScan).populatePruningVector(vectors[partitionColumnIndex], record, column, partition.file);
          }
        }

        for(ValueVector v : vectors){
          if(v == null){
            continue;
          }
          v.getMutator().setValueCount(partitions.size());
        }
      }

      @Override
      protected MajorType getVectorType(GroupScan groupScan, SchemaPath column) {
        return ((ParquetGroupScan)groupScan).getTypeForColumn(column);
      }

      @Override
      protected List<String> getFiles(DrillScanRel scanRel) {
        ParquetGroupScan groupScan = (ParquetGroupScan) scanRel.getGroupScan();
        return new ArrayList(groupScan.getFileSet());
      }
    };
  }

  // Using separate rules for Parquet column based partition pruning. In the future, we may want to see if we can combine these into
  // a single rule which handles both types of pruning

  public static final RelOptRule getFilterOnScanParquet(OptimizerRulesContext optimizerRulesContext){
    return new PruneScanRule(
        RelOptHelper.some(DrillFilterRel.class, RelOptHelper.any(DrillScanRel.class)),
        "PruneScanRule:Filter_On_Scan_Parquet", optimizerRulesContext) {

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

      @Override
      protected PartitionDescriptor getPartitionDescriptor(PlannerSettings settings, DrillScanRel scanRel) {
        return new ParquetPartitionDescriptor(scanRel.getGroupScan().getPartitionColumns());
      }

      @Override
      protected void populatePartitionVectors(ValueVector[] vectors, List<PathPartition> partitions, BitSet partitionColumnBitSet, Map<Integer, String> fieldNameMap, GroupScan groupScan) {
        int record = 0;
        for(Iterator<PathPartition> iter = partitions.iterator(); iter.hasNext(); record++){
          final PathPartition partition = iter.next();
          for(int partitionColumnIndex : BitSets.toIter(partitionColumnBitSet)){
            SchemaPath column = SchemaPath.getSimplePath(fieldNameMap.get(partitionColumnIndex));
            ((ParquetGroupScan)groupScan).populatePruningVector(vectors[partitionColumnIndex], record, column, partition.file);
          }
        }

        for(ValueVector v : vectors){
          if(v == null){
            continue;
          }
          v.getMutator().setValueCount(partitions.size());
        }
      }

      @Override
      protected MajorType getVectorType(GroupScan groupScan, SchemaPath column) {
        return ((ParquetGroupScan)groupScan).getTypeForColumn(column);
      }

      @Override
      protected List<String> getFiles(DrillScanRel scanRel) {
        ParquetGroupScan groupScan = (ParquetGroupScan) scanRel.getGroupScan();
        return new ArrayList(groupScan.getFileSet());
      }
    };
  }

   final OptimizerRulesContext optimizerContext;

   private PruneScanRule(RelOptRuleOperand operand, String id, OptimizerRulesContext optimizerContext) {
     super(operand, id);
     this.optimizerContext = optimizerContext;
   }

   protected abstract PartitionDescriptor getPartitionDescriptor(PlannerSettings settings, DrillScanRel scanRel);

   protected void doOnMatch(RelOptRuleCall call, DrillFilterRel filterRel, DrillProjectRel projectRel, DrillScanRel scanRel) {
     final PlannerSettings settings = PrelUtil.getPlannerSettings(call.getPlanner());
     PartitionDescriptor descriptor = getPartitionDescriptor(settings, scanRel);
     final BufferAllocator allocator = optimizerContext.getAllocator();


     RexNode condition = null;
     if(projectRel == null){
       condition = filterRel.getCondition();
     }else{
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
       for(String field : fieldNames){
         final Integer partitionIndex = descriptor.getIdIfValid(field);
         if(partitionIndex != null){
           fieldNameMap.put(partitionIndex, field);
           partitionColumnBitSet.set(partitionIndex);
           columnBitset.set(relColIndex);
         }
         relColIndex++;
       }
     }

     if(partitionColumnBitSet.isEmpty()){
       return;
     }

     FindPartitionConditions c = new FindPartitionConditions(columnBitset, filterRel.getCluster().getRexBuilder());
     c.analyze(condition);
     RexNode pruneCondition = c.getFinalCondition();

     if(pruneCondition == null){
       return;
     }


     // set up the partitions
     final GroupScan groupScan = scanRel.getGroupScan();
     final FormatSelection origSelection = (FormatSelection)scanRel.getDrillTable().getSelection();
     final List<String> files = getFiles(scanRel);
     final String selectionRoot = origSelection.getSelection().selectionRoot;
     List<PathPartition> partitions = Lists.newLinkedList();

     // let's only deal with one batch of files for now.
     if(files.size() > Character.MAX_VALUE){
       return;
     }

     for(String f : files){
       partitions.add(new PathPartition(descriptor.getMaxHierarchyLevel(), selectionRoot, f));
     }

     final NullableBitVector output = new NullableBitVector(MaterializedField.create("", Types.optional(MinorType.BIT)), allocator);
     final VectorContainer container = new VectorContainer();

     try{
       final ValueVector[] vectors = new ValueVector[descriptor.getMaxHierarchyLevel()];
       for(int partitionColumnIndex : BitSets.toIter(partitionColumnBitSet)){
         SchemaPath column = SchemaPath.getSimplePath(fieldNameMap.get(partitionColumnIndex));
         MajorType type = getVectorType(groupScan, column);
         MaterializedField field = MaterializedField.create(column, type);
         ValueVector v = TypeHelper.getNewVector(field, allocator);
         v.allocateNew();
         vectors[partitionColumnIndex] = v;
         container.add(v);
       }

       // populate partition vectors.

       populatePartitionVectors(vectors, partitions, partitionColumnBitSet, fieldNameMap, groupScan);

       // materialize the expression
       logger.debug("Attempting to prune {}", pruneCondition);
       LogicalExpression expr = DrillOptiq.toDrill(new DrillParseContext(settings), scanRel, pruneCondition);
       ErrorCollectorImpl errors = new ErrorCollectorImpl();
       LogicalExpression materializedExpr = ExpressionTreeMaterializer.materialize(expr, container, errors, optimizerContext.getFunctionRegistry());
       if (errors.getErrorCount() != 0) {
         logger.warn("Failure while materializing expression [{}].  Errors: {}", expr, errors);
       }

       output.allocateNew(partitions.size());
       InterpreterEvaluator.evaluate(partitions.size(), optimizerContext, container, output, materializedExpr);
       int record = 0;

       List<String> newFiles = Lists.newArrayList();
       for(Iterator<PathPartition> iter = partitions.iterator(); iter.hasNext(); record++){
         PathPartition part = iter.next();
         if(!output.getAccessor().isNull(record) && output.getAccessor().get(record) == 1){
           newFiles.add(part.file);
         }
       }

       boolean canDropFilter = true;

       if(newFiles.isEmpty()){
         newFiles.add(files.get(0));
         canDropFilter = false;
       }

       if(newFiles.size() == files.size()){
         return;
       }

       logger.debug("Pruned {} => {}", files, newFiles);


       List<RexNode> conjuncts = RelOptUtil.conjunctions(condition);
       List<RexNode> pruneConjuncts = RelOptUtil.conjunctions(pruneCondition);
       conjuncts.removeAll(pruneConjuncts);
       RexNode newCondition = RexUtil.composeConjunction(filterRel.getCluster().getRexBuilder(), conjuncts, false);

       RewriteCombineBinaryOperators reverseVisitor = new RewriteCombineBinaryOperators(true, filterRel.getCluster().getRexBuilder());

       condition = condition.accept(reverseVisitor);
       pruneCondition = pruneCondition.accept(reverseVisitor);

       final FileSelection newFileSelection = new FileSelection(newFiles, selectionRoot, true);
       final FileGroupScan newScan = ((FileGroupScan)scanRel.getGroupScan()).clone(newFileSelection);
       final DrillScanRel newScanRel =
           new DrillScanRel(scanRel.getCluster(),
               scanRel.getTraitSet().plus(DrillRel.DRILL_LOGICAL),
               scanRel.getTable(),
               newScan,
               scanRel.getRowType(),
               scanRel.getColumns());

       RelNode inputRel = newScanRel;

       if(projectRel != null){
         inputRel = projectRel.copy(projectRel.getTraitSet(), Collections.singletonList(inputRel));
       }

       if (newCondition.isAlwaysTrue() && canDropFilter) {
         call.transformTo(inputRel);
       } else {
         final RelNode newFilter = filterRel.copy(filterRel.getTraitSet(), Collections.singletonList(inputRel));
         call.transformTo(newFilter);
       }

     }catch(Exception e){
       logger.warn("Exception while trying to prune partition.", e);
     }finally{
       container.clear();
       if(output !=null){
         output.clear();
       }
     }
   }

   protected abstract void populatePartitionVectors(ValueVector[] vectors, List<PathPartition> partitions, BitSet partitionColumnBitSet, Map<Integer, String> fieldNameMap, GroupScan groupScan);

   protected abstract MajorType getVectorType(GroupScan groupScan, SchemaPath column);

   protected abstract List<String> getFiles(DrillScanRel scanRel);

   private static class PathPartition {
        final String[] dirs;
        final String file;

        public PathPartition(int max, String selectionRoot, String file){
          this.file = file;
          this.dirs = new String[max];
          int start = file.indexOf(selectionRoot) + selectionRoot.length();
          String postPath = file.substring(start);
          if (postPath.length() == 0) {
            return;
          }
          if(postPath.charAt(0) == '/'){
            postPath = postPath.substring(1);
          }
          String[] mostDirs = postPath.split("/");
          int maxLoop = Math.min(max, mostDirs.length - 1);
          for(int i =0; i < maxLoop; i++){
            this.dirs[i] = mostDirs[i];
          }
        }


      }

 }
