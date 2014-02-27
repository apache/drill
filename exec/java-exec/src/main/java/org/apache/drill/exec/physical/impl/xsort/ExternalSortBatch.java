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
package org.apache.drill.exec.physical.impl.xsort;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.ExternalSort;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.physical.impl.sort.SortRecordBatchBuilder;
import org.apache.drill.exec.physical.impl.svremover.RemovingRecordBatch;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.util.Utilities;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.allocator.VectorAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.eigenbase.rel.RelFieldCollation.Direction;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ExternalSortBatch extends AbstractRecordBatch<ExternalSort> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExternalSortBatch.class);

  private static final long MAX_SORT_BYTES = 1L * 1024 * 1024 * 1024;
  public static int SPILL_TARGET_RECORD_COUNT;
  public static int TARGET_RECORD_COUNT;
  public static int SPILL_BATCH_GROUP_SIZE;
  public static int SPILL_THRESHOLD;
  public static List<String> SPILL_DIRECTORIES;
  private Iterator<String> dirs;

  public final MappingSet MAIN_MAPPING = new MappingSet( (String) null, null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  public final MappingSet LEFT_MAPPING = new MappingSet("leftIndex", null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  public final MappingSet RIGHT_MAPPING = new MappingSet("rightIndex", null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  GeneratorMapping COPIER_MAPPING = new GeneratorMapping("doSetup", "doCopy", null, null);
  public final MappingSet COPIER_MAPPING_SET = new MappingSet(COPIER_MAPPING, COPIER_MAPPING);


  private final RecordBatch incoming;
  private BatchSchema schema;
  private SingleBatchSorter sorter;
  private SortRecordBatchBuilder builder;
  private MSorter mSorter;
  private PriorityQueueSelector selector;
  private PriorityQueueCopier copier;
  private LinkedList<BatchGroup> batchGroups = Lists.newLinkedList();
  private SelectionVector4 sv4;
  private FileSystem fs;
  private int spillCount = 0;
  private int batchesSinceLastSpill = 0;
  private long uid;//used for spill files to ensure multiple sorts within same fragment don't clobber each others' files

  public ExternalSortBatch(ExternalSort popConfig, FragmentContext context, RecordBatch incoming) {
    super(popConfig, context);
    this.incoming = incoming;
    DrillConfig config = context.getConfig();
    Configuration conf = new Configuration();
    conf.set("fs.default.name", config.getString(ExecConstants.EXTERNAL_SORT_SPILL_FILESYSTEM));
    try {
      this.fs = FileSystem.get(conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    SPILL_TARGET_RECORD_COUNT = config.getInt(ExecConstants.EXTERNAL_SORT_TARGET_SPILL_BATCH_SIZE);
    TARGET_RECORD_COUNT = config.getInt(ExecConstants.EXTERNAL_SORT_TARGET_BATCH_SIZE);
    SPILL_BATCH_GROUP_SIZE = config.getInt(ExecConstants.EXTERNAL_SORT_SPILL_GROUP_SIZE);
    SPILL_THRESHOLD = config.getInt(ExecConstants.EXTERNAL_SORT_SPILL_THRESHOLD);
    SPILL_DIRECTORIES = config.getStringList(ExecConstants.EXTERNAL_SORT_SPILL_DIRS);
    dirs = Iterators.cycle(Lists.newArrayList(SPILL_DIRECTORIES));
    uid = System.nanoTime();
  }

  @Override
  public int getRecordCount() {
    return sv4.getCount();
  }

  @Override
  public void kill() {
    incoming.kill();
    cleanup();
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    return this.sv4;
  }



  @Override
  public void cleanup() {
    super.cleanup();
    incoming.cleanup();
    if (batchGroups != null) {
      for (BatchGroup group: batchGroups) {
        try {
          group.cleanup();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
    if (builder != null) {
      builder.clear();
    }
    sv4.clear();
  }

  @Override
  public IterOutcome next() {
    if(schema != null){
      if (spillCount == 0) {
        if(schema != null){
          if(getSelectionVector4().next()){
            return IterOutcome.OK;
          }else{
            return IterOutcome.NONE;
          }
        }
      } else {
        Stopwatch w = new Stopwatch();
        w.start();
        int count = selector.next();
        if(count > 0){
          long t = w.elapsed(TimeUnit.MICROSECONDS);
          logger.debug("Took {} us to merge {} records", t, count);
          container.setRecordCount(count);
          return IterOutcome.OK;
        }else{
          cleanup();
          return IterOutcome.NONE;
        }
      }
    }

    try{
      outer: while (true) {
        Stopwatch watch = new Stopwatch();
        watch.start();
        IterOutcome upstream = incoming.next();
        logger.debug("Took {} us to get next", watch.elapsed(TimeUnit.MICROSECONDS));
        switch (upstream) {
        case NONE:
          break outer;
        case NOT_YET:
          throw new UnsupportedOperationException();
        case STOP:
          cleanup();
          return upstream;
        case OK_NEW_SCHEMA:
          // only change in the case that the schema truly changes.  Artificial schema changes are ignored.
          if(!incoming.getSchema().equals(schema)){
            if (schema != null) throw new UnsupportedOperationException("Sort doesn't currently support sorts with changing schemas.");
            this.schema = incoming.getSchema();
            this.sorter = createNewSorter(context, incoming);
          }
          // fall through.
        case OK:
          SelectionVector2 sv2;
          if (incoming.getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.TWO_BYTE) {
            sv2 = incoming.getSelectionVector2();
          } else {
            sv2 = newSV2();
          }
          int count = sv2.getCount();
          assert sv2.getCount() > 0;
          sorter.setup(context, sv2, incoming);
          Stopwatch w = new Stopwatch();
          w.start();
          sorter.sort(sv2);
          logger.debug("Took {} us to sort {} records", w.elapsed(TimeUnit.MICROSECONDS), sv2.getCount());
          batchGroups.add(new BatchGroup(new RecordBatchData(incoming).getContainer(), sv2));
          batchesSinceLastSpill++;
          if (batchGroups.size() > SPILL_THRESHOLD && batchesSinceLastSpill >= SPILL_BATCH_GROUP_SIZE) {
            mergeAndSpill();
            batchesSinceLastSpill = 0;
          }
          long t = w.elapsed(TimeUnit.MICROSECONDS);
          logger.debug("Took {} us to sort {} records", t, count);
          break;
        default:
          throw new UnsupportedOperationException();
        }
      }

      if (schema == null){
        // builder may be null at this point if the first incoming batch is empty
        return IterOutcome.NONE;
      }

      if (spillCount == 0) {
        Stopwatch watch = new Stopwatch();
        watch.start();
        if (schema == null){
          // builder may be null at this point if the first incoming batch is empty
          return IterOutcome.NONE;
        }

        builder = new SortRecordBatchBuilder(context.getAllocator(), MAX_SORT_BYTES);

        for (BatchGroup group : batchGroups) {
          RecordBatchData rbd = new RecordBatchData(group.getFirstContainer());
          rbd.setSv2(group.getSv2());
          builder.add(rbd);
        }

        builder.build(context, container);
        sv4 = builder.getSv4();
        mSorter = createNewMSorter();
        mSorter.setup(context, getSelectionVector4(), this.container);
        mSorter.sort(this.container);

        sv4 = mSorter.getSV4();

        long t = watch.elapsed(TimeUnit.MICROSECONDS);
        logger.debug("Took {} us to sort {} records", t, sv4.getTotalCount());
      } else {
        constructHyperBatch(batchGroups, this.container);
        constructSV4();
        selector = createSelector();
        selector.setup(context, this, sv4, batchGroups);
        selector.next();
      }

      container.buildSchema(BatchSchema.SelectionVectorMode.FOUR_BYTE);
      return IterOutcome.OK_NEW_SCHEMA;

    }catch(SchemaChangeException | ClassTransformationException | IOException ex){
      kill();
      logger.error("Failure during query", ex);
      context.fail(ex);
      return IterOutcome.STOP;
    } catch (UnsupportedOperationException e) {
      logger.error(e.getMessage());
      throw new RuntimeException(e);
    }
  }

  public void mergeAndSpill() throws SchemaChangeException {
    VectorContainer hyperBatch = new VectorContainer();
    VectorContainer outputContainer = new VectorContainer();
    List<BatchGroup> batchGroupList = Lists.newArrayList();
    for (int i = 0; i < SPILL_BATCH_GROUP_SIZE; i++) {
      batchGroupList.add(batchGroups.pollLast());
    }
    constructHyperBatch(batchGroupList, hyperBatch);
    createCopier(hyperBatch, batchGroupList, outputContainer);

    int count = copier.next();
    assert count > 0;

    VectorContainer c1 = VectorContainer.getTransferClone(outputContainer);
    c1.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    c1.setRecordCount(count);

    count = copier.next();
    assert count > 0;


    VectorContainer c2 = VectorContainer.getTransferClone(outputContainer);
    c2.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    c2.setRecordCount(count);

    String outputFile = String.format(Utilities.getFileNameForQueryFragment(context, dirs.next(), "spill" + uid + "_" + spillCount++));
    BatchGroup newGroup = new BatchGroup(c1, c2, fs, outputFile, context.getAllocator());

    try {
      while ((count = copier.next()) > 0) {
        outputContainer.buildSchema(BatchSchema.SelectionVectorMode.NONE);
        outputContainer.setRecordCount(count);
        newGroup.addBatch(outputContainer);
      }
      newGroup.closeOutputStream();
      batchGroups.add(newGroup);
      for (BatchGroup group : batchGroupList) {
          group.cleanup();
      }
      hyperBatch.clear();
    } catch (IOException e) {
        throw new RuntimeException(e);
      }
  }

  private SelectionVector2 newSV2() {
    SelectionVector2 sv2 = new SelectionVector2(context.getAllocator());
    sv2.allocateNew(incoming.getRecordCount());
    for (int i = 0; i < incoming.getRecordCount(); i++) {
      sv2.setIndex(i, (char) i);
    }
    sv2.setRecordCount(incoming.getRecordCount());
    return sv2;
  }

  private void constructHyperBatch(List<BatchGroup> batchGroupList, VectorContainer cont) {
    for (MaterializedField field : schema) {
      ValueVector[] vectors = new ValueVector[batchGroupList.size() * 2];
      int i = 0;
      for (BatchGroup group : batchGroupList) {
        vectors[i++] = group.getValueAccessorById(group.getValueVectorId(new SchemaPath(field.getName(),ExpressionPosition.UNKNOWN)).getFieldId(),
                field.getValueClass()).getValueVector();
        if (group.hasSecond()) {
          VectorContainer c = group.getSecondContainer();
          vectors[i++] = c.getValueAccessorById(c.getValueVectorId(new SchemaPath(field.getName(),ExpressionPosition.UNKNOWN)).getFieldId(),
                  field.getValueClass()).getValueVector();
        } else {
          vectors[i] = vectors[i - 1].getTransferPair().getTo(); //this vector should never be used. Just want to avoid having null elements in the hyper vector
          i++;
        }
      }
      cont.add(vectors);
    }
    cont.buildSchema(BatchSchema.SelectionVectorMode.FOUR_BYTE);
  }

  private void constructSV4() throws SchemaChangeException {
    BufferAllocator.PreAllocator preAlloc = context.getAllocator().getNewPreAllocator();
    preAlloc.preAllocate(4 * TARGET_RECORD_COUNT);
    sv4 = new SelectionVector4(preAlloc.getAllocation(), TARGET_RECORD_COUNT, TARGET_RECORD_COUNT);
  }

  private MSorter createNewMSorter() throws ClassTransformationException, IOException, SchemaChangeException {
    return createNewMSorter(this.context, this.popConfig.getOrderings(), this, MAIN_MAPPING, LEFT_MAPPING, RIGHT_MAPPING);
  }

  private MSorter createNewMSorter(FragmentContext context, List<Ordering> orderings, VectorAccessible batch, MappingSet mainMapping, MappingSet leftMapping, MappingSet rightMapping)
          throws ClassTransformationException, IOException, SchemaChangeException{
    CodeGenerator<MSorter> cg = CodeGenerator.get(MSorter.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    ClassGenerator<MSorter> g = cg.getRoot();
    g.setMappingSet(mainMapping);

    for(Ordering od : orderings){
      // first, we rewrite the evaluation stack for each side of the comparison.
      ErrorCollector collector = new ErrorCollectorImpl();
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(od.getExpr(), batch, collector, context.getFunctionRegistry());
      if(collector.hasErrors()) throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
      g.setMappingSet(leftMapping);
      HoldingContainer left = g.addExpr(expr, false);
      g.setMappingSet(rightMapping);
      HoldingContainer right = g.addExpr(expr, false);
      g.setMappingSet(mainMapping);

      // next we wrap the two comparison sides and add the expression block for the comparison.
      LogicalExpression fh = FunctionGenerationHelper.getComparator(left, right, context.getFunctionRegistry());
      HoldingContainer out = g.addExpr(fh, false);
      JConditional jc = g.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));

      if(od.getDirection() == Direction.ASCENDING){
        jc._then()._return(out.getValue());
      }else{
        jc._then()._return(out.getValue().minus());
      }
    }

    g.getEvalBlock()._return(JExpr.lit(0));

    return context.getImplementationClass(cg);


  }

  public SingleBatchSorter createNewSorter(FragmentContext context, VectorAccessible batch)
          throws ClassTransformationException, IOException, SchemaChangeException{
    CodeGenerator<SingleBatchSorter> cg = CodeGenerator.get(SingleBatchSorter.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    ClassGenerator<SingleBatchSorter> g = cg.getRoot();

    generateComparisons(g, batch);

    return context.getImplementationClass(cg);
  }

  private void generateComparisons(ClassGenerator g, VectorAccessible batch) throws SchemaChangeException {
    g.setMappingSet(MAIN_MAPPING);

    for(Ordering od : popConfig.getOrderings()){
      // first, we rewrite the evaluation stack for each side of the comparison.
      ErrorCollector collector = new ErrorCollectorImpl();
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(od.getExpr(), batch, collector,context.getFunctionRegistry());
      if(collector.hasErrors()) throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
      g.setMappingSet(LEFT_MAPPING);
      HoldingContainer left = g.addExpr(expr, false);
      g.setMappingSet(RIGHT_MAPPING);
      HoldingContainer right = g.addExpr(expr, false);
      g.setMappingSet(MAIN_MAPPING);

      // next we wrap the two comparison sides and add the expression block for the comparison.
      LogicalExpression fh = FunctionGenerationHelper.getComparator(left, right, context.getFunctionRegistry());
      HoldingContainer out = g.addExpr(fh, false);
      JConditional jc = g.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));

      if(od.getDirection() == Direction.ASCENDING){
        jc._then()._return(out.getValue());
      }else{
        jc._then()._return(out.getValue().minus());
      }
    }

    g.getEvalBlock()._return(JExpr.lit(0));
  }

  private PriorityQueueSelector createSelector() throws SchemaChangeException {
    CodeGenerator<PriorityQueueSelector> cg = CodeGenerator.get(PriorityQueueSelector.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    ClassGenerator<PriorityQueueSelector> g = cg.getRoot();

    generateComparisons(g, this);

    try {
      PriorityQueueSelector c = context.getImplementationClass(cg);
      return c;
    } catch (ClassTransformationException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void createCopier(VectorAccessible batch, List<BatchGroup> batchGroupList, VectorContainer outputContainer) throws SchemaChangeException {
    try {
      if (copier == null) {
        CodeGenerator<PriorityQueueCopier> cg = CodeGenerator.get(PriorityQueueCopier.TEMPLATE_DEFINITION, context.getFunctionRegistry());
        ClassGenerator<PriorityQueueCopier> g = cg.getRoot();

        generateComparisons(g, batch);

        g.setMappingSet(COPIER_MAPPING_SET);
        RemovingRecordBatch.generateCopies(g, batch, true);
        g.setMappingSet(MAIN_MAPPING);
        copier = context.getImplementationClass(cg);
      }

      List<VectorAllocator> allocators = Lists.newArrayList();
      for(VectorWrapper<?> i : batch){
        ValueVector v = TypeHelper.getNewVector(i.getField(), context.getAllocator());
        outputContainer.add(v);
        allocators.add(VectorAllocator.getAllocator(v, 110));
      }
      copier.setup(context, batch, batchGroupList, outputContainer, allocators);
    } catch (ClassTransformationException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  @Override
  public WritableBatch getWritableBatch() {
    throw new UnsupportedOperationException("A sort batch is not writable.");
  }

  @Override
  protected void killIncoming() {
    incoming.kill();
  }

}
