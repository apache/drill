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

import io.netty.buffer.DrillBuf;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
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
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.memory.OutOfMemoryRuntimeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.ExternalSort;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.physical.impl.sort.SortRecordBatchBuilder;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.testing.ControlsInjector;
import org.apache.drill.exec.testing.ControlsInjectorFactory;
import org.apache.drill.exec.vector.CopyUtil;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractContainerVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.calcite.rel.RelFieldCollation.Direction;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;

public class ExternalSortBatch extends AbstractRecordBatch<ExternalSort> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExternalSortBatch.class);
  private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(ExternalSortBatch.class);

  private static final long MAX_SORT_BYTES = 1L * 1024 * 1024 * 1024;
  private static final GeneratorMapping COPIER_MAPPING = new GeneratorMapping("doSetup", "doCopy", null, null);
  private static final MappingSet MAIN_MAPPING = new MappingSet( (String) null, null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  private static final MappingSet LEFT_MAPPING = new MappingSet("leftIndex", null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  private static final MappingSet RIGHT_MAPPING = new MappingSet("rightIndex", null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  private static final MappingSet COPIER_MAPPING_SET = new MappingSet(COPIER_MAPPING, COPIER_MAPPING);

  private final int SPILL_BATCH_GROUP_SIZE;
  private final int SPILL_THRESHOLD;
  private final List<String> SPILL_DIRECTORIES;
  private final Iterator<String> dirs;
  private final RecordBatch incoming;
  private final BufferAllocator copierAllocator;

  private BatchSchema schema;
  private SingleBatchSorter sorter;
  private SortRecordBatchBuilder builder;
  private MSorter mSorter;
  private PriorityQueueCopier copier;
  private LinkedList<BatchGroup> batchGroups = Lists.newLinkedList();
  private LinkedList<BatchGroup> spilledBatchGroups = Lists.newLinkedList();
  private SelectionVector4 sv4;
  private FileSystem fs;
  private int spillCount = 0;
  private int batchesSinceLastSpill = 0;
  private boolean first = true;
  private long totalSizeInMemory = 0;
  private long highWaterMark = Long.MAX_VALUE;
  private int targetRecordCount;
  private final String fileName;
  private int firstSpillBatchCount = 0;

  public static final String INTERRUPTION_AFTER_SORT = "after-sort";
  public static final String INTERRUPTION_AFTER_SETUP = "after-setup";


  public ExternalSortBatch(ExternalSort popConfig, FragmentContext context, RecordBatch incoming) throws OutOfMemoryException {
    super(popConfig, context, true);
    this.incoming = incoming;
    DrillConfig config = context.getConfig();
    Configuration conf = new Configuration();
    conf.set("fs.default.name", config.getString(ExecConstants.EXTERNAL_SORT_SPILL_FILESYSTEM));
    try {
      this.fs = FileSystem.get(conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    SPILL_BATCH_GROUP_SIZE = config.getInt(ExecConstants.EXTERNAL_SORT_SPILL_GROUP_SIZE);
    SPILL_THRESHOLD = config.getInt(ExecConstants.EXTERNAL_SORT_SPILL_THRESHOLD);
    SPILL_DIRECTORIES = config.getStringList(ExecConstants.EXTERNAL_SORT_SPILL_DIRS);
    dirs = Iterators.cycle(Lists.newArrayList(SPILL_DIRECTORIES));
    copierAllocator = oContext.getAllocator().getChildAllocator(
        context, PriorityQueueCopier.initialAllocation, PriorityQueueCopier.maxAllocation, true);
    FragmentHandle handle = context.getHandle();
    fileName = String.format("%s/major_fragment_%s/minor_fragment_%s/operator_%s", QueryIdHelper.getQueryId(handle.getQueryId()),
        handle.getMajorFragmentId(), handle.getMinorFragmentId(), popConfig.getOperatorId());
  }

  @Override
  public int getRecordCount() {
    if (sv4 != null) {
      return sv4.getCount();
    }
    return container.getRecordCount();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    return sv4;
  }

  @Override
  public void close() {
    try {
      if (batchGroups != null) {
        for (BatchGroup group: batchGroups) {
          try {
            group.cleanup();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    } finally {
      if (builder != null) {
        builder.clear();
        builder.close();
      }
      if (sv4 != null) {
        sv4.clear();
      }
      if (copier != null) {
        copier.cleanup();
      }
      copierAllocator.close();
      super.close();

      if(mSorter != null) {
        mSorter.clear();
      }
    }
  }

  @Override
  public void buildSchema() throws SchemaChangeException {
    IterOutcome outcome = next(incoming);
    switch (outcome) {
      case OK:
      case OK_NEW_SCHEMA:
        for (VectorWrapper w : incoming) {
          ValueVector v = container.addOrGet(w.getField());
          if (v instanceof AbstractContainerVector) {
            w.getValueVector().makeTransferPair(v); // Can we remove this hack?
            v.clear();
          }
          v.allocateNew(); // Can we remove this? - SVR fails with NPE (TODO)
        }
        container.buildSchema(SelectionVectorMode.NONE);
        container.setRecordCount(0);
        break;
      case STOP:
        state = BatchState.STOP;
        break;
      case OUT_OF_MEMORY:
        state = BatchState.OUT_OF_MEMORY;
        break;
      case NONE:
        state = BatchState.DONE;
        break;
    }
  }

  @Override
  public IterOutcome innerNext() {
    if (schema != null) {
      if (spillCount == 0) {
        return (getSelectionVector4().next()) ? IterOutcome.OK : IterOutcome.NONE;
      } else {
        Stopwatch w = new Stopwatch();
        w.start();
        int count = copier.next(targetRecordCount);
        if (count > 0) {
          long t = w.elapsed(TimeUnit.MICROSECONDS);
          logger.debug("Took {} us to merge {} records", t, count);
          container.setRecordCount(count);
          return IterOutcome.OK;
        } else {
          logger.debug("copier returned 0 records");
          return IterOutcome.NONE;
        }
      }
    }

    int totalCount = 0;

    try{
      container.clear();
      outer: while (true) {
        Stopwatch watch = new Stopwatch();
        watch.start();
        IterOutcome upstream;
        if (first) {
          upstream = IterOutcome.OK_NEW_SCHEMA;
        } else {
          upstream = next(incoming);
        }
        if (upstream == IterOutcome.OK && sorter == null) {
          upstream = IterOutcome.OK_NEW_SCHEMA;
        }
//        logger.debug("Took {} us to get next", watch.elapsed(TimeUnit.MICROSECONDS));
        switch (upstream) {
        case NONE:
          if (first) {
            return upstream;
          }
          break outer;
        case NOT_YET:
          throw new UnsupportedOperationException();
        case STOP:
          return upstream;
        case OK_NEW_SCHEMA:
          // only change in the case that the schema truly changes.  Artificial schema changes are ignored.
          if (!incoming.getSchema().equals(schema)) {
            if (schema != null) {
              throw new SchemaChangeException();
            }
            this.schema = incoming.getSchema();
            this.sorter = createNewSorter(context, incoming);
          }
          // fall through.
        case OK:
          if (first) {
            first = false;
          }
          if (incoming.getRecordCount() == 0) {
            for (VectorWrapper w : incoming) {
              w.clear();
            }
            break;
          }
          totalSizeInMemory += getBufferSize(incoming);
          SelectionVector2 sv2;
          if (incoming.getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.TWO_BYTE) {
            sv2 = incoming.getSelectionVector2();
            if (sv2.getBuffer(false).isRootBuffer()) {
              oContext.getAllocator().takeOwnership(sv2.getBuffer(false));
            }
          } else {
            try {
              sv2 = newSV2();
            } catch(InterruptedException e) {
              return IterOutcome.STOP;
            } catch (OutOfMemoryException e) {
              throw new OutOfMemoryRuntimeException(e);
            }
          }
          int count = sv2.getCount();
          totalCount += count;
          sorter.setup(context, sv2, incoming);
          Stopwatch w = new Stopwatch();
          w.start();
          sorter.sort(sv2);
//          logger.debug("Took {} us to sort {} records", w.elapsed(TimeUnit.MICROSECONDS), count);
          RecordBatchData rbd = new RecordBatchData(incoming);
          boolean success = false;
          try {
            if (incoming.getSchema().getSelectionVectorMode() == SelectionVectorMode.NONE) {
              rbd.setSv2(sv2);
            }
            batchGroups.add(new BatchGroup(rbd.getContainer(), rbd.getSv2()));
            batchesSinceLastSpill++;
            if (// We have spilled at least once and the current memory used is more than the 75% of peak memory used.
                (spillCount > 0 && totalSizeInMemory > .75 * highWaterMark) ||
                // If we haven't spilled so far, do we have enough memory for MSorter if this turns out to be the last incoming batch?
                (spillCount == 0 && !hasMemoryForInMemorySort(totalCount)) ||
                // current memory used is more than 95% of memory usage limit of this operator
                (totalSizeInMemory > .95 * popConfig.getMaxAllocation()) ||
                // current memory used is more than 95% of memory usage limit of this fragment
                (totalSizeInMemory > .95 * oContext.getAllocator().getFragmentLimit()) ||
                // Number of incoming batches (BatchGroups) exceed the limit and number of incoming batches accumulated
                // since the last spill exceed the defined limit
                (batchGroups.size() > SPILL_THRESHOLD && batchesSinceLastSpill >= SPILL_BATCH_GROUP_SIZE)) {

              if (firstSpillBatchCount == 0) {
                firstSpillBatchCount = batchGroups.size();
              }

              if (spilledBatchGroups.size() > firstSpillBatchCount / 2) {
                logger.info("Merging spills");
                spilledBatchGroups.addFirst(mergeAndSpill(spilledBatchGroups));
              }
              spilledBatchGroups.add(mergeAndSpill(batchGroups));
              batchesSinceLastSpill = 0;
            }
            long t = w.elapsed(TimeUnit.MICROSECONDS);
//          logger.debug("Took {} us to sort {} records", t, count);
            success = true;
          } finally {
            if (!success) {
              rbd.clear();
            }
          }
          break;
        case OUT_OF_MEMORY:
          logger.debug("received OUT_OF_MEMORY, trying to spill");
          highWaterMark = totalSizeInMemory;
          if (batchesSinceLastSpill > 2) {
            spilledBatchGroups.add(mergeAndSpill(batchGroups));
            batchesSinceLastSpill = 0;
          } else {
            logger.debug("not enough batches to spill, sending OUT_OF_MEMORY downstream");
            return IterOutcome.OUT_OF_MEMORY;
          }
          break;
        default:
          throw new UnsupportedOperationException();
        }
      }

      if (totalCount == 0) {
        return IterOutcome.NONE;
      }
      if (spillCount == 0) {
        Stopwatch watch = new Stopwatch();
        watch.start();

        if (builder != null) {
          builder.clear();
          builder.close();
        }
        builder = new SortRecordBatchBuilder(oContext.getAllocator(), MAX_SORT_BYTES);

        for (BatchGroup group : batchGroups) {
          RecordBatchData rbd = new RecordBatchData(group.getContainer());
          rbd.setSv2(group.getSv2());
          builder.add(rbd);
        }

        builder.build(context, container);
        sv4 = builder.getSv4();
        mSorter = createNewMSorter();
        mSorter.setup(context, oContext.getAllocator(), getSelectionVector4(), this.container);

        // For testing memory-leak purpose, inject exception after mSorter finishes setup
        injector.injectUnchecked(context.getExecutionControls(), INTERRUPTION_AFTER_SETUP);
        mSorter.sort(this.container);

        // sort may have prematurely exited due to should continue returning false.
        if (!context.shouldContinue()) {
          return IterOutcome.STOP;
        }

        // For testing memory-leak purpose, inject exception after mSorter finishes sorting
        injector.injectUnchecked(context.getExecutionControls(), INTERRUPTION_AFTER_SORT);
        sv4 = mSorter.getSV4();

        long t = watch.elapsed(TimeUnit.MICROSECONDS);
//        logger.debug("Took {} us to sort {} records", t, sv4.getTotalCount());
        container.buildSchema(SelectionVectorMode.FOUR_BYTE);
      } else {
        spilledBatchGroups.add(mergeAndSpill(batchGroups));
        batchGroups.addAll(spilledBatchGroups);
        logger.warn("Starting to merge. {} batch groups. Current allocated memory: {}", batchGroups.size(), oContext.getAllocator().getAllocatedMemory());
        VectorContainer hyperBatch = constructHyperBatch(batchGroups);
        createCopier(hyperBatch, batchGroups, container);

        int estimatedRecordSize = 0;
        for (VectorWrapper w : batchGroups.get(0)) {
          try {
            estimatedRecordSize += TypeHelper.getSize(w.getField().getType());
          } catch (UnsupportedOperationException e) {
            estimatedRecordSize += 50;
          }
        }
        targetRecordCount = Math.min(MAX_BATCH_SIZE, Math.max(1, 250 * 1000 / estimatedRecordSize));
        int count = copier.next(targetRecordCount);
        container.buildSchema(SelectionVectorMode.NONE);
        container.setRecordCount(count);
      }

      return IterOutcome.OK_NEW_SCHEMA;

    } catch (SchemaChangeException ex) {
      kill(false);
      context.fail(UserException.unsupportedError(ex)
        .message("Sort doesn't currently support sorts with changing schemas").build(logger));
      return IterOutcome.STOP;
    } catch(ClassTransformationException | IOException ex) {
      kill(false);
      context.fail(ex);
      return IterOutcome.STOP;
    } catch (UnsupportedOperationException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean hasMemoryForInMemorySort(int currentRecordCount) {
    long currentlyAvailable =  popConfig.getMaxAllocation() - oContext.getAllocator().getAllocatedMemory();

    long neededForInMemorySort = SortRecordBatchBuilder.memoryNeeded(currentRecordCount) +
        MSortTemplate.memoryNeeded(currentRecordCount);

    return currentlyAvailable > neededForInMemorySort;
  }

  public BatchGroup mergeAndSpill(LinkedList<BatchGroup> batchGroups) throws SchemaChangeException {
    logger.debug("Copier allocator current allocation {}", copierAllocator.getAllocatedMemory());
    VectorContainer outputContainer = new VectorContainer();
    List<BatchGroup> batchGroupList = Lists.newArrayList();
    int batchCount = batchGroups.size();
    for (int i = 0; i < batchCount / 2; i++) {
      if (batchGroups.size() == 0) {
        break;
      }
      BatchGroup batch = batchGroups.pollLast();
      batchGroupList.add(batch);
      long bufferSize = getBufferSize(batch);
      totalSizeInMemory -= bufferSize;
    }
    if (batchGroupList.size() == 0) {
      return null;
    }
    int estimatedRecordSize = 0;
    for (VectorWrapper w : batchGroupList.get(0)) {
      try {
        estimatedRecordSize += TypeHelper.getSize(w.getField().getType());
      } catch (UnsupportedOperationException e) {
        estimatedRecordSize += 50;
      }
    }
    int targetRecordCount = Math.max(1, 250 * 1000 / estimatedRecordSize);
    VectorContainer hyperBatch = constructHyperBatch(batchGroupList);
    createCopier(hyperBatch, batchGroupList, outputContainer);

    int count = copier.next(targetRecordCount);
    assert count > 0;

    VectorContainer c1 = VectorContainer.getTransferClone(outputContainer);
    c1.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    c1.setRecordCount(count);

    String outputFile = Joiner.on("/").join(dirs.next(), fileName, spillCount++);
    BatchGroup newGroup = new BatchGroup(c1, fs, outputFile, oContext.getAllocator());

    logger.info("Merging and spilling to {}", outputFile);
    try {
      while ((count = copier.next(targetRecordCount)) > 0) {
        outputContainer.buildSchema(BatchSchema.SelectionVectorMode.NONE);
        outputContainer.setRecordCount(count);
        newGroup.addBatch(outputContainer);
      }
      newGroup.closeOutputStream();
      for (BatchGroup group : batchGroupList) {
        group.cleanup();
      }
      hyperBatch.clear();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    takeOwnership(c1);
    totalSizeInMemory += getBufferSize(c1);
    logger.info("Completed spilling to {}", outputFile);
    return newGroup;
  }

  private void takeOwnership(VectorAccessible batch) {
    for (VectorWrapper w : batch) {
      DrillBuf[] bufs = w.getValueVector().getBuffers(false);
      for (DrillBuf buf : bufs) {
        if (buf.isRootBuffer()) {
          oContext.getAllocator().takeOwnership(buf);
        }
      }
    }
  }

  private long getBufferSize(VectorAccessible batch) {
    long size = 0;
    for (VectorWrapper w : batch) {
      DrillBuf[] bufs = w.getValueVector().getBuffers(false);
      for (DrillBuf buf : bufs) {
        if (buf.isRootBuffer()) {
          size += buf.capacity();
        }
      }
    }
    return size;
  }

  private SelectionVector2 newSV2() throws OutOfMemoryException, InterruptedException {
    SelectionVector2 sv2 = new SelectionVector2(oContext.getAllocator());
    if (!sv2.allocateNew(incoming.getRecordCount())) {
      try {
        spilledBatchGroups.addFirst(mergeAndSpill(batchGroups));
      } catch (SchemaChangeException e) {
        throw new RuntimeException();
      }
      batchesSinceLastSpill = 0;
      int waitTime = 1;
      while (true) {
        try {
          Thread.sleep(waitTime * 1000);
        } catch(final InterruptedException e) {
          if (!context.shouldContinue()) {
            throw e;
          }
        }
        waitTime *= 2;
        if (sv2.allocateNew(incoming.getRecordCount())) {
          break;
        }
        if (waitTime >= 32) {
          throw new OutOfMemoryException("Unable to allocate sv2 buffer after repeated attempts");
        }
      }
    }
    for (int i = 0; i < incoming.getRecordCount(); i++) {
      sv2.setIndex(i, (char) i);
    }
    sv2.setRecordCount(incoming.getRecordCount());
    return sv2;
  }

  private VectorContainer constructHyperBatch(List<BatchGroup> batchGroupList) {
    VectorContainer cont = new VectorContainer();
    for (MaterializedField field : schema) {
      ValueVector[] vectors = new ValueVector[batchGroupList.size()];
      int i = 0;
      for (BatchGroup group : batchGroupList) {
        vectors[i++] = group.getValueAccessorById(
            field.getValueClass(),
            group.getValueVectorId(field.getPath()).getFieldIds())
            .getValueVector();
      }
      cont.add(vectors);
    }
    cont.buildSchema(BatchSchema.SelectionVectorMode.FOUR_BYTE);
    return cont;
  }

  private MSorter createNewMSorter() throws ClassTransformationException, IOException, SchemaChangeException {
    return createNewMSorter(this.context, this.popConfig.getOrderings(), this, MAIN_MAPPING, LEFT_MAPPING, RIGHT_MAPPING);
  }

  private MSorter createNewMSorter(FragmentContext context, List<Ordering> orderings, VectorAccessible batch, MappingSet mainMapping, MappingSet leftMapping, MappingSet rightMapping)
          throws ClassTransformationException, IOException, SchemaChangeException{
    CodeGenerator<MSorter> cg = CodeGenerator.get(MSorter.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    ClassGenerator<MSorter> g = cg.getRoot();
    g.setMappingSet(mainMapping);

    for (Ordering od : orderings) {
      // first, we rewrite the evaluation stack for each side of the comparison.
      ErrorCollector collector = new ErrorCollectorImpl();
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(od.getExpr(), batch, collector, context.getFunctionRegistry());
      if (collector.hasErrors()) {
        throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
      }
      g.setMappingSet(leftMapping);
      HoldingContainer left = g.addExpr(expr, false);
      g.setMappingSet(rightMapping);
      HoldingContainer right = g.addExpr(expr, false);
      g.setMappingSet(mainMapping);

      // next we wrap the two comparison sides and add the expression block for the comparison.
      LogicalExpression fh =
          FunctionGenerationHelper.getOrderingComparator(od.nullsSortHigh(), left, right,
                                                         context.getFunctionRegistry());
      HoldingContainer out = g.addExpr(fh, false);
      JConditional jc = g.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));

      if (od.getDirection() == Direction.ASCENDING) {
        jc._then()._return(out.getValue());
      }else{
        jc._then()._return(out.getValue().minus());
      }
      g.rotateBlock();
    }

    g.rotateBlock();
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

    for (Ordering od : popConfig.getOrderings()) {
      // first, we rewrite the evaluation stack for each side of the comparison.
      ErrorCollector collector = new ErrorCollectorImpl();
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(od.getExpr(), batch, collector,context.getFunctionRegistry());
      if (collector.hasErrors()) {
        throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
      }
      g.setMappingSet(LEFT_MAPPING);
      HoldingContainer left = g.addExpr(expr, false);
      g.setMappingSet(RIGHT_MAPPING);
      HoldingContainer right = g.addExpr(expr, false);
      g.setMappingSet(MAIN_MAPPING);

      // next we wrap the two comparison sides and add the expression block for the comparison.
      LogicalExpression fh =
          FunctionGenerationHelper.getOrderingComparator(od.nullsSortHigh(), left, right,
                                                         context.getFunctionRegistry());
      HoldingContainer out = g.addExpr(fh, false);
      JConditional jc = g.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));

      if (od.getDirection() == Direction.ASCENDING) {
        jc._then()._return(out.getValue());
      }else{
        jc._then()._return(out.getValue().minus());
      }
      g.rotateBlock();
    }

    g.rotateBlock();
    g.getEvalBlock()._return(JExpr.lit(0));
  }

  private void createCopier(VectorAccessible batch, List<BatchGroup> batchGroupList, VectorContainer outputContainer) throws SchemaChangeException {
    try {
      if (copier == null) {
        CodeGenerator<PriorityQueueCopier> cg = CodeGenerator.get(PriorityQueueCopier.TEMPLATE_DEFINITION, context.getFunctionRegistry());
        ClassGenerator<PriorityQueueCopier> g = cg.getRoot();

        generateComparisons(g, batch);

        g.setMappingSet(COPIER_MAPPING_SET);
        CopyUtil.generateCopies(g, batch, true);
        g.setMappingSet(MAIN_MAPPING);
        copier = context.getImplementationClass(cg);
      } else {
        copier.cleanup();
      }

      for (VectorWrapper<?> i : batch) {
        ValueVector v = TypeHelper.getNewVector(i.getField(), copierAllocator);
        outputContainer.add(v);
      }
      copier.setup(context, copierAllocator, batch, batchGroupList, outputContainer);
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
  protected void killIncoming(boolean sendUpstream) {
    incoming.kill(sendUpstream);
  }

  private String getFileName(int spill) {
     /*
     * From the context, get the query id, major fragment id, minor fragment id. This will be used as the file name to
     * which we will dump the incoming buffer data
     */
    FragmentHandle handle = context.getHandle();

    String qid = QueryIdHelper.getQueryId(handle.getQueryId());

    int majorFragmentId = handle.getMajorFragmentId();
    int minorFragmentId = handle.getMinorFragmentId();

    String fileName = String.format("%s//%s//major_fragment_%s//minor_fragment_%s//operator_%s//%s", dirs.next(), qid, majorFragmentId, minorFragmentId, popConfig.getOperatorId(), spill);

    return fileName;
  }

}
