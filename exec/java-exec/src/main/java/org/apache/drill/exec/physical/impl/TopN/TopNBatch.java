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
package org.apache.drill.exec.physical.impl.TopN;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.drill.common.DrillAutoCloseables;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.CodeCompiler;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.drill.exec.expr.fn.FunctionLookupContext;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.TopN;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.physical.impl.sort.SortRecordBatchBuilder;
import org.apache.drill.exec.physical.impl.svremover.Copier;
import org.apache.drill.exec.physical.impl.svremover.GenericSV4Copier;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.ExpandableHyperContainer;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.SchemaUtil;
import org.apache.drill.exec.record.SimpleRecordBatch;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractContainerVector;

import com.google.common.base.Stopwatch;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;

/**
 * Operator Batch which implements the TopN functionality. It is more efficient than (sort + limit) since unlike sort
 * it doesn't have to store all the input data to sort it first and then apply limit on the sorted data. Instead
 * internally it maintains a priority queue backed by a heap with the size being same as limit value.
 */
public class TopNBatch extends AbstractRecordBatch<TopN> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TopNBatch.class);

  private final MappingSet mainMapping = createMainMappingSet();
  private final MappingSet leftMapping = createLeftMappingSet();
  private final MappingSet rightMapping = createRightMappingSet();

  private final int batchPurgeThreshold;
  private final boolean codegenDump;

  private final RecordBatch incoming;
  private BatchSchema schema;
  private boolean schemaChanged = false;
  private PriorityQueue priorityQueue;
  private TopN config;
  SelectionVector4 sv4;
  private long countSincePurge;
  private int batchCount;
  private Copier copier;
  private boolean first = true;
  private int recordCount = 0;

  public TopNBatch(TopN popConfig, FragmentContext context, RecordBatch incoming) throws OutOfMemoryException {
    super(popConfig, context);
    this.incoming = incoming;
    this.config = popConfig;
    DrillConfig drillConfig = context.getConfig();
    batchPurgeThreshold = drillConfig.getInt(ExecConstants.BATCH_PURGE_THRESHOLD);
    codegenDump = drillConfig.getBoolean(CodeCompiler.ENABLE_SAVE_CODE_FOR_DEBUG_TOPN);
  }

  @Override
  public int getRecordCount() {
    return recordCount;
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    return sv4;
  }

  @Override
  public void close() {
    if (sv4 != null) {
      sv4.clear();
    }
    if (priorityQueue != null) {
      priorityQueue.cleanup();
    }
    super.close();
  }

  @Override
  public void buildSchema() throws SchemaChangeException {
    VectorContainer c = new VectorContainer(oContext);
    IterOutcome outcome = next(incoming);
    switch (outcome) {
      case OK:
      case OK_NEW_SCHEMA:
        for (VectorWrapper<?> w : incoming) {
          // TODO: Not sure why the special handling for AbstractContainerVector is needed since creation of child
          // vectors is taken care correctly if the field is retrieved from incoming vector and passed to it rather than
          // creating a new Field instance just based on name and type.
          @SuppressWarnings("resource")
          ValueVector v = c.addOrGet(w.getField());
          if (v instanceof AbstractContainerVector) {
            w.getValueVector().makeTransferPair(v);
            v.clear();
          }
        }
        for (VectorWrapper<?> w : c) {
          @SuppressWarnings("resource")
          ValueVector v = container.addOrGet(w.getField());
          if (v instanceof AbstractContainerVector) {
            w.getValueVector().makeTransferPair(v);
            v.clear();
          }
          v.allocateNew();
        }
        container.buildSchema(SelectionVectorMode.NONE);
        container.setRecordCount(0);

        return;
      case STOP:
        state = BatchState.STOP;
        return;
      case OUT_OF_MEMORY:
        state = BatchState.OUT_OF_MEMORY;
        return;
      case NONE:
        state = BatchState.DONE;
      default:
        return;
    }
  }

  @Override
  public IterOutcome innerNext() {
    recordCount = 0;
    if (state == BatchState.DONE) {
      return IterOutcome.NONE;
    }
    if (schema != null) {
      if (getSelectionVector4().next()) {
        recordCount = sv4.getCount();
        return IterOutcome.OK;
      } else {
        recordCount = 0;
        return IterOutcome.NONE;
      }
    }

    try{
      outer: while (true) {
        Stopwatch watch = Stopwatch.createStarted();
        IterOutcome upstream;
        if (first) {
          upstream = IterOutcome.OK_NEW_SCHEMA;
          first = false;
        } else {
          upstream = next(incoming);
        }
        if (upstream == IterOutcome.OK && schema == null) {
          upstream = IterOutcome.OK_NEW_SCHEMA;
          container.clear();
        }
        logger.debug("Took {} us to get next", watch.elapsed(TimeUnit.MICROSECONDS));
        switch (upstream) {
        case NONE:
          break outer;
        case NOT_YET:
          throw new UnsupportedOperationException();
        case OUT_OF_MEMORY:
        case STOP:
          return upstream;
        case OK_NEW_SCHEMA:
          // only change in the case that the schema truly changes.  Artificial schema changes are ignored.

          if (!incoming.getSchema().equals(schema)) {
            if (schema != null) {
              if (!unionTypeEnabled) {
                throw new UnsupportedOperationException("Sort doesn't currently support sorts with changing schemas.");
              } else {
                this.schema = SchemaUtil.mergeSchemas(this.schema, incoming.getSchema());
                purgeAndResetPriorityQueue();
                this.schemaChanged = true;
              }
            } else {
              this.schema = incoming.getSchema();
            }
          }
          // fall through.
        case OK:
          if (incoming.getRecordCount() == 0) {
            for (VectorWrapper<?> w : incoming) {
              w.clear();
            }
            break;
          }
          countSincePurge += incoming.getRecordCount();
          batchCount++;
          RecordBatchData batch;
          if (schemaChanged) {
            batch = new RecordBatchData(SchemaUtil.coerceContainer(incoming, this.schema, oContext), oContext.getAllocator());
          } else {
            batch = new RecordBatchData(incoming, oContext.getAllocator());
          }
          boolean success = false;
          try {
            if (priorityQueue == null) {
              assert !schemaChanged;
              priorityQueue = createNewPriorityQueue(new ExpandableHyperContainer(batch.getContainer()), config.getLimit());
            }
            priorityQueue.add(batch);
            // Based on static threshold of number of batches, perform purge operation to release the memory for
            // RecordBatches which are of no use or doesn't fall under TopN category
            if (countSincePurge > config.getLimit() && batchCount > batchPurgeThreshold) {
              purge();

              // FixMe: I think below members should be initialized to limit and 1 respectively since after purge
              // PriorityQueue will contain that many records and only 1 batch.
              countSincePurge = 0;
              batchCount = 0;
            }
            success = true;
          } finally {
            if (!success) {
              batch.clear();
            }
          }
          break;
        default:
          throw new UnsupportedOperationException();
        }
      }

      if (schema == null || priorityQueue == null) {
        // builder may be null at this point if the first incoming batch is empty
        state = BatchState.DONE;
        return IterOutcome.NONE;
      }

      priorityQueue.generate();

      this.sv4 = priorityQueue.getFinalSv4();
      container.clear();
      for (VectorWrapper<?> w : priorityQueue.getHyperBatch()) {
        container.add(w.getValueVectors());
      }
      container.buildSchema(BatchSchema.SelectionVectorMode.FOUR_BYTE);
      recordCount = sv4.getCount();
      return IterOutcome.OK_NEW_SCHEMA;

    } catch(SchemaChangeException | ClassTransformationException | IOException ex) {
      kill(false);
      logger.error("Failure during query", ex);
      context.getExecutorState().fail(ex);
      return IterOutcome.STOP;
    }
  }

  /**
   * When PriorityQueue is built up then it stores the list of limit number of record indexes (in heapSv4) which falls
   * under TopN category. But it also stores all the incoming RecordBatches with all records inside a HyperContainer
   * (hyperBatch). When a certain threshold of batches are reached then this method is called which copies the limit
   * number of records whose indexes are stored in heapSv4 out of HyperBatch to a new VectorContainer and releases
   * all other records and their batches. Later this new VectorContainer is stored inside the HyperBatch and it's
   * corresponding indexes are stored in the heapSv4 vector. This is done to avoid holding up lot's of Record Batches
   * which can create OutOfMemory condition.
   * @throws SchemaChangeException
   */
  private void purge() throws SchemaChangeException {
    Stopwatch watch = Stopwatch.createStarted();
    VectorContainer c = priorityQueue.getHyperBatch();

    // Simple VectorConatiner which stores limit number of records only. The records whose indexes are stored inside
    // selectionVector4 below are only copied from Hyper container to this simple container.
    VectorContainer newContainer = new VectorContainer(oContext);
    @SuppressWarnings("resource")
    // SV4 storing the limit number of indexes
    SelectionVector4 selectionVector4 = priorityQueue.getSv4();
    SimpleSV4RecordBatch batch = new SimpleSV4RecordBatch(c, selectionVector4, context);
    SimpleSV4RecordBatch newBatch = new SimpleSV4RecordBatch(newContainer, null, context);
    if (copier == null) {
      copier = GenericSV4Copier.createCopier(batch, newContainer, null);
    } else {
      for (VectorWrapper<?> i : batch) {

        @SuppressWarnings("resource")
        ValueVector v = TypeHelper.getNewVector(i.getField(), oContext.getAllocator());
        newContainer.add(v);
      }
      copier.setup(batch, newContainer);
    }
    @SuppressWarnings("resource")
    SortRecordBatchBuilder builder = new SortRecordBatchBuilder(oContext.getAllocator());
    try {
      do {
        // count is the limit number of records required by TopN batch
        int count = selectionVector4.getCount();
        // Transfers count number of records from hyperBatch to simple container
        int copiedRecords = copier.copyRecords(0, count);
        assert copiedRecords == count;
        for (VectorWrapper<?> v : newContainer) {
          ValueVector.Mutator m = v.getValueVector().getMutator();
          m.setValueCount(count);
        }
        newContainer.buildSchema(BatchSchema.SelectionVectorMode.NONE);
        newContainer.setRecordCount(count);
        // Store all the batches containing limit number of records
        builder.add(newBatch);
      } while (selectionVector4.next());
      // Release the memory stored for the priority queue heap to store indexes
      selectionVector4.clear();
      // Release the memory from HyperBatch container
      c.clear();
      // New VectorContainer that contains only limit number of records and is later passed to resetQueue to create a
      // HyperContainer backing the priority queue out of it
      VectorContainer newQueue = new VectorContainer();
      builder.build(newQueue);
      priorityQueue.resetQueue(newQueue, builder.getSv4().createNewWrapperCurrent());
      builder.getSv4().clear();
      selectionVector4.clear();
    } finally {
      DrillAutoCloseables.closeNoChecked(builder);
    }
    logger.debug("Took {} us to purge", watch.elapsed(TimeUnit.MICROSECONDS));
  }

  private PriorityQueue createNewPriorityQueue(VectorAccessible batch, int limit)
    throws SchemaChangeException, ClassTransformationException, IOException {
    return createNewPriorityQueue(
      mainMapping, leftMapping, rightMapping, context.getOptions(), context.getFunctionRegistry(), context.getCompiler(),
      config.getOrderings(), batch, unionTypeEnabled, codegenDump, limit, oContext.getAllocator(), schema.getSelectionVectorMode());
  }

  public static MappingSet createMainMappingSet() {
    return new MappingSet((String) null, null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  }

  public static MappingSet createLeftMappingSet() {
    return new MappingSet("leftIndex", null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  }

  public static MappingSet createRightMappingSet() {
    return new MappingSet("rightIndex", null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  }

  public static PriorityQueue createNewPriorityQueue(
    MappingSet mainMapping, MappingSet leftMapping, MappingSet rightMapping,
    OptionSet optionSet, FunctionLookupContext functionLookupContext, CodeCompiler codeCompiler,
    List<Ordering> orderings, VectorAccessible batch, boolean unionTypeEnabled, boolean codegenDump,
    int limit, BufferAllocator allocator, SelectionVectorMode mode)
          throws ClassTransformationException, IOException, SchemaChangeException {
    CodeGenerator<PriorityQueue> cg = CodeGenerator.get(PriorityQueue.TEMPLATE_DEFINITION, optionSet);
    cg.plainJavaCapable(true);
    // Uncomment out this line to debug the generated code.
    cg.saveCodeForDebugging(codegenDump);
    ClassGenerator<PriorityQueue> g = cg.getRoot();
    g.setMappingSet(mainMapping);

    for (Ordering od : orderings) {
      // first, we rewrite the evaluation stack for each side of the comparison.
      ErrorCollector collector = new ErrorCollectorImpl();
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(od.getExpr(), batch, collector, functionLookupContext, unionTypeEnabled);
      if (collector.hasErrors()) {
        throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
      }
      g.setMappingSet(leftMapping);
      HoldingContainer left = g.addExpr(expr, ClassGenerator.BlkCreateMode.FALSE);
      g.setMappingSet(rightMapping);
      HoldingContainer right = g.addExpr(expr, ClassGenerator.BlkCreateMode.FALSE);
      g.setMappingSet(mainMapping);

      // next we wrap the two comparison sides and add the expression block for the comparison.
      LogicalExpression fh =
        FunctionGenerationHelper.getOrderingComparator(od.nullsSortHigh(), left, right, functionLookupContext);
      HoldingContainer out = g.addExpr(fh, ClassGenerator.BlkCreateMode.FALSE);
      JConditional jc = g.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));

      if (od.getDirection() == Direction.ASCENDING) {
        jc._then()._return(out.getValue());
      } else {
        jc._then()._return(out.getValue().minus());
      }
      g.rotateBlock();
    }

    g.rotateBlock();
    g.getEvalBlock()._return(JExpr.lit(0));

    PriorityQueue q = codeCompiler.createInstance(cg);
    q.init(limit, allocator, mode == BatchSchema.SelectionVectorMode.TWO_BYTE);
    return q;
  }

  /**
   * Handle schema changes during execution.
   * 1. Purge existing batches
   * 2. Promote newly created container for new schema.
   * 3. Recreate priority queue and reset with coerced container.
   * @throws SchemaChangeException
   */
  public void purgeAndResetPriorityQueue() throws SchemaChangeException, ClassTransformationException, IOException {
    final Stopwatch watch = Stopwatch.createStarted();
    final VectorContainer c = priorityQueue.getHyperBatch();
    final VectorContainer newContainer = new VectorContainer(oContext);
    @SuppressWarnings("resource")
    final SelectionVector4 selectionVector4 = priorityQueue.getSv4();
    final SimpleSV4RecordBatch batch = new SimpleSV4RecordBatch(c, selectionVector4, context);
    final SimpleSV4RecordBatch newBatch = new SimpleSV4RecordBatch(newContainer, null, context);
    copier = GenericSV4Copier.createCopier(batch, newContainer, null);
    @SuppressWarnings("resource")
    SortRecordBatchBuilder builder = new SortRecordBatchBuilder(oContext.getAllocator());
    try {
      do {
        final int count = selectionVector4.getCount();
        final int copiedRecords = copier.copyRecords(0, count);
        assert copiedRecords == count;
        for (VectorWrapper<?> v : newContainer) {
          ValueVector.Mutator m = v.getValueVector().getMutator();
          m.setValueCount(count);
        }
        newContainer.buildSchema(BatchSchema.SelectionVectorMode.NONE);
        newContainer.setRecordCount(count);
        builder.add(newBatch);
      } while (selectionVector4.next());
      selectionVector4.clear();
      c.clear();
      final VectorContainer oldSchemaContainer = new VectorContainer(oContext);
      builder.build(oldSchemaContainer);
      oldSchemaContainer.setRecordCount(builder.getSv4().getCount());
      final VectorContainer newSchemaContainer =  SchemaUtil.coerceContainer(oldSchemaContainer, this.schema, oContext);
      newSchemaContainer.buildSchema(SelectionVectorMode.FOUR_BYTE);
      priorityQueue.cleanup();
      priorityQueue = createNewPriorityQueue(newSchemaContainer, config.getLimit());
      priorityQueue.resetQueue(newSchemaContainer, builder.getSv4().createNewWrapperCurrent());
    } finally {
      builder.clear();
      builder.close();
    }
    logger.debug("Took {} us to purge and recreate queue for new schema", watch.elapsed(TimeUnit.MICROSECONDS));
  }

  @Override
  public WritableBatch getWritableBatch() {
    throw new UnsupportedOperationException("A sort batch is not writable.");
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    incoming.kill(sendUpstream);
  }

  public static class SimpleSV4RecordBatch extends SimpleRecordBatch {
    private SelectionVector4 sv4;

    public SimpleSV4RecordBatch(VectorContainer container, SelectionVector4 sv4, FragmentContext context) {
      super(container, context);
      this.sv4 = sv4;
    }

    @Override
    public int getRecordCount() {
      if (sv4 != null) {
        return sv4.getCount();
      } else {
        return super.getRecordCount();
      }
    }

    @Override
    public SelectionVector4 getSelectionVector4() {
      return sv4;
    }
  }
}
