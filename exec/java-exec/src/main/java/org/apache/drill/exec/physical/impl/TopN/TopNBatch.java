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
import org.apache.commons.lang3.ArrayUtils;
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
import org.apache.drill.exec.record.HyperVectorWrapper;
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

import static org.apache.drill.exec.record.RecordBatch.IterOutcome.EMIT;
import static org.apache.drill.exec.record.RecordBatch.IterOutcome.NONE;
import static org.apache.drill.exec.record.RecordBatch.IterOutcome.OK;
import static org.apache.drill.exec.record.RecordBatch.IterOutcome.OK_NEW_SCHEMA;

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
  private SelectionVector4 sv4;
  private long countSincePurge;
  private int batchCount;
  private Copier copier;
  private boolean first = true;
  private int recordCount = 0;
  private IterOutcome laskKnownOutcome = OK;
  private boolean firstBatchForSchema = true;

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
    releaseResource();
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
      case EMIT:
        throw new IllegalStateException("Unexpected EMIT outcome received in buildSchema phase");
      default:
        throw new IllegalStateException("Unexpected outcome received in buildSchema phase");
    }
  }

  @Override
  public IterOutcome innerNext() {
    recordCount = 0;
    if (state == BatchState.DONE) {
      return NONE;
    }

    // If both schema and priorityQueue are non-null and priority queue is not reset, that means we still have data
    // to be sent downstream for the current record boundary
    if (schema != null && priorityQueue != null && priorityQueue.isInitialized()) {
      if (sv4.next()) {
        recordCount = sv4.getCount();
        container.setRecordCount(recordCount);
      } else {
        recordCount = 0;
        container.setRecordCount(0);
      }
      return getFinalOutcome();
    }

    try{
      outer: while (true) {
        Stopwatch watch = Stopwatch.createStarted();
        if (first) {
          laskKnownOutcome = IterOutcome.OK_NEW_SCHEMA;
          first = false;
        } else {
          laskKnownOutcome = next(incoming);
        }
        if (laskKnownOutcome == OK && schema == null) {
          laskKnownOutcome = IterOutcome.OK_NEW_SCHEMA;
          container.clear();
        }
        logger.debug("Took {} us to get next", watch.elapsed(TimeUnit.MICROSECONDS));
        switch (laskKnownOutcome) {
        case NONE:
          break outer;
        case NOT_YET:
          throw new UnsupportedOperationException();
        case OUT_OF_MEMORY:
        case STOP:
          return laskKnownOutcome;
        case OK_NEW_SCHEMA:
          // only change in the case that the schema truly changes.  Artificial schema changes are ignored.
          // schema change handling in case when EMIT is also seen is same as without EMIT. i.e. only if union type
          // is enabled it will be handled.
          container.clear();
          firstBatchForSchema = true;
          if (!incoming.getSchema().equals(schema)) {
            if (schema != null) {
              if (!unionTypeEnabled) {
                throw new UnsupportedOperationException(String.format("TopN currently doesn't support changing " +
                  "schemas with union type disabled. Please try enabling union type: %s and re-execute the query",
                  ExecConstants.ENABLE_UNION_TYPE_KEY));
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
        case EMIT:
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
              priorityQueue = createNewPriorityQueue(new ExpandableHyperContainer(batch.getContainer()), config.getLimit());
            } else if (!priorityQueue.isInitialized()) {
              // means priority queue is cleaned up after producing output for first record boundary. We should
              // initialize it for next record boundary
              priorityQueue.init(config.getLimit(), oContext.getAllocator(),
                schema.getSelectionVectorMode() == SelectionVectorMode.TWO_BYTE);
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

        // If the last seen outcome is EMIT then break the loop. We do it here since we want to process the batch
        // with records and EMIT outcome in above case statements
        if (laskKnownOutcome == EMIT) {
          break;
        }
      }

      // PriorityQueue can be null here if first batch is received with OK_NEW_SCHEMA and is empty and second next()
      // call returned NONE or EMIT. In case of NONE it will change state to DONE and return NONE whereas in case of
      // EMIT it has to still continue working for future records.
      if (schema == null || priorityQueue == null) {
        // builder may be null at this point if the first incoming batch is empty
        if (laskKnownOutcome == NONE) { // that means we saw NONE
          state = BatchState.DONE;
        } else if (laskKnownOutcome == EMIT) {
          // since priority queue is null that means it has not seen any batch with data
          assert (countSincePurge == 0 && batchCount == 0);
          container.zeroVectors();
        }
        return laskKnownOutcome;
      }

      priorityQueue.generate();
      prepareOutputContainer();

      // With EMIT outcome control will come here multiple times whereas without EMIT outcome control will only come
      // here once. In EMIT outcome case if there is schema change in any iteration then that will be handled by
      // lastKnownOutcome.
      return getFinalOutcome();
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
      // Purge all the existing batches to a new batch which only holds the selected records
      copyToPurge(newContainer, builder);
      // New VectorContainer that contains only limit number of records and is later passed to resetQueue to create a
      // HyperContainer backing the priority queue out of it
      VectorContainer newQueue = new VectorContainer();
      builder.build(newQueue);
      priorityQueue.resetQueue(newQueue, builder.getSv4().createNewWrapperCurrent());
      builder.getSv4().clear();
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
    copier = GenericSV4Copier.createCopier(batch, newContainer, null);
    @SuppressWarnings("resource")
    SortRecordBatchBuilder builder = new SortRecordBatchBuilder(oContext.getAllocator());
    try {
      // Purge all the existing batches to a new batch which only holds the selected records
      copyToPurge(newContainer, builder);
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

  /**
   * Resets TopNBatch state to process next incoming batches independent of already seen incoming batches.
   */
  private void resetTopNState() {
    laskKnownOutcome = OK;
    countSincePurge = 0;
    batchCount = 0;
    releaseResource();
  }

  /**
   * Cleanup resources held by TopN Batch such as sv4, priority queue and outgoing container
   */
  private void releaseResource() {
    if (sv4 != null) {
      sv4.clear();
    }

    if (priorityQueue != null) {
      priorityQueue.cleanup();
    }
    container.zeroVectors();
  }

  /**
   * Returns the final IterOutcome which TopN should return for this next call. Return OK_NEW_SCHEMA with first output
   * batch after a new schema is seen. This is indicated by firstBatchSchema flag. It is also true for very first
   * output batch after buildSchema()phase too since in buildSchema() a dummy schema was returned downstream without
   * correct SelectionVectorMode.
   * In other cases when there is no schema change then either OK or EMIT is returned with output batches depending upon
   * if EMIT is seen or not. In cases when EMIT is not seen then OK is always returned with an output batch. When all
   * the data is returned then NONE is sent in the end.
   *
   * @return - IterOutcome - outcome to send downstream
   */
  private IterOutcome getFinalOutcome() {
    IterOutcome outcomeToReturn;

    if (firstBatchForSchema) {
      outcomeToReturn = OK_NEW_SCHEMA;
      firstBatchForSchema = false;
    } else if (recordCount == 0) {
      // get the outcome to return before calling refresh since that resets the lastKnowOutcome to OK
      outcomeToReturn = laskKnownOutcome == EMIT ? EMIT : NONE;
      resetTopNState();
    } else {
      outcomeToReturn = OK;
    }

    return outcomeToReturn;
  }

  /**
   * Copies all the selected records into the new container to purge all the incoming batches into a single batch.
   * @param newContainer - New container holding the ValueVectors with selected records
   * @param batchBuilder - Builder to build hyper vectors batches
   * @throws SchemaChangeException
   */
  private void copyToPurge(VectorContainer newContainer, SortRecordBatchBuilder batchBuilder)
    throws SchemaChangeException {
    final VectorContainer c = priorityQueue.getHyperBatch();
    final SelectionVector4 queueSv4 = priorityQueue.getSv4();
    final SimpleSV4RecordBatch newBatch = new SimpleSV4RecordBatch(newContainer, null, context);

    do {
      // count is the limit number of records required by TopN batch
      final int count = queueSv4.getCount();
      // Transfers count number of records from hyperBatch to simple container
      final int copiedRecords = copier.copyRecords(0, count);
      assert copiedRecords == count;
      for (VectorWrapper<?> v : newContainer) {
        ValueVector.Mutator m = v.getValueVector().getMutator();
        m.setValueCount(count);
      }
      newContainer.buildSchema(BatchSchema.SelectionVectorMode.NONE);
      newContainer.setRecordCount(count);
      // Store all the batches containing limit number of records
      batchBuilder.add(newBatch);
    } while (queueSv4.next());
    // Release the memory stored for the priority queue heap to store indexes
    queueSv4.clear();
    // Release the memory from HyperBatch container
    c.clear();
  }

  /**
   * Prepares an output container with batches from Priority Queue for each record boundary. In case when this is the
   * first batch for the known schema (indicated by true value of firstBatchForSchema) the output container is cleared
   * and recreated with new HyperVectorWrapper objects and ValueVectors from PriorityQueue. In cases when the schema
   * has not changed then it prepares the container keeping the VectorWrapper and SV4 references as is since that is
   * what is needed by downstream operator.
   */
  private void prepareOutputContainer() {
    container.zeroVectors();
    // Check if this is the first output batch for the new known schema. If yes then prepare the output container
    // with the proper vectors, otherwise re-use the previous vectors.
    final VectorContainer queueContainer = priorityQueue.getHyperBatch();
    if (firstBatchForSchema) {
      container.clear();
      for (VectorWrapper<?> w : queueContainer) {
        container.add(w.getValueVectors());
      }
      container.buildSchema(BatchSchema.SelectionVectorMode.FOUR_BYTE);
      sv4 = priorityQueue.getFinalSv4();
    } else {
      // Schema didn't changed so we should keep the reference of HyperVectorWrapper in outgoing container intact and
      // populate the HyperVectorWrapper with new list of vectors. Here the assumption is order of ValueVectors is same
      // across multiple record boundary unless a new schema is observed
      int index = 0;
      for (VectorWrapper<?> w : queueContainer) {
        HyperVectorWrapper wrapper = (HyperVectorWrapper<?>) container.getValueVector(index++);
        wrapper.addVectors(w.getValueVectors());
      }
      // Since the reference of SV4 is held by downstream operator and there is no schema change, so just copy the
      // underlying buffer from priority queue sv4.
      this.sv4.copy(priorityQueue.getFinalSv4());
    }
    recordCount = sv4.getCount();
    container.setRecordCount(recordCount);
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
