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
package org.apache.drill.exec.physical.impl.aggregate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.inject.Named;

import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.compile.sig.RuntimeOverridden;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.config.HashAggregate;
import org.apache.drill.exec.physical.impl.common.ChainedHashTable;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.physical.impl.common.HashTableConfig;
import org.apache.drill.exec.physical.impl.common.HashTableStats;
import org.apache.drill.exec.physical.impl.common.IndexPointer;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.ObjectVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;

public abstract class HashAggTemplate implements HashAggregator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashAggregator.class);

  private static final long ALLOCATOR_INITIAL_RESERVATION = 1 * 1024 * 1024;
  private static final long ALLOCATOR_MAX_RESERVATION = 20L * 1000 * 1000 * 1000;
  private static final int VARIABLE_WIDTH_VALUE_SIZE = 50;

  private static final boolean EXTRA_DEBUG_1 = false;
  private static final boolean EXTRA_DEBUG_2 = false;
  private static final String TOO_BIG_ERROR =
      "Couldn't add value to an empty batch.  This likely means that a single value is too long for a varlen field.";
  private boolean newSchema = false;
  private int underlyingIndex = 0;
  private int currentIndex = 0;
  private IterOutcome outcome;
  private int outputCount = 0;
  private int numGroupedRecords = 0;
  private int outBatchIndex = 0;
  private int lastBatchOutputCount = 0;
  private RecordBatch incoming;
  private BatchSchema schema;
  private HashAggBatch outgoing;
  private VectorContainer outContainer;
  private FragmentContext context;
  private BufferAllocator allocator;

  private HashAggregate hashAggrConfig;
  private HashTable htable;
  private ArrayList<BatchHolder> batchHolders;
  private IndexPointer htIdxHolder; // holder for the Hashtable's internal index returned by put()
  private IndexPointer outStartIdxHolder;
  private IndexPointer outNumRecordsHolder;
  private int numGroupByOutFields = 0; // Note: this should be <= number of group-by fields

  ErrorCollector collector = new ErrorCollectorImpl();

  private MaterializedField[] materializedValueFields;
  private boolean allFlushed = false;
  private boolean buildComplete = false;

  private OperatorStats stats = null;
  private HashTableStats htStats = new HashTableStats();

  public enum Metric implements MetricDef {

    NUM_BUCKETS,
    NUM_ENTRIES,
    NUM_RESIZING,
    RESIZING_TIME;

    // duplicate for hash ag

    @Override
    public int metricId() {
      return ordinal();
    }
  }


  public class BatchHolder {

    private VectorContainer aggrValuesContainer; // container for aggr values (workspace variables)
    private int maxOccupiedIdx = -1;
    private int batchOutputCount = 0;

    private int capacity = Integer.MAX_VALUE;
    private boolean allocatedNextBatch = false;

    private BatchHolder() {

      aggrValuesContainer = new VectorContainer();
      boolean success = false;
      try {
        ValueVector vector;

        for (int i = 0; i < materializedValueFields.length; i++) {
          MaterializedField outputField = materializedValueFields[i];
          // Create a type-specific ValueVector for this value
          vector = TypeHelper.getNewVector(outputField, allocator);

          // Try to allocate space to store BATCH_SIZE records. Key stored at index i in HashTable has its workspace
          // variables (such as count, sum etc) stored at index i in HashAgg. HashTable and HashAgg both have
          // BatchHolders. Whenever a BatchHolder in HashAgg reaches its capacity, a new BatchHolder is added to
          // HashTable. If HashAgg can't store BATCH_SIZE records in a BatchHolder, it leaves empty slots in current
          // BatchHolder in HashTable, causing the HashTable to be space inefficient. So it is better to allocate space
          // to fit as close to as BATCH_SIZE records.
          if (vector instanceof FixedWidthVector) {
            ((FixedWidthVector) vector).allocateNew(HashTable.BATCH_SIZE);
          } else if (vector instanceof VariableWidthVector) {
            ((VariableWidthVector) vector).allocateNew(HashTable.VARIABLE_WIDTH_VECTOR_SIZE * HashTable.BATCH_SIZE,
                HashTable.BATCH_SIZE);
          } else if (vector instanceof ObjectVector) {
            ((ObjectVector) vector).allocateNew(HashTable.BATCH_SIZE);
          } else {
            vector.allocateNew();
          }

          capacity = Math.min(capacity, vector.getValueCapacity());

          aggrValuesContainer.add(vector);
        }
        success = true;
      } finally {
        if (!success) {
          aggrValuesContainer.clear();
        }
      }
    }

    private boolean updateAggrValues(int incomingRowIdx, int idxWithinBatch) {
      updateAggrValuesInternal(incomingRowIdx, idxWithinBatch);
      maxOccupiedIdx = Math.max(maxOccupiedIdx, idxWithinBatch);
      return true;
    }

    private void setup() {
      setupInterior(incoming, outgoing, aggrValuesContainer);
    }

    private void outputValues(IndexPointer outStartIdxHolder, IndexPointer outNumRecordsHolder) {
      outStartIdxHolder.value = batchOutputCount;
      outNumRecordsHolder.value = 0;
      for (int i = batchOutputCount; i <= maxOccupiedIdx; i++) {
        outputRecordValues(i, batchOutputCount);
        if (EXTRA_DEBUG_2) {
          logger.debug("Outputting values to output index: {}", batchOutputCount);
        }
        batchOutputCount++;
        outNumRecordsHolder.value++;
      }
    }

    private void clear() {
      aggrValuesContainer.clear();
    }

    private int getNumGroups() {
      return maxOccupiedIdx + 1;
    }

    private int getNumPendingOutput() {
      return getNumGroups() - batchOutputCount;
    }

    // Code-generated methods (implemented in HashAggBatch)

    @RuntimeOverridden
    public void setupInterior(@Named("incoming") RecordBatch incoming, @Named("outgoing") RecordBatch outgoing,
        @Named("aggrValuesContainer") VectorContainer aggrValuesContainer) {
    }

    @RuntimeOverridden
    public void updateAggrValuesInternal(@Named("incomingRowIdx") int incomingRowIdx, @Named("htRowIdx") int htRowIdx) {
    }

    @RuntimeOverridden
    public void outputRecordValues(@Named("htRowIdx") int htRowIdx, @Named("outRowIdx") int outRowIdx) {
    }
  }


  @Override
  public void setup(HashAggregate hashAggrConfig, HashTableConfig htConfig, FragmentContext context,
      OperatorStats stats, BufferAllocator allocator, RecordBatch incoming, HashAggBatch outgoing,
      LogicalExpression[] valueExprs, List<TypedFieldId> valueFieldIds, TypedFieldId[] groupByOutFieldIds,
      VectorContainer outContainer) throws SchemaChangeException, ClassTransformationException, IOException {

    if (valueExprs == null || valueFieldIds == null) {
      throw new IllegalArgumentException("Invalid aggr value exprs or workspace variables.");
    }
    if (valueFieldIds.size() < valueExprs.length) {
      throw new IllegalArgumentException("Wrong number of workspace variables.");
    }

    this.context = context;
    this.stats = stats;
    this.allocator = allocator;
    this.incoming = incoming;
    this.schema = incoming.getSchema();
    this.outgoing = outgoing;
    this.outContainer = outContainer;

    this.hashAggrConfig = hashAggrConfig;

    // currently, hash aggregation is only applicable if there are group-by expressions.
    // For non-grouped (a.k.a Plain) aggregations that don't involve DISTINCT, there is no
    // need to create hash table.  However, for plain aggregations with DISTINCT ..
    //      e.g SELECT COUNT(DISTINCT a1) FROM t1 ;
    // we need to build a hash table on the aggregation column a1.
    // TODO:  This functionality will be added later.
    if (hashAggrConfig.getGroupByExprs().size() == 0) {
      throw new IllegalArgumentException("Currently, hash aggregation is only applicable if there are group-by " +
          "expressions.");
    }

    this.htIdxHolder = new IndexPointer();
    this.outStartIdxHolder = new IndexPointer();
    this.outNumRecordsHolder = new IndexPointer();

    materializedValueFields = new MaterializedField[valueFieldIds.size()];

    if (valueFieldIds.size() > 0) {
      int i = 0;
      FieldReference ref =
          new FieldReference("dummy", ExpressionPosition.UNKNOWN, valueFieldIds.get(0).getIntermediateType());
      for (TypedFieldId id : valueFieldIds) {
        materializedValueFields[i++] = MaterializedField.create(ref.getAsNamePart().getName(), id.getIntermediateType());
      }
    }

    ChainedHashTable ht =
        new ChainedHashTable(htConfig, context, allocator, incoming, null /* no incoming probe */, outgoing,
            true /* nulls are equal */);
    this.htable = ht.createAndSetupHashTable(groupByOutFieldIds);

    numGroupByOutFields = groupByOutFieldIds.length;
    batchHolders = new ArrayList<BatchHolder>();
    // First BatchHolder is created when the first put request is received.

    doSetup(incoming);
  }

  @Override
  public AggOutcome doWork() {
    try {
      // Note: Keeping the outer and inner try blocks here to maintain some similarity with
      // StreamingAggregate which does somethings conditionally in the outer try block.
      // In the future HashAggregate may also need to perform some actions conditionally
      // in the outer try block.

      outside:
      while (true) {
        // loop through existing records, aggregating the values as necessary.
        if (EXTRA_DEBUG_1) {
          logger.debug("Starting outer loop of doWork()...");
        }
        for (; underlyingIndex < incoming.getRecordCount(); incIndex()) {
          if (EXTRA_DEBUG_2) {
            logger.debug("Doing loop with values underlying {}, current {}", underlyingIndex, currentIndex);
          }
          checkGroupAndAggrValues(currentIndex);
        }

        if (EXTRA_DEBUG_1) {
          logger.debug("Processed {} records", underlyingIndex);
        }

        try {

          while (true) {
            // Cleanup the previous batch since we are done processing it.
            for (VectorWrapper<?> v : incoming) {
              v.getValueVector().clear();
            }
            IterOutcome out = outgoing.next(0, incoming);
            if (EXTRA_DEBUG_1) {
              logger.debug("Received IterOutcome of {}", out);
            }
            switch (out) {
              case OUT_OF_MEMORY:
              case NOT_YET:
                this.outcome = out;
                return AggOutcome.RETURN_OUTCOME;

              case OK_NEW_SCHEMA:
                if (EXTRA_DEBUG_1) {
                  logger.debug("Received new schema.  Batch has {} records.", incoming.getRecordCount());
                }
                newSchema = true;
                this.cleanup();
                // TODO: new schema case needs to be handled appropriately
                return AggOutcome.UPDATE_AGGREGATOR;

              case OK:
                resetIndex();
                if (incoming.getRecordCount() == 0) {
                  continue;
                } else {
                  checkGroupAndAggrValues(currentIndex);
                  incIndex();

                  if (EXTRA_DEBUG_1) {
                    logger.debug("Continuing outside loop");
                  }
                  continue outside;
                }

              case NONE:
                // outcome = out;

                buildComplete = true;

                updateStats(htable);

                // output the first batch; remaining batches will be output
                // in response to each next() call by a downstream operator

                outputCurrentBatch();

                // return setOkAndReturn();
                return AggOutcome.RETURN_OUTCOME;

              case STOP:
              default:
                outcome = out;
                return AggOutcome.CLEANUP_AND_RETURN;
            }
          }

        } finally {
          // placeholder...
        }
      }
    } finally {
    }
  }

  private void allocateOutgoing(int records) {
    // Skip the keys and only allocate for outputting the workspace values
    // (keys will be output through splitAndTransfer)
    Iterator<VectorWrapper<?>> outgoingIter = outContainer.iterator();
    for (int i = 0; i < numGroupByOutFields; i++) {
      outgoingIter.next();
    }
    while (outgoingIter.hasNext()) {
      ValueVector vv = outgoingIter.next().getValueVector();
      MajorType type = vv.getField().getType();

      /*
       * In build schema we use the allocation model that specifies exact record count
       * so we need to stick with that allocation model until DRILL-2211 is resolved. Using
       * 50 as the average bytes per value as is used in HashTable.
       */
      AllocationHelper.allocatePrecomputedChildCount(vv, records, VARIABLE_WIDTH_VALUE_SIZE, 0);
    }
  }

  @Override
  public IterOutcome getOutcome() {
    return outcome;
  }

  @Override
  public int getOutputCount() {
    // return outputCount;
    return lastBatchOutputCount;
  }

  @Override
  public void cleanup() {
    if (htable != null) {
      htable.clear();
      htable = null;
    }
    htIdxHolder = null;
    materializedValueFields = null;
    outStartIdxHolder = null;
    outNumRecordsHolder = null;

    if (batchHolders != null) {
      for (BatchHolder bh : batchHolders) {
        bh.clear();
      }
      batchHolders.clear();
      batchHolders = null;
    }
  }

  private final AggOutcome setOkAndReturn() {
    this.outcome = IterOutcome.OK;
    for (VectorWrapper<?> v : outgoing) {
      v.getValueVector().getMutator().setValueCount(outputCount);
    }
    return AggOutcome.RETURN_OUTCOME;
  }

  private final void incIndex() {
    underlyingIndex++;
    if (underlyingIndex >= incoming.getRecordCount()) {
      currentIndex = Integer.MAX_VALUE;
      return;
    }
    currentIndex = getVectorIndex(underlyingIndex);
  }

  private final void resetIndex() {
    underlyingIndex = -1;
    incIndex();
  }

  private void addBatchHolder() {
    BatchHolder bh = new BatchHolder();
    batchHolders.add(bh);

    if (EXTRA_DEBUG_1) {
      logger.debug("HashAggregate: Added new batch; num batches = {}.", batchHolders.size());
    }

    bh.setup();
  }

  public IterOutcome outputCurrentBatch() {
    if (outBatchIndex >= batchHolders.size()) {
      this.outcome = IterOutcome.NONE;
      return outcome;
    }

    // get the number of records in the batch holder that are pending output
    int numPendingOutput = batchHolders.get(outBatchIndex).getNumPendingOutput();

    if (numPendingOutput == 0) {
      this.outcome = IterOutcome.NONE;
      return outcome;
    }

    allocateOutgoing(numPendingOutput);

    batchHolders.get(outBatchIndex).outputValues(outStartIdxHolder, outNumRecordsHolder);
    int numOutputRecords = outNumRecordsHolder.value;

    if (EXTRA_DEBUG_1) {
      logger.debug("After output values: outStartIdx = {}, outNumRecords = {}", outStartIdxHolder.value, outNumRecordsHolder.value);
    }
    this.htable.outputKeys(outBatchIndex, this.outContainer, outStartIdxHolder.value, outNumRecordsHolder.value);

    // set the value count for outgoing batch value vectors
    for (VectorWrapper<?> v : outgoing) {
      v.getValueVector().getMutator().setValueCount(numOutputRecords);
    }

    outputCount += numOutputRecords;

    this.outcome = IterOutcome.OK;

    logger.debug("HashAggregate: Output current batch index {} with {} records.", outBatchIndex, numOutputRecords);

    lastBatchOutputCount = numOutputRecords;
    outBatchIndex++;
    if (outBatchIndex == batchHolders.size()) {
      allFlushed = true;

      logger.debug("HashAggregate: All batches flushed.");

      // cleanup my internal state since there is nothing more to return
      this.cleanup();
    }

    return this.outcome;
  }

  public boolean allFlushed() {
    return allFlushed;
  }

  public boolean buildComplete() {
    return buildComplete;
  }

  public int numGroupedRecords() {
    return numGroupedRecords;
  }

  // Check if a group is present in the hash table; if not, insert it in the hash table.
  // The htIdxHolder contains the index of the group in the hash table container; this same
  // index is also used for the aggregation values maintained by the hash aggregate.
  private void checkGroupAndAggrValues(int incomingRowIdx) {
    if (incomingRowIdx < 0) {
      throw new IllegalArgumentException("Invalid incoming row index.");
    }

    /** for debugging
     Object tmp = (incoming).getValueAccessorById(0, BigIntVector.class).getValueVector();
     BigIntVector vv0 = null;
     BigIntHolder holder = null;

     if (tmp != null) {
     vv0 = ((BigIntVector) tmp);
     holder = new BigIntHolder();
     holder.value = vv0.getAccessor().get(incomingRowIdx) ;
     }
     */

    htable.put(incomingRowIdx, htIdxHolder, 1 /* retry count */);

    int currentIdx = htIdxHolder.value;

    // get the batch index and index within the batch
    if (currentIdx >= batchHolders.size() * HashTable.BATCH_SIZE) {
      addBatchHolder();
    }
    BatchHolder bh = batchHolders.get((currentIdx >>> 16) & HashTable.BATCH_MASK);
    int idxWithinBatch = currentIdx & HashTable.BATCH_MASK;

    // Check if we have almost filled up the workspace vectors and add a batch if necessary
    if ((idxWithinBatch == (bh.capacity - 1)) && (bh.allocatedNextBatch == false)) {
      htable.addNewKeyBatch();
      addBatchHolder();
      bh.allocatedNextBatch = true;
    }


    if (bh.updateAggrValues(incomingRowIdx, idxWithinBatch)) {
      numGroupedRecords++;
    }
  }

  private void updateStats(HashTable htable) {
    htable.getStats(htStats);
    this.stats.setLongStat(Metric.NUM_BUCKETS, htStats.numBuckets);
    this.stats.setLongStat(Metric.NUM_ENTRIES, htStats.numEntries);
    this.stats.setLongStat(Metric.NUM_RESIZING, htStats.numResizing);
    this.stats.setLongStat(Metric.RESIZING_TIME, htStats.resizingTime);
  }

  // Code-generated methods (implemented in HashAggBatch)
  public abstract void doSetup(@Named("incoming") RecordBatch incoming);

  public abstract int getVectorIndex(@Named("recordIndex") int recordIndex);

  public abstract boolean resetValues();

}
