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
package org.apache.drill.exec.physical.impl.aggregate;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import javax.inject.Named;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.drill.common.exceptions.RetryAfterSpillException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.sig.RuntimeOverridden;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.TypeHelper;

import org.apache.drill.exec.memory.BaseAllocator;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.config.HashAggregate;
import org.apache.drill.exec.physical.impl.common.ChainedHashTable;
import org.apache.drill.exec.physical.impl.common.CodeGenMemberInjector;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.physical.impl.common.HashTableConfig;
import org.apache.drill.exec.physical.impl.common.HashTableStats;
import org.apache.drill.exec.physical.impl.common.IndexPointer;

import org.apache.drill.exec.physical.impl.common.SpilledState;
import org.apache.drill.exec.record.RecordBatchSizer;

import org.apache.drill.exec.physical.impl.spill.SpillSet;
import org.apache.drill.exec.planner.physical.AggPrelBase;

import org.apache.drill.exec.record.MaterializedField;

import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.BatchSchema;

import org.apache.drill.exec.record.VectorContainer;

import org.apache.drill.exec.record.TypedFieldId;

import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.AllocationHelper;

import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.ObjectVector;
import org.apache.drill.exec.vector.ValueVector;

import org.apache.drill.exec.vector.VariableWidthVector;

import static org.apache.drill.exec.physical.impl.common.HashTable.BATCH_MASK;
import static org.apache.drill.exec.record.RecordBatch.MAX_BATCH_ROW_COUNT;

public abstract class HashAggTemplate implements HashAggregator, HashAggBatchAllocator {
  protected static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashAggregator.class);

  private static final int VARIABLE_MAX_WIDTH_VALUE_SIZE = 50;
  private static final int VARIABLE_MIN_WIDTH_VALUE_SIZE = 8;

  private static final boolean EXTRA_DEBUG_1 = false;
  private static final boolean EXTRA_DEBUG_2 = false;
  private static final boolean EXTRA_DEBUG_SPILL = false;

  // Fields needed for partitioning (the groups into partitions)
  private int nextPartitionToReturnIndex = 0; // which partition to return the next batch from
  // The following members are used for logging, metrics, etc.
  private int rowsReturnedEarly = 0;

  private AggPrelBase.OperatorPhase phase;
  private boolean canSpill = true; // make it false in case can not spill/return-early
  private ChainedHashTable baseHashTable;
  private boolean earlyOutput = false; // when 1st phase returns a partition due to no memory
  private int earlyPartition = 0; // which partition to return early
  private boolean retrySameIndex = false; // in case put failed during 1st phase - need to output early, then retry
  private boolean useMemoryPrediction = false; // whether to use memory prediction to decide when to spill
  private long estMaxBatchSize = 0; // used for adjusting #partitions and deciding when to spill
  private long estRowWidth = 0; // the size of the internal "row" (keys + values + extra columns)
  private long estValuesRowWidth = 0; // the size of the internal values ( values + extra )
  private long estOutputRowWidth = 0; // the size of the output "row" (no extra columns)
  private long estValuesBatchSize = 0; // used for "reserving" memory for the Values batch to overcome an OOM
  private long estOutgoingAllocSize = 0; // used for "reserving" memory for the Outgoing Output Values to overcome an OOM
  private long reserveValueBatchMemory; // keep "reserve memory" for Values Batch
  private long reserveOutgoingMemory; // keep "reserve memory" for the Outgoing (Values only) output
  private int maxColumnWidth = VARIABLE_MIN_WIDTH_VALUE_SIZE; // to control memory allocation for varchars
  private long minBatchesPerPartition; // for tuning - num partitions and spill decision
  private long plannedBatches = 0; // account for planned, but not yet allocated batches

  private int underlyingIndex = 0;
  private int currentIndex = 0;
  private IterOutcome outcome;
  private int numGroupedRecords = 0;
  private int currentBatchRecordCount = 0; // Performance: Avoid repeated calls to getRecordCount()

  private int lastBatchOutputCount = 0;
  private RecordBatch incoming;
  private BatchSchema schema;
  private HashAggBatch outgoing;
  private VectorContainer outContainer;

  protected FragmentContext context;
  protected ClassGenerator<?> cg;
  private OperatorContext oContext;
  private BufferAllocator allocator;

  private HashAggMemoryCalculator memoryCalculator = new HashAggMemoryCalculatorNoop();
  private HashAggPartition[] partitions = new HashAggPartition[0];

  // For handling spilling
  private HashAggUpdater updater = new HashAggUpdater();
  private SpilledState<HashAggSpilledPartition> spilledState = new SpilledState<>();
  private SpillSet spillSet;
  SpilledRecordbatch newIncoming; // when reading a spilled file - work like an "incoming"
  private int originalPartition = -1; // the partition a secondary reads from

  private IndexPointer htIdxHolder; // holder for the Hashtable's internal index returned by put()
  private int numGroupByOutFields = 0; // Note: this should be <= number of group-by fields
  private TypedFieldId[] groupByOutFieldIds;

  private MaterializedField[] materializedValueFields;
  private boolean allFlushed = false;
  private boolean buildComplete = false;
  private boolean handlingSpills = false; // True once starting to process spill files
  private boolean handleEmit = false; // true after receiving an EMIT, till finish handling it

  private OperatorStats stats = null;
  private HashTableStats htStats = new HashTableStats();

  public class HashAggUpdater implements SpilledState.Updater {

    @Override
    public void cleanup() {
      this.cleanup();
    }

    @Override
    public String getFailureMessage() {
      return null;
    }

    @Override
    public long getMemLimit() {
      return allocator.getLimit();
    }

    @Override
    public boolean hasPartitionLimit() {
      return false;
    }
  }

  public enum Metric implements MetricDef {

    NUM_BUCKETS,
    NUM_ENTRIES,
    NUM_RESIZING,
    RESIZING_TIME_MS,
    NUM_PARTITIONS,
    SPILLED_PARTITIONS, // number of original partitions spilled to disk
    SPILL_MB,         // Number of MB of data spilled to disk. This amount is first written,
                      // then later re-read. So, disk I/O is twice this amount.
                      // For first phase aggr -- this is an estimate of the amount of data
                      // returned early (analogous to a spill in the 2nd phase).
    SPILL_CYCLE,       // 0 - no spill, 1 - spill, 2 - SECONDARY, 3 - TERTIARY
    INPUT_BATCH_COUNT,
    AVG_INPUT_BATCH_BYTES,
    AVG_INPUT_ROW_BYTES,
    INPUT_RECORD_COUNT,
    OUTPUT_BATCH_COUNT,
    AVG_OUTPUT_BATCH_BYTES,
    AVG_OUTPUT_ROW_BYTES,
    OUTPUT_RECORD_COUNT;

    @Override
    public int metricId() {
      return ordinal();
    }
  }

  public class BatchHolder implements HashAggBatchHolder {
    private VectorContainer aggrValuesContainer; // container for aggr values (workspace variables)
    private int maxOccupiedIdx = -1;
    private int targetBatchRowCount = 0;

    @Override
    public int getTargetBatchRowCount() {
      return targetBatchRowCount;
    }

    @Override
    public void setTargetBatchRowCount(int batchRowCount) {
      this.targetBatchRowCount = batchRowCount;
    }

    @Override
    public int getCurrentRowCount() {
      return (maxOccupiedIdx + 1);
    }

    @SuppressWarnings("resource")
    public BatchHolder(int batchRowCount) {

      aggrValuesContainer = new VectorContainer();
      boolean success = false;
      this.targetBatchRowCount = batchRowCount;

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
            ((FixedWidthVector) vector).allocateNew(batchRowCount);
          } else if (vector instanceof VariableWidthVector) {
            // This case is never used .... a varchar falls under ObjectVector which is allocated on the heap !
            ((VariableWidthVector) vector).allocateNew(maxColumnWidth, batchRowCount);
          } else if (vector instanceof ObjectVector) {
            ((ObjectVector) vector).allocateNew(batchRowCount);
          } else {
            vector.allocateNew();
          }

          aggrValuesContainer.add(vector);
        }
        success = true;
      } finally {
        if (!success) {
          aggrValuesContainer.clear();
        }
      }
    }

    @Override
    public boolean updateAggrValues(int incomingRowIdx, int idxWithinBatch) {
      try { updateAggrValuesInternal(incomingRowIdx, idxWithinBatch); }
      catch (SchemaChangeException sc) { throw new UnsupportedOperationException(sc); }
      maxOccupiedIdx = Math.max(maxOccupiedIdx, idxWithinBatch);
      return true;
    }

    @Override
    public void setup() {
      try { setupInterior(incoming, outgoing, aggrValuesContainer); }
      catch (SchemaChangeException sc) { throw new UnsupportedOperationException(sc);}
    }

    @Override
    public void outputValues() {
      for (int i = 0; i <= maxOccupiedIdx; i++) {
        try {
          outputRecordValues(i, i);
        }
        catch (SchemaChangeException sc) { throw new UnsupportedOperationException(sc);}
      }
    }

    @Override
    public void clear() {
      aggrValuesContainer.clear();
    }

    private int getNumGroups() {
      return maxOccupiedIdx + 1;
    }

    @Override
    public int getNumPendingOutput() {
      return getNumGroups();
    }

    // Code-generated methods (implemented in HashAggBatch)

    @RuntimeOverridden
    public void setupInterior(@Named("incoming") RecordBatch incoming, @Named("outgoing") RecordBatch outgoing,
        @Named("aggrValuesContainer") VectorContainer aggrValuesContainer) throws SchemaChangeException {
    }

    @RuntimeOverridden
    public void updateAggrValuesInternal(@Named("incomingRowIdx") int incomingRowIdx, @Named("htRowIdx") int htRowIdx) throws SchemaChangeException{
    }

    @RuntimeOverridden
    public void outputRecordValues(@Named("htRowIdx") int htRowIdx, @Named("outRowIdx") int outRowIdx) throws SchemaChangeException{
    }
  }

  @Override
  public void setup(HashAggregate hashAggrConfig, HashTableConfig htConfig, FragmentContext context, OperatorContext oContext,
                    RecordBatch incoming, HashAggBatch outgoing, LogicalExpression[] valueExprs, List<TypedFieldId> valueFieldIds,
                    ClassGenerator<?> cg, TypedFieldId[] groupByOutFieldIds, VectorContainer outContainer, int extraRowBytes) throws SchemaChangeException, IOException {

    if (valueExprs == null || valueFieldIds == null) {
      throw new IllegalArgumentException("Invalid aggr value exprs or workspace variables.");
    }
    if (valueFieldIds.size() < valueExprs.length) {
      throw new IllegalArgumentException("Wrong number of workspace variables.");
    }

    this.context = context;
    this.stats = oContext.getStats();
    this.allocator = oContext.getAllocator();
    this.oContext = oContext;
    this.incoming = incoming;
    this.outgoing = outgoing;
    this.cg = cg;
    this.outContainer = outContainer;
    this.useMemoryPrediction = context.getOptions().getOption(ExecConstants.HASHAGG_USE_MEMORY_PREDICTION_VALIDATOR);
    this.phase = hashAggrConfig.getAggPhase();
    canSpill = phase.hasTwo(); // single phase can not spill

    // Typically for testing - force a spill after a partition has more than so many batches
    minBatchesPerPartition = context.getOptions().getOption(ExecConstants.HASHAGG_MIN_BATCHES_PER_PARTITION_VALIDATOR);

    // Set the memory limit
    long memoryLimit = allocator.getLimit();
    // Optional configured memory limit, typically used only for testing.
    long configLimit = context.getOptions().getOption(ExecConstants.HASHAGG_MAX_MEMORY_VALIDATOR);
    if (configLimit > 0) {
      logger.warn("Memory limit was changed to {}",configLimit);
      memoryLimit = Math.min(memoryLimit, configLimit);
      allocator.setLimit(memoryLimit); // enforce at the allocator
    }

    // All the settings that require the number of partitions were moved into delayedSetup()
    // which would be called later, after the actuall data first arrives

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
    materializedValueFields = new MaterializedField[valueFieldIds.size()];

    if (valueFieldIds.size() > 0) {
      int i = 0;
      FieldReference ref =
          new FieldReference("dummy", ExpressionPosition.UNKNOWN, valueFieldIds.get(0).getIntermediateType());
      for (TypedFieldId id : valueFieldIds) {
        materializedValueFields[i++] = MaterializedField.create(ref.getAsNamePart().getName(), id.getIntermediateType());
      }
    }

    spillSet = new SpillSet(context, hashAggrConfig);
    baseHashTable =
        new ChainedHashTable(htConfig, context, allocator, incoming, null /* no incoming probe */, outgoing);
    this.groupByOutFieldIds = groupByOutFieldIds; // retain these for delayedSetup, and to allow recreating hash tables (after a spill)
    numGroupByOutFields = groupByOutFieldIds.length;

    // Start calculating the row widths (with the extra columns; the rest would be done in updateEstMaxBatchSize() )
    estRowWidth = extraRowBytes;
    estValuesRowWidth = extraRowBytes;

    doSetup(incoming);
  }

  /**
   *  Delayed setup are the parts from setup() that can only be set after actual data arrives in incoming
   *  This data is used to compute the number of partitions.
   */
  private void delayedSetup() {

    final boolean fallbackEnabled = context.getOptions().getOption(ExecConstants.HASHAGG_FALLBACK_ENABLED_KEY).bool_val;

    // Set the number of partitions from the configuration (raise to a power of two, if needed)
    int numPartitions = (int)context.getOptions().getOption(ExecConstants.HASHAGG_NUM_PARTITIONS_VALIDATOR);
    if ( numPartitions == 1 && phase.is2nd() ) { // 1st phase can still do early return with 1 partition
      canSpill = false;
      logger.warn("Spilling is disabled due to configuration setting of num_partitions to 1");
    }
    numPartitions = BaseAllocator.nextPowerOfTwo(numPartitions); // in case not a power of 2

    if ( schema == null ) { estValuesBatchSize = estOutgoingAllocSize = estMaxBatchSize = 0; } // incoming was an empty batch
    else {
      // Estimate the max batch size; should use actual data (e.g. lengths of varchars)
      updateEstMaxBatchSize(incoming);
    }
    // create "reserved memory" and adjust the memory limit down
    reserveValueBatchMemory = reserveOutgoingMemory = estValuesBatchSize;
    long newMemoryLimit = allocator.getLimit() - reserveValueBatchMemory - reserveOutgoingMemory;
    long memAvail = newMemoryLimit - allocator.getAllocatedMemory();
    if ( memAvail <= 0 ) { throw new OutOfMemoryException("Too little memory available"); }
    allocator.setLimit(newMemoryLimit);

    if ( !canSpill ) { // single phase, or spill disabled by configuation
      numPartitions = 1; // single phase should use only a single partition (to save memory)
    } else { // two phase
      // Adjust down the number of partitions if needed - when the memory available can not hold as
      // many batches (configurable option), plus overhead (e.g. hash table, links, hash values))
      while ( numPartitions * ( estMaxBatchSize * minBatchesPerPartition + 2 * 1024 * 1024) > memAvail ) {
        numPartitions /= 2;
        if ( numPartitions < 2) {
          if (phase.is2nd()) {
            canSpill = false;  // 2nd phase needs at least 2 to make progress

            if (fallbackEnabled) {
              logger.warn("Spilling is disabled - not enough memory available for internal partitioning. Falling back"
                  + " to use unbounded memory");
            } else {
              throw UserException.resourceError()
                  .message(String.format("Not enough memory for internal partitioning and fallback mechanism for "
                      + "HashAgg to use unbounded memory is disabled. Either enable fallback config %s using Alter "
                      + "session/system command or increase memory limit for Drillbit",
                      ExecConstants.HASHAGG_FALLBACK_ENABLED_KEY))
                  .build(logger);
            }
          }
          break;
        }
      }
    }
    logger.debug("{} phase. Number of partitions chosen: {}. {} spill", phase.getName(),
        numPartitions, canSpill ? "Can" : "Cannot");

    // The following initial safety check should be revisited once we can lower the number of rows in a batch
    // In cases of very tight memory -- need at least memory to process one batch, plus overhead (e.g. hash table)
    if ( numPartitions == 1 && ! canSpill ) {
      // if too little memory - behave like the old code -- practically no memory limit for hash aggregate
      // (but 1st phase can still spill, so it will maintain the original memory limit)
      allocator.setLimit(AbstractBase.MAX_ALLOCATION);  // 10_000_000_000L
    }

    spilledState.initialize(numPartitions);

    partitions = new HashAggPartition[numPartitions];
    plannedBatches = numPartitions; // each partition should allocate its first batch

    memoryCalculator = new HashAggMemoryCalculatorImpl(
          allocator,
          estOutgoingAllocSize,
          estValuesBatchSize,
          reserveOutgoingMemory,
          reserveValueBatchMemory);

    // initialize every (per partition) entry in the arrays
    for (int i = 0; i < numPartitions; i++ ) {
      partitions[i] = new HashAggPartitionImpl(baseHashTable,
        groupByOutFieldIds,
        i,
        outgoing,
        outContainer,
        spilledState,
        spillSet,
        memoryCalculator,
        this,
        outgoing.getRecordBatchMemoryManager(),
        outgoing.getRecordBatchStatsContext());
    }

    // Initialize the value vectors in the generated code (which point to the incoming or outgoing fields)
    partitions[0].updateBatches();
  }
  /**
   * get new incoming: (when reading spilled files like an "incoming")
   * @return The (newly replaced) incoming
   */
  @Override
  public RecordBatch getNewIncoming() { return newIncoming; }

  private void initializeSetup(RecordBatch newIncoming) throws SchemaChangeException, IOException {
    baseHashTable.updateIncoming(newIncoming, null); // after a spill - a new incoming
    this.incoming = newIncoming;
    currentBatchRecordCount = newIncoming.getRecordCount(); // first batch in this spill file
    nextPartitionToReturnIndex = 0;
    for (int i = 0; i < spilledState.getNumPartitions(); i++ ) {
      partitions[i].setup(newIncoming);
    }
  }

  /**
   *  Update the estimated max batch size to be used in the Hash Aggr Op.
   *  using the record batch size to get the row width.
   * @param incoming
   */
  private void updateEstMaxBatchSize(RecordBatch incoming) {
    if ( estMaxBatchSize > 0 ) { return; }  // no handling of a schema (or varchar) change
    // Use the sizer to get the input row width and the length of the longest varchar column
    RecordBatchSizer sizer = outgoing.getRecordBatchMemoryManager().getRecordBatchSizer();
    logger.trace("Incoming sizer: {}",sizer);
    // An empty batch only has the schema, can not tell actual length of varchars
    // else use the actual varchars length, each capped at 50 (to match the space allocation)
    long estInputRowWidth = sizer.rowCount() == 0 ? sizer.getStdRowWidth() : sizer.getNetRowWidthCap50();

    // Get approx max (varchar) column width to get better memory allocation
    maxColumnWidth = Math.max(sizer.getMaxAvgColumnSize(), VARIABLE_MIN_WIDTH_VALUE_SIZE);
    maxColumnWidth = Math.min(maxColumnWidth, VARIABLE_MAX_WIDTH_VALUE_SIZE);

    //
    // Calculate the estimated max (internal) batch (i.e. Keys batch + Values batch) size
    // (which is used to decide when to spill)
    // Also calculate the values batch size (used as a reserve to overcome an OOM)
    //
    Iterator<VectorWrapper<?>> outgoingIter = outContainer.iterator();
    int fieldId = 0;
    while (outgoingIter.hasNext()) {
      ValueVector vv = outgoingIter.next().getValueVector();
      MaterializedField mr = vv.getField();
      int fieldSize = vv instanceof VariableWidthVector ? maxColumnWidth :
          TypeHelper.getSize(mr.getType());
      estRowWidth += fieldSize;
      estOutputRowWidth += fieldSize;
      if ( fieldId < numGroupByOutFields ) { fieldId++; }
      else { estValuesRowWidth += fieldSize; }
    }
    // multiply by the max number of rows in a batch to get the final estimated max size
    long estimatedMaxWidth = Math.max(estRowWidth, estInputRowWidth);
    estMaxBatchSize = estimatedMaxWidth * MAX_BATCH_ROW_COUNT;
    // estimated batch size should not exceed the configuration given size
    int configuredBatchSize = outgoing.getRecordBatchMemoryManager().getOutputBatchSize();
    estMaxBatchSize = Math.min(estMaxBatchSize, configuredBatchSize);
    // work back the number of rows (may have been reduced from MAX_BATCH_ROW_COUNT)
    long rowsInBatch = estMaxBatchSize / estimatedMaxWidth;
    // (When there are no aggr functions, use '1' as later code relies on this size being non-zero)
    estValuesBatchSize = Math.max(estValuesRowWidth, 1) * rowsInBatch;
    estOutgoingAllocSize = estValuesBatchSize; // initially assume same size

    logger.trace("{} phase. Estimated internal row width: {} Values row width: {} batch size: {}  memory limit: {}  max column width: {}",
      phase.getName(),estRowWidth,estValuesRowWidth,estMaxBatchSize,allocator.getLimit(),maxColumnWidth);

    if ( estMaxBatchSize > allocator.getLimit() ) {
      logger.warn("HashAggregate: Estimated max batch size {} is larger than the memory limit {}",estMaxBatchSize,allocator.getLimit());
    }
  }

  /**
   *  Read and process (i.e., insert into the hash table and aggregate) records from the current batch.
   *  Once complete, get the incoming NEXT batch and process it as well, etc.
   *  For 1st phase, may return when an early output needs to be performed.
   *
   * @return Agg outcome status
   */
  @Override
  public AggOutcome doWork() {

    while (true) {

      // This would be called only once - first time actual data arrives on incoming
      if ( schema == null && incoming.getRecordCount() > 0 ) {
        this.schema = incoming.getSchema();
        currentBatchRecordCount = incoming.getRecordCount(); // initialize for first non empty batch
        // Calculate the number of partitions based on actual incoming data
        delayedSetup();
        // Update the record batch manager since this is the first batch with data; we need to
        // perform the update before any processing.
        // NOTE - We pass the incoming record batch explicitly because it could be a spilled record (different
        //        from the instance owned by the HashAggBatch).
        outgoing.getRecordBatchMemoryManager().update(incoming);
      }

      //
      //  loop through existing records in this batch, aggregating the values as necessary.
      //
      if (EXTRA_DEBUG_1) {
        logger.debug("Starting outer loop of doWork()...");
      }
      while (underlyingIndex < currentBatchRecordCount) {
        if (EXTRA_DEBUG_2) {
          logger.debug("Doing loop with values underlying {}, current {}", underlyingIndex, currentIndex);
        }
        checkGroupAndAggrValues(currentIndex);

        if ( retrySameIndex ) { retrySameIndex = false; }  // need to retry this row (e.g. we had an OOM)
        else { incIndex(); } // next time continue with the next incoming row

        // If adding a group discovered a memory pressure during 1st phase, then start
        // outputing some partition downstream in order to free memory.
        if ( earlyOutput ) {
          outputCurrentBatch();
          return AggOutcome.RETURN_OUTCOME;
        }
      }

      if (EXTRA_DEBUG_1) {
        logger.debug("Processed {} records", underlyingIndex);
      }

      // Cleanup the previous batch since we are done processing it.
      for (VectorWrapper<?> v : incoming) {
        v.getValueVector().clear();
      }

      if ( handleEmit ) {
        outcome = IterOutcome.NONE; // finished behaving like OK, now behave like NONE
      }
      else {
        //
        // Get the NEXT input batch, initially from the upstream, later (if there was a spill)
        // from one of the spill files (The spill case is handled differently here to avoid
        // collecting stats on the spilled records)
        //
        long memAllocBeforeNext = allocator.getAllocatedMemory();
        if (handlingSpills) {
          outcome = incoming.next(); // get it from the SpilledRecordBatch
        } else {
          // Get the next RecordBatch from the incoming (i.e. upstream operator)
          outcome = outgoing.next(0, incoming);
        }
        long memAllocAfterNext = allocator.getAllocatedMemory();
        long incomingBatchSize = memAllocAfterNext - memAllocBeforeNext;

        // If incoming batch is bigger than our estimate - adjust the estimate to match
        if (estMaxBatchSize < incomingBatchSize) {
          logger.debug("Found a bigger next {} batch: {} , prior estimate was: {}, mem allocated {}", handlingSpills ? "spill" : "incoming", incomingBatchSize, estMaxBatchSize, memAllocAfterNext);
          estMaxBatchSize = incomingBatchSize;
        }

        if (EXTRA_DEBUG_1) {
          logger.debug("Received IterOutcome of {}", outcome);
        }
      }
      // Handle various results from getting the next batch
      switch (outcome) {
        case OUT_OF_MEMORY:
        case NOT_YET:
          return AggOutcome.RETURN_OUTCOME;

        case OK_NEW_SCHEMA:
          if (EXTRA_DEBUG_1) {
            logger.debug("Received new schema.  Batch has {} records.", incoming.getRecordCount());
          }
          this.cleanup();
          // TODO: new schema case needs to be handled appropriately
          return AggOutcome.UPDATE_AGGREGATOR;

        case EMIT:
          handleEmit = true;
          // remember EMIT, but continue like handling OK

        case OK:
          // NOTE - We pass the incoming record batch explicitly because it could be a spilled record (different
          //        from the instance owned by the HashAggBatch).
          outgoing.getRecordBatchMemoryManager().update(incoming);

          currentBatchRecordCount = incoming.getRecordCount(); // size of next batch

          resetIndex(); // initialize index (a new batch needs to be processed)

          if (EXTRA_DEBUG_1) {
            logger.debug("Continue to start processing the next batch");
          }
          break;

        case NONE:
          resetIndex(); // initialize index (in case spill files need to be processed)

          // Either flag buildComplete or handleEmit (or earlyOutput) would cause returning of
          // the outgoing batch downstream (see innerNext() in HashAggBatch).
          buildComplete = true; // now should go and return outgoing

          if ( handleEmit ) {
            buildComplete = false; // This was not a real NONE - more incoming is expected
            // don't aggregate this incoming batch again (in the loop above; when doWork() is called again)
            currentBatchRecordCount = 0;
          }
          updateStats();

          // output the first batch; remaining batches will be output
          // in response to each next() call by a downstream operator
          AggIterOutcome aggOutcome = outputCurrentBatch();

          switch ( aggOutcome ) {
            case AGG_RESTART:
              // Output of first batch returned a RESTART (all new partitions were spilled)
              return AggOutcome.CALL_WORK_AGAIN; // need to read/process the next partition
            case AGG_EMIT:
              // Following an incoming EMIT, if the output was only a single batch
              // outcome is set to IterOutcome.EMIT;
              break;
            case AGG_NONE: // no output
              break;
            default:
              // Regular output (including after EMIT, when more output batches are planned)
              outcome = IterOutcome.OK;
          }

          return AggOutcome.RETURN_OUTCOME;

        case STOP:
        default:
          return AggOutcome.CLEANUP_AND_RETURN;
      }
    }
  }

  /**
   *   Allocate space for the returned aggregate columns
   *   (Note DRILL-5588: Maybe can eliminate this allocation (and copy))
   * @param records
   */
  @Override
  public void allocateOutgoing(int records) {
    // Skip the keys and only allocate for outputting the workspace values
    // (keys will be output through splitAndTransfer)
    Iterator<VectorWrapper<?>> outgoingIter = outContainer.iterator();
    for (int i = 0; i < numGroupByOutFields; i++) {
      outgoingIter.next();
    }

    // try to preempt an OOM by using the reserved memory
    memoryCalculator.useReservedOutgoingMemory();
    long allocatedBefore = allocator.getAllocatedMemory();

    while (outgoingIter.hasNext()) {
      @SuppressWarnings("resource")
      ValueVector vv = outgoingIter.next().getValueVector();

      AllocationHelper.allocatePrecomputedChildCount(vv, records, maxColumnWidth, 0);
    }

    long memAdded = allocator.getAllocatedMemory() - allocatedBefore;
    if ( memAdded > estOutgoingAllocSize ) {
      logger.trace("Output values allocated {} but the estimate was only {}. Adjusting ...",memAdded,estOutgoingAllocSize);
      estOutgoingAllocSize = memAdded;
    }
    outContainer.setRecordCount(records);
    // try to restore the reserve
    memoryCalculator.restoreReservedMemory();
  }

  @Override
  public IterOutcome getOutcome() {
    return outcome;
  }

  @Override
  public int getOutputCount() {
    return lastBatchOutputCount;
  }

  @Override
  public void cleanup() {
    if ( schema == null ) { return; } // not set up; nothing to clean
    if ( phase.is2nd() && spillSet.getWriteBytes() > 0 ) {
      stats.setLongStat(Metric.SPILL_MB, // update stats - total MB spilled
          (int) Math.round(spillSet.getWriteBytes() / 1024.0D / 1024.0));
    }

    // clean (and deallocate) each partition
    for (HashAggPartition partition: partitions) {
      partition.cleanup();
    }

    // delete any spill file left in unread spilled partitions
    while (!spilledState.isEmpty()) {
        HashAggSpilledPartition sp = spilledState.getNextSpilledPartition();
        try {
          spillSet.delete(sp.getSpillFile());
        } catch(IOException e) {
          logger.warn("Cleanup: Failed to delete spill file {}",sp.getSpillFile());
        }
    }
    // Delete the currently handled (if any) spilled file
    if ( newIncoming != null ) { newIncoming.close();  }
    spillSet.close(); // delete the spill directory(ies)
    htIdxHolder = null;
    materializedValueFields = null;
  }

  private final void incIndex() {
    underlyingIndex++;
    if (underlyingIndex >= currentBatchRecordCount) {
      currentIndex = Integer.MAX_VALUE;
      return;
    }
    try { currentIndex = getVectorIndex(underlyingIndex); }
    catch (SchemaChangeException sc) { throw new UnsupportedOperationException(sc);}
  }

  private final void resetIndex() {
    underlyingIndex = -1; // will become 0 in incIndex()
    incIndex();
  }

  /**
   * Which partition to choose for flushing out (i.e. spill or return) ?
   * - The current partition (to which a new bach holder is added) has a priority,
   *   because its last batch holder is full.
   * - Also the largest prior spilled partition has some priority, as it is already spilled;
   *   but spilling too few rows (e.g. a single batch) gets us nothing.
   * - So the largest non-spilled partition has some priority, to get more memory freed.
   * Need to weigh the above three options.
   *
   *  @param currPart - The partition that hit the memory limit (gets a priority)
   *  @param tryAvoidCurr - When true, give negative priority to the current partition
   * @return The partition (number) chosen to be spilled
   */
  private int chooseAPartitionToFlush(final int currPart, boolean tryAvoidCurr) {
    if ( phase.is1st() && ! tryAvoidCurr) { return currPart; } // 1st phase: just use the current partition

    final HashAggPartition currentPartition = partitions[currPart];
    int currPartSize = currentPartition.getBatchHolderCount();
    if ( currPartSize == 1 ) { currPartSize = -1; } // don't pick current if size is 1
    // first find the largest spilled partition
    int maxSizeSpilled = -1;
    int indexMaxSpilled = -1;
    for (int isp = 0; isp < spilledState.getNumPartitions(); isp++ ) {
      final HashAggPartition partition = partitions[isp];

      if ( partition.isSpilled() && maxSizeSpilled < partition.getBatchHolderCount() ) {
        maxSizeSpilled = partition.getBatchHolderCount();
        indexMaxSpilled = isp;
      }
    }
    // Give the current (if already spilled) some priority
    if ( ! tryAvoidCurr && currentPartition.isSpilled() && ( currPartSize + 1 >= maxSizeSpilled )) {
      maxSizeSpilled = currPartSize;
      indexMaxSpilled = currPart;
    }
    // now find the largest non-spilled partition
    int maxSize = -1;
    int indexMax = -1;
    // Use the largest spilled (if found) as a base line, with a factor of 4
    if ( indexMaxSpilled > -1 && maxSizeSpilled > 1 ) {
      indexMax = indexMaxSpilled;
      maxSize = 4 * maxSizeSpilled;
    }
    for ( int insp = 0; insp < spilledState.getNumPartitions(); insp++) {
      final HashAggPartition partition = partitions[insp];

      if ( !partition.isSpilled() && maxSize < partition.getBatchHolderCount() ) {
        indexMax = insp;
        maxSize = partition.getBatchHolderCount();
      }
    }
    // again - priority to the current partition
    if ( ! tryAvoidCurr && ! currentPartition.isSpilled() && (currPartSize + 1 >= maxSize) ) {
      return currPart;
    }
    if ( maxSize <= 1 ) { // Can not make progress by spilling a single batch!
      return -1; // try skipping this spill
    }
    return indexMax;
  }

  // These methods are overridden in the generated class when created as plain Java code.
  protected BatchHolder newBatchHolder(int batchRowCount) {
    return this.injectMembers(new BatchHolder(batchRowCount));
  }

  protected BatchHolder injectMembers(BatchHolder batchHolder) {
    CodeGenMemberInjector.injectMembers(cg, batchHolder, context);
    return batchHolder;
  }

  /**
   * Output the next batch from partition "nextPartitionToReturnIndex"
   *
   * @return iteration outcome (e.g., OK, NONE ...)
   */
  @Override
  public AggIterOutcome outputCurrentBatch() {
    // Handle the case of an EMIT with an empty batch
    if ( handleEmit && ( partitions.length == 0 || partitions[0].getBatchHolderCount() == 0 ) ) {
      lastBatchOutputCount = 0; // empty
      allocateOutgoing(0);
      for (VectorWrapper<?> v : outgoing) {
        v.getValueVector().getMutator().setValueCount(0);
      }
      outgoing.getContainer().setRecordCount(0);
      // When returning the last outgoing batch (following an incoming EMIT), then replace OK with EMIT
      this.outcome = IterOutcome.EMIT;
      handleEmit = false; // finish handling EMIT

      if (partitions.length > 0) {
        partitions[0].resetOutBatchIndex();
      }

      return AggIterOutcome.AGG_EMIT;
    }

    // when incoming was an empty batch, just finish up
    if ( schema == null ) {
      logger.trace("Incoming was empty; output is an empty batch.");
      this.outcome = IterOutcome.NONE; // no records were read
      allFlushed = true;
      return AggIterOutcome.AGG_NONE;
    }

    // Initialization (covers the case of early output)
    HashAggPartition partitionToReturn = partitions[earlyPartition];

    if ( ! earlyOutput ) {
      // Update the next partition to return (if needed)
      // skip fully returned (or spilled) partitions
      while (nextPartitionToReturnIndex < spilledState.getNumPartitions()) {
        final HashAggPartition nextPartitionToReturn = partitions[nextPartitionToReturnIndex];

        if ( nextPartitionToReturn.isSpilled() ) {
          // If this partition was spilled - spill the rest of it and skip it
          nextPartitionToReturn.finishSpilling(originalPartition);
        } else if (nextPartitionToReturn.hasPendingRows()) {
          // Emit the pending rows in this partition
          partitionToReturn = nextPartitionToReturn;
          break;
        }
        nextPartitionToReturnIndex++; // else check next partition
      }

      // if passed the last partition - either done or need to restart and read spilled partitions
      if (nextPartitionToReturnIndex >= spilledState.getNumPartitions()) {
        // The following "if" is probably never used; due to a similar check at the end of this method
        if (spilledState.isEmpty()) { // and no spilled partitions
          allFlushed = true;
          this.outcome = IterOutcome.NONE;
          if ( phase.is2nd() && spillSet.getWriteBytes() > 0 ) {
            stats.setLongStat(Metric.SPILL_MB, // update stats - total MB spilled
                (int) Math.round(spillSet.getWriteBytes() / 1024.0D / 1024.0));
          }
          return AggIterOutcome.AGG_NONE;  // then return NONE
        }
        // Else - there are still spilled partitions to process - pick one and handle just like a new incoming
        buildComplete = false; // go back and call doWork() again
        handlingSpills = true; // beginning to work on the spill files
        // pick a spilled partition; set a new incoming ...
        HashAggSpilledPartition sp = spilledState.getNextSpilledPartition();
        // Create a new "incoming" out of the spilled partition spill file
        newIncoming = new SpilledRecordbatch(sp.getSpillFile(), sp.getSpilledBatches(), context, schema, oContext, spillSet);
        originalPartition = sp.getOriginPartition(); // used for the filename
        logger.trace("Reading back spilled original partition {} as an incoming",originalPartition);
        // Initialize .... new incoming, new set of partitions
        try {
          initializeSetup(newIncoming);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }

        spilledState.updateCycle(stats, sp, updater);
        return AggIterOutcome.AGG_RESTART;
      }
    }

    this.outcome = IterOutcome.OK;
    lastBatchOutputCount = partitionToReturn.outputCurrentBatch();

    // if just flushed the last batch in the partition
    if (partitionToReturn.doneOutputting()) {
      // deallocate memory used by this partition, and re-initialize
      partitionToReturn.reinitPartition();

      if ( earlyOutput ) {

        if ( EXTRA_DEBUG_SPILL ) {
          logger.debug("HASH AGG: Finished (early) re-init partition {}, mem allocated: {}", earlyPartition, allocator.getAllocatedMemory());
        }

        earlyOutput = false; // done with early output
      }
      else if ( handleEmit ) {
        // When returning the last outgoing batch (following an incoming EMIT), then replace OK with EMIT
        this.outcome = IterOutcome.EMIT;
        handleEmit = false; // finished handling EMIT
        return AggIterOutcome.AGG_EMIT;
      }
      else if ((partitionToReturn.getPartitionIndex() + 1 == spilledState.getNumPartitions()) && spilledState.isEmpty()) { // last partition ?

        allFlushed = true; // next next() call will return NONE

        logger.trace("HashAggregate: All batches flushed.");

        // cleanup my internal state since there is nothing more to return
        this.cleanup();
      }
    }

    return AggIterOutcome.AGG_OK;
  }

  @Override
  public boolean allFlushed() {
    return allFlushed;
  }

  @Override
  public boolean buildComplete() {
    return buildComplete;
  }

  @Override
  public boolean handlingEmit() {
    return handleEmit;
  }

  @Override
  public boolean earlyOutput() { return earlyOutput; }

  public int numGroupedRecords() {
    return numGroupedRecords;
  }

  /**
   *  Generate a detailed error message in case of "Out Of Memory"
   * @return err msg
   * @param prefix
   */
  private String getOOMErrorMsg(String prefix) {
    String errmsg;
    if (!phase.hasTwo()) {
      errmsg = "Single Phase Hash Aggregate operator can not spill.";
    } else if (!canSpill) {  // 2nd phase, with only 1 partition
      errmsg = "Too little memory available to operator to facilitate spilling.";
    } else { // a bug ?
      errmsg = prefix + " OOM at " + phase.getName() + " Phase. Partitions: " + spilledState.getNumPartitions() +
      ". Estimated batch size: " + estMaxBatchSize + ". values size: " + estValuesBatchSize + ". Output alloc size: " + estOutgoingAllocSize;
      if ( plannedBatches > 0 ) { errmsg += ". Planned batches: " + plannedBatches; }
    }
    errmsg += " Memory limit: " + allocator.getLimit() + " so far allocated: " + allocator.getAllocatedMemory() + ". ";

    return errmsg;
  }

  private int getTargetBatchCount() {
    return outgoing.getOutputRowCount();
  }

  // Check if a group is present in the hash table; if not, insert it in the hash table.
  // The htIdxHolder contains the index of the group in the hash table container; this same
  // index is also used for the aggregation values maintained by the hash aggregate.
  private void checkGroupAndAggrValues(int incomingRowIdx) {
    assert incomingRowIdx >= 0;
    assert ! earlyOutput;

    // The hash code is computed once, then its lower bits are used to determine the
    // partition to use, and the higher bits determine the location in the hash table.
    int hashCode;
    try {
      hashCode = partitions[0].buildHashcode(incomingRowIdx);
    } catch (SchemaChangeException e) {
      throw new UnsupportedOperationException("Unexpected schema change", e);
    }

    // right shift hash code for secondary (or tertiary...) spilling
    for (int i = 0; i < spilledState.getCycle(); i++) {
      hashCode >>>= spilledState.getBitsInMask();
    }

    final int currentPartitionIndex = hashCode & spilledState.getPartitionMask();
    final HashAggPartition currentPartition = partitions[currentPartitionIndex];
    hashCode >>>= spilledState.getBitsInMask();
    HashTable.PutStatus putStatus;
    long allocatedBeforeHTput = allocator.getAllocatedMemory();
    String tryingTo = phase.is1st() ? "early return" : "spill";

    // Proactive spill - in case there is no reserve memory - spill and retry putting later
    if ( reserveValueBatchMemory == 0 && canSpill ) {
      logger.trace("Reserved memory runs short, trying to {} a partition and retry Hash Table put() again.", tryingTo);

      doSpill(currentPartitionIndex); // spill to free some memory

      retrySameIndex = true;
      return; // to retry this put()
    }

    // ==========================================
    // Insert the key columns into the hash table
    // ==========================================
    try {
      putStatus = currentPartition.put(incomingRowIdx, htIdxHolder, hashCode, getTargetBatchCount());
    } catch (RetryAfterSpillException re) {
      if ( ! canSpill ) { throw new OutOfMemoryException(getOOMErrorMsg("Can not spill")); }

      logger.trace("HT put failed with an OOM, trying to {} a partition and retry Hash Table put() again.", tryingTo);

      // for debugging - in case there's a leak
      long memDiff = allocator.getAllocatedMemory() - allocatedBeforeHTput;
      if ( memDiff > 0 ) { logger.warn("Leak: HashTable put() OOM left behind {} bytes allocated",memDiff); }

      doSpill(currentPartitionIndex); // spill to free some memory

      retrySameIndex = true;
      return; // to retry this put()
    } catch (OutOfMemoryException exc) {
        throw new OutOfMemoryException(getOOMErrorMsg("HT was: " + allocatedBeforeHTput), exc);
    } catch (SchemaChangeException e) {
        throw new UnsupportedOperationException("Unexpected schema change", e);
    }
    long allocatedBeforeAggCol = allocator.getAllocatedMemory();
    boolean needToCheckIfSpillIsNeeded = allocatedBeforeAggCol > allocatedBeforeHTput;

    // Add an Aggr batch if needed:
    //
    //       In case put() added a new batch (for the keys) inside the hash table,
    //       then a matching batch (for the aggregate columns) needs to be created
    //
    if ( putStatus == HashTable.PutStatus.NEW_BATCH_ADDED ) {
      try {

        memoryCalculator.useReservedValuesMemory(); // try to preempt an OOM by using the reserve

        final BatchHolder batchHolder = newBatchHolder(getTargetBatchCount());
        currentPartition.addBatchHolder(batchHolder);  // allocate a new (internal) values batch

        memoryCalculator.restoreReservedMemory(); // restore the reserve, if possible
        // A reason to check for a spill - In case restore-reserve failed
        needToCheckIfSpillIsNeeded = ( 0 == reserveValueBatchMemory );

        if ( plannedBatches > 0 ) { plannedBatches--; } // just allocated a planned batch
        long totalAddedMem = allocator.getAllocatedMemory() - allocatedBeforeHTput;
        long aggValuesAddedMem = allocator.getAllocatedMemory() - allocatedBeforeAggCol;
        logger.trace("MEMORY CHECK AGG: allocated now {}, added {}, total (with HT) added {}", allocator.getAllocatedMemory(),
            aggValuesAddedMem, totalAddedMem);
        // resize the batch estimates if needed (e.g., varchars may take more memory than estimated)
        if (totalAddedMem > estMaxBatchSize) {
          logger.trace("Adjusting Batch size estimate from {} to {}", estMaxBatchSize, totalAddedMem);
          estMaxBatchSize = totalAddedMem;
          needToCheckIfSpillIsNeeded = true;
        }
        if (aggValuesAddedMem > estValuesBatchSize) {
          logger.trace("Adjusting Values Batch size from {} to {}",estValuesBatchSize, aggValuesAddedMem);
          estValuesBatchSize = aggValuesAddedMem;
          needToCheckIfSpillIsNeeded = true;
        }
      } catch (OutOfMemoryException exc) {
          throw new OutOfMemoryException(getOOMErrorMsg("AGGR"), exc);
      }
    } else if ( putStatus == HashTable.PutStatus.KEY_ADDED_LAST ) {
        // If a batch just became full (i.e. another batch would be allocated soon) -- then need to
        // check (later, see below) if the memory limits are too close, and if so -- then spill !
        plannedBatches++; // planning to allocate one more batch
        needToCheckIfSpillIsNeeded = true;
    }

    // =================================================================
    // Locate the matching aggregate columns and perform the aggregation
    // =================================================================
    int currentIdx = htIdxHolder.value;
    HashAggBatchHolder bh = currentPartition.getBatchHolder((currentIdx >>> 16) & BATCH_MASK);
    int idxWithinBatch = currentIdx & BATCH_MASK;
    if (bh.updateAggrValues(incomingRowIdx, idxWithinBatch)) {
      numGroupedRecords++;
    }

    // ===================================================================================
    // If the last batch just became full, or other "memory growing" events happened, then
    // this is the time to check the memory limits !!
    // If the limits were exceeded, then need to spill (if 2nd phase) or output early (1st)
    // (Skip this if cannot spill, or not checking memory limits; in such case an OOM may
    // be encountered later - and some OOM cases are recoverable by spilling and retrying)
    // ===================================================================================
    if ( needToCheckIfSpillIsNeeded && canSpill && useMemoryPrediction ) {
      spillIfNeeded(currentPartitionIndex);
    }
  }

  private void spillIfNeeded(int currentPartition) { spillIfNeeded(currentPartition, false);}
  private void doSpill(int currentPartition) { spillIfNeeded(currentPartition, true);}
  /**
   *  Spill (or return early, if 1st phase) if too little available memory is left
   *  @param currentPartitionIndex - the preferred candidate for spilling
   * @param forceSpill -- spill unconditionally (no memory checks)
   */
  private void spillIfNeeded(int currentPartitionIndex, boolean forceSpill) {
    final HashAggPartition currentPartition = partitions[currentPartitionIndex];

    long maxMemoryNeeded = 0;
    if ( !forceSpill ) { // need to check the memory in order to decide
      // calculate the (max) new memory needed now; plan ahead for at least MIN batches
      maxMemoryNeeded = minBatchesPerPartition * Math.max(1, plannedBatches) * (estMaxBatchSize + MAX_BATCH_ROW_COUNT * (4 + 4 /* links + hash-values */));
      // Add the (max) size of the current hash table, in case it will double
      int maxSize = 1;
      for (HashAggPartition hashAggPartition: partitions) {
        maxSize = Math.max(maxSize, hashAggPartition.getBatchHolderCount());
      }
      maxMemoryNeeded += MAX_BATCH_ROW_COUNT * 2 * 2 * 4 * maxSize; // 2 - double, 2 - max when %50 full, 4 - Uint4

      // log a detailed debug message explaining why a spill may be needed
      logger.trace("MEMORY CHECK: Allocated mem: {}, agg phase: {}, trying to add to partition {} with {} batches. " + "Max memory needed {}, Est batch size {}, mem limit {}",
          allocator.getAllocatedMemory(), phase.getName(), currentPartitionIndex, currentPartition.getBatchHolderCount(), maxMemoryNeeded,
          estMaxBatchSize, allocator.getLimit());
    }
    //
    //   Spill if (forced, or) the allocated memory plus the memory needed exceed the memory limit.
    //
    if ( forceSpill || allocator.getAllocatedMemory() + maxMemoryNeeded > allocator.getLimit() ) {

      // Pick a "victim" partition to spill or return
      int victimPartitionIndex = chooseAPartitionToFlush(currentPartitionIndex, forceSpill);

      // In case no partition has more than one batch and
      // non-forced spill -- try and "push the limits";
      // maybe next time the spill could work.
      if (victimPartitionIndex < 0) {
        // In the case of the forced spill, there is not enough memory to continue.
        // Throws OOM to avoid the infinite loop.
        if (forceSpill) {
          throw new OutOfMemoryException(getOOMErrorMsg("AGGR"));
        }
        return;
      }

      if ( phase.is2nd() ) {
        long before = allocator.getAllocatedMemory();

        final HashAggPartition victimPartition = partitions[victimPartitionIndex];
        victimPartition.spill();
        logger.trace("RAN OUT OF MEMORY: Spilled partition {}",victimPartitionIndex);

        // Re-initialize (free memory, then recreate) the partition just spilled/returned
        victimPartition.reinitPartition();

        // In case spilling did not free enough memory to recover the reserves
        boolean spillAgain = reserveOutgoingMemory == 0 || reserveValueBatchMemory == 0;
        // in some "edge" cases (e.g. testing), spilling one partition may not be enough
        if ( spillAgain || allocator.getAllocatedMemory() + maxMemoryNeeded > allocator.getLimit() ) {
          int victimPartition2Index = chooseAPartitionToFlush(victimPartitionIndex, true);
          final HashAggPartition victimPartition2 = partitions[victimPartition2Index];

          if (victimPartition2Index < 0) {
            // In the case of the forced spill, there is not enough memory to continue.
            // Throws OOM to avoid the infinite loop.
            if (forceSpill) {
              throw new OutOfMemoryException(getOOMErrorMsg("AGGR"));
            }
            return;
          }
          long after = allocator.getAllocatedMemory();
          victimPartition2.spill();
          victimPartition2.reinitPartition();
          logger.warn("A Second Spill was Needed: allocated before {}, after first spill {}, after second {}, memory needed {}",
              before, after, allocator.getAllocatedMemory(), maxMemoryNeeded);
          logger.trace("Second Partition Spilled: {}",victimPartition2Index);
        }
      }
      else {
        // 1st phase need to return a partition early in order to free some memory
        earlyOutput = true;
        earlyPartition = victimPartitionIndex;

        if ( EXTRA_DEBUG_SPILL ) {
          logger.debug("picked partition {} for early output", victimPartitionIndex);
        }
      }
    }
  }

  /**
   * Updates the stats at the time after all the input was read.
   * Note: For spilled partitions, their hash-table stats from before the spill are lost.
   * And the SPILLED_PARTITIONS only counts the spilled partitions in the primary, not SECONDARY etc.
   */
  private void updateStats() {
    if (!spilledState.isFirstCycle() ||  // These stats are only for before processing spilled files
      handleEmit ) { return; } // and no stats collecting when handling an EMIT
    long numSpilled = 0;

    // sum the stats from all the partitions
    for (HashAggPartition partition: partitions) {
      partition.addStats(htStats);

      if (partition.isSpilled()) {
        numSpilled++;
      }
    }
    this.stats.setLongStat(Metric.NUM_BUCKETS, htStats.numBuckets);
    this.stats.setLongStat(Metric.NUM_ENTRIES, htStats.numEntries);
    this.stats.setLongStat(Metric.NUM_RESIZING, htStats.numResizing);
    this.stats.setLongStat(Metric.RESIZING_TIME_MS, htStats.resizingTime);
    this.stats.setLongStat(Metric.NUM_PARTITIONS, spilledState.getNumPartitions());
    this.stats.setLongStat(Metric.SPILL_CYCLE, spilledState.getCycle()); // Put 0 in case no spill
    if ( phase.is2nd() ) {
      this.stats.setLongStat(Metric.SPILLED_PARTITIONS, numSpilled);
    }
    if ( rowsReturnedEarly > 0 ) {
      stats.setLongStat(Metric.SPILL_MB, // update stats - est. total MB returned early
          (int) Math.round( rowsReturnedEarly * estOutputRowWidth / 1024.0D / 1024.0));
    }
  }

  @Override
  public String toString() {
    // The fields are excluded because they are passed from HashAggBatch
    String[] excludedFields = new String[] {
        "baseHashTable", "incoming", "outgoing", "context", "oContext", "allocator", "htables", "newIncoming"};
    return ReflectionToStringBuilder.toStringExclude(this, excludedFields);
  }

  // Code-generated methods (implemented in HashAggBatch)
  public abstract void doSetup(@Named("incoming") RecordBatch incoming) throws SchemaChangeException;

  public abstract int getVectorIndex(@Named("recordIndex") int recordIndex) throws SchemaChangeException;

  public abstract boolean resetValues() throws SchemaChangeException;

}
