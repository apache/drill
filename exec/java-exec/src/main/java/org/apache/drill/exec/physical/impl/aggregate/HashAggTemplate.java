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
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Named;

import com.google.common.base.Stopwatch;
import org.apache.drill.common.exceptions.RetryAfterSpillException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.cache.VectorAccessibleSerializable;
import org.apache.drill.exec.compile.sig.RuntimeOverridden;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
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
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.physical.impl.common.HashTableConfig;
import org.apache.drill.exec.physical.impl.common.HashTableStats;
import org.apache.drill.exec.physical.impl.common.IndexPointer;

import org.apache.drill.exec.physical.impl.spill.RecordBatchSizer;

import org.apache.drill.exec.physical.impl.spill.SpillSet;
import org.apache.drill.exec.planner.physical.AggPrelBase;

import org.apache.drill.exec.record.MaterializedField;

import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.BatchSchema;

import org.apache.drill.exec.record.VectorContainer;

import org.apache.drill.exec.record.TypedFieldId;

import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;

import org.apache.drill.exec.vector.AllocationHelper;

import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.ObjectVector;
import org.apache.drill.exec.vector.ValueVector;

import org.apache.drill.exec.vector.VariableWidthVector;

import static org.apache.drill.exec.record.RecordBatch.MAX_BATCH_SIZE;

public abstract class HashAggTemplate implements HashAggregator {
  protected static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashAggregator.class);

  private static final int VARIABLE_MAX_WIDTH_VALUE_SIZE = 50;
  private static final int VARIABLE_MIN_WIDTH_VALUE_SIZE = 8;

  private static final boolean EXTRA_DEBUG_1 = false;
  private static final boolean EXTRA_DEBUG_2 = false;
  private static final boolean EXTRA_DEBUG_SPILL = false;

  // Fields needed for partitioning (the groups into partitions)
  private int numPartitions = 0; // must be 2 to the power of bitsInMask (set in setup())
  private int partitionMask; // numPartitions - 1
  private int bitsInMask; // number of bits in the MASK
  private int nextPartitionToReturn = 0; // which partition to return the next batch from
  // The following members are used for logging, metrics, etc.
  private int rowsInPartition = 0; // counts #rows in each partition
  private int rowsNotSpilled = 0;
  private int rowsSpilled = 0;
  private int rowsSpilledReturned = 0;
  private int rowsReturnedEarly = 0;

  private boolean isTwoPhase = false; // 1 phase or 2 phase aggr?
  private boolean is2ndPhase = false;
  private boolean is1stPhase = false;
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

  private FragmentContext context;
  private OperatorContext oContext;
  private BufferAllocator allocator;

  private HashTable htables[];
  private ArrayList<BatchHolder> batchHolders[];
  private int outBatchIndex[];

  // For handling spilling
  private SpillSet spillSet;
  SpilledRecordbatch newIncoming; // when reading a spilled file - work like an "incoming"
  private OutputStream outputStream[]; // an output stream for each spilled partition
  private int spilledBatchesCount[]; // count number of batches spilled, in each partition
  private String spillFiles[];
  private int cycleNum = 0; // primary, secondary, tertiary, etc.
  private int originalPartition = -1; // the partition a secondary reads from

  private static class SpilledPartition { public int spilledBatches; public String spillFile; int cycleNum; int origPartn; int prevOrigPartn; }

  private ArrayList<SpilledPartition> spilledPartitionsList;
  private int operatorId; // for the spill file name

  private IndexPointer htIdxHolder; // holder for the Hashtable's internal index returned by put()
  private IndexPointer outStartIdxHolder;
  private IndexPointer outNumRecordsHolder;
  private int numGroupByOutFields = 0; // Note: this should be <= number of group-by fields
  private TypedFieldId[] groupByOutFieldIds;

  private MaterializedField[] materializedValueFields;
  private boolean allFlushed = false;
  private boolean buildComplete = false;
  private boolean handlingSpills = false; // True once starting to process spill files

  private OperatorStats stats = null;
  private HashTableStats htStats = new HashTableStats();

  public enum Metric implements MetricDef {

    NUM_BUCKETS,
    NUM_ENTRIES,
    NUM_RESIZING,
    RESIZING_TIME_MS,
    NUM_PARTITIONS,
    SPILLED_PARTITIONS, // number of partitions spilled to disk
    SPILL_MB,         // Number of MB of data spilled to disk. This amount is first written,
                      // then later re-read. So, disk I/O is twice this amount.
                      // For first phase aggr -- this is an estimate of the amount of data
                      // returned early (analogous to a spill in the 2nd phase).
    SPILL_CYCLE       // 0 - no spill, 1 - spill, 2 - SECONDARY, 3 - TERTIARY
    ;

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

    @SuppressWarnings("resource")
    public BatchHolder() {

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
            // This case is never used .... a varchar falls under ObjectVector which is allocated on the heap !
            ((VariableWidthVector) vector).allocateNew(maxColumnWidth, HashTable.BATCH_SIZE);
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
      try { updateAggrValuesInternal(incomingRowIdx, idxWithinBatch); }
      catch (SchemaChangeException sc) { throw new UnsupportedOperationException(sc); }
      maxOccupiedIdx = Math.max(maxOccupiedIdx, idxWithinBatch);
      return true;
    }

    private void setup() {
      try { setupInterior(incoming, outgoing, aggrValuesContainer); }
      catch (SchemaChangeException sc) { throw new UnsupportedOperationException(sc);}
    }

    private void outputValues(IndexPointer outStartIdxHolder, IndexPointer outNumRecordsHolder) {
      outStartIdxHolder.value = batchOutputCount;
      outNumRecordsHolder.value = 0;
      for (int i = batchOutputCount; i <= maxOccupiedIdx; i++) {
        try { outputRecordValues(i, batchOutputCount); }
        catch (SchemaChangeException sc) { throw new UnsupportedOperationException(sc);}
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
  public void setup(HashAggregate hashAggrConfig, HashTableConfig htConfig, FragmentContext context, OperatorContext oContext, RecordBatch incoming, HashAggBatch outgoing, LogicalExpression[] valueExprs, List<TypedFieldId> valueFieldIds, TypedFieldId[] groupByOutFieldIds, VectorContainer outContainer, int extraRowBytes) throws SchemaChangeException, IOException {

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
    this.outContainer = outContainer;
    this.operatorId = hashAggrConfig.getOperatorId();
    this.useMemoryPrediction = context.getOptions().getOption(ExecConstants.HASHAGG_USE_MEMORY_PREDICTION_VALIDATOR);

    is2ndPhase = hashAggrConfig.getAggPhase() == AggPrelBase.OperatorPhase.PHASE_2of2;
    isTwoPhase = hashAggrConfig.getAggPhase() != AggPrelBase.OperatorPhase.PHASE_1of1;
    is1stPhase = isTwoPhase && ! is2ndPhase ;
    canSpill = isTwoPhase; // single phase can not spill

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
    numPartitions = (int)context.getOptions().getOption(ExecConstants.HASHAGG_NUM_PARTITIONS_VALIDATOR);
    if ( numPartitions == 1 && is2ndPhase  ) { // 1st phase can still do early return with 1 partition
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
    reserveValueBatchMemory = reserveOutgoingMemory = estValuesBatchSize ;
    long newMemoryLimit = allocator.getLimit() - reserveValueBatchMemory - reserveOutgoingMemory ;
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
          if (is2ndPhase) {
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
    logger.debug("{} phase. Number of partitions chosen: {}. {} spill", isTwoPhase?(is2ndPhase?"2nd":"1st"):"Single",
        numPartitions, canSpill ? "Can" : "Cannot");

    // The following initial safety check should be revisited once we can lower the number of rows in a batch
    // In cases of very tight memory -- need at least memory to process one batch, plus overhead (e.g. hash table)
    if ( numPartitions == 1 && ! canSpill ) {
      // if too little memory - behave like the old code -- practically no memory limit for hash aggregate
      // (but 1st phase can still spill, so it will maintain the original memory limit)
      allocator.setLimit(AbstractBase.MAX_ALLOCATION);  // 10_000_000_000L
    }
    // Based on the number of partitions: Set the mask and bit count
    partitionMask = numPartitions - 1; // e.g. 32 --> 0x1F
    bitsInMask = Integer.bitCount(partitionMask); // e.g. 0x1F -> 5

    // Create arrays (one entry per partition)
    htables = new HashTable[numPartitions] ;
    batchHolders = (ArrayList<BatchHolder>[]) new ArrayList<?>[numPartitions] ;
    outBatchIndex = new int[numPartitions] ;
    outputStream = new OutputStream[numPartitions];
    spilledBatchesCount = new int[numPartitions];
    spillFiles = new String[numPartitions];
    spilledPartitionsList = new ArrayList<SpilledPartition>();

    plannedBatches = numPartitions; // each partition should allocate its first batch

    // initialize every (per partition) entry in the arrays
    for (int i = 0; i < numPartitions; i++ ) {
      try {
        this.htables[i] = baseHashTable.createAndSetupHashTable(groupByOutFieldIds, numPartitions);
        this.htables[i].setMaxVarcharSize(maxColumnWidth);
      } catch (ClassTransformationException e) {
        throw UserException.unsupportedError(e)
            .message("Code generation error - likely an error in the code.")
            .build(logger);
      } catch (IOException e) {
        throw UserException.resourceError(e)
            .message("IO Error while creating a hash table.")
            .build(logger);
      } catch (SchemaChangeException sce) {
        throw new IllegalStateException("Unexpected Schema Change while creating a hash table",sce);
      }
      this.batchHolders[i] = new ArrayList<BatchHolder>(); // First BatchHolder is created when the first put request is received.
    }
    // Initialize the value vectors in the generated code (which point to the incoming or outgoing fields)
    try { htables[0].updateBatches(); } catch (SchemaChangeException sc) { throw new UnsupportedOperationException(sc); };
  }
  /**
   * get new incoming: (when reading spilled files like an "incoming")
   * @return The (newly replaced) incoming
   */
  @Override
  public RecordBatch getNewIncoming() { return newIncoming; }

  private void initializeSetup(RecordBatch newIncoming) throws SchemaChangeException, IOException {
    baseHashTable.updateIncoming(newIncoming); // after a spill - a new incoming
    this.incoming = newIncoming;
    currentBatchRecordCount = newIncoming.getRecordCount(); // first batch in this spill file
    nextPartitionToReturn = 0;
    for (int i = 0; i < numPartitions; i++ ) {
      htables[i].reinit(newIncoming);
      if ( batchHolders[i] != null) {
        for (BatchHolder bh : batchHolders[i]) {
          bh.clear();
        }
        batchHolders[i].clear();
        batchHolders[i] = new ArrayList<BatchHolder>();
      }
      outBatchIndex[i] = 0;
      outputStream[i] = null;
      spilledBatchesCount[i] = 0;
      spillFiles[i] = null;
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
    RecordBatchSizer sizer = new RecordBatchSizer(incoming);
    logger.trace("Incoming sizer: {}",sizer);
    // An empty batch only has the schema, can not tell actual length of varchars
    // else use the actual varchars length, each capped at 50 (to match the space allocation)
    long estInputRowWidth = sizer.rowCount() == 0 ? sizer.stdRowWidth() : sizer.netRowWidthCap50();

    // Get approx max (varchar) column width to get better memory allocation
    maxColumnWidth = Math.max(sizer.maxAvgColumnSize(), VARIABLE_MIN_WIDTH_VALUE_SIZE);
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
    estMaxBatchSize = Math.max(estRowWidth, estInputRowWidth) * MAX_BATCH_SIZE;
    // (When there are no aggr functions, use '1' as later code relies on this size being non-zero)
    estValuesBatchSize = Math.max(estValuesRowWidth, 1) * MAX_BATCH_SIZE;
    estOutgoingAllocSize = estValuesBatchSize; // initially assume same size

    logger.trace("{} phase. Estimated internal row width: {} Values row width: {} batch size: {}  memory limit: {}  max column width: {}",
        isTwoPhase?(is2ndPhase?"2nd":"1st"):"Single",estRowWidth,estValuesRowWidth,estMaxBatchSize,allocator.getLimit(),maxColumnWidth);

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
      //
      // Get the NEXT input batch, initially from the upstream, later (if there was a spill)
      // from one of the spill files (The spill case is handled differently here to avoid
      // collecting stats on the spilled records)
      //
      long memAllocBeforeNext = allocator.getAllocatedMemory();
      if ( handlingSpills ) {
        outcome = incoming.next(); // get it from the SpilledRecordBatch
      } else {
        // Get the next RecordBatch from the incoming (i.e. upstream operator)
        outcome = outgoing.next(0, incoming);
      }
      long memAllocAfterNext = allocator.getAllocatedMemory();
      long incomingBatchSize = memAllocAfterNext - memAllocBeforeNext;

      // If incoming batch is bigger than our estimate - adjust the estimate to match
      if ( estMaxBatchSize < incomingBatchSize) {
        logger.debug("Found a bigger next {} batch: {} , prior estimate was: {}, mem allocated {}",handlingSpills ? "spill" : "incoming",
            incomingBatchSize, estMaxBatchSize, memAllocAfterNext);
        estMaxBatchSize = incomingBatchSize;
      }

      if (EXTRA_DEBUG_1) {
        logger.debug("Received IterOutcome of {}", outcome);
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

        case OK:
          currentBatchRecordCount = incoming.getRecordCount(); // size of next batch

          resetIndex(); // initialize index (a new batch needs to be processed)

          if (EXTRA_DEBUG_1) {
            logger.debug("Continue to start processing the next batch");
          }
          break;

        case NONE:
          resetIndex(); // initialize index (in case spill files need to be processed)

          buildComplete = true;

          updateStats(htables);

          // output the first batch; remaining batches will be output
          // in response to each next() call by a downstream operator
          AggIterOutcome aggOutcome = outputCurrentBatch();

          if ( aggOutcome == AggIterOutcome.AGG_RESTART ) {
            // Output of first batch returned a RESTART (all new partitions were spilled)
            return AggOutcome.CALL_WORK_AGAIN; // need to read/process the next partition
          }

          if ( aggOutcome != AggIterOutcome.AGG_NONE ) { outcome = IterOutcome.OK; }

          return AggOutcome.RETURN_OUTCOME;

        case STOP:
        default:
          return AggOutcome.CLEANUP_AND_RETURN;
      }
    }
  }

  /**
   *   Use reserved values memory (if available) to try and preemp an OOM
   */
  private void useReservedValuesMemory() {
    // try to preempt an OOM by using the reserved memory
    long reservedMemory = reserveValueBatchMemory;
    if ( reservedMemory > 0 ) { allocator.setLimit(allocator.getLimit() + reservedMemory); }

    reserveValueBatchMemory = 0;
  }
  /**
   *   Use reserved outgoing output memory (if available) to try and preemp an OOM
   */
  private void useReservedOutgoingMemory() {
    // try to preempt an OOM by using the reserved memory
    long reservedMemory = reserveOutgoingMemory;
    if ( reservedMemory > 0 ) { allocator.setLimit(allocator.getLimit() + reservedMemory); }

    reserveOutgoingMemory = 0;
  }
  /**
   *  Restore the reserve memory (both)
   *
   */
  private void restoreReservedMemory() {
    if ( 0 == reserveOutgoingMemory ) { // always restore OutputValues first (needed for spilling)
      long memAvail = allocator.getLimit() - allocator.getAllocatedMemory();
      if ( memAvail > estOutgoingAllocSize) {
        allocator.setLimit(allocator.getLimit() - estOutgoingAllocSize);
        reserveOutgoingMemory = estOutgoingAllocSize;
      }
    }
    if ( 0 == reserveValueBatchMemory ) {
      long memAvail = allocator.getLimit() - allocator.getAllocatedMemory();
      if ( memAvail > estValuesBatchSize) {
        allocator.setLimit(allocator.getLimit() - estValuesBatchSize);
        reserveValueBatchMemory = estValuesBatchSize;
      }
    }
  }
  /**
   *   Allocate space for the returned aggregate columns
   *   (Note DRILL-5588: Maybe can eliminate this allocation (and copy))
   * @param records
   */
  private void allocateOutgoing(int records) {
    // Skip the keys and only allocate for outputting the workspace values
    // (keys will be output through splitAndTransfer)
    Iterator<VectorWrapper<?>> outgoingIter = outContainer.iterator();
    for (int i = 0; i < numGroupByOutFields; i++) {
      outgoingIter.next();
    }

    // try to preempt an OOM by using the reserved memory
    useReservedOutgoingMemory();
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
    // try to restore the reserve
    restoreReservedMemory();
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
    if ( is2ndPhase && spillSet.getWriteBytes() > 0 ) {
      stats.setLongStat(Metric.SPILL_MB, // update stats - total MB spilled
          (int) Math.round(spillSet.getWriteBytes() / 1024.0D / 1024.0));
    }
    // clean (and deallocate) each partition
    for ( int i = 0; i < numPartitions; i++) {
          if (htables[i] != null) {
              htables[i].clear();
              htables[i] = null;
          }
          if ( batchHolders[i] != null) {
              for (BatchHolder bh : batchHolders[i]) {
                    bh.clear();
              }
              batchHolders[i].clear();
              batchHolders[i] = null;
          }

          // delete any (still active) output spill file
          if ( outputStream[i] != null && spillFiles[i] != null) {
            try {
              outputStream[i].close();
              outputStream[i] = null;
              spillSet.delete(spillFiles[i]);
              spillFiles[i] = null;
            } catch(IOException e) {
              logger.warn("Cleanup: Failed to delete spill file {}",spillFiles[i]);
            }
          }
    }
    // delete any spill file left in unread spilled partitions
    while ( ! spilledPartitionsList.isEmpty() ) {
        SpilledPartition sp = spilledPartitionsList.remove(0);
        try {
          spillSet.delete(sp.spillFile);
        } catch(IOException e) {
          logger.warn("Cleanup: Failed to delete spill file {}",sp.spillFile);
        }
    }
    // Delete the currently handled (if any) spilled file
    if ( newIncoming != null ) { newIncoming.close();  }
    spillSet.close(); // delete the spill directory(ies)
    htIdxHolder = null;
    materializedValueFields = null;
    outStartIdxHolder = null;
    outNumRecordsHolder = null;
  }

  // First free the memory used by the given (spilled) partition (i.e., hash table plus batches)
  // then reallocate them in pristine state to allow the partition to continue receiving rows
  private void reinitPartition(int part) /* throws SchemaChangeException /*, IOException */ {
    assert htables[part] != null;
    htables[part].reset();
    if ( batchHolders[part] != null) {
      for (BatchHolder bh : batchHolders[part]) {
        bh.clear();
      }
      batchHolders[part].clear();
    }
    batchHolders[part] = new ArrayList<BatchHolder>(); // First BatchHolder is created when the first put request is received.

    // in case the reserve memory was used, try to restore
    restoreReservedMemory();
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

  private boolean isSpilled(int part) {
    return outputStream[part] != null;
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
  private int chooseAPartitionToFlush(int currPart, boolean tryAvoidCurr) {
    if ( is1stPhase && ! tryAvoidCurr) { return currPart; } // 1st phase: just use the current partition
    int currPartSize = batchHolders[currPart].size();
    if ( currPartSize == 1 ) { currPartSize = -1; } // don't pick current if size is 1
    // first find the largest spilled partition
    int maxSizeSpilled = -1;
    int indexMaxSpilled = -1;
    for (int isp = 0; isp < numPartitions; isp++ ) {
      if ( isSpilled(isp) && maxSizeSpilled < batchHolders[isp].size() ) {
        maxSizeSpilled = batchHolders[isp].size();
        indexMaxSpilled = isp;
      }
    }
    // Give the current (if already spilled) some priority
    if ( ! tryAvoidCurr && isSpilled(currPart) && ( currPartSize + 1 >= maxSizeSpilled )) {
      maxSizeSpilled = currPartSize ;
      indexMaxSpilled = currPart;
    }
    // now find the largest non-spilled partition
    int maxSize = -1;
    int indexMax = -1;
    // Use the largest spilled (if found) as a base line, with a factor of 4
    if ( indexMaxSpilled > -1 && maxSizeSpilled > 1 ) {
      indexMax = indexMaxSpilled;
      maxSize = 4 * maxSizeSpilled ;
    }
    for ( int insp = 0; insp < numPartitions; insp++) {
      if ( ! isSpilled(insp) && maxSize < batchHolders[insp].size() ) {
        indexMax = insp;
        maxSize = batchHolders[insp].size();
      }
    }
    // again - priority to the current partition
    if ( ! tryAvoidCurr && ! isSpilled(currPart) && (currPartSize + 1 >= maxSize) ) {
      return currPart;
    }
    if ( maxSize <= 1 ) { // Can not make progress by spilling a single batch!
      return -1; // try skipping this spill
    }
    return indexMax;
  }

  /**
   * Iterate through the batches of the given partition, writing them to a file
   *
   * @param part The partition (number) to spill
   */
  private void spillAPartition(int part) {

    ArrayList<BatchHolder> currPartition = batchHolders[part];
    rowsInPartition = 0;
    if ( EXTRA_DEBUG_SPILL ) {
      logger.debug("HashAggregate: Spilling partition {} current cycle {} part size {}", part, cycleNum, currPartition.size());
    }

    if ( currPartition.size() == 0 ) { return; } // in case empty - nothing to spill

    // If this is the first spill for this partition, create an output stream
    if ( ! isSpilled(part) ) {

      spillFiles[part] = spillSet.getNextSpillFile(cycleNum > 0 ? Integer.toString(cycleNum) : null);

      try {
        outputStream[part] = spillSet.openForOutput(spillFiles[part]);
      } catch (IOException ioe) {
        throw UserException.resourceError(ioe)
            .message("Hash Aggregation failed to open spill file: " + spillFiles[part])
            .build(logger);
      }
    }

    for (int currOutBatchIndex = 0; currOutBatchIndex < currPartition.size(); currOutBatchIndex++ ) {

      // get the number of records in the batch holder that are pending output
      int numPendingOutput = currPartition.get(currOutBatchIndex).getNumPendingOutput();

      rowsInPartition += numPendingOutput;  // for logging
      rowsSpilled += numPendingOutput;

      allocateOutgoing(numPendingOutput);

      currPartition.get(currOutBatchIndex).outputValues(outStartIdxHolder, outNumRecordsHolder);
      int numOutputRecords = outNumRecordsHolder.value;

      this.htables[part].outputKeys(currOutBatchIndex, this.outContainer, outStartIdxHolder.value, outNumRecordsHolder.value, numPendingOutput);

      // set the value count for outgoing batch value vectors
      /* int i = 0; */
      for (VectorWrapper<?> v : outgoing) {
        v.getValueVector().getMutator().setValueCount(numOutputRecords);
        /*
        // print out the first row to be spilled ( varchar, varchar, bigint )
        try {
          if (i++ < 2) {
            NullableVarCharVector vv = ((NullableVarCharVector) v.getValueVector());
            logger.info("FIRST ROW = {}", vv.getAccessor().get(0));
          } else {
            NullableBigIntVector vv = ((NullableBigIntVector) v.getValueVector());
            logger.info("FIRST ROW = {}", vv.getAccessor().get(0));
          }
        } catch (Exception e) { logger.info("While printing the first row - Got an exception = {}",e); }
        */
      }

      outContainer.setRecordCount(numPendingOutput);
      WritableBatch batch = WritableBatch.getBatchNoHVWrap(numPendingOutput, outContainer, false);
      VectorAccessibleSerializable outputBatch = new VectorAccessibleSerializable(batch, allocator);
      Stopwatch watch = Stopwatch.createStarted();
      try {
        outputBatch.writeToStream(outputStream[part]);
      } catch (IOException ioe) {
        throw UserException.dataWriteError(ioe)
            .message("Hash Aggregation failed to write to output stream: " + outputStream[part].toString())
            .build(logger);
      }
      outContainer.zeroVectors();
      logger.trace("HASH AGG: Took {} us to spill {} records", watch.elapsed(TimeUnit.MICROSECONDS), numPendingOutput);
    }

    spilledBatchesCount[part] += currPartition.size(); // update count of spilled batches

    logger.trace("HASH AGG: Spilled {} rows from {} batches of partition {}", rowsInPartition, currPartition.size(), part);
  }

  private void addBatchHolder(int part) {

    BatchHolder bh = newBatchHolder();
    batchHolders[part].add(bh);
    if (EXTRA_DEBUG_1) {
      logger.debug("HashAggregate: Added new batch; num batches = {}.", batchHolders[part].size());
    }

    bh.setup();
  }

  // These methods are overridden in the generated class when created as plain Java code.
  protected BatchHolder newBatchHolder() {
    return new BatchHolder();
  }

  /**
   * Output the next batch from partition "nextPartitionToReturn"
   *
   * @return iteration outcome (e.g., OK, NONE ...)
   */
  @Override
  public AggIterOutcome outputCurrentBatch() {

    // when incoming was an empty batch, just finish up
    if ( schema == null ) {
      logger.trace("Incoming was empty; output is an empty batch.");
      this.outcome = IterOutcome.NONE; // no records were read
      allFlushed = true;
      return AggIterOutcome.AGG_NONE;
    }

    // Initialization (covers the case of early output)
    ArrayList<BatchHolder> currPartition = batchHolders[earlyPartition];
    int currOutBatchIndex = outBatchIndex[earlyPartition];
    int partitionToReturn = earlyPartition;

    if ( ! earlyOutput ) {
      // Update the next partition to return (if needed)
      // skip fully returned (or spilled) partitions
      while (nextPartitionToReturn < numPartitions) {
        //
        // If this partition was spilled - spill the rest of it and skip it
        //
        if ( isSpilled(nextPartitionToReturn) ) {
          spillAPartition(nextPartitionToReturn); // spill the rest
          SpilledPartition sp = new SpilledPartition();
          sp.spillFile = spillFiles[nextPartitionToReturn];
          sp.spilledBatches = spilledBatchesCount[nextPartitionToReturn];
          sp.cycleNum = cycleNum; // remember the current cycle
          sp.origPartn = nextPartitionToReturn; // for debugging / filename
          sp.prevOrigPartn = originalPartition; // for debugging / filename
          spilledPartitionsList.add(sp);

          reinitPartition(nextPartitionToReturn); // free the memory
          long posn = spillSet.getPosition(outputStream[nextPartitionToReturn]);
          spillSet.tallyWriteBytes(posn); // for the IO stats
          try {
            outputStream[nextPartitionToReturn].close();
          } catch (IOException ioe) {
            throw UserException.resourceError(ioe)
                .message("IO Error while closing output stream")
                .build(logger);
          }
          outputStream[nextPartitionToReturn] = null;
        }
        else {
          currPartition = batchHolders[nextPartitionToReturn];
          currOutBatchIndex = outBatchIndex[nextPartitionToReturn];
          // If curr batch (partition X index) is not empty - proceed to return it
          if (currOutBatchIndex < currPartition.size() && 0 != currPartition.get(currOutBatchIndex).getNumPendingOutput()) {
            break;
          }
        }
        nextPartitionToReturn++; // else check next partition
      }

      // if passed the last partition - either done or need to restart and read spilled partitions
      if (nextPartitionToReturn >= numPartitions) {
        // The following "if" is probably never used; due to a similar check at the end of this method
        if ( spilledPartitionsList.isEmpty() ) { // and no spilled partitions
          allFlushed = true;
          this.outcome = IterOutcome.NONE;
          if ( is2ndPhase && spillSet.getWriteBytes() > 0 ) {
            stats.setLongStat(Metric.SPILL_MB, // update stats - total MB spilled
                (int) Math.round(spillSet.getWriteBytes() / 1024.0D / 1024.0));
          }
          return AggIterOutcome.AGG_NONE;  // then return NONE
        }
        // Else - there are still spilled partitions to process - pick one and handle just like a new incoming
        buildComplete = false; // go back and call doWork() again
        handlingSpills = true; // beginning to work on the spill files
        // pick a spilled partition; set a new incoming ...
        SpilledPartition sp = spilledPartitionsList.remove(0);
        // Create a new "incoming" out of the spilled partition spill file
        newIncoming = new SpilledRecordbatch(sp.spillFile, sp.spilledBatches, context, schema, oContext, spillSet);
        originalPartition = sp.origPartn; // used for the filename
        logger.trace("Reading back spilled original partition {} as an incoming",originalPartition);
        // Initialize .... new incoming, new set of partitions
        try { initializeSetup(newIncoming); } catch (Exception e) { throw new RuntimeException(e); }
        // update the cycle num if needed
        // The current cycle num should always be one larger than in the spilled partition
        if ( cycleNum == sp.cycleNum ) {
          cycleNum = 1 + sp.cycleNum;
          stats.setLongStat(Metric.SPILL_CYCLE, cycleNum); // update stats
          // report first spill or memory stressful situations
          if ( cycleNum == 1 ) { logger.info("Started reading spilled records "); }
          if ( cycleNum == 2 ) { logger.info("SECONDARY SPILLING "); }
          if ( cycleNum == 3 ) { logger.warn("TERTIARY SPILLING "); }
          if ( cycleNum == 4 ) { logger.warn("QUATERNARY SPILLING "); }
          if ( cycleNum == 5 ) { logger.warn("QUINARY SPILLING "); }
        }
        if ( EXTRA_DEBUG_SPILL ) {
          logger.debug("Start reading spilled partition {} (prev {}) from cycle {} (with {} batches). More {} spilled partitions left.",
              sp.origPartn, sp.prevOrigPartn, sp.cycleNum, sp.spilledBatches, spilledPartitionsList.size());
        }
        return AggIterOutcome.AGG_RESTART;
      }

      partitionToReturn = nextPartitionToReturn ;

    }

    // get the number of records in the batch holder that are pending output
    int numPendingOutput = currPartition.get(currOutBatchIndex).getNumPendingOutput();

    // The following accounting is for logging, metrics, etc.
    rowsInPartition += numPendingOutput ;
    if ( ! handlingSpills ) { rowsNotSpilled += numPendingOutput; }
    else { rowsSpilledReturned += numPendingOutput; }
    if ( earlyOutput ) { rowsReturnedEarly += numPendingOutput; }

    allocateOutgoing(numPendingOutput);

    currPartition.get(currOutBatchIndex).outputValues(outStartIdxHolder, outNumRecordsHolder);
    int numOutputRecords = outNumRecordsHolder.value;

    if (EXTRA_DEBUG_1) {
      logger.debug("After output values: outStartIdx = {}, outNumRecords = {}", outStartIdxHolder.value, outNumRecordsHolder.value);
    }

    this.htables[partitionToReturn].outputKeys(currOutBatchIndex, this.outContainer, outStartIdxHolder.value, outNumRecordsHolder.value, numPendingOutput);

    // set the value count for outgoing batch value vectors
    for (VectorWrapper<?> v : outgoing) {
      v.getValueVector().getMutator().setValueCount(numOutputRecords);
    }

    this.outcome = IterOutcome.OK;

    if ( EXTRA_DEBUG_SPILL && is2ndPhase ) {
      logger.debug("So far returned {} + SpilledReturned {}  total {} (spilled {})",rowsNotSpilled,rowsSpilledReturned,
        rowsNotSpilled+rowsSpilledReturned,
        rowsSpilled);
    }

    lastBatchOutputCount = numOutputRecords;
    outBatchIndex[partitionToReturn]++;
    // if just flushed the last batch in the partition
    if (outBatchIndex[partitionToReturn] == currPartition.size()) {

      if ( EXTRA_DEBUG_SPILL ) {
        logger.debug("HashAggregate: {} Flushed partition {} with {} batches total {} rows",
            earlyOutput ? "(Early)" : "",
            partitionToReturn, outBatchIndex[partitionToReturn], rowsInPartition);
      }
      rowsInPartition = 0; // reset to count for the next partition

      // deallocate memory used by this partition, and re-initialize
      reinitPartition(partitionToReturn);

      if ( earlyOutput ) {

        if ( EXTRA_DEBUG_SPILL ) {
          logger.debug("HASH AGG: Finished (early) re-init partition {}, mem allocated: {}", earlyPartition, allocator.getAllocatedMemory());
        }
        outBatchIndex[earlyPartition] = 0; // reset, for next time
        earlyOutput = false ; // done with early output
      }
      else if ( (partitionToReturn + 1 == numPartitions) && spilledPartitionsList.isEmpty() ) { // last partition ?

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
    if ( !isTwoPhase ) {
      errmsg = "Single Phase Hash Aggregate operator can not spill." ;
    } else if ( ! canSpill ) {  // 2nd phase, with only 1 partition
      errmsg = "Too little memory available to operator to facilitate spilling.";
    } else { // a bug ?
      errmsg = prefix + " OOM at " + (is2ndPhase ? "Second Phase" : "First Phase") + ". Partitions: " + numPartitions +
      ". Estimated batch size: " + estMaxBatchSize + ". values size: " + estValuesBatchSize + ". Output alloc size: " + estOutgoingAllocSize;
      if ( plannedBatches > 0 ) { errmsg += ". Planned batches: " + plannedBatches; }
      if ( rowsSpilled > 0 ) { errmsg += ". Rows spilled so far: " + rowsSpilled; }
    }
    errmsg += " Memory limit: " + allocator.getLimit() + " so far allocated: " + allocator.getAllocatedMemory() + ". ";

    return errmsg;
  }

  // Check if a group is present in the hash table; if not, insert it in the hash table.
  // The htIdxHolder contains the index of the group in the hash table container; this same
  // index is also used for the aggregation values maintained by the hash aggregate.
  private void checkGroupAndAggrValues(int incomingRowIdx) {
    assert incomingRowIdx >= 0;
    assert ! earlyOutput;

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
    /*
    if ( handlingSpills && ( incomingRowIdx == 0 ) ) {
      // for debugging -- show the first row from a spilled batch
      Object tmp0 = (incoming).getValueAccessorById(NullableVarCharVector.class, 0).getValueVector();
      Object tmp1 = (incoming).getValueAccessorById(NullableVarCharVector.class, 1).getValueVector();
      Object tmp2 = (incoming).getValueAccessorById(NullableBigIntVector.class, 2).getValueVector();

      if (tmp0 != null && tmp1 != null && tmp2 != null) {
        NullableVarCharVector vv0 = ((NullableVarCharVector) tmp0);
        NullableVarCharVector vv1 = ((NullableVarCharVector) tmp1);
        NullableBigIntVector  vv2 = ((NullableBigIntVector) tmp2);
        logger.debug("The first row = {} , {} , {}", vv0.getAccessor().get(incomingRowIdx), vv1.getAccessor().get(incomingRowIdx), vv2.getAccessor().get(incomingRowIdx));
      }
    }
    */
    // The hash code is computed once, then its lower bits are used to determine the
    // partition to use, and the higher bits determine the location in the hash table.
    int hashCode;
    try {
      // htables[0].updateBatches();
      hashCode = htables[0].getHashCode(incomingRowIdx);
    } catch (SchemaChangeException e) {
      throw new UnsupportedOperationException("Unexpected schema change", e);
    }

    // right shift hash code for secondary (or tertiary...) spilling
    for (int i = 0; i < cycleNum; i++) { hashCode >>>= bitsInMask; }

    int currentPartition = hashCode & partitionMask ;
    hashCode >>>= bitsInMask;
    HashTable.PutStatus putStatus = null;
    long allocatedBeforeHTput = allocator.getAllocatedMemory();

    // Proactive spill - in case there is no reserve memory - spill and retry putting later
    if ( reserveValueBatchMemory == 0 && canSpill ) {
      logger.trace("Reserved memory runs short, trying to {} a partition and retry Hash Table put() again.",
        is1stPhase ? "early return" : "spill");

      doSpill(currentPartition); // spill to free some memory

      retrySameIndex = true;
      return; // to retry this put()
    }

    // ==========================================
    // Insert the key columns into the hash table
    // ==========================================
    try {

      putStatus = htables[currentPartition].put(incomingRowIdx, htIdxHolder, hashCode);

    } catch (RetryAfterSpillException re) {
      if ( ! canSpill ) { throw new OutOfMemoryException(getOOMErrorMsg("Can not spill")); }

      logger.trace("HT put failed with an OOM, trying to {} a partition and retry Hash Table put() again.",
            is1stPhase ? "early return" : "spill");

      // for debugging - in case there's a leak
      long memDiff = allocator.getAllocatedMemory() - allocatedBeforeHTput;
      if ( memDiff > 0 ) { logger.warn("Leak: HashTable put() OOM left behind {} bytes allocated",memDiff); }

      doSpill(currentPartition); // spill to free some memory

      retrySameIndex = true;
      return; // to retry this put()
    } catch (OutOfMemoryException exc) {
        throw new OutOfMemoryException(getOOMErrorMsg("HT was: " + allocatedBeforeHTput), exc);
    } catch (SchemaChangeException e) {
        throw new UnsupportedOperationException("Unexpected schema change", e);
    }
    long allocatedBeforeAggCol = allocator.getAllocatedMemory();
    boolean needToCheckIfSpillIsNeeded = allocatedBeforeAggCol > allocatedBeforeHTput ;

    // Add an Aggr batch if needed:
    //
    //       In case put() added a new batch (for the keys) inside the hash table,
    //       then a matching batch (for the aggregate columns) needs to be created
    //
    if ( putStatus == HashTable.PutStatus.NEW_BATCH_ADDED ) {
      try {

        useReservedValuesMemory(); // try to preempt an OOM by using the reserve

        addBatchHolder(currentPartition);  // allocate a new (internal) values batch

        restoreReservedMemory(); // restore the reserve, if possible
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
    BatchHolder bh = batchHolders[currentPartition].get((currentIdx >>> 16) & HashTable.BATCH_MASK);
    int idxWithinBatch = currentIdx & HashTable.BATCH_MASK;
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
      spillIfNeeded(currentPartition);
    }
  }

  private void spillIfNeeded(int currentPartition) { spillIfNeeded(currentPartition, false);}
  private void doSpill(int currentPartition) { spillIfNeeded(currentPartition, true);}
  /**
   *  Spill (or return early, if 1st phase) if too little available memory is left
   *  @param currentPartition - the preferred candidate for spilling
   * @param forceSpill -- spill unconditionally (no memory checks)
   */
  private void spillIfNeeded(int currentPartition, boolean forceSpill) {
    long maxMemoryNeeded = 0;
    if ( !forceSpill ) { // need to check the memory in order to decide
      // calculate the (max) new memory needed now; plan ahead for at least MIN batches
      maxMemoryNeeded = minBatchesPerPartition * Math.max(1, plannedBatches) * (estMaxBatchSize + MAX_BATCH_SIZE * (4 + 4 /* links + hash-values */));
      // Add the (max) size of the current hash table, in case it will double
      int maxSize = 1;
      for (int insp = 0; insp < numPartitions; insp++) {
        maxSize = Math.max(maxSize, batchHolders[insp].size());
      }
      maxMemoryNeeded += MAX_BATCH_SIZE * 2 * 2 * 4 * maxSize; // 2 - double, 2 - max when %50 full, 4 - Uint4

      // log a detailed debug message explaining why a spill may be needed
      logger.trace("MEMORY CHECK: Allocated mem: {}, agg phase: {}, trying to add to partition {} with {} batches. " + "Max memory needed {}, Est batch size {}, mem limit {}",
          allocator.getAllocatedMemory(), isTwoPhase ? (is2ndPhase ? "2ND" : "1ST") : "Single", currentPartition, batchHolders[currentPartition].size(), maxMemoryNeeded,
          estMaxBatchSize, allocator.getLimit());
    }
    //
    //   Spill if (forced, or) the allocated memory plus the memory needed exceed the memory limit.
    //
    if ( forceSpill || allocator.getAllocatedMemory() + maxMemoryNeeded > allocator.getLimit() ) {

      // Pick a "victim" partition to spill or return
      int victimPartition = chooseAPartitionToFlush(currentPartition, forceSpill);

      // In case no partition has more than one batch -- try and "push the limits"; maybe next
      // time the spill could work.
      if ( victimPartition < 0 ) { return; }

      if ( is2ndPhase ) {
        long before = allocator.getAllocatedMemory();

        spillAPartition(victimPartition);
        logger.trace("RAN OUT OF MEMORY: Spilled partition {}",victimPartition);

        // Re-initialize (free memory, then recreate) the partition just spilled/returned
        reinitPartition(victimPartition);

        // In case spilling did not free enough memory to recover the reserves
        boolean spillAgain = reserveOutgoingMemory == 0 || reserveValueBatchMemory == 0;
        // in some "edge" cases (e.g. testing), spilling one partition may not be enough
        if ( spillAgain || allocator.getAllocatedMemory() + maxMemoryNeeded > allocator.getLimit() ) {
            int victimPartition2 = chooseAPartitionToFlush(victimPartition, true);
            if ( victimPartition2 < 0 ) { return; }
            long after = allocator.getAllocatedMemory();
            spillAPartition(victimPartition2);
            reinitPartition(victimPartition2);
            logger.warn("A Second Spill was Needed: allocated before {}, after first spill {}, after second {}, memory needed {}",
                before, after, allocator.getAllocatedMemory(), maxMemoryNeeded);
            logger.trace("Second Partition Spilled: {}",victimPartition2);
        }
      }
      else {
        // 1st phase need to return a partition early in order to free some memory
        earlyOutput = true;
        earlyPartition = victimPartition;

        if ( EXTRA_DEBUG_SPILL ) {
          logger.debug("picked partition {} for early output", victimPartition);
        }
      }
    }
  }

  /**
   * Updates the stats at the time after all the input was read.
   * Note: For spilled partitions, their hash-table stats from before the spill are lost.
   * And the SPILLED_PARTITIONS only counts the spilled partitions in the primary, not SECONDARY etc.
   * @param htables
   */
  private void updateStats(HashTable[] htables) {
    if ( cycleNum > 0 ) { return; } // These stats are only for before processing spilled files
    long numSpilled = 0;
    HashTableStats newStats = new HashTableStats();
    // sum the stats from all the partitions
    for (int ind = 0; ind < numPartitions; ind++) {
      htables[ind].getStats(newStats);
      htStats.addStats(newStats);
      if (isSpilled(ind)) {
        numSpilled++;
      }
    }
    this.stats.setLongStat(Metric.NUM_BUCKETS, htStats.numBuckets);
    this.stats.setLongStat(Metric.NUM_ENTRIES, htStats.numEntries);
    this.stats.setLongStat(Metric.NUM_RESIZING, htStats.numResizing);
    this.stats.setLongStat(Metric.RESIZING_TIME_MS, htStats.resizingTime);
    this.stats.setLongStat(Metric.NUM_PARTITIONS, numPartitions);
    this.stats.setLongStat(Metric.SPILL_CYCLE, cycleNum); // Put 0 in case no spill
    if ( is2ndPhase ) {
      this.stats.setLongStat(Metric.SPILLED_PARTITIONS, numSpilled);
    }
    if ( rowsReturnedEarly > 0 ) {
      stats.setLongStat(Metric.SPILL_MB, // update stats - est. total MB returned early
          (int) Math.round( rowsReturnedEarly * estOutputRowWidth / 1024.0D / 1024.0));
    }
  }

  // Code-generated methods (implemented in HashAggBatch)
  public abstract void doSetup(@Named("incoming") RecordBatch incoming) throws SchemaChangeException;

  public abstract int getVectorIndex(@Named("recordIndex") int recordIndex) throws SchemaChangeException;

  public abstract boolean resetValues() throws SchemaChangeException;

}
