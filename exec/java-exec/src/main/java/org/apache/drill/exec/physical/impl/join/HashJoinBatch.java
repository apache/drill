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
package org.apache.drill.exec.physical.impl.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.commons.io.FileUtils;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.memory.BaseAllocator;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.config.HashJoinPOP;
import org.apache.drill.exec.physical.impl.aggregate.SpilledRecordbatch;
import org.apache.drill.exec.physical.impl.common.ChainedHashTable;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.physical.impl.common.HashTableConfig;
import org.apache.drill.exec.physical.impl.common.HashTableStats;
import org.apache.drill.exec.physical.impl.common.Comparator;
import org.apache.drill.exec.physical.impl.common.HashPartition;
import org.apache.drill.exec.physical.impl.spill.SpillSet;
import org.apache.drill.exec.record.AbstractBinaryRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.JoinBatchMemoryManager;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatchSizer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractContainerVector;
import org.apache.calcite.rel.core.JoinRelType;

/**
 *   This class implements the runtime execution for the Hash-Join operator
 *   supporting INNER, LEFT OUTER, RIGHT OUTER, and FULL OUTER joins
 *
 *   This implementation splits the incoming Build side rows into multiple Partitions, thus allowing spilling of
 *   some of these partitions to disk if memory gets tight. Each partition is implemented as a {@link HashPartition}.
 *   After the build phase is over, in the most general case, some of the partitions were spilled, and the others
 *   are in memory. Each of the partitions in memory would get a {@link HashTable} built.
 *      Next the Probe side is read, and each row is key matched with a Build partition. If that partition is in
 *   memory, then the key is used to probe and perform the join, and the results are added to the outgoing batch.
 *   But if that build side partition was spilled, then the matching Probe size partition is spilled as well.
 *      After all the Probe side was processed, we are left with pairs of spilled partitions. Then each pair is
 *   processed individually (that Build partition should be smaller than the original, hence likely fit whole into
 *   memory to allow probing; if not -- see below).
 *      Processing of each spilled pair is EXACTLY like processing the original Build/Probe incomings. (As a fact,
 *   the {@Link #innerNext() innerNext} method calls itself recursively !!). Thus the spilled build partition is
 *   read and divided into new partitions, which in turn may spill again (and again...).
 *   The code tracks these spilling "cycles". Normally any such "again" (i.e. cycle of 2 or greater) is a waste,
 *   indicating that the number of partitions chosen was too small.
 */
public class HashJoinBatch extends AbstractBinaryRecordBatch<HashJoinPOP> {
  protected static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashJoinBatch.class);

  /**
   * The maximum number of records within each internal batch.
   */
  private int RECORDS_PER_BATCH; // internal batches

  // Join type, INNER, LEFT, RIGHT or OUTER
  private final JoinRelType joinType;

  // Join conditions
  private final List<JoinCondition> conditions;

  // Runtime generated class implementing HashJoinProbe interface
  private HashJoinProbe hashJoinProbe = null;

  private final List<NamedExpression> rightExpr;

  /**
   * Names of the join columns. This names are used in order to help estimate the size of the {@link HashTable}s.
   */
  private final Set<String> buildJoinColumns;

  // Fields used for partitioning

  /**
   * The number of {@link HashPartition}s. This is configured via a system option and set in {@link #partitionNumTuning(int, HashJoinMemoryCalculator.BuildSidePartitioning)}.
   */
  private int numPartitions = 1; // must be 2 to the power of bitsInMask
  private int partitionMask = 0; // numPartitions - 1
  private int bitsInMask = 0; // number of bits in the MASK

  /**
   * The master class used to generate {@link HashTable}s.
   */
  private ChainedHashTable baseHashTable;
  private boolean buildSideIsEmpty = true;
  private boolean canSpill = true;
  private boolean wasKilled; // a kill was received, may need to clean spilled partns

  /**
   * This array holds the currently active {@link HashPartition}s.
   */
  HashPartition partitions[];

  // Number of records in the output container
  private int outputRecords;

  // Schema of the build side
  private BatchSchema rightSchema;
  // Schema of the probe side
  private BatchSchema probeSchema;


  private int rightHVColPosition;
  private BufferAllocator allocator;
  // Local fields for left/right incoming - may be replaced when reading from spilled
  private RecordBatch buildBatch;
  private RecordBatch probeBatch;

  // For handling spilling
  private SpillSet spillSet;
  HashJoinPOP popConfig;

  private int cycleNum = 0; // 1-primary, 2-secondary, 3-tertiary, etc.
  private int originalPartition = -1; // the partition a secondary reads from
  IntVector read_right_HV_vector; // HV vector that was read from the spilled batch
  private int maxBatchesInMemory;

  /**
   * This holds information about the spilled partitions for the build and probe side.
   */
  public static class HJSpilledPartition {
    public int innerSpilledBatches;
    public String innerSpillFile;
    public int outerSpilledBatches;
    public String outerSpillFile;
    int cycleNum;
    int origPartn;
    int prevOrigPartn;
  }

  /**
   * Queue of spilled partitions to process.
   */
  private ArrayList<HJSpilledPartition> spilledPartitionsList;
  private HJSpilledPartition spilledInners[]; // for the outer to find the partition

  public enum Metric implements MetricDef {
    NUM_BUCKETS,
    NUM_ENTRIES,
    NUM_RESIZING,
    RESIZING_TIME_MS,
    NUM_PARTITIONS,
    SPILLED_PARTITIONS, // number of original partitions spilled to disk
    SPILL_MB,         // Number of MB of data spilled to disk. This amount is first written,
                      // then later re-read. So, disk I/O is twice this amount.
    SPILL_CYCLE,       // 0 - no spill, 1 - spill, 2 - SECONDARY, 3 - TERTIARY
    LEFT_INPUT_BATCH_COUNT,
    LEFT_AVG_INPUT_BATCH_BYTES,
    LEFT_AVG_INPUT_ROW_BYTES,
    LEFT_INPUT_RECORD_COUNT,
    RIGHT_INPUT_BATCH_COUNT,
    RIGHT_AVG_INPUT_BATCH_BYTES,
    RIGHT_AVG_INPUT_ROW_BYTES,
    RIGHT_INPUT_RECORD_COUNT,
    OUTPUT_BATCH_COUNT,
    AVG_OUTPUT_BATCH_BYTES,
    AVG_OUTPUT_ROW_BYTES,
    OUTPUT_RECORD_COUNT;

    // duplicate for hash ag

    @Override
    public int metricId() { return ordinal(); }
  }

  @Override
  public int getRecordCount() {
    return outputRecords;
  }

  @Override
  protected void buildSchema() throws SchemaChangeException {
    if (! prefetchFirstBatchFromBothSides()) {
      return;
    }

    // Initialize the hash join helper context
    if (rightUpstream != IterOutcome.NONE) {
      setupHashTable();
    }
    setupOutputContainerSchema();
    try {
      hashJoinProbe = setupHashJoinProbe();
    } catch (IOException | ClassTransformationException e) {
      throw new SchemaChangeException(e);
    }

    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
  }

  @Override
  protected boolean prefetchFirstBatchFromBothSides() {
    leftUpstream = sniffNonEmptyBatch(0, left);
    rightUpstream = sniffNonEmptyBatch(1, right);

    // For build side, use aggregate i.e. average row width across batches
    batchMemoryManager.update(LEFT_INDEX, 0);
    batchMemoryManager.update(RIGHT_INDEX, 0, true);

    logger.debug("BATCH_STATS, incoming left: {}", batchMemoryManager.getRecordBatchSizer(LEFT_INDEX));
    logger.debug("BATCH_STATS, incoming right: {}", batchMemoryManager.getRecordBatchSizer(RIGHT_INDEX));

    if (leftUpstream == IterOutcome.STOP || rightUpstream == IterOutcome.STOP) {
      state = BatchState.STOP;
      return false;
    }

    if (leftUpstream == IterOutcome.OUT_OF_MEMORY || rightUpstream == IterOutcome.OUT_OF_MEMORY) {
      state = BatchState.OUT_OF_MEMORY;
      return false;
    }

    if (checkForEarlyFinish(leftUpstream, rightUpstream)) {
      state = BatchState.DONE;
      return false;
    }

    state = BatchState.FIRST;  // Got our first batches on both sides
    return true;
  }

  /**
   * Currently in order to accurately predict memory usage for spilling, the first non-empty build side and probe side batches are needed. This method
   * fetches the first non-empty batch from the left or right side.
   * @param inputIndex Index specifying whether to work with the left or right input.
   * @param recordBatch The left or right record batch.
   * @return The {@link org.apache.drill.exec.record.RecordBatch.IterOutcome} for the left or right record batch.
   */
  private IterOutcome sniffNonEmptyBatch(int inputIndex, RecordBatch recordBatch) {
    while (true) {
      IterOutcome outcome = next(inputIndex, recordBatch);

      switch (outcome) {
        case OK_NEW_SCHEMA:
          if ( inputIndex == 0 ) {
            // Indicate that a schema was seen (in case probe side is empty)
            probeSchema = probeBatch.getSchema();
          } else {
            // We need to have the schema of the build side even when the build side is empty
            rightSchema = buildBatch.getSchema();
            // position of the new "column" for keeping the hash values (after the real columns)
            rightHVColPosition = buildBatch.getContainer().getNumberOfColumns();
            // new schema can also contain records
          }
        case OK:
          if (recordBatch.getRecordCount() == 0) {
            continue;
          }
          // We got a non empty batch
        default:
          // Other cases termination conditions
          return outcome;
      }
    }
  }

  /**
   * Determines the memory calculator to use. If maxNumBatches is configured simple batch counting is used to spill. Otherwise
   * memory calculations are used to determine when to spill.
   * @return The memory calculator to use.
   */
  public HashJoinMemoryCalculator getCalculatorImpl() {
    if (maxBatchesInMemory == 0) {
      final double safetyFactor = context.getOptions().getDouble(ExecConstants.HASHJOIN_SAFETY_FACTOR_KEY);
      final double fragmentationFactor = context.getOptions().getDouble(ExecConstants.HASHJOIN_FRAGMENTATION_FACTOR_KEY);
      final double hashTableDoublingFactor = context.getOptions().getDouble(ExecConstants.HASHJOIN_HASH_DOUBLE_FACTOR_KEY);
      final String hashTableCalculatorType = context.getOptions().getString(ExecConstants.HASHJOIN_HASHTABLE_CALC_TYPE_KEY);

      return new HashJoinMemoryCalculatorImpl(safetyFactor, fragmentationFactor, hashTableDoublingFactor, hashTableCalculatorType);
    } else {
      return new HashJoinMechanicalMemoryCalculator(maxBatchesInMemory);
    }
  }

  @Override
  public IterOutcome innerNext() {
    // In case incoming was killed before, just cleanup and return
    if ( wasKilled ) {
      this.cleanup();
      super.close();
      return IterOutcome.NONE;
    }

    try {
      /* If we are here for the first time, execute the build phase of the
       * hash join and setup the run time generated class for the probe side
       */
      if (state == BatchState.FIRST) {
        // Build the hash table, using the build side record batches.
        executeBuildPhase();
        // Update the hash table related stats for the operator
        updateStats();
        // Initialize various settings for the probe side
        hashJoinProbe.setupHashJoinProbe(probeBatch, this, joinType, leftUpstream, partitions, cycleNum, container, spilledInners, buildSideIsEmpty, numPartitions, rightHVColPosition);
      }

      // Try to probe and project, or recursively handle a spilled partition
      if ( ! buildSideIsEmpty ||  // If there are build-side rows
           joinType != JoinRelType.INNER) {  // or if this is a left/full outer join

        // Allocate the memory for the vectors in the output container
        batchMemoryManager.allocateVectors(container);
        hashJoinProbe.setTargetOutputCount(batchMemoryManager.getOutputRowCount());

        outputRecords = hashJoinProbe.probeAndProject();

        for (final VectorWrapper<?> v : container) {
          v.getValueVector().getMutator().setValueCount(outputRecords);
        }
        container.setRecordCount(outputRecords);

        batchMemoryManager.updateOutgoingStats(outputRecords);
        if (logger.isDebugEnabled()) {
          logger.debug("BATCH_STATS, outgoing: {}", new RecordBatchSizer(this));
        }

        /* We are here because of one the following
         * 1. Completed processing of all the records and we are done
         * 2. We've filled up the outgoing batch to the maximum and we need to return upstream
         * Either case build the output container's schema and return
         */
        if (outputRecords > 0 || state == BatchState.FIRST) {
          if (state == BatchState.FIRST) {
            state = BatchState.NOT_FIRST;
          }

          return IterOutcome.OK;
        }

        // Free all partitions' in-memory data structures
        // (In case need to start processing spilled partitions)
        for ( HashPartition partn : partitions ) {
          partn.cleanup(false); // clean, but do not delete the spill files !!
        }

        //
        //  (recursively) Handle the spilled partitions, if any
        //
        if ( !buildSideIsEmpty && !spilledPartitionsList.isEmpty()) {
          // Get the next (previously) spilled partition to handle as incoming
          HJSpilledPartition currSp = spilledPartitionsList.remove(0);

          // Create a BUILD-side "incoming" out of the inner spill file of that partition
          buildBatch = new SpilledRecordbatch(currSp.innerSpillFile, currSp.innerSpilledBatches, context, rightSchema, oContext, spillSet);
          // The above ctor call also got the first batch; need to update the outcome
          rightUpstream = ((SpilledRecordbatch) buildBatch).getInitialOutcome();

          if ( currSp.outerSpilledBatches > 0 ) {
            // Create a PROBE-side "incoming" out of the outer spill file of that partition
            probeBatch = new SpilledRecordbatch(currSp.outerSpillFile, currSp.outerSpilledBatches, context, probeSchema, oContext, spillSet);
            // The above ctor call also got the first batch; need to update the outcome
            leftUpstream = ((SpilledRecordbatch) probeBatch).getInitialOutcome();
          } else {
            probeBatch = left; // if no outer batch then reuse left - needed for updateIncoming()
            leftUpstream = IterOutcome.NONE;
            hashJoinProbe.changeToFinalProbeState();
          }

          // update the cycle num if needed
          // The current cycle num should always be one larger than in the spilled partition
          if (cycleNum == currSp.cycleNum) {
            cycleNum = 1 + currSp.cycleNum;
            stats.setLongStat(Metric.SPILL_CYCLE, cycleNum); // update stats
            // report first spill or memory stressful situations
            if (cycleNum == 1) { logger.info("Started reading spilled records "); }
            if (cycleNum == 2) { logger.info("SECONDARY SPILLING "); }
            if (cycleNum == 3) { logger.warn("TERTIARY SPILLING ");  }
            if (cycleNum == 4) { logger.warn("QUATERNARY SPILLING "); }
            if (cycleNum == 5) { logger.warn("QUINARY SPILLING "); }
            if ( cycleNum * bitsInMask > 20 ) {
              spilledPartitionsList.add(currSp); // so cleanup() would delete the curr spill files
              this.cleanup();
              throw UserException
                .unsupportedError()
                .message("Hash-Join can not partition the inner data any further (probably due to too many join-key duplicates)\n"
                + "On cycle num %d mem available %d num partitions %d", cycleNum, allocator.getLimit(), numPartitions)
                .build(logger);
            }
          }
          logger.debug("Start reading spilled partition {} (prev {}) from cycle {} (with {}-{} batches)." +
              " More {} spilled partitions left.",
            currSp.origPartn, currSp.prevOrigPartn, currSp.cycleNum, currSp.outerSpilledBatches,
            currSp.innerSpilledBatches, spilledPartitionsList.size());

          state = BatchState.FIRST;  // TODO need to determine if this is still necessary since prefetchFirstBatchFromBothSides sets this

          return innerNext(); // start processing the next spilled partition "recursively"
        }

      } else {
        // Our build side is empty, we won't have any matches, clear the probe side
        if (leftUpstream == IterOutcome.OK_NEW_SCHEMA || leftUpstream == IterOutcome.OK) {
          for (final VectorWrapper<?> wrapper : probeBatch) {
            wrapper.getValueVector().clear();
          }
          probeBatch.kill(true);
          leftUpstream = next(HashJoinHelper.LEFT_INPUT, probeBatch);
          while (leftUpstream == IterOutcome.OK_NEW_SCHEMA || leftUpstream == IterOutcome.OK) {
            for (final VectorWrapper<?> wrapper : probeBatch) {
              wrapper.getValueVector().clear();
            }
            leftUpstream = next(HashJoinHelper.LEFT_INPUT, probeBatch);
          }
        }
      }

      // No more output records, clean up and return
      state = BatchState.DONE;

      this.cleanup();

      return IterOutcome.NONE;
    } catch (SchemaChangeException e) {
      context.getExecutorState().fail(e);
      killIncoming(false);
      return IterOutcome.STOP;
    }
  }

  private void setupHashTable() throws SchemaChangeException {
    final List<Comparator> comparators = Lists.newArrayListWithExpectedSize(conditions.size());
    conditions.forEach(cond->comparators.add(JoinUtils.checkAndReturnSupportedJoinComparator(cond)));

    // Setup the hash table configuration object
    List<NamedExpression> leftExpr = new ArrayList<>(conditions.size());

    // Create named expressions from the conditions
    for (int i = 0; i < conditions.size(); i++) {
      leftExpr.add(new NamedExpression(conditions.get(i).getLeft(), new FieldReference("probe_side_" + i)));
    }

    // Set the left named expression to be null if the probe batch is empty.
    if (leftUpstream != IterOutcome.OK_NEW_SCHEMA && leftUpstream != IterOutcome.OK) {
      leftExpr = null;
    } else {
      if (probeBatch.getSchema().getSelectionVectorMode() != BatchSchema.SelectionVectorMode.NONE) {
        final String errorMsg = new StringBuilder().append("Hash join does not support probe batch with selection vectors. ").append("Probe batch has selection mode = ").append
          (probeBatch.getSchema().getSelectionVectorMode()).toString();
        throw new SchemaChangeException(errorMsg);
      }
    }

    final HashTableConfig htConfig = new HashTableConfig((int) context.getOptions().getOption(ExecConstants.MIN_HASH_TABLE_SIZE),
      true, HashTable.DEFAULT_LOAD_FACTOR, rightExpr, leftExpr, comparators);

    // Create the chained hash table
    baseHashTable =
      new ChainedHashTable(htConfig, context, allocator, buildBatch, probeBatch, null);
  }

  /**
   *  Call only after num partitions is known
   */
  private void delayedSetup() {
    //
    //  Find out the estimated max batch size, etc
    //  and compute the max numPartitions possible
    //  See partitionNumTuning()
    //

    partitionMask = numPartitions - 1; // e.g. 32 --> 0x1F
    bitsInMask = Integer.bitCount(partitionMask); // e.g. 0x1F -> 5

    // Create the FIFO list of spilled partitions (pairs - inner/outer)
    spilledPartitionsList = new ArrayList<>();

    // Create array for the partitions
    partitions = new HashPartition[numPartitions];

    buildSideIsEmpty = false;
  }

  /**
   * Initialize fields (that may be reused when reading spilled partitions)
   */
  private void initializeBuild() {
    baseHashTable.updateIncoming(buildBatch, probeBatch); // in case we process the spilled files
    // Recreate the partitions every time build is initialized
    for (int part = 0; part < numPartitions; part++ ) {
      partitions[part] = new HashPartition(context, allocator, baseHashTable, buildBatch, probeBatch,
        RECORDS_PER_BATCH, spillSet, part, cycleNum, numPartitions);
    }

    spilledInners = new HJSpilledPartition[numPartitions];

  }

  /**
   * Tunes the number of partitions used by {@link HashJoinBatch}. If it is not possible to spill it gives up and reverts
   * to unbounded in memory operation.
   * @param maxBatchSize
   * @param buildCalc
   * @return
   */
  private HashJoinMemoryCalculator.BuildSidePartitioning partitionNumTuning(
    int maxBatchSize,
    HashJoinMemoryCalculator.BuildSidePartitioning buildCalc) {
    // Get auto tuning result
    numPartitions = buildCalc.getNumPartitions();

    if (logger.isDebugEnabled()) {
      logger.debug(buildCalc.makeDebugString());
    }

    if (buildCalc.getMaxReservedMemory() > allocator.getLimit()) {
      // We don't have enough memory to do any spilling. Give up and do no spilling and have no limits

      // TODO dirty hack to prevent regressions. Remove this once batch sizing is implemented.
      // We don't have enough memory to do partitioning, we have to do everything in memory
      final String message = String.format("When using the minimum number of partitions %d we require %s memory but only have %s available. " +
          "Forcing legacy behavoir of using unbounded memory in order to prevent regressions.",
        numPartitions,
        FileUtils.byteCountToDisplaySize(buildCalc.getMaxReservedMemory()),
        FileUtils.byteCountToDisplaySize(allocator.getLimit()));
      logger.warn(message);

      // create a Noop memory calculator
      final HashJoinMemoryCalculator calc = getCalculatorImpl();
      calc.initialize(false);
      buildCalc = calc.next();

      buildCalc.initialize(true,
        true, // TODO Fix after growing hash values bug fixed
        buildBatch,
        probeBatch,
        buildJoinColumns,
        allocator.getLimit(),
        numPartitions,
        RECORDS_PER_BATCH,
        RECORDS_PER_BATCH,
        maxBatchSize,
        maxBatchSize,
        batchMemoryManager.getOutputRowCount(),
        batchMemoryManager.getOutputBatchSize(),
        HashTable.DEFAULT_LOAD_FACTOR);

      disableSpilling(null);
    }

    return buildCalc;
  }

  /**
   *  Disable spilling - use only a single partition and set the memory limit to the max ( 10GB )
   *  @param reason If not null - log this as warning, else check fallback setting to either warn or fail.
   */
  private void disableSpilling(String reason) {
    // Fail, or just issue a warning if a reason was given, or a fallback option is enabled
    if ( reason == null ) {
      final boolean fallbackEnabled = context.getOptions().getOption(ExecConstants.HASHJOIN_FALLBACK_ENABLED_KEY).bool_val;
      if (fallbackEnabled) {
        logger.warn("Spilling is disabled - not enough memory available for internal partitioning. Falling back" +
          " to use unbounded memory");
      } else {
        throw UserException.resourceError().message(String.format("Not enough memory for internal partitioning and fallback mechanism for " +
          "HashJoin to use unbounded memory is disabled. Either enable fallback config %s using Alter " +
          "session/system command or increase memory limit for Drillbit", ExecConstants.HASHJOIN_FALLBACK_ENABLED_KEY)).build(logger);
      }
    } else {
      logger.warn(reason);
    }

    numPartitions = 1; // We are only using one partition
    canSpill = false; // We cannot spill
    allocator.setLimit(AbstractBase.MAX_ALLOCATION); // Violate framework and force unbounded memory
  }

  /**
   *  Execute the BUILD phase; first read incoming and split rows into partitions;
   *  may decide to spill some of the partitions
   *
   * @throws SchemaChangeException
   */
  public void executeBuildPhase() throws SchemaChangeException {
    if (rightUpstream == IterOutcome.NONE) {
      // empty right
      return;
    }

    HashJoinMemoryCalculator.BuildSidePartitioning buildCalc;
    boolean firstCycle = cycleNum == 0;

    {
      // Initializing build calculator
      // Limit scope of these variables to this block
      int maxBatchSize = firstCycle? RecordBatch.MAX_BATCH_SIZE: RECORDS_PER_BATCH;
      boolean hasProbeData = leftUpstream != IterOutcome.NONE;
      boolean doMemoryCalculation = canSpill && hasProbeData;
      HashJoinMemoryCalculator calc = getCalculatorImpl();

      calc.initialize(doMemoryCalculation);
      buildCalc = calc.next();

      // We've sniffed first non empty build and probe batches so we have enough information to create a calculator
      buildCalc.initialize(firstCycle, true, // TODO Fix after growing hash values bug fixed
        buildBatch,
        probeBatch,
        buildJoinColumns,
        allocator.getLimit(),
        numPartitions,
        RECORDS_PER_BATCH,
        RECORDS_PER_BATCH,
        maxBatchSize,
        maxBatchSize,
        batchMemoryManager.getOutputRowCount(),
        batchMemoryManager.getOutputBatchSize(),
        HashTable.DEFAULT_LOAD_FACTOR);

      if (firstCycle && doMemoryCalculation) {
        // Do auto tuning
        buildCalc = partitionNumTuning(maxBatchSize, buildCalc);
      }
    }

    if (firstCycle) {
      // Do initial setup only on the first cycle
      delayedSetup();
    }

    initializeBuild();

    // Make the calculator aware of our partitions
    final HashJoinMemoryCalculator.PartitionStatSet partitionStatSet = new HashJoinMemoryCalculator.PartitionStatSet(partitions);
    buildCalc.setPartitionStatSet(partitionStatSet);

    boolean moreData = true;
    while (moreData) {
      switch (rightUpstream) {
      case OUT_OF_MEMORY:
      case NONE:
      case NOT_YET:
      case STOP:
        moreData = false;
        continue;

      case OK_NEW_SCHEMA:
        if (!rightSchema.equals(buildBatch.getSchema())) {
          throw SchemaChangeException.schemaChanged("Hash join does not support schema changes in build side.", rightSchema, buildBatch.getSchema());
        }
        for (HashPartition partn : partitions) { partn.updateBatches(); }
        // Fall through
      case OK:
        batchMemoryManager.update(buildBatch, RIGHT_INDEX, 0, true);
        // Special treatment (when no spill, and single partition) -- use the incoming vectors as they are (no row copy)
        if ( numPartitions == 1 ) {
          partitions[0].appendBatch(buildBatch);
          break;
        }
        final int currentRecordCount = buildBatch.getRecordCount();

        if ( cycleNum > 0 ) {
          read_right_HV_vector = (IntVector) buildBatch.getContainer().getLast();
        }

        // For every record in the build batch, hash the key columns and keep the result
        for (int ind = 0; ind < currentRecordCount; ind++) {
          int hashCode = ( cycleNum == 0 ) ? partitions[0].getBuildHashCode(ind)
            : read_right_HV_vector.getAccessor().get(ind); // get the hash value from the HV column
          int currPart = hashCode & partitionMask ;
          hashCode >>>= bitsInMask;
          // Append the new inner row to the appropriate partition; spill (that partition) if needed
          partitions[currPart].appendInnerRow(buildBatch.getContainer(), ind, hashCode, buildCalc); // may spill if needed
        }

        if ( read_right_HV_vector != null ) {
          read_right_HV_vector.clear();
          read_right_HV_vector = null;
        }
        break;
      }
      // Get the next incoming record batch
      rightUpstream = next(HashJoinHelper.RIGHT_INPUT, buildBatch);
    }

    // Move the remaining current batches into their temp lists, or spill
    // them if the partition is spilled. Add the spilled partitions into
    // the spilled partitions list
    if ( numPartitions > 1 ) { // a single partition needs no completion
      for (HashPartition partn : partitions) {
        partn.completeAnInnerBatch(false, partn.isSpilled());
      }
    }

    HashJoinMemoryCalculator.PostBuildCalculations postBuildCalc = buildCalc.next();
    postBuildCalc.initialize();

    //
    //  Traverse all the in-memory partitions' incoming batches, and build their hash tables
    //

    for (int index = 0; index < partitions.length; index++) {
      final HashPartition partn = partitions[index];

      if (partn.isSpilled()) {
        // Don't build hash tables for spilled partitions
        continue;
      }

      try {
        if (postBuildCalc.shouldSpill()) {
          // Spill this partition if we need to make room
          partn.spillThisPartition();
        } else {
          // Only build hash tables for partitions that are not spilled
          partn.buildContainersHashTableAndHelper();
        }
      } catch (OutOfMemoryException e) {
        final String message = "Failed building hash table on partition " + index + ":\n"
          + makeDebugString() + "\n"
          + postBuildCalc.makeDebugString();
        // Include debug info
        throw new OutOfMemoryException(message, e);
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug(postBuildCalc.makeDebugString());
    }

    for (HashPartition partn : partitions) {
      if ( partn.isSpilled() ) {
        HJSpilledPartition sp = new HJSpilledPartition();
        sp.innerSpillFile = partn.getSpillFile();
        sp.innerSpilledBatches = partn.getPartitionBatchesCount();
        sp.cycleNum = cycleNum; // remember the current cycle
        sp.origPartn = partn.getPartitionNum(); // for debugging / filename
        sp.prevOrigPartn = originalPartition; // for debugging / filename
        spilledPartitionsList.add(sp);

        spilledInners[partn.getPartitionNum()] = sp; // for the outer to find the SP later
        partn.closeWriter();
      }
    }
  }

  private void setupOutputContainerSchema() {

    if (rightSchema != null) {
      for (final MaterializedField field : rightSchema) {
        final MajorType inputType = field.getType();
        final MajorType outputType;
        // If left or full outer join, then the output type must be nullable. However, map types are
        // not nullable so we must exclude them from the check below (see DRILL-2197).
        if ((joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL) && inputType.getMode() == DataMode.REQUIRED
            && inputType.getMinorType() != TypeProtos.MinorType.MAP) {
          outputType = Types.overrideMode(inputType, DataMode.OPTIONAL);
        } else {
          outputType = inputType;
        }

        // make sure to project field with children for children to show up in the schema
        final MaterializedField projected = field.withType(outputType);
        // Add the vector to our output container
        container.addOrGet(projected);
      }
    }

    if (probeSchema != null) { // a probe schema was seen (even though the probe may had no rows)
      for (final VectorWrapper<?> vv : probeBatch) {
        final MajorType inputType = vv.getField().getType();
        final MajorType outputType;

        // If right or full outer join then the output type should be optional. However, map types are
        // not nullable so we must exclude them from the check below (see DRILL-2771, DRILL-2197).
        if ((joinType == JoinRelType.RIGHT || joinType == JoinRelType.FULL) && inputType.getMode() == DataMode.REQUIRED
            && inputType.getMinorType() != TypeProtos.MinorType.MAP) {
          outputType = Types.overrideMode(inputType, DataMode.OPTIONAL);
        } else {
          outputType = inputType;
        }

        final ValueVector v = container.addOrGet(MaterializedField.create(vv.getField().getName(), outputType));
        if (v instanceof AbstractContainerVector) {
          vv.getValueVector().makeTransferPair(v);
          v.clear();
        }
      }
    }

  }

  // (After the inner side was read whole) - Has that inner partition spilled
  public boolean isSpilledInner(int part) {
    if ( spilledInners == null ) { return false; } // empty inner
    return spilledInners[part] != null;
  }

  /**
   *  The constructor
   *
   * @param popConfig
   * @param context
   * @param left  -- probe/outer side incoming input
   * @param right -- build/iner side incoming input
   * @throws OutOfMemoryException
   */
  public HashJoinBatch(HashJoinPOP popConfig, FragmentContext context,
      RecordBatch left, /*Probe side record batch*/
      RecordBatch right /*Build side record batch*/
  ) throws OutOfMemoryException {
    super(popConfig, context, true, left, right);
    this.buildBatch = right;
    this.probeBatch = left;
    joinType = popConfig.getJoinType();
    conditions = popConfig.getConditions();
    this.popConfig = popConfig;

    rightExpr = new ArrayList<>(conditions.size());
    buildJoinColumns = Sets.newHashSet();

    for (int i = 0; i < conditions.size(); i++) {
      final SchemaPath rightPath = (SchemaPath) conditions.get(i).getRight();
      final PathSegment.NameSegment nameSegment = (PathSegment.NameSegment)rightPath.getLastSegment();
      buildJoinColumns.add(nameSegment.getPath());
      final String refName = "build_side_" + i;
      rightExpr.add(new NamedExpression(conditions.get(i).getRight(), new FieldReference(refName)));
    }

    this.allocator = oContext.getAllocator();

    numPartitions = (int)context.getOptions().getOption(ExecConstants.HASHJOIN_NUM_PARTITIONS_VALIDATOR);
    if ( numPartitions == 1 ) { //
      disableSpilling("Spilling is disabled due to configuration setting of num_partitions to 1");
    }

    numPartitions = BaseAllocator.nextPowerOfTwo(numPartitions); // in case not a power of 2

    final long memLimit = context.getOptions().getOption(ExecConstants.HASHJOIN_MAX_MEMORY_VALIDATOR);

    if (memLimit != 0) {
      allocator.setLimit(memLimit);
    }

    RECORDS_PER_BATCH = (int)context.getOptions().getOption(ExecConstants.HASHJOIN_NUM_ROWS_IN_BATCH_VALIDATOR);
    maxBatchesInMemory = (int)context.getOptions().getOption(ExecConstants.HASHJOIN_MAX_BATCHES_IN_MEMORY_VALIDATOR);

    logger.info("Memory limit {} bytes", FileUtils.byteCountToDisplaySize(allocator.getLimit()));
    spillSet = new SpillSet(context, popConfig);

    // Create empty partitions (in the ctor - covers the case where right side is empty)
    partitions = new HashPartition[0];

    // get the output batch size from config.
    int configuredBatchSize = (int) context.getOptions().getOption(ExecConstants.OUTPUT_BATCH_SIZE_VALIDATOR);
    batchMemoryManager = new JoinBatchMemoryManager(configuredBatchSize, left, right, new HashSet<>());
    logger.debug("BATCH_STATS, configured output batch size: {}", configuredBatchSize);
  }

  /**
   * This method is called when {@link HashJoinBatch} closes. It cleans up left over spilled files that are in the spill queue, and closes the
   * spillSet.
   */
  private void cleanup() {
    if ( buildSideIsEmpty ) { return; } // not set up; nothing to clean
    if ( spillSet.getWriteBytes() > 0 ) {
      stats.setLongStat(Metric.SPILL_MB, // update stats - total MB spilled
        (int) Math.round(spillSet.getWriteBytes() / 1024.0D / 1024.0));
    }
    // clean (and deallocate) each partition, and delete its spill file
    for (HashPartition partn : partitions) {
      partn.close();
    }

    // delete any spill file left in unread spilled partitions
    while ( ! spilledPartitionsList.isEmpty() ) {
      HJSpilledPartition sp = spilledPartitionsList.remove(0);
      try {
        spillSet.delete(sp.innerSpillFile);
      } catch(IOException e) {
        logger.warn("Cleanup: Failed to delete spill file {}",sp.innerSpillFile);
      }
      try { // outer file is added later; may be null if cleaning prematurely
        if ( sp.outerSpillFile != null ) { spillSet.delete(sp.outerSpillFile); }
      } catch(IOException e) {
        logger.warn("Cleanup: Failed to delete spill file {}",sp.outerSpillFile);
      }
    }
    // Delete the currently handled (if any) spilled files
    spillSet.close(); // delete the spill directory(ies)
  }

  /**
   * This creates a string that summarizes the memory usage of the operator.
   * @return A memory dump string.
   */
  public String makeDebugString() {
    final StringBuilder sb = new StringBuilder();

    for (int partitionIndex = 0; partitionIndex < partitions.length; partitionIndex++) {
      final String partitionPrefix = "Partition " + partitionIndex + ": ";
      final HashPartition hashPartition = partitions[partitionIndex];
      sb.append(partitionPrefix).append(hashPartition.makeDebugString()).append("\n");
    }

    return sb.toString();
  }

  /**
   * Updates the {@link HashTable} and spilling stats after the original build side is processed.
   *
   * Note: this does not update all the stats. The cycleNum is updated dynamically in {@link #innerNext()} and the total bytes
   * written is updated at close time in {@link #cleanup()}.
   */
  private void updateStats() {
    if ( buildSideIsEmpty ) { return; } // no stats when the right side is empty
    if ( cycleNum > 0 ) { return; } // These stats are only for before processing spilled files

    final HashTableStats htStats = new HashTableStats();
    long numSpilled = 0;
    HashTableStats newStats = new HashTableStats();
    // sum the stats from all the partitions
    for ( HashPartition partn : partitions ) {
      if ( partn.isSpilled() ) { numSpilled++; }
      partn.getStats(newStats);
      htStats.addStats(newStats);
    }

    this.stats.setLongStat(Metric.NUM_BUCKETS, htStats.numBuckets);
    this.stats.setLongStat(Metric.NUM_ENTRIES, htStats.numEntries);
    this.stats.setLongStat(Metric.NUM_RESIZING, htStats.numResizing);
    this.stats.setLongStat(Metric.RESIZING_TIME_MS, htStats.resizingTime);
    this.stats.setLongStat(Metric.NUM_PARTITIONS, numPartitions);
    this.stats.setLongStat(Metric.SPILL_CYCLE, cycleNum); // Put 0 in case no spill
    this.stats.setLongStat(Metric.SPILLED_PARTITIONS, numSpilled);
  }

  @Override
  public void killIncoming(boolean sendUpstream) {
    wasKilled = true;
    probeBatch.kill(sendUpstream);
    buildBatch.kill(sendUpstream);
  }

  public void updateMetrics() {
    stats.setLongStat(HashJoinBatch.Metric.LEFT_INPUT_BATCH_COUNT, batchMemoryManager.getNumIncomingBatches(LEFT_INDEX));
    stats.setLongStat(HashJoinBatch.Metric.LEFT_AVG_INPUT_BATCH_BYTES, batchMemoryManager.getAvgInputBatchSize(LEFT_INDEX));
    stats.setLongStat(HashJoinBatch.Metric.LEFT_AVG_INPUT_ROW_BYTES, batchMemoryManager.getAvgInputRowWidth(LEFT_INDEX));
    stats.setLongStat(HashJoinBatch.Metric.LEFT_INPUT_RECORD_COUNT, batchMemoryManager.getTotalInputRecords(LEFT_INDEX));

    stats.setLongStat(HashJoinBatch.Metric.RIGHT_INPUT_BATCH_COUNT, batchMemoryManager.getNumIncomingBatches(RIGHT_INDEX));
    stats.setLongStat(HashJoinBatch.Metric.RIGHT_AVG_INPUT_BATCH_BYTES, batchMemoryManager.getAvgInputBatchSize(RIGHT_INDEX));
    stats.setLongStat(HashJoinBatch.Metric.RIGHT_AVG_INPUT_ROW_BYTES, batchMemoryManager.getAvgInputRowWidth(RIGHT_INDEX));
    stats.setLongStat(HashJoinBatch.Metric.RIGHT_INPUT_RECORD_COUNT, batchMemoryManager.getTotalInputRecords(RIGHT_INDEX));

    stats.setLongStat(HashJoinBatch.Metric.OUTPUT_BATCH_COUNT, batchMemoryManager.getNumOutgoingBatches());
    stats.setLongStat(HashJoinBatch.Metric.AVG_OUTPUT_BATCH_BYTES, batchMemoryManager.getAvgOutputBatchSize());
    stats.setLongStat(HashJoinBatch.Metric.AVG_OUTPUT_ROW_BYTES, batchMemoryManager.getAvgOutputRowWidth());
    stats.setLongStat(HashJoinBatch.Metric.OUTPUT_RECORD_COUNT, batchMemoryManager.getTotalOutputRecords());
  }

  @Override
  public void close() {
    if ( cycleNum > 0 ) { // spilling happened
      // In case closing due to cancellation, BaseRootExec.close() does not close the open
      // SpilledRecordBatch "scanners" as it only knows about the original left/right ops.
      killIncoming(false);
    }

    updateMetrics();

    if (logger.isDebugEnabled()) {
      logger.debug("BATCH_STATS, incoming aggregate left: batch count : {}, avg bytes : {},  avg row bytes : {}, record count : {}",
        batchMemoryManager.getNumIncomingBatches(JoinBatchMemoryManager.LEFT_INDEX), batchMemoryManager.getAvgInputBatchSize(JoinBatchMemoryManager.LEFT_INDEX),
        batchMemoryManager.getAvgInputRowWidth(JoinBatchMemoryManager.LEFT_INDEX), batchMemoryManager.getTotalInputRecords(JoinBatchMemoryManager.LEFT_INDEX));

      logger.debug("BATCH_STATS, incoming aggregate right: batch count : {}, avg bytes : {},  avg row bytes : {}, record count : {}",
        batchMemoryManager.getNumIncomingBatches(JoinBatchMemoryManager.RIGHT_INDEX), batchMemoryManager.getAvgInputBatchSize(JoinBatchMemoryManager.RIGHT_INDEX),
        batchMemoryManager.getAvgInputRowWidth(JoinBatchMemoryManager.RIGHT_INDEX), batchMemoryManager.getTotalInputRecords(JoinBatchMemoryManager.RIGHT_INDEX));

      logger.debug("BATCH_STATS, outgoing aggregate: batch count : {}, avg bytes : {},  avg row bytes : {}, record count : {}",
        batchMemoryManager.getNumOutgoingBatches(), batchMemoryManager.getAvgOutputBatchSize(),
        batchMemoryManager.getAvgOutputRowWidth(), batchMemoryManager.getTotalOutputRecords());
    }

    this.cleanup();
    super.close();
  }

  public HashJoinProbe setupHashJoinProbe() throws ClassTransformationException, IOException {
    final CodeGenerator<HashJoinProbe> cg = CodeGenerator.get(HashJoinProbe.TEMPLATE_DEFINITION, context.getOptions());
    cg.plainJavaCapable(true);
    // cg.saveCodeForDebugging(true);

    //  No real code generation !!

    final HashJoinProbe hj = context.getImplementationClass(cg);
    return hj;
  }

}
