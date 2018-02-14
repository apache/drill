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
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.RetryAfterSpillException;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.FieldReference;
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
import org.apache.drill.exec.memory.BaseAllocator;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
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
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractContainerVector;
import org.apache.calcite.rel.core.JoinRelType;

public class HashJoinBatch extends AbstractBinaryRecordBatch<HashJoinPOP> {
  protected static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashJoinBatch.class);

  private int RECORDS_PER_BATCH = 128; // 1024; // internal batches
  private static final int TARGET_RECORDS_PER_BATCH = 4000;

  // Join type, INNER, LEFT, RIGHT or OUTER
  private final JoinRelType joinType;

  // Join conditions
  private final List<JoinCondition> conditions;

  private final List<Comparator> comparators;

  // Fields used for partitioning
  private int numPartitions = 1; // must be 2 to the power of bitsInMask (set in setup())
  private int partitionMask = 0; // numPartitions - 1
  private int bitsInMask = 0; // number of bits in the MASK
  private ChainedHashTable baseHashTable;
  private boolean buildSideIsEmpty = true;
  private boolean canSpill = true;
  private boolean wasKilled; // a kill was received, may need to clean spilled partns

  HashPartition partitions[];

  // Number of records in the output container
  private int outputRecords;

  // Schema of the build side
  private BatchSchema rightSchema = null;

  private final HashTableStats htStats = new HashTableStats();

  private final MajorType HVtype = MajorType.newBuilder()
    .setMinorType(org.apache.drill.common.types.TypeProtos.MinorType.INT /* dataType */ )
    .setMode(DataMode.REQUIRED /* mode */ )
    .build();

  private int rightHVColPosition;
  private BufferAllocator allocator;
  // Local fields for left/right incoming - may be replaced when reading from spilled
  private RecordBatch buildBatch;
  private RecordBatch probeBatch;

  // For handling spilling
  private SpillSet spillSet;
  HashJoinPOP popConfig;

  private int cycleNum = 0; // primary, secondary, tertiary, etc.
  private int originalPartition = -1; // the partition a secondary reads from
  IntVector read_HV_vector; // HV vector that was read from the spilled batch
  private int MAX_BATCHES_IN_MEMORY;
  private int MAX_BATCHES_PER_PARTITION;

  public class inMemBatchCounter {
    private int inMemBatches;
    public void inc() { inMemBatches++; }
    public void dec() { inMemBatches--; }
    public int value() { return inMemBatches; }
  }
  public inMemBatchCounter inMemBatches = new inMemBatchCounter();

  private static class HJSpilledPartition {
    public int innerSpilledBatches;
    public String innerSpillFile;
    public int outerSpilledBatches;
    public String outerSpillFile;
    int cycleNum;
    int origPartn;
    int prevOrigPartn; }

  private ArrayList<HJSpilledPartition> spilledPartitionsList;
  private HJSpilledPartition spilledInners[]; // for the outer to find the partition

  private int operatorId; // for the spill file name
  public enum Metric implements MetricDef {

    NUM_BUCKETS,
    NUM_ENTRIES,
    NUM_RESIZING,
    RESIZING_TIME_MS,
    NUM_PARTITIONS,
    SPILLED_PARTITIONS, // number of original partitions spilled to disk
    SPILL_MB,         // Number of MB of data spilled to disk. This amount is first written,
                      // then later re-read. So, disk I/O is twice this amount.
    SPILL_CYCLE       // 0 - no spill, 1 - spill, 2 - SECONDARY, 3 - TERTIARY
    ;

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
    // Build the container schema and set the counts
    for (final VectorWrapper<?> w : container) {
      w.getValueVector().allocateNew();
    }
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    container.setRecordCount(outputRecords);
  }

  @Override
  public IterOutcome innerNext() {
    try {
      /* If we are here for the first time, execute the build phase of the
       * hash join and setup the run time generated class for the probe side
       */
      if (state == BatchState.FIRST) {
        // Build the hash table, using the build side record batches.
        executeBuildPhase();
        // Update the hash table related stats for the operator
        updateStats();
        //
        setupProbe();
      }

      // Store the number of records projected

      if ( ! buildSideIsEmpty ||  // If there are build-side rows
           joinType != JoinRelType.INNER) {  // or if this is a left/full outer join

        // Allocate the memory for the vectors in the output container
        allocateVectors();

        outputRecords = probeAndProject();

        /* We are here because of one the following
         * 1. Completed processing of all the records and we are done
         * 2. We've filled up the outgoing batch to the maximum and we need to return upstream
         * Either case build the output container's schema and return
         */
        if (outputRecords > 0 || state == BatchState.FIRST) {
          if (state == BatchState.FIRST) {
            state = BatchState.NOT_FIRST;
          }

          for (final VectorWrapper<?> v : container) {
            v.getValueVector().getMutator().setValueCount(outputRecords);
          }

          return IterOutcome.OK;
        }

        // Free all partitions' in-memory data structures
        // (In case need to start processing spilled partitions)
        for ( HashPartition partn : partitions ) {
          partn.close();
        }

        //
        //  (recursively) Handle the spilled partitions, if any
        //
        if ( !buildSideIsEmpty && !wasKilled && !spilledPartitionsList.isEmpty()) {
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
            changeToFinalProbeState();
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
                .message("Hash-Join can not partition inner data any further (too many join-key duplicates? - try merge-join)")
                .build(logger);
            }
          }
          logger.debug("Start reading spilled partition {} (prev {}) from cycle {} (with {}-{} batches). More {} spilled partitions left.", currSp.origPartn, currSp.prevOrigPartn, currSp.cycleNum, currSp.outerSpilledBatches, currSp.innerSpilledBatches, spilledPartitionsList.size());

          state = BatchState.FIRST;  // build again, initialize probe, etc

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
    // Setup the hash table configuration object
    int conditionsSize = conditions.size();
    final List<NamedExpression> rightExpr = new ArrayList<>(conditionsSize);
    List<NamedExpression> leftExpr = new ArrayList<>(conditionsSize);

    // Create named expressions from the conditions
    for (int i = 0; i < conditionsSize; i++) {
      rightExpr.add(new NamedExpression(conditions.get(i).getRight(), new FieldReference("build_side_" + i)));
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
    final HashTableConfig htConfig = new HashTableConfig((int) context.getOptions().getOption(ExecConstants.MIN_HASH_TABLE_SIZE), HashTable.DEFAULT_LOAD_FACTOR, rightExpr, leftExpr, comparators);


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
    //
    // numPartitions = 8; // just for initial work; change later
    // partitionMask = 7;
    // bitsInMask = 3;

    //  SET FROM CONFIGURATION OPTIONS :
    //  ================================

    // Set the number of partitions from the configuration (raise to a power of two, if needed)
    numPartitions = (int)context.getOptions().getOption(ExecConstants.HASHJOIN_NUM_PARTITIONS_VALIDATOR);
    if ( numPartitions == 1 ) { //
      canSpill = false;
      logger.warn("Spilling is disabled due to configuration setting of num_partitions to 1");
    }
    numPartitions = BaseAllocator.nextPowerOfTwo(numPartitions); // in case not a power of 2
    // Based on the number of partitions: Set the mask and bit count
    partitionMask = numPartitions - 1; // e.g. 32 --> 0x1F
    bitsInMask = Integer.bitCount(partitionMask); // e.g. 0x1F -> 5

    RECORDS_PER_BATCH = (int)context.getOptions().getOption(ExecConstants.HASHJOIN_NUM_ROWS_IN_BATCH_VALIDATOR);

    MAX_BATCHES_IN_MEMORY = (int)context.getOptions().getOption(ExecConstants.HASHJOIN_MAX_BATCHES_IN_MEMORY_VALIDATOR);
    MAX_BATCHES_PER_PARTITION = (int)context.getOptions().getOption(ExecConstants.HASHJOIN_MAX_BATCHES_PER_PARTITION_VALIDATOR);

    //  =================================

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
    assert inMemBatches.value() == 0; // check that no in-memory batches left
    baseHashTable.updateIncoming(buildBatch, probeBatch); // in case we process the spilled files
    // Recreate the partitions every time build is initialized
    for (int part = 0; part < numPartitions; part++ ) {
      partitions[part] = new HashPartition(context, allocator, baseHashTable, buildBatch, probeBatch,
        RECORDS_PER_BATCH, spillSet, part, inMemBatches, cycleNum);
    }

    spilledInners = new HJSpilledPartition[numPartitions];

  }
  /**
   *  Execute the BUILD phase; first read incoming and split rows into partitions;
   *  may decide to spill some of the partitions
   *
   * @throws SchemaChangeException
   */
  public void executeBuildPhase() throws SchemaChangeException {
    final HashJoinMemoryCalculator.BuildSidePartitioning buildCalc = new HashJoinMemoryCalculatorImpl().next();
    boolean hasProbeData = leftUpstream != IterOutcome.NONE;

    if ( rightUpstream == IterOutcome.NONE ) { return; } // empty right

    // skip first batch if count is zero, as it may be an empty schema batch
    if (false && buildBatch.getRecordCount() == 0) {
      for (final VectorWrapper<?> w : buildBatch) {
        w.clear();
      }
      rightUpstream = next(buildBatch);
    }

    //Setup the underlying hash table
    if ( cycleNum == 0 ) { delayedSetup(); } // first time only

    initializeBuild();

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
        final int currentRecordCount = buildBatch.getRecordCount();

        if ( cycleNum > 0 ) {
          read_HV_vector = (IntVector) buildBatch.getContainer().getLast();
        }
        // For every record in the build batch, hash the key columns and keep the result
        for (int ind = 0; ind < currentRecordCount; ind++) {
          int hashCode = ( cycleNum == 0 ) ? partitions[0].getBuildHashCode(ind)
            : read_HV_vector.getAccessor().get(ind); // get the hash value from the HV column
          int currPart = hashCode & partitionMask ;
          hashCode >>>= bitsInMask;
/*
          int pos = currentBatches[currPart].appendRow(buildBatch.getContainer(),ind);
          currHVVectors[currPart].getMutator().set(pos, hashCode);   // store the hash value in the new column
          if ( pos + 1 == RECORDS_PER_BATCH ) {
            // The current decision on when-to-spill is crude
            completeAnInnerBatch(currPart,true,
              isSpilled(currPart) ||  // once spilled - then spill every new full batch
                canSpill &&
                  ( inMemBatches > MAX_BATCHES_IN_MEMORY ||
                    tmpBatchesList[currPart].size() > MAX_BATCHES_PER_PARTITION ));
          }
*/

          // Append the new inner row to the appropriate partition; spill (that partition) if needed
          partitions[currPart].appendInnerRow(buildBatch.getContainer(), ind, hashCode,
            // The current decision on when-to-spill is crude ...
            partitions[currPart].isSpilled() || // once spilled - then spill every new full batch
            canSpill &&
              ( inMemBatches.value() > MAX_BATCHES_IN_MEMORY ||
                partitions[currPart].getPartitionBatchesCount() > MAX_BATCHES_PER_PARTITION ) ); // may spill if needed
        }

        if ( read_HV_vector != null ) {
          read_HV_vector.clear();
          read_HV_vector = null;
        }
        break;
      }
      // Get the next incoming record batch
      rightUpstream = next(HashJoinHelper.RIGHT_INPUT, buildBatch);
    }

    // Move the remaining current batches into their temp lists, or spill
    // them if the partition is spilled. Add the spilled partitions into
    // the spilled partitions list
    for (HashPartition partn : partitions) {
      partn.completeAnInnerBatch(false, partn.isSpilled() );
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

    //
    //  Traverse all the in-memory partitions' incoming batches, and build their hash tables
    //
/*
    for (int currPart = 0; currPart < numPartitions; currPart++) {

      // each partition is a regular array of batches
      ArrayList<VectorContainer> thisPart = new ArrayList<>();

      for (int curr = 0; curr < partitionBatchesCount[currPart]; curr++) {
        VectorContainer nextBatch = tmpBatchesList[currPart].get(curr);
        final int currentRecordCount = nextBatch.getRecordCount();

        // For every incoming build batch, we create a matching helper batch
        hjHelpers[currPart].addNewBatch(currentRecordCount);

        // Holder contains the global index where the key is hashed into using the hash table
        final IndexPointer htIndex = new IndexPointer();

        hashTables[currPart].updateIncoming(nextBatch, probeBatch );

        IntVector HV_vector = (IntVector) nextBatch.getValueVector(rightHVColPosition).getValueVector();

        for (int recInd = 0; recInd < currentRecordCount; recInd++) {
          int hashCode = HV_vector.getAccessor().get(recInd);
          try {
            hashTables[currPart].put(recInd, htIndex, hashCode);
          } catch (RetryAfterSpillException RE) {
            throw new OutOfMemoryException("HT put");
          } // Hash Join can not retry yet
                        // Use the global index returned by the hash table, to store
                        //the current record index and batch index. This will be used
                        // later when we probe and find a match.
                        //
          hjHelpers[currPart].setCurrentIndex(htIndex.value, curr , recInd);
        }

        thisPart.add(nextBatch);
      }

      partitionContainers.add(thisPart);
*/
    for (HashPartition partn : partitions) {
      partn.buildContainersHashTableAndHelper();
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

    if (leftUpstream == IterOutcome.OK || leftUpstream == IterOutcome.OK_NEW_SCHEMA) {
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

  private void allocateVectors() {
    for (final VectorWrapper<?> v : container) {
      v.getValueVector().allocateNew();
    }
    container.setRecordCount(0); // reset container's counter back to zero records
  }

  // (After the inner side was read whole) - Has that inner partition spilled
  public boolean isSpilledInner(int part) {
    if ( spilledInners == null ) { return false; } // empty inner
    return spilledInners[part] != null;
  }

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

    comparators = Lists.newArrayListWithExpectedSize(conditions.size());
    // When DRILL supports Java 8, use the following instead of the for() loop
    // conditions.forEach(cond->comparators.add(JoinUtils.checkAndReturnSupportedJoinComparator(cond)));
    for (int i=0; i<conditions.size(); i++) {
      JoinCondition cond = conditions.get(i);
      comparators.add(JoinUtils.checkAndReturnSupportedJoinComparator(cond));
    }
    this.allocator = oContext.getAllocator();

    final long memLimit = context.getOptions().getOption(ExecConstants.HASHJOIN_MAX_MEMORY_VALIDATOR);

    if (memLimit != 0) {
      allocator.setLimit(memLimit);
    }

    RECORDS_PER_BATCH = (int)context.getOptions().getOption(ExecConstants.HASHJOIN_NUM_ROWS_IN_BATCH_VALIDATOR);
    maxBatchesInMemory = (int)context.getOptions().getOption(ExecConstants.HASHJOIN_MAX_BATCHES_IN_MEMORY_VALIDATOR);

    spillSet = new SpillSet(context, popConfig);

    // Create empty partitions (in the ctor - covers the case where right side is empty)
    partitions = new HashPartition[0];
  }

  public void cleanup() {
    if ( buildSideIsEmpty ) { return; } // not set up; nothing to clean
    if ( spillSet.getWriteBytes() > 0 ) {
      stats.setLongStat(Metric.SPILL_MB, // update stats - total MB spilled
        (int) Math.round(spillSet.getWriteBytes() / 1024.0D / 1024.0));
    }
    // clean (and deallocate) each partition
    for (HashPartition partn : partitions) {
      partn.clearHashTableAndHelper();
      partn.closeWriterAndDeleteFile();
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

  private void updateStats() {
    if ( buildSideIsEmpty ) { return; } // no stats when the right side is empty
    if ( cycleNum > 0 ) { return; } // These stats are only for before processing spilled files
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

  @Override
  public void close() {
    for ( HashPartition partn : partitions ) {
      partn.close();
    }
    cleanup();
    super.close();
  }

  // ==============================================================
  //
  //    Methods used for the probe
  //
  // ============================================================
  private BatchSchema probeSchema;

  enum ProbeState {
    PROBE_PROJECT, PROJECT_RIGHT, DONE
  }

  private int currRightPartition = 0; // for returning RIGHT/FULL

  // Number of records to process on the probe side
  private int recordsToProcess = 0;

  // Number of records processed on the probe side
  private int recordsProcessed = 0;

  // Indicate if we should drain the next record from the probe side
  private boolean getNextRecord = true;

  // Contains both batch idx and record idx of the matching record in the build side
  private int currentCompositeIdx = -1;

  // Current state the hash join algorithm is in
  private ProbeState probeState = ProbeState.PROBE_PROJECT;

  // For outer or right joins, this is a list of unmatched records that needs to be projected
  private List<Integer> unmatchedBuildIndexes = null;

  // While probing duplicates, retain current build-side partition in case need to continue
  // probing later on the same chain of duplicates
  private HashPartition currPartition;

  /**
   * Various initialization needed to perform the probe
   * Must be called AFTER the build completes
   */
  private void setupProbe() {
    currRightPartition = 0; // In case it's a Right/Full outer join
    recordsProcessed = 0;
    recordsToProcess = 0;

    probeSchema = probeBatch.getSchema();
    probeState = ProbeState.PROBE_PROJECT;

    // A special case - if the left was an empty file
    if ( leftUpstream == IterOutcome.NONE ){
      changeToFinalProbeState();
    } else {
      this.recordsToProcess = probeBatch.getRecordCount();
    }

    // for those outer partitions that need spilling (cause their matching inners spilled)
    // initialize those partitions' current batches and hash-value vectors
    for ( HashPartition partn : partitions ) {
      partn.allocateNewCurrentBatchAndHV();
    }

    if ( cycleNum > 0 ) {
      if ( read_HV_vector != null ) { read_HV_vector.clear();}
      if ( leftUpstream != IterOutcome.NONE ) { // Skip when outer spill was empty
        read_HV_vector = (IntVector) probeBatch.getContainer().getLast();
      }
    }
  }

  private void executeProjectRightPhase(int currBuildPart) {
    while (outputRecords < TARGET_RECORDS_PER_BATCH && recordsProcessed < recordsToProcess) {
      outputRecords =
        container.appendRow(partitions[currBuildPart].getContainers(), unmatchedBuildIndexes.get(recordsProcessed),
          null /* no probeBatch */, 0 /* no probe index */ );
      recordsProcessed++;
    }
  }

  private void executeProbePhase() throws SchemaChangeException {

    while (outputRecords < TARGET_RECORDS_PER_BATCH && probeState != ProbeState.DONE && probeState != ProbeState.PROJECT_RIGHT) {

      // Check if we have processed all records in this batch we need to invoke next
      if (recordsProcessed == recordsToProcess) {

        // Done processing all records in the previous batch, clean up!
        for (VectorWrapper<?> wrapper : probeBatch) {
          wrapper.getValueVector().clear();
        }

        IterOutcome leftUpstream = next(HashJoinHelper.LEFT_INPUT, probeBatch);

        switch (leftUpstream) {
          case NONE:
          case NOT_YET:
          case STOP:
            recordsProcessed = 0;
            recordsToProcess = 0;
            changeToFinalProbeState();
            // in case some outer partitions were spilled, need to spill their last batches
            for ( HashPartition partn : partitions ) {
              if ( ! partn.isSpilled() ) { continue; } // skip non-spilled
              partn.completeAnOuterBatch(false);
              // update the partition's spill record with the outer side
              HJSpilledPartition sp = spilledInners[partn.getPartitionNum()];
              sp.outerSpillFile = partn.getSpillFile();
              sp.outerSpilledBatches = partn.getPartitionBatchesCount();

              partn.closeWriter();
            }

            continue;

          case OK_NEW_SCHEMA:
            if (probeBatch.getSchema().equals(probeSchema)) {
              for ( HashPartition partn : partitions ) { partn.updateBatches(); }

            } else {
              throw SchemaChangeException.schemaChanged("Hash join does not support schema changes in probe side.",
                probeSchema,
                probeBatch.getSchema());
            }
          case OK:
            recordsToProcess = probeBatch.getRecordCount();
            recordsProcessed = 0;
            // If we received an empty batch do nothing
            if (recordsToProcess == 0) {
              continue;
            }
            if ( cycleNum > 0 ) {
              read_HV_vector = (IntVector) probeBatch.getContainer().getLast(); // Needed ?
            }
        }
      }
      int probeIndex = -1;
      // Check if we need to drain the next row in the probe side
      if (getNextRecord) {

        if ( !buildSideIsEmpty ) {
          int hashCode = ( cycleNum == 0 ) ?
            partitions[0].getProbeHashCode(recordsProcessed)
            : read_HV_vector.getAccessor().get(recordsProcessed);
          int currBuildPart = hashCode & partitionMask ;
          hashCode >>>= bitsInMask;

          // Set and keep the current partition (may be used again on subsequent probe calls as
          // inner rows of duplicate key are processed)
          currPartition = partitions[currBuildPart]; // inner if not spilled, else outer

          // If the matching inner partition was spilled
          if ( isSpilledInner(currBuildPart) ) {
            // add this row to its outer partition (may cause a spill, when the batch is full)

            currPartition.appendOuterRow(hashCode, recordsProcessed);

            recordsProcessed++; // done with this outer record
            continue; // on to the next outer record
          }

          probeIndex = currPartition.probeForKey(recordsProcessed, hashCode);

        }

        if (probeIndex != -1) {

          /* The current probe record has a key that matches. Get the index
           * of the first row in the build side that matches the current key
           * (and record this match in the bitmap, in case of a FULL/RIGHT join)
           */
          currentCompositeIdx = currPartition.getStartIndex(probeIndex);

          outputRecords =
            container.appendRow(currPartition.getContainers(), currentCompositeIdx,
              probeBatch.getContainer(), recordsProcessed);

          /* Projected single row from the build side with matching key but there
           * may be more rows with the same key. Check if that's the case
           */
          currentCompositeIdx = currPartition.getNextIndex(currentCompositeIdx);
          if (currentCompositeIdx == -1) {
            /* We only had one row in the build side that matched the current key
             * from the probe side. Drain the next row in the probe side.
             */
            recordsProcessed++;
          } else {
            /* There is more than one row with the same key on the build side
             * don't drain more records from the probe side till we have projected
             * all the rows with this key
             */
            getNextRecord = false;
          }
        } else { // No matching key

          // If we have a left outer join, project the outer side
          if (joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL) {

            outputRecords =
              container.appendOuterRow(probeBatch.getContainer(), recordsProcessed, rightHVColPosition);
          }
          recordsProcessed++;
        }
      }
      else { // match the next inner row with the same key

        currPartition.setRecordMatched(currentCompositeIdx);

        outputRecords =
          container.appendRow(currPartition.getContainers(), currentCompositeIdx,
            probeBatch.getContainer(), recordsProcessed);

        currentCompositeIdx = currPartition.getNextIndex(currentCompositeIdx);

        if (currentCompositeIdx == -1) {
          // We don't have any more rows matching the current key on the build side, move on to the next probe row
          getNextRecord = true;
          recordsProcessed++;
        }
      }
    }
  }

  /**
   *  Perform the probe and project the results
   *
   * @return number of output records
   * @throws SchemaChangeException
   */
  private int probeAndProject() throws SchemaChangeException {

    outputRecords = 0;

    // When handling spilled partitions, the state becomes DONE at the end of each partition
    if ( probeState == ProbeState.DONE ) {
      return outputRecords; // that is zero
    }

    if (probeState == ProbeState.PROBE_PROJECT) {
      executeProbePhase();
    }

    if (probeState == ProbeState.PROJECT_RIGHT) {
      // Inner probe is done; now we are here because we still have a RIGHT OUTER (or a FULL) join

      do {

        if (unmatchedBuildIndexes == null) { // first time for this partition ?
          if ( buildSideIsEmpty ) { return outputRecords; } // in case of an empty right
          // Get this partition's list of build indexes that didn't match any record on the probe side
          unmatchedBuildIndexes = partitions[currRightPartition].getNextUnmatchedIndex();
          recordsProcessed = 0;
          recordsToProcess = unmatchedBuildIndexes.size();
        }

        // Project the list of unmatched records on the build side
        executeProjectRightPhase(currRightPartition);

        if ( recordsProcessed < recordsToProcess ) { // more records in this partition?
          return outputRecords;  // outgoing is full; report and come back later
        } else {
          currRightPartition++; // on to the next right partition
          unmatchedBuildIndexes = null;
        }

      }   while ( currRightPartition < numPartitions );

      probeState = ProbeState.DONE; // last right partition was handled; we are done now
    }

    return outputRecords;
  }

  private void changeToFinalProbeState() {
    // We are done with the (left) probe phase.
    // If it's a RIGHT or a FULL join then need to get the unmatched indexes from the build side
    probeState =
      (joinType == JoinRelType.RIGHT || joinType == JoinRelType.FULL) ? ProbeState.PROJECT_RIGHT :
      ProbeState.DONE; // else we're done
  }

}  // public class HashJoinBatch
