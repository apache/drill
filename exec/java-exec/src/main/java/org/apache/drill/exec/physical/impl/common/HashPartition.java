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
package org.apache.drill.exec.physical.impl.common;

import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.RetryAfterSpillException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.cache.VectorSerializer;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.join.HashJoinBatch;
import org.apache.drill.exec.physical.impl.join.HashJoinHelper;
import org.apache.drill.exec.physical.impl.spill.SpillSet;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ObjectVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *  The class HashPartition
 *
 *    Created to represent an active partition for the Hash-Join operator
 *  (active means: currently receiving data, or its data is being probed; as opposed to fully
 *   spilled partitions).
 *    After all the build/iner data is read for this partition - if all its data is in memory, then
 *  a hash table and a helper are created, and later this data would be probed.
 *    If all this partition's build/inner data was spilled, then it begins to work as an outer
 *  partition (see the flag "processingOuter") -- reusing some of the fields (e.g., currentBatch,
 *  currHVVector, writer, spillFile, partitionBatchesCount) for the outer.
 */
public class HashPartition {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashPartition.class);

  private int partitionNum = -1; // the current number of this partition, as used by the operator

  private static final int VARIABLE_MIN_WIDTH_VALUE_SIZE = 8;
  private int maxColumnWidth = VARIABLE_MIN_WIDTH_VALUE_SIZE; // to control memory allocation for varchars

  private final MajorType HVtype = MajorType.newBuilder()
    .setMinorType(MinorType.INT /* dataType */ )
    .setMode(DataMode.REQUIRED /* mode */ )
    .build();

  // The vector containers storing all the inner rows
  // * Records are retrieved from these containers when there is a matching record
  // * on the probe side
  private ArrayList<VectorContainer> containers;

  // While build data is incoming - temporarily keep the list of in-memory
  // incoming batches, per each partition (these may be spilled at some point)
  private List<VectorContainer> tmpBatchesList;
  // A batch and HV vector to hold incoming rows - per each partition
  private VectorContainer currentBatch; // The current (newest) batch
  private IntVector currHVVector; // The HV vectors for the currentBatches

  /* Helper class
   * Maintains linked list of build side records with the same key
   * Keeps information about which build records have a corresponding
   * matching key in the probe side (for outer, right joins)
   */
  private HashJoinHelper hjHelper;

  // Underlying hashtable used by the hash join
  private HashTable hashTable;

  private VectorSerializer.Writer writer; // a vector writer for each spilled partition
  private int partitionBatchesCount; // count number of batches spilled
  private String spillFile;

  private BufferAllocator allocator;
  private FragmentContext context;
  private int RECORDS_PER_BATCH;
  ChainedHashTable baseHashTable;
  private SpillSet spillSet;
  private boolean isSpilled; // is this partition spilled ?
  private boolean processingOuter; // is (inner done spilling and) now the outer is processed?
  private boolean outerBatchNotNeeded; // when the inner is whole in memory
  private RecordBatch buildBatch;
  private RecordBatch probeBatch;
  private HashJoinBatch.inMemBatchCounter inMemBatches; // shared among all partitions
  private int cycleNum;

  public HashPartition(FragmentContext context, BufferAllocator allocator, ChainedHashTable baseHashTable,
                       RecordBatch buildBatch, RecordBatch probeBatch,
                       int recordsPerBatch, SpillSet spillSet, int partNum,
                       HashJoinBatch.inMemBatchCounter inMemBatches, int cycleNum) {
    this.context = context;
    this.allocator = allocator;
    this.baseHashTable = baseHashTable;
    this.buildBatch = buildBatch;
    this.probeBatch = probeBatch;
    this.RECORDS_PER_BATCH = recordsPerBatch;
    this.spillSet = spillSet;
    this.partitionNum = partNum;
    this.inMemBatches = inMemBatches;
    this.cycleNum = cycleNum;

    try {
      this.hashTable = baseHashTable.createAndSetupHashTable(null);
      this.hashTable.setMaxVarcharSize(maxColumnWidth);
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
    this.hjHelper = new HashJoinHelper(context, allocator);
    tmpBatchesList = new ArrayList<>();
    allocateNewCurrentBatchAndHV();
  }

  /**
   * Allocate a new vector container for either right or left record batch
   * Add an additional special vector for the hash values
   * Note: this call may OOM !!
   * @param rb - either the right or the left record batch
   * @return the new vector container
   */
  private VectorContainer allocateNewVectorContainer(RecordBatch rb) {
    VectorContainer newVC = new VectorContainer();
    VectorContainer fromVC = rb.getContainer();
    Iterator<VectorWrapper<?>> vci = fromVC.iterator();
    boolean success = false;

    try {
      while (vci.hasNext()) {
        VectorWrapper vw = vci.next();
        // If processing a spilled container, skip the last column (HV)
        if ( cycleNum > 0 && ! vci.hasNext() ) { break; }
        ValueVector vv = vw.getValueVector();
        ValueVector newVV = TypeHelper.getNewVector(vv.getField(), allocator);
        newVC.add(newVV); // add first to allow dealloc in case of an OOM

        if (newVV instanceof FixedWidthVector) {
          ((FixedWidthVector) newVV).allocateNew(RECORDS_PER_BATCH);
        } else if (newVV instanceof VariableWidthVector) {
          // Need to check - (is this case ever used?) if a varchar falls under ObjectVector which is allocated on the heap !
          ((VariableWidthVector) newVV).allocateNew(maxColumnWidth * RECORDS_PER_BATCH, RECORDS_PER_BATCH);
        } else if (newVV instanceof ObjectVector) {
          ((ObjectVector) newVV).allocateNew(RECORDS_PER_BATCH);
        } else {
          newVV.allocateNew();
        }
      }

      newVC.setRecordCount(0);
      inMemBatches.inc(); ; // one more batch in memory
      success = true;
    } finally {
      if ( !success ) {
        newVC.clear(); // in case of an OOM
      }
    }
    return newVC;
  }

  /**
   *  Allocate a new current Vector Container and current HV vector
   */
  public void allocateNewCurrentBatchAndHV() {
    if ( outerBatchNotNeeded ) { return; } // skip when the inner is whole in memory
    currentBatch = allocateNewVectorContainer(processingOuter ? probeBatch : buildBatch);
    currHVVector = new IntVector(MaterializedField.create("Hash_Values", HVtype), allocator);
    currHVVector.allocateNew(RECORDS_PER_BATCH);
  }

  /**
   *  Spills if needed
   */
  public void appendInnerRow(VectorContainer buildContainer, int ind, int hashCode, boolean needsSpill) {

    int pos = currentBatch.appendRow(buildContainer,ind);
    currHVVector.getMutator().set(pos, hashCode);   // store the hash value in the new column
    if ( pos + 1 == RECORDS_PER_BATCH ) {
      completeAnInnerBatch(true, needsSpill);
    }
  }

  /**
   *  Outer always spills when batch is full
   *
   */
  public void appendOuterRow(int hashCode, int recordsProcessed) {
    int pos = currentBatch.appendRow(probeBatch.getContainer(),recordsProcessed);
    currHVVector.getMutator().set(pos, hashCode);   // store the hash value in the new column
    if ( pos + 1 == RECORDS_PER_BATCH ) {
      completeAnOuterBatch(true);
    }
  }

  public void completeAnOuterBatch(boolean toInitialize) {
    completeABatch(toInitialize, true);
  }
  public void completeAnInnerBatch(boolean toInitialize, boolean needsSpill) {
    completeABatch(toInitialize, needsSpill);
  }
  /**
   *     A current batch is full (or no more rows incoming) - complete processing this batch
   * I.e., add it to its partition's tmp list, if needed - spill that list, and if needed -
   * (that is, more rows are coming) - initialize with a new current batch for that partition
   * */
  private void completeABatch(boolean toInitialize, boolean needsSpill) {
    if ( currentBatch.hasRecordCount() && currentBatch.getRecordCount() > 0) {
      currentBatch.add(currHVVector);
      currentBatch.buildSchema(BatchSchema.SelectionVectorMode.NONE);
      tmpBatchesList.add(currentBatch);
      partitionBatchesCount++;
    } else {
      freeCurrentBatchAndHVVector();
    }
    if ( needsSpill ) { // spill this batch/partition and free its memory
      spillThisPartition(tmpBatchesList, processingOuter ? "outer" : "inner");
    }
    if ( toInitialize ) { // allocate a new batch and HV vector
      allocateNewCurrentBatchAndHV();
    } else {
      currentBatch = null;
      currHVVector = null;
    }
  }

  private void spillThisPartition(List<VectorContainer> vcList, String side) {
    if ( vcList.size() == 0 ) { return; } // in case empty - nothing to spill
    logger.debug("HashJoin: Spilling partition {}, current cycle {}, part size {} batches", partitionNum, cycleNum, vcList.size());

    // If this is the first spill for this partition, create an output stream
    if ( writer == null ) {
      // A special case - when (outer is) empty
      if ( vcList.get(0).getRecordCount() == 0 ) {
        VectorContainer vc = vcList.remove(0);
        inMemBatches.dec();
        vc.zeroVectors();
        return;
      }
      String suffix = cycleNum > 0 ? side + "_" + Integer.toString(cycleNum) : side;
      spillFile = spillSet.getNextSpillFile(suffix);

      try {
        writer = spillSet.writer(spillFile);
      } catch (IOException ioe) {
        throw UserException.resourceError(ioe)
          .message("Hash Join failed to open spill file: " + spillFile)
          .build(logger);
      }

      isSpilled = true;
    }

    while ( vcList.size() > 0 ) {
      VectorContainer vc = vcList.remove(0);
      inMemBatches.dec();

      int numRecords = vc.getRecordCount();
      if (numRecords == 0) { // Spilling should to skip an empty batch
        vc.zeroVectors();
        continue;
      }

      // set the value count for outgoing batch value vectors
      for (VectorWrapper<?> v : vc) {
        v.getValueVector().getMutator().setValueCount(numRecords);
      }

      WritableBatch batch = WritableBatch.getBatchNoHVWrap(numRecords, vc, false);
      try {
        writer.write(batch, null);
      } catch (IOException ioe) {
        throw UserException.dataWriteError(ioe)
          .message("Hash Join failed to write to output file: " + spillFile)
          .build(logger);
      } finally {
        batch.clear();
      }
      vc.zeroVectors();
      logger.trace("HASH JOIN: Took {} us to spill {} records", writer.time(TimeUnit.MICROSECONDS), numRecords);

    }
  }

  //
  // ===== Methods to probe the hash table and to get indices out of the helper =======
  //

  public int probeForKey(int recordsProcessed, int hashCode) throws SchemaChangeException {
    return hashTable.probeForKey(recordsProcessed, hashCode);
  }
  public int getStartIndex(int probeIndex) {
    /* The current probe record has a key that matches. Get the index
     * of the first row in the build side that matches the current key
     */
    int compositeIndex = hjHelper.getStartIndex(probeIndex);
    /* Record in the build side at currentCompositeIdx has a matching record in the probe
     * side. Set the bit corresponding to this index so if we are doing a FULL or RIGHT
     * join we keep track of which records we need to project at the end
     */
    hjHelper.setRecordMatched(compositeIndex);
    return compositeIndex;
  }
  public int getNextIndex(int compositeIndex) {
    // in case of iner rows with duplicate keys, get the next one
    return hjHelper.getNextIndex(compositeIndex);
  }
  public void setRecordMatched(int compositeIndex) {
    hjHelper.setRecordMatched(compositeIndex);
  }
  public List<Integer> getNextUnmatchedIndex() {
    return hjHelper.getNextUnmatchedIndex();
  }
  //
  // =====================================================================================
  //

  public int getBuildHashCode(int ind) throws SchemaChangeException {
    return hashTable.getBuildHashCode(ind);
  }
  public int getProbeHashCode(int ind) throws SchemaChangeException {
    return hashTable.getProbeHashCode(ind);
  }
  public ArrayList<VectorContainer> getContainers() {
    return containers;
  }

  public void updateBatches() throws SchemaChangeException {
    hashTable.updateBatches();
  }
  public boolean isSpilled() {
    return isSpilled;
  }
  public String getSpillFile() {
    return spillFile;
  }

  public int getPartitionBatchesCount() {
    return partitionBatchesCount;
  }
  public int getPartitionNum() {
    return partitionNum;
  }

  private void freeCurrentBatchAndHVVector() {
    if ( currentBatch != null ) {
      inMemBatches.dec();
      currentBatch.clear();
      currentBatch = null;
    }
    if ( currHVVector != null ) {
      currHVVector.clear();
      currHVVector = null;
    }
  }

  public void closeWriterAndDeleteFile() {
    closeWriterInternal(true);
  }
  public void closeWriter() { // no deletion !!
    closeWriterInternal(false);
    processingOuter = true; // After the spill file was closed
  }
  /**
   * If exists - close the writer for this partition
   *
   * @param doDeleteFile Also delete the associated file
   */
  private void closeWriterInternal(boolean doDeleteFile) {
    try {
      if ( writer != null ) {
        spillSet.close(writer);
      }
      if ( doDeleteFile && spillFile != null ) {
        spillSet.delete(spillFile);
      }
    } catch (IOException ioe) {
      throw UserException.resourceError(ioe)
        .message("IO Error while closing %s spill file %s",
          doDeleteFile ? "and deleting" : "",
          spillFile)
        .build(logger);
    }
    spillFile = null;
    writer = null;
    partitionBatchesCount = 0;
  }

  /**
   *
   */
  public void buildContainersHashTableAndHelper() throws SchemaChangeException {
    if ( isSpilled ) { return; } // no building for spilled partitions
    containers = new ArrayList<>();
    for (int curr = 0; curr < partitionBatchesCount; curr++) {
      VectorContainer nextBatch = tmpBatchesList.get(curr);
      final int currentRecordCount = nextBatch.getRecordCount();

      // For every incoming build batch, we create a matching helper batch
      hjHelper.addNewBatch(currentRecordCount);

      // Holder contains the global index where the key is hashed into using the hash table
      final IndexPointer htIndex = new IndexPointer();

      assert nextBatch != null;
      assert probeBatch != null;

      hashTable.updateIncoming(nextBatch, probeBatch );

      // IntVector HV_vector = (IntVector) nextBatch.getValueVector(rightHVColPosition).getValueVector();
      IntVector HV_vector = (IntVector) nextBatch.getLast();

      for (int recInd = 0; recInd < currentRecordCount; recInd++) {
        int hashCode = HV_vector.getAccessor().get(recInd);
        try {
          hashTable.put(recInd, htIndex, hashCode);
        } catch (RetryAfterSpillException RE) {
          throw new OutOfMemoryException("HT put");
        } // Hash Join can not retry yet
        /* Use the global index returned by the hash table, to store
         * the current record index and batch index. This will be used
         * later when we probe and find a match.
         */
        hjHelper.setCurrentIndex(htIndex.value, curr /* buildBatchIndex */, recInd);
      }

      containers.add(nextBatch);
    }
    outerBatchNotNeeded = true; // the inner is whole in memory, no need for an outer batch
  }

  public void getStats(HashTableStats newStats) {
    hashTable.getStats(newStats);
  }

  public void clearHashTableAndHelper() {
    if (hashTable != null) {
      hashTable.clear();
      hashTable = null;
    }
    if (hjHelper != null) {
      hjHelper.clear();
      hjHelper = null;
    }
  }

  public void close() {
    freeCurrentBatchAndHVVector();
    if (containers != null && !containers.isEmpty()) {
      for (VectorContainer vc : containers) {
        vc.clear();
      }
    }
    while ( tmpBatchesList.size() > 0 ) {
      VectorContainer vc = tmpBatchesList.remove(0);
      inMemBatches.dec();
      vc.clear();
    }
    closeWriter();
    partitionBatchesCount = 0;
    spillFile = null;
    clearHashTableAndHelper();
    if ( containers != null ) { containers.clear(); }
  }

} // class HashPartition
