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

import java.util.ArrayList;
import java.util.Iterator;

import javax.inject.Named;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.compile.sig.RuntimeOverridden;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;
import org.apache.drill.common.exceptions.RetryAfterSpillException;

public abstract class HashTableTemplate implements HashTable {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashTable.class);
  private static final boolean EXTRA_DEBUG = false;

  private static final int EMPTY_SLOT = -1;

  // A hash 'bucket' consists of the start index to indicate start of a hash chain

  // Array of start indexes. start index is a global index across all batch holders
  //  This is the "classic hash table", where  Hash-Value % size-of-table  yields
  // the offset/position (in the startIndices) of the beginning of the hash chain.
  private IntVector startIndices;

  // Array of batch holders..each batch holder can hold up to BATCH_SIZE entries
  private ArrayList<BatchHolder> batchHolders;

  // Current size of the hash table in terms of number of buckets
  private int tableSize = 0;

  // Original size of the hash table (needed when re-initializing)
  private int originalTableSize;

  // Threshold after which we rehash; It must be the tableSize * loadFactor
  private int threshold;

  // Actual number of entries in the hash table
  private int numEntries = 0;

  // current available (free) slot globally across all batch holders
  private int freeIndex = 0;

  // Placeholder for the current index while probing the hash table
  private IndexPointer currentIdxHolder;

  private BufferAllocator allocator;

  // The incoming build side record batch
  private RecordBatch incomingBuild;

  // The incoming probe side record batch (may be null)
  private RecordBatch incomingProbe;

  // The outgoing record batch
  private RecordBatch outgoing;

  // Hash table configuration parameters
  private HashTableConfig htConfig;

  // The original container from which others may be cloned
  private VectorContainer htContainerOrig;

  private MaterializedField dummyIntField;

  private int numResizing = 0;

  private int resizingTime = 0;

  private int maxVarcharSize = 8; // for varchar allocation

  // This class encapsulates the links, keys and values for up to BATCH_SIZE
  // *unique* records. Thus, suppose there are N incoming record batches, each
  // of size BATCH_SIZE..but they have M unique keys altogether, the number of
  // BatchHolders will be (M/BATCH_SIZE) + 1
  public class BatchHolder {

    // Container of vectors to hold type-specific keys
    private VectorContainer htContainer;

    // Array of 'link' values
    private IntVector links;

    // Array of hash values - this is useful when resizing the hash table
    private IntVector hashValues;

    private int maxOccupiedIdx = -1;
//    private int batchOutputCount = 0;

    private int batchIndex = 0;

    public BatchHolder(int idx) {

      this.batchIndex = idx;

      htContainer = new VectorContainer();
      boolean success = false;
      try {
        for (VectorWrapper<?> w : htContainerOrig) {
          ValueVector vv = TypeHelper.getNewVector(w.getField(), allocator);
          htContainer.add(vv); // add to container before actual allocation (to allow clearing in case of an OOM)

          // Capacity for "hashValues" and "links" vectors is BATCH_SIZE records. It is better to allocate space for
          // "key" vectors to store as close to as BATCH_SIZE records. A new BatchHolder is created when either BATCH_SIZE
          // records are inserted or "key" vectors ran out of space. Allocating too less space for "key" vectors will
          // result in unused space in "hashValues" and "links" vectors in the BatchHolder. Also for each new
          // BatchHolder we create a SV4 vector of BATCH_SIZE in HashJoinHelper.
          if (vv instanceof FixedWidthVector) {
            ((FixedWidthVector) vv).allocateNew(BATCH_SIZE);
          } else if (vv instanceof VariableWidthVector) {
            long beforeMem = allocator.getAllocatedMemory();
            ((VariableWidthVector) vv).allocateNew(maxVarcharSize * BATCH_SIZE, BATCH_SIZE);
            logger.trace("HT allocated {} for varchar of max width {}",allocator.getAllocatedMemory() - beforeMem, maxVarcharSize);
          } else {
            vv.allocateNew();
          }
        }

        links = allocMetadataVector(HashTable.BATCH_SIZE, EMPTY_SLOT);
        hashValues = allocMetadataVector(HashTable.BATCH_SIZE, 0);
        success = true;
      } finally {
        if (!success) {
          htContainer.clear();
          if (links != null) { links.clear();}
        }
      }
    }

    private void init(IntVector links, IntVector hashValues, int size) {
      for (int i = 0; i < size; i++) {
        links.getMutator().set(i, EMPTY_SLOT);
      }
      for (int i = 0; i < size; i++) {
        hashValues.getMutator().set(i, 0);
      }
      links.getMutator().setValueCount(size);
      hashValues.getMutator().setValueCount(size);
    }

    protected void setup() throws SchemaChangeException {
      setupInterior(incomingBuild, incomingProbe, outgoing, htContainer);
    }

    // Check if the key at the currentIdx position in hash table matches the key
    // at the incomingRowIdx. if the key does not match, update the
    // currentIdxHolder with the index of the next link.
    private boolean isKeyMatch(int incomingRowIdx,
        IndexPointer currentIdxHolder,
        boolean isProbe) throws SchemaChangeException {

      int currentIdxWithinBatch = currentIdxHolder.value & BATCH_MASK;
      boolean match = false;

      if (currentIdxWithinBatch >= HashTable.BATCH_SIZE) {
        logger.debug("Batch size = {}, incomingRowIdx = {}, currentIdxWithinBatch = {}.", HashTable.BATCH_SIZE,
            incomingRowIdx, currentIdxWithinBatch);
      }
      assert (currentIdxWithinBatch < HashTable.BATCH_SIZE);
      assert (incomingRowIdx < HashTable.BATCH_SIZE);

      if (isProbe) {
        match = isKeyMatchInternalProbe(incomingRowIdx, currentIdxWithinBatch);
      } else {
        match = isKeyMatchInternalBuild(incomingRowIdx, currentIdxWithinBatch);
      }

      if (!match) {
        currentIdxHolder.value = links.getAccessor().get(currentIdxWithinBatch);
      }
      return match;
    }

    // Insert a new <key1, key2...keyN> entry coming from the incoming batch into the hash table
    // container at the specified index
    private void insertEntry(int incomingRowIdx, int currentIdx, int hashValue, BatchHolder lastEntryBatch, int lastEntryIdxWithinBatch) throws SchemaChangeException {
      int currentIdxWithinBatch = currentIdx & BATCH_MASK;

      setValue(incomingRowIdx, currentIdxWithinBatch);
      // setValue may OOM when doubling of one of the VarChar Key Value Vectors
      // This would be caught and retried later (setValue() is idempotent)

      // the previous entry in this hash chain should now point to the entry in this currentIdx
      if (lastEntryBatch != null) {
        lastEntryBatch.updateLinks(lastEntryIdxWithinBatch, currentIdx);
      }

      // since this is the last entry in the hash chain, the links array at position currentIdx
      // will point to a null (empty) slot
      links.getMutator().set(currentIdxWithinBatch, EMPTY_SLOT);
      hashValues.getMutator().set(currentIdxWithinBatch, hashValue);

      maxOccupiedIdx = Math.max(maxOccupiedIdx, currentIdxWithinBatch);

      if (EXTRA_DEBUG) {
        logger.debug("BatchHolder: inserted key at incomingRowIdx = {}, currentIdx = {}, hash value = {}.",
            incomingRowIdx, currentIdx, hashValue);
      }
    }

    private void updateLinks(int lastEntryIdxWithinBatch, int currentIdx) {
      links.getMutator().set(lastEntryIdxWithinBatch, currentIdx);
    }

    private void rehash(int numbuckets, IntVector newStartIndices, int batchStartIdx) {

      logger.debug("Rehashing entries within the batch: {}; batchStartIdx = {}, total numBuckets in hash table = {}.", batchIndex, batchStartIdx, numbuckets);

      int size = links.getAccessor().getValueCount();
      IntVector newLinks = allocMetadataVector(size, EMPTY_SLOT);
      IntVector newHashValues = allocMetadataVector(size, 0);

      for (int i = 0; i <= maxOccupiedIdx; i++) {
        int entryIdxWithinBatch = i;
        int entryIdx = entryIdxWithinBatch + batchStartIdx;
        int hash = hashValues.getAccessor().get(entryIdxWithinBatch); // get the already saved hash value
        int bucketIdx = getBucketIndex(hash, numbuckets);
        int newStartIdx = newStartIndices.getAccessor().get(bucketIdx);

        if (newStartIdx == EMPTY_SLOT) { // new bucket was empty
          newStartIndices.getMutator().set(bucketIdx, entryIdx); // update the start index to point to entry
          newLinks.getMutator().set(entryIdxWithinBatch, EMPTY_SLOT);
          newHashValues.getMutator().set(entryIdxWithinBatch, hash);

          if (EXTRA_DEBUG) {
            logger.debug("New bucket was empty. bucketIdx = {}, newStartIndices[ {} ] = {}, newLinks[ {} ] = {}, " +
                "hash value = {}.", bucketIdx, bucketIdx, newStartIndices.getAccessor().get(bucketIdx),
                entryIdxWithinBatch, newLinks.getAccessor().get(entryIdxWithinBatch),
                newHashValues.getAccessor().get(entryIdxWithinBatch));
          }

        } else {
          // follow the new table's hash chain until we encounter empty slot. Note that the hash chain could
          // traverse multiple batch holders, so make sure we are accessing the right batch holder.
          int idx = newStartIdx;
          int idxWithinBatch = 0;
          BatchHolder bh = this;
          while (true) {
            if (idx != EMPTY_SLOT) {
              idxWithinBatch = idx & BATCH_MASK;
              int batchIdx = ((idx >>> 16) & BATCH_MASK);
              bh = batchHolders.get(batchIdx);
            }

            if (bh == this && newLinks.getAccessor().get(idxWithinBatch) == EMPTY_SLOT) {
              newLinks.getMutator().set(idxWithinBatch, entryIdx);
              newLinks.getMutator().set(entryIdxWithinBatch, EMPTY_SLOT);
              newHashValues.getMutator().set(entryIdxWithinBatch, hash);

              if (EXTRA_DEBUG) {
                logger.debug("Followed hash chain in new bucket. bucketIdx = {}, newLinks[ {} ] = {}, " +
                    "newLinks[ {} ] = {}, hash value = {}.", bucketIdx, idxWithinBatch,
                    newLinks.getAccessor().get(idxWithinBatch), entryIdxWithinBatch,
                    newLinks.getAccessor().get(entryIdxWithinBatch), newHashValues.getAccessor().get
                        (entryIdxWithinBatch));
              }

              break;
            } else if (bh != this && bh.links.getAccessor().get(idxWithinBatch) == EMPTY_SLOT) {
              bh.links.getMutator().set(idxWithinBatch, entryIdx); // update the link in the other batch
              newLinks.getMutator().set(entryIdxWithinBatch, EMPTY_SLOT); // update the newLink entry in this
              // batch to mark end of the hash chain
              newHashValues.getMutator().set(entryIdxWithinBatch, hash);

              if (EXTRA_DEBUG) {
                logger.debug("Followed hash chain in new bucket. bucketIdx = {}, newLinks[ {} ] = {}, " +
                    "newLinks[ {} ] = {}, hash value = {}.", bucketIdx, idxWithinBatch,
                    newLinks.getAccessor().get(idxWithinBatch), entryIdxWithinBatch,
                    newLinks.getAccessor().get(entryIdxWithinBatch),
                    newHashValues.getAccessor().get(entryIdxWithinBatch));
              }

              break;
            }
            if (bh == this) {
              idx = newLinks.getAccessor().get(idxWithinBatch);
            } else {
              idx = bh.links.getAccessor().get(idxWithinBatch);
            }
          }

        }

      }

      links.clear();
      hashValues.clear();

      links = newLinks;
      hashValues = newHashValues;
    }

    private boolean outputKeys(VectorContainer outContainer, int outStartIndex, int numRecords, int numExpectedRecords) {

      /** for debugging
      BigIntVector vv0 = getValueVector(0);
      BigIntHolder holder = new BigIntHolder();
      */

      // set the value count for htContainer's value vectors before the transfer ..
      setValueCount();

      Iterator<VectorWrapper<?>> outgoingIter = outContainer.iterator();

      for (VectorWrapper<?> sourceWrapper : htContainer) {
        @SuppressWarnings("resource")
        ValueVector sourceVV = sourceWrapper.getValueVector();
        @SuppressWarnings("resource")
        ValueVector targetVV = outgoingIter.next().getValueVector();
        TransferPair tp = sourceVV.makeTransferPair(targetVV);
        if ( outStartIndex == 0 && numRecords == numExpectedRecords ) {
          // The normal case: The whole column key(s) are transfered as is
          tp.transfer();
        } else {
          // Transfer just the required section (does this ever happen ?)
          // Requires an expensive allocation and copy
          logger.debug("Performing partial output of keys, from index {}, num {} (out of {})",
              outStartIndex,numRecords,numExpectedRecords);
          tp.splitAndTransfer(outStartIndex, numRecords);
        }
      }

/*
      logger.debug("Attempting to output keys for batch index: {} from index {} to maxOccupiedIndex {}.",
      this.batchIndex, 0, maxOccupiedIdx);
      for (int i = batchOutputCount; i <= maxOccupiedIdx; i++) {
        if (outputRecordKeys(i, batchOutputCount) ) {
          if (EXTRA_DEBUG) logger.debug("Outputting keys to output index: {}", batchOutputCount) ;

          // debugging
          // holder.value = vv0.getAccessor().get(i);
          // if (holder.value == 100018 || holder.value == 100021) {
          //  logger.debug("Outputting key = {} at index - {} to outgoing index = {}.", holder.value, i,
          //      batchOutputCount);
          // }

          batchOutputCount++;
        } else {
          return false;
        }
      }
 */
      return true;
    }

    private void setValueCount() {
      for (VectorWrapper<?> vw : htContainer) {
        @SuppressWarnings("resource")
        ValueVector vv = vw.getValueVector();
        vv.getMutator().setValueCount(maxOccupiedIdx + 1);
      }
    }

    private void dump(int idx) {
      while (true) {
        int idxWithinBatch = idx & BATCH_MASK;
        if (idxWithinBatch == EMPTY_SLOT) {
          break;
        } else {
          logger.debug("links[ {} ] = {}, hashValues[ {} ] = {}.", idxWithinBatch,
              links.getAccessor().get(idxWithinBatch), idxWithinBatch, hashValues.getAccessor().get(idxWithinBatch));
          idx = links.getAccessor().get(idxWithinBatch);
        }
      }
    }

    private void clear() {
      htContainer.clear();
      if ( links != null ) { links.clear(); }
      if ( hashValues != null ) { hashValues.clear(); }
    }

    // Only used for internal debugging. Get the value vector at a particular index from the htContainer.
    // By default this assumes the VV is a BigIntVector.
    private ValueVector getValueVector(int index) {
      Object tmp = (htContainer).getValueAccessorById(BigIntVector.class, index).getValueVector();
      if (tmp != null) {
        BigIntVector vv0 = ((BigIntVector) tmp);
        return vv0;
      }
      return null;
    }

    // These methods will be code-generated

    @RuntimeOverridden
    protected void setupInterior(
        @Named("incomingBuild") RecordBatch incomingBuild,
        @Named("incomingProbe") RecordBatch incomingProbe,
        @Named("outgoing") RecordBatch outgoing,
        @Named("htContainer") VectorContainer htContainer) throws SchemaChangeException {
    }

    @RuntimeOverridden
    protected boolean isKeyMatchInternalBuild(
        @Named("incomingRowIdx") int incomingRowIdx, @Named("htRowIdx") int htRowIdx) throws SchemaChangeException {
      return false;
    }

    @RuntimeOverridden
    protected boolean isKeyMatchInternalProbe(
        @Named("incomingRowIdx") int incomingRowIdx, @Named("htRowIdx") int htRowIdx) throws SchemaChangeException {
      return false;
    }

    @RuntimeOverridden
    protected void setValue(@Named("incomingRowIdx") int incomingRowIdx, @Named("htRowIdx") int htRowIdx) throws SchemaChangeException {
    }

    @RuntimeOverridden
    protected void outputRecordKeys(@Named("htRowIdx") int htRowIdx, @Named("outRowIdx") int outRowIdx) throws SchemaChangeException {
    }

  } // class BatchHolder


  @Override
  public void setup(HashTableConfig htConfig, BufferAllocator allocator, RecordBatch incomingBuild, RecordBatch incomingProbe, RecordBatch outgoing, VectorContainer htContainerOrig) {
    float loadf = htConfig.getLoadFactor();
    int initialCap = htConfig.getInitialCapacity();

    if (loadf <= 0 || Float.isNaN(loadf)) {
      throw new IllegalArgumentException("Load factor must be a valid number greater than 0");
    }
    if (initialCap <= 0) {
      throw new IllegalArgumentException("The initial capacity must be greater than 0");
    }
    if (initialCap > MAXIMUM_CAPACITY) {
      throw new IllegalArgumentException("The initial capacity must be less than maximum capacity allowed");
    }

    if (htConfig.getKeyExprsBuild() == null || htConfig.getKeyExprsBuild().size() == 0) {
      throw new IllegalArgumentException("Hash table must have at least 1 key expression");
    }

    this.htConfig = htConfig;
    this.allocator = allocator;
    this.incomingBuild = incomingBuild;
    this.incomingProbe = incomingProbe;
    this.outgoing = outgoing;
    this.htContainerOrig = htContainerOrig;

    // round up the initial capacity to nearest highest power of 2
    tableSize = roundUpToPowerOf2(initialCap);
    if (tableSize > MAXIMUM_CAPACITY) {
      tableSize = MAXIMUM_CAPACITY;
    }
    originalTableSize = tableSize ; // retain original size

    threshold = (int) Math.ceil(tableSize * loadf);

    dummyIntField = MaterializedField.create("dummy", Types.required(MinorType.INT));

    startIndices = allocMetadataVector(tableSize, EMPTY_SLOT);

    // Create the first batch holder
    batchHolders = new ArrayList<BatchHolder>();
    // First BatchHolder is created when the first put request is received.

    try {
      doSetup(incomingBuild, incomingProbe);
    } catch (SchemaChangeException e) {
      throw new IllegalStateException("Unexpected schema change", e);
    }

    currentIdxHolder = new IndexPointer();
  }

  @Override
  public void updateBatches() throws SchemaChangeException {
    doSetup(incomingBuild, incomingProbe);
    for (BatchHolder batchHolder : batchHolders) {
      batchHolder.setup();
    }
  }

  public int numBuckets() {
    return startIndices.getAccessor().getValueCount();
  }

  public int numResizing() {
    return numResizing;
  }

  @Override
  public int size() {
    return numEntries;
  }

  @Override
  public void getStats(HashTableStats stats) {
    assert stats != null;
    stats.numBuckets = numBuckets();
    stats.numEntries = numEntries;
    stats.numResizing = numResizing;
    stats.resizingTime = resizingTime;
  }

  @Override
  public boolean isEmpty() {
    return numEntries == 0;
  }

  @Override
  public void clear() {
    if (batchHolders != null) {
      for (BatchHolder bh : batchHolders) {
        bh.clear();
      }
      batchHolders.clear();
      batchHolders = null;
    }
    startIndices.clear();
    // currentIdxHolder = null; // keep IndexPointer in case HT is reused
    numEntries = 0;
  }

  private int getBucketIndex(int hash, int numBuckets) {
    return hash & (numBuckets - 1);
  }

  private static int roundUpToPowerOf2(int number) {
    int rounded = number >= MAXIMUM_CAPACITY
        ? MAXIMUM_CAPACITY
        : (rounded = Integer.highestOneBit(number)) != 0
        ? (Integer.bitCount(number) > 1) ? rounded << 1 : rounded
        : 1;

    return rounded;
  }

  private void retryAfterOOM(boolean batchAdded) throws RetryAfterSpillException {
    // If a batch was added then undo; otherwise when retrying this put() we'd miss a NEW_BATCH_ADDED
    if ( batchAdded ) {
      logger.trace("OOM - Removing index {} from the batch holders list",batchHolders.size() - 1);
      BatchHolder bh = batchHolders.remove(batchHolders.size() - 1);
      bh.clear();
    }
    freeIndex--;
    throw new RetryAfterSpillException();
  }

  public int getHashCode(int incomingRowIdx) throws SchemaChangeException {
    return getHashBuild(incomingRowIdx, 0);
  }

  /** put() uses the hash code (from gethashCode() above) to insert the key(s) from the incoming
   * row into the hash table. The code selects the bucket in the startIndices, then the keys are
   * placed into the chained list - by storing the key values into a batch, and updating its
   * "links" member. Last it modifies the index holder to the batch offset so that the caller
   * can store the remaining parts of the row into a matching batch (outside the hash table).
   * Returning
   *
   * @param incomingRowIdx - position of the incoming row
   * @param htIdxHolder - to return batch + batch-offset (for caller to manage a matching batch)
   * @param hashCode - computed over the key(s) by calling getHashCode()
   * @return Status - the key(s) was ADDED or was already PRESENT
   */
  @Override
  public PutStatus put(int incomingRowIdx, IndexPointer htIdxHolder, int hashCode) throws SchemaChangeException, RetryAfterSpillException {

    int bucketIndex = getBucketIndex(hashCode, numBuckets());
    int startIdx = startIndices.getAccessor().get(bucketIndex);
    int currentIdx;
    BatchHolder lastEntryBatch = null;
    int lastEntryIdxWithinBatch = EMPTY_SLOT;

    // if startIdx is non-empty, follow the hash chain links until we find a matching
    // key or reach the end of the chain (and remember the last link there)
    for ( currentIdxHolder.value = startIdx;
          currentIdxHolder.value != EMPTY_SLOT;
          /* isKeyMatch() below also advances the currentIdxHolder to the next link */) {

      // remember the current link, which would be the last when the next link is empty
      lastEntryBatch = batchHolders.get((currentIdxHolder.value >>> 16) & HashTable.BATCH_MASK);
      lastEntryIdxWithinBatch = currentIdxHolder.value & BATCH_MASK;

      if (lastEntryBatch.isKeyMatch(incomingRowIdx, currentIdxHolder, false)) {
        htIdxHolder.value = currentIdxHolder.value;
        return PutStatus.KEY_PRESENT;
      }
    }

    // no match was found, so insert a new entry
    currentIdx = freeIndex++;
    boolean addedBatch = false;
    try {  // ADD A BATCH
      addedBatch = addBatchIfNeeded(currentIdx);
    } catch (OutOfMemoryException OOME) {
      retryAfterOOM( currentIdx < batchHolders.size() * BATCH_SIZE );
    }

    try { // INSERT ENTRY
      BatchHolder bh = batchHolders.get((currentIdx >>> 16) & BATCH_MASK);

      bh.insertEntry(incomingRowIdx, currentIdx, hashCode, lastEntryBatch, lastEntryIdxWithinBatch);
      numEntries++;
    } catch (OutOfMemoryException OOME) { retryAfterOOM( addedBatch ); }

    try {  // RESIZE HT
      /* Resize hash table if needed and transfer the metadata
       * Resize only after inserting the current entry into the hash table
       * Otherwise our calculated lastEntryBatch and lastEntryIdx
       * becomes invalid after resize.
       */
      resizeAndRehashIfNeeded();
    } catch (OutOfMemoryException OOME) {
      numEntries--; // undo - insert entry
      if (lastEntryBatch != null) { // undo last added link in chain (if any)
        lastEntryBatch.updateLinks(lastEntryIdxWithinBatch, EMPTY_SLOT);
      }
      retryAfterOOM( addedBatch );
    }

    if (EXTRA_DEBUG) {
      logger.debug("No match was found for incomingRowIdx = {}; inserting new entry at currentIdx = {}.", incomingRowIdx, currentIdx);
    }

    // if there was no hash chain at this bucket, need to update the start index array
    if (startIdx == EMPTY_SLOT) {
      startIndices.getMutator().set(getBucketIndex(hashCode, numBuckets()), currentIdx);
    }
    htIdxHolder.value = currentIdx;
    return  addedBatch ? PutStatus.NEW_BATCH_ADDED :
        ( freeIndex + 1 > batchHolders.size() * BATCH_SIZE ) ?
        PutStatus.KEY_ADDED_LAST : // the last key in the batch
        PutStatus.KEY_ADDED;     // otherwise
  }

  // Return -1 if key is not found in the hash table. Otherwise, return the global index of the key
  @Override
  public int containsKey(int incomingRowIdx, boolean isProbe) throws SchemaChangeException {
    int seedValue = 0;
    int hash = isProbe ? getHashProbe(incomingRowIdx, seedValue) : getHashBuild(incomingRowIdx, seedValue);
    int bucketIndex = getBucketIndex(hash, numBuckets());

    for ( currentIdxHolder.value = startIndices.getAccessor().get(bucketIndex);
          currentIdxHolder.value != EMPTY_SLOT; ) {
      BatchHolder bh = batchHolders.get((currentIdxHolder.value >>> 16) & BATCH_MASK);
      if (bh.isKeyMatch(incomingRowIdx, currentIdxHolder, isProbe)) {
        return currentIdxHolder.value;
      }
    }
    return -1;
  }

  // Add a new BatchHolder to the list of batch holders if needed. This is based on the supplied
  // currentIdx; since each BatchHolder can hold up to BATCH_SIZE entries, if the currentIdx exceeds
  // the capacity, we will add a new BatchHolder. Return true if a new batch was added.
  private boolean addBatchIfNeeded(int currentIdx) throws SchemaChangeException {
    int totalBatchSize = batchHolders.size() * BATCH_SIZE;

    if (currentIdx >= totalBatchSize) {
      BatchHolder bh = newBatchHolder(batchHolders.size());
      batchHolders.add(bh);
      bh.setup();
      if (EXTRA_DEBUG) {
        logger.debug("HashTable: Added new batch. Num batches = {}.", batchHolders.size());
      }
      return true;
    }
    return false;
  }

  protected BatchHolder newBatchHolder(int index) { // special method to allow debugging of gen code
    return new BatchHolder(index);
  }

  // Resize the hash table if needed by creating a new one with double the number of buckets.
  // For each entry in the old hash table, re-hash it to the new table and update the metadata
  // in the new table.. the metadata consists of the startIndices, links and hashValues.
  // Note that the keys stored in the BatchHolders are not moved around.
  private void resizeAndRehashIfNeeded() {
    if (numEntries < threshold) {
      return;
    }

    if (EXTRA_DEBUG) {
      logger.debug("Hash table numEntries = {}, threshold = {}; resizing the table...", numEntries, threshold);
    }

    // If the table size is already MAXIMUM_CAPACITY, don't resize
    // the table, but set the threshold to Integer.MAX_VALUE such that
    // future attempts to resize will return immediately.
    if (tableSize == MAXIMUM_CAPACITY) {
      threshold = Integer.MAX_VALUE;
      return;
    }

    int newTableSize = 2 * tableSize;
    newTableSize = roundUpToPowerOf2(newTableSize);

    // if not enough memory available to allocate the new hash-table, plus the new links and
    // the new hash-values (to replace the existing ones - inside rehash() ), then OOM
    if ( 4 /* sizeof(int) */ * ( newTableSize + 2 * HashTable.BATCH_SIZE /* links + hashValues */)
        >= allocator.getLimit() - allocator.getAllocatedMemory()) {
      throw new OutOfMemoryException("Resize Hash Table");
    }

    tableSize = newTableSize;
    if (tableSize > MAXIMUM_CAPACITY) {
      tableSize = MAXIMUM_CAPACITY;
    }

    long t0 = System.currentTimeMillis();

    // set the new threshold based on the new table size and load factor
    threshold = (int) Math.ceil(tableSize * htConfig.getLoadFactor());

    IntVector newStartIndices = allocMetadataVector(tableSize, EMPTY_SLOT);

    for (int i = 0; i < batchHolders.size(); i++) {
      BatchHolder bh = batchHolders.get(i);
      int batchStartIdx = i * BATCH_SIZE;
      bh.rehash(tableSize, newStartIndices, batchStartIdx);
    }

    startIndices.clear();
    startIndices = newStartIndices;

    if (EXTRA_DEBUG) {
      logger.debug("After resizing and rehashing, dumping the hash table...");
      logger.debug("Number of buckets = {}.", startIndices.getAccessor().getValueCount());
      for (int i = 0; i < startIndices.getAccessor().getValueCount(); i++) {
        logger.debug("Bucket: {}, startIdx[ {} ] = {}.", i, i, startIndices.getAccessor().get(i));
        int idx = startIndices.getAccessor().get(i);
        BatchHolder bh = batchHolders.get((idx >>> 16) & BATCH_MASK);
        bh.dump(idx);
      }
    }
    resizingTime += System.currentTimeMillis() - t0;
    numResizing++;
  }

  /**
   * Reinit the hash table to its original size, and clear up all its prior batch holder
   *
   */
  public void reset() {
    this.clear(); // Clear all current batch holders and hash table (i.e. free their memory)

    freeIndex = 0; // all batch holders are gone
    // reallocate batch holders, and the hash table to the original size
    batchHolders = new ArrayList<BatchHolder>();
    startIndices = allocMetadataVector(originalTableSize, EMPTY_SLOT);
  }
  public void reinit(RecordBatch newIncoming) {
    incomingBuild = newIncoming;
    reset();
    try {
      updateBatches();  // Needed to update the value vectors in the generated code with the new incoming
    } catch (SchemaChangeException e) {
      throw new IllegalStateException("Unexpected schema change", e);
    } catch(IndexOutOfBoundsException ioob) {
      throw new IllegalStateException("reinit update batches", ioob);
    }
  }

  @Override
  public boolean outputKeys(int batchIdx, VectorContainer outContainer, int outStartIndex, int numRecords, int numExpectedRecords) {
    assert batchIdx < batchHolders.size();
    return batchHolders.get(batchIdx).outputKeys(outContainer, outStartIndex, numRecords, numExpectedRecords);
  }

  private IntVector allocMetadataVector(int size, int initialValue) {
    IntVector vector = (IntVector) TypeHelper.getNewVector(dummyIntField, allocator);
    vector.allocateNew(size);
    for (int i = 0; i < size; i++) {
      vector.getMutator().set(i, initialValue);
    }
    vector.getMutator().setValueCount(size);
    return vector;
  }

  @Override
  public void setMaxVarcharSize(int size) { maxVarcharSize = size; }

  // These methods will be code-generated in the context of the outer class
  protected abstract void doSetup(@Named("incomingBuild") RecordBatch incomingBuild, @Named("incomingProbe") RecordBatch incomingProbe) throws SchemaChangeException;

  protected abstract int getHashBuild(@Named("incomingRowIdx") int incomingRowIdx, @Named("seedValue") int seedValue) throws SchemaChangeException;

  protected abstract int getHashProbe(@Named("incomingRowIdx") int incomingRowIdx, @Named("seedValue") int seedValue) throws SchemaChangeException;

}
