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
package org.apache.drill.exec.record;

import java.util.Iterator;
import java.util.Map;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeMap;

/**
 * RecordIterator iterates over incoming record batches one record at a time.
 * It allows to mark a position during iteration and reset back.
 * RecordIterator will hold onto multiple record batches in order to support resetting beyond record batch boundary.
 */
public class RecordIterator implements VectorAccessible {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RecordIterator.class);

  private final RecordBatch incoming;
  private final AbstractRecordBatch<?> outgoing;
  private long outerPosition;       // Tracks total records consumed so far, works across batches.
  private int innerPosition;        // Index within current vector container.
  private int innerRecordCount;     // Records in current container.
  private long totalRecordCount;    // Total records read so far.
  private long startBatchPosition;  // Start offset of current batch.
  private int markedInnerPosition;
  private long markedOuterPosition;
  private IterOutcome lastOutcome;
  private int inputIndex;           // For two way merge join 0:left, 1:right
  private boolean lastBatchRead;    // True if all batches are consumed.
  private boolean initialized;
  private OperatorContext oContext;

  private final VectorContainer container; // Holds VectorContainer of current record batch
  private final TreeRangeMap<Long, RecordBatchData> batches = TreeRangeMap.create();

  public RecordIterator(RecordBatch incoming,
                        AbstractRecordBatch<?> outgoing,
                        OperatorContext oContext,
                        int inputIndex) {
    this.incoming = incoming;
    this.outgoing = outgoing;
    this.inputIndex = inputIndex;
    this.lastBatchRead = false;
    this.container = new VectorContainer(oContext);
    this.oContext = oContext;
    resetIndices();
    this.initialized = false;
  }

  private void resetIndices() {
    this.innerPosition = -1;
    this.startBatchPosition = -1;
    this.outerPosition = -1;
    this.totalRecordCount = 0;
    this.innerRecordCount = 0;
    this.markedInnerPosition = -1;
    this.markedOuterPosition = -1;
  }

  // Get next record batch.
  private void nextBatch() {
    // We have already seen last batch.
    if (lastBatchRead) {
      return;
    }
    lastOutcome = outgoing.next(inputIndex, incoming);
  }

  public void mark() {
    // Release all batches before current batch. [0 to startBatchPosition).
    final Map<Range<Long>,RecordBatchData> oldBatches = batches.subRangeMap(Range.closedOpen(0l, startBatchPosition)).asMapOfRanges();
    for (Range<Long> range : oldBatches.keySet()) {
      oldBatches.get(range).clear();
    }
    batches.remove(Range.closedOpen(0l, startBatchPosition));
    markedInnerPosition = innerPosition;
    markedOuterPosition = outerPosition;
  }

  public void reset() {
    if (markedOuterPosition >= 0) {
      // Move to rbd for markedOuterPosition.
      final RecordBatchData rbdNew = batches.get(markedOuterPosition);
      final RecordBatchData rbdOld = batches.get(startBatchPosition);
      Preconditions.checkArgument(rbdOld != null);
      Preconditions.checkArgument(rbdNew != null);
      if (rbdNew != rbdOld) {
        container.transferOut(rbdOld.getContainer());
        container.transferIn(rbdNew.getContainer());
      }
      innerPosition = markedInnerPosition;
      outerPosition = markedOuterPosition;
      final Range<Long> markedBatchRange = batches.getEntry(outerPosition).getKey();
      startBatchPosition = markedBatchRange.lowerEndpoint();
      innerRecordCount = (int)(markedBatchRange.upperEndpoint() - startBatchPosition);
      markedInnerPosition = -1;
      markedOuterPosition = -1;
    }
  }

  // Move forward by delta (may cross one or more record batches)
  public void forward(long delta) {
    Preconditions.checkArgument(delta >= 0);
    Preconditions.checkArgument(delta + outerPosition < totalRecordCount);
    final long nextOuterPosition = delta + outerPosition;
    final RecordBatchData rbdNew = batches.get(nextOuterPosition);
    final RecordBatchData rbdOld = batches.get(outerPosition);
    Preconditions.checkArgument(rbdNew != null);
    Preconditions.checkArgument(rbdOld != null);
    container.transferOut(rbdOld.getContainer());
    // Get vectors from new position.
    container.transferIn(rbdNew.getContainer());
    outerPosition = nextOuterPosition;
    final Range<Long> markedBatchRange = batches.getEntry(outerPosition).getKey();
    startBatchPosition = markedBatchRange.lowerEndpoint();
    innerPosition = (int)(outerPosition - startBatchPosition);
    innerRecordCount = (int)(markedBatchRange.upperEndpoint() - startBatchPosition);
  }

  /**
   * buildSchema calls next() in order to read schema quikcly.
   * Make sure we have fetched next non-empty batch at the end of the prepare.
   * After prepare position of iterator is at 0.
   */
  public void prepare() {
    while (!lastBatchRead && outerPosition == -1) {
      next();
    }
  }

  /**
   * Move iterator to next record.
   * @return
   *     Status of current record batch read.
   */
  public IterOutcome next() {
    if (finished()) {
      return lastOutcome;
    }
    long nextOuterPosition = outerPosition + 1;
    final int nextInnerPosition = innerPosition + 1;
    if (!initialized || nextOuterPosition >= totalRecordCount) {
      nextBatch();
      switch (lastOutcome) {
        case NONE:
        case STOP:
          // No more data, disallow reads unless reset is called.
          outerPosition = nextOuterPosition;
          lastBatchRead = true;
          break;
        case OK_NEW_SCHEMA:
        case OK:
          // If Schema changes in the middle of the execution clear out data.
          if (initialized && lastOutcome == IterOutcome.OK_NEW_SCHEMA) {
            clear();
            resetIndices();
            initialized = false;
            nextOuterPosition = 0;
          }
          // Transfer vectors from incoming record batch.
          final RecordBatchData rbd = new RecordBatchData(incoming, oContext.getAllocator());
          innerRecordCount = incoming.getRecordCount();
          if (!initialized) {
            for (VectorWrapper<?> w : rbd.getContainer()) {
              container.addOrGet(w.getField());
            }
            container.buildSchema(rbd.getContainer().getSchema().getSelectionVectorMode());
            initialized = true;
          }
          if (innerRecordCount > 0) {
            // Transfer vectors back to old batch.
            if (startBatchPosition != -1 && batches.get(startBatchPosition) != null) {
              container.transferOut(batches.get(outerPosition).getContainer());
            }
            container.transferIn(rbd.getContainer());
            startBatchPosition = nextOuterPosition;
            batches.put(Range.closedOpen(nextOuterPosition, nextOuterPosition + innerRecordCount), rbd);
            innerPosition = 0;
            outerPosition = nextOuterPosition;
            totalRecordCount += innerRecordCount;
          } else {
            // Release schema/empty batches.
            rbd.clear();
          }
          break;
        case OUT_OF_MEMORY:
          return lastOutcome;
        case NOT_YET:
        default:
          throw new UnsupportedOperationException("Unsupported outcome received " + lastOutcome);
      }
    } else {
      if (nextInnerPosition >= innerRecordCount) {
        // move to next batch
        final RecordBatchData rbdNew = batches.get(nextOuterPosition);
        final RecordBatchData rbdOld = batches.get(outerPosition);
        Preconditions.checkArgument(rbdNew != null);
        Preconditions.checkArgument(rbdOld != null);
        Preconditions.checkArgument(rbdOld != rbdNew);
        container.transferOut(rbdOld.getContainer());
        container.transferIn(rbdNew.getContainer());
        innerPosition = 0;
        outerPosition = nextOuterPosition;
        startBatchPosition = batches.getEntry(outerPosition).getKey().lowerEndpoint();
        innerRecordCount = (int)(batches.getEntry(outerPosition).getKey().upperEndpoint() - startBatchPosition);
      } else {
        outerPosition = nextOuterPosition;
        innerPosition = nextInnerPosition;
      }
    }
    return lastOutcome;
  }

  public boolean finished() {
    return lastBatchRead && outerPosition  >= totalRecordCount;
  }

  public IterOutcome getLastOutcome() {
    return lastOutcome;
  }

  public long getTotalRecordCount() {
    return totalRecordCount;
  }

  public int getInnerRecordCount() {
    return innerRecordCount;
  }

  public long getOuterPosition() {
    return outerPosition;
  }

  public int getCurrentPosition() {
    Preconditions.checkArgument(initialized);
    Preconditions.checkArgument(innerPosition >= 0 && innerPosition < innerRecordCount,
      String.format("innerPosition:%d, outerPosition:%d, innerRecordCount:%d, totalRecordCount:%d",
        innerPosition, outerPosition, innerRecordCount, totalRecordCount));
    return innerPosition;
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids) {
    Preconditions.checkArgument(initialized);
    return container.getValueAccessorById(clazz, ids);
  }

  @Override
  public TypedFieldId getValueVectorId(SchemaPath path) {
    Preconditions.checkArgument(initialized);
    return container.getValueVectorId(path);
  }

  @Override
  public BatchSchema getSchema() {
    Preconditions.checkArgument(initialized);
    return container.getSchema();
  }

  @Override
  public int getRecordCount() {
    Preconditions.checkArgument(initialized);
    return innerRecordCount;
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    Preconditions.checkArgument(initialized);
    return container.iterator();
  }

  // Release all vectors held by record batches, clear out range map.
  public void clear() {
    if (container != null) {
      container.clear();
    }
    for (RecordBatchData d : batches.asMapOfRanges().values()) {
      d.clear();
    }
    batches.clear();
  }

  // Deplete incoming batches.
  public void clearInflightBatches() {
    while (lastOutcome == IterOutcome.OK || lastOutcome == IterOutcome.OK_NEW_SCHEMA) {
      // Clear all buffers from incoming.
      for (VectorWrapper<?> wrapper : incoming) {
        wrapper.getValueVector().clear();
      }
      lastOutcome = incoming.next();
    }
  }

  public void close() {
    clear();
    clearInflightBatches();
  }
}