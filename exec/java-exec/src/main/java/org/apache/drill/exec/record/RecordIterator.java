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


public class RecordIterator implements VectorAccessible {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RecordIterator.class);

  private final RecordBatch incoming;
  private final AbstractRecordBatch<?> outgoing;
  private int outerPosition;       // Tracks total records consumed so far works across batches.
  private int innerPosition;       // Index within current vector container.
  private int innerRecordCount;    // Records in current container.
  private int totalRecordCount;    // Total records read so far.
  private int startBatchPosition;  // Start offset of current batch.
  private int markedOuterPosition;
  private int markedInnerPosition;
  private IterOutcome lastOutcome;
  private int inputIndex;          // For two way merge join 0:left, 1:right
  private boolean lastBatchRead;   // True if all batches are consumed.
  private boolean initialized;


  private VectorContainer container; // Holds VectorContainer of current record batch
  TreeRangeMap<Integer, RecordBatchData> batches = TreeRangeMap.create();

  public RecordIterator(RecordBatch incoming,
                             AbstractRecordBatch<?> outgoing,
                             OperatorContext oContext,
                             int inputIndex) {
    this.incoming = incoming;
    this.innerPosition = this.outerPosition = this.startBatchPosition = -1;
    this.totalRecordCount = this.innerRecordCount = 0;
    this.markedInnerPosition = this.markedOuterPosition = -1;
    this.outgoing = outgoing;
    this.inputIndex = inputIndex;
    this.lastBatchRead = false;
    this.container = new VectorContainer(oContext);
    this.initialized = false;
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
    final Map<Range<Integer>,RecordBatchData> oldBatches = batches.subRangeMap(
      Range.closedOpen(0, startBatchPosition)).asMapOfRanges();
    for (Range<Integer> range : oldBatches.keySet()) {
      oldBatches.get(range.lowerEndpoint()).clear();
    }
    batches.remove(Range.closedOpen(0, startBatchPosition));
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
      startBatchPosition = batches.getEntry(outerPosition).getKey().lowerEndpoint();
      innerRecordCount = batches.getEntry(outerPosition).getKey().upperEndpoint() - startBatchPosition;
      markedInnerPosition = markedOuterPosition = -1;
    }
  }

  // TODO: Experimental: Move forward by delta (may cross one or more record batches)
  public void forward(int delta) {
    Preconditions.checkArgument(delta >= 0);
    Preconditions.checkArgument(delta + outerPosition < totalRecordCount);
    final int nextOuterPosition = delta + outerPosition;
    final RecordBatchData rbdNew = batches.get(nextOuterPosition);
    final RecordBatchData rbdOld = batches.get(outerPosition);
    Preconditions.checkArgument(rbdNew != null);
    Preconditions.checkArgument(rbdOld != null);
    container.transferOut(rbdOld.getContainer());
    // Get vectors from new position.
    container.transferIn(rbdNew.getContainer());
    outerPosition = nextOuterPosition;
    startBatchPosition = batches.getEntry(outerPosition).getKey().lowerEndpoint();
    innerPosition = outerPosition - startBatchPosition;
    innerRecordCount = batches.getEntry(outerPosition).getKey().upperEndpoint() - startBatchPosition;
  }

  public IterOutcome next() {
    if (finished()) {
      return lastOutcome;
    }
    final int nextOuterPosition = outerPosition + 1;
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
          // Transfer vectors from incoming record batch.
          final RecordBatchData rbd = new RecordBatchData(incoming);
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
        default:
          // On errors return lastOutcome.
          break;
      }
    } else {
      outerPosition = nextOuterPosition;
      innerPosition = nextInnerPosition;
    }
    return lastOutcome;
  }

  public boolean finished() {
    return lastBatchRead && outerPosition  >= totalRecordCount;
  }

  public IterOutcome getLastOutcome() {
    return lastOutcome;
  }

  public int getTotalRecordCount() {
    return totalRecordCount;
  }

  public int getInnerRecordCount() {
    return innerRecordCount;
  }

  public int getCurrentPosition() {
    Preconditions.checkArgument(initialized);
    Preconditions.checkArgument(innerPosition >= 0 && innerPosition < innerRecordCount);
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

  public void close() {
    if (container != null) {
      container.clear();
    }
    for (RecordBatchData d : batches.asMapOfRanges().values()) {
      d.clear();
    }
    batches.clear();
    while (lastOutcome == IterOutcome.OK || lastOutcome == IterOutcome.OK_NEW_SCHEMA) {
      // Clear all buffers from incoming.
      for (VectorWrapper<?> wrapper : incoming) {
        wrapper.getValueVector().clear();
      }
      lastOutcome = incoming.next();
    }
  }
}
