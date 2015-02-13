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
package org.apache.drill.exec.physical.impl.join;

import io.netty.buffer.DrillBuf;

import java.util.List;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.AllocationReservation;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.collect.ArrayListMultimap;

public class MergeJoinBatchBuilder implements AutoCloseable {
  private final ArrayListMultimap<BatchSchema, RecordBatchData> queuedRightBatches = ArrayListMultimap.create();
  private final VectorContainer container;
  private int runningBytes = 0;
  private int runningBatches = 0;
  private int recordCount = 0;
  private final AllocationReservation allocationReservation;
  private final JoinStatus status;

  public MergeJoinBatchBuilder(final BufferAllocator allocator, final JoinStatus status) {
    container = new VectorContainer();
    this.status = status;
    allocationReservation = allocator.newReservation();
  }

  public boolean add(final RecordBatch batch) {
    if (batch.getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.FOUR_BYTE) {
      throw new UnsupportedOperationException("A merge join cannot currently work against a sv4 batch.");
    }
    if (batch.getRecordCount() == 0) {
      return true;      // skip over empty record batches.
    }

    // resource checks
    final long batchBytes = getSize(batch);
    if (batchBytes + runningBytes > Integer.MAX_VALUE) {
      return false;     // TODO: 2GB is arbitrary
    }
    if (runningBatches + 1 >= Character.MAX_VALUE) {
      return false;     // allowed in batch.
    }
    if (!allocationReservation.add(batch.getRecordCount() * 4)) {
      return false;     // requested allocation unavailable.
    }

    // transfer VVs to a new RecordBatchData
    final RecordBatchData bd = new RecordBatchData(batch);
    ++runningBatches;
    runningBytes += batchBytes;
    queuedRightBatches.put(batch.getSchema(), bd);
    recordCount += bd.getRecordCount();
    return true;
  }

  private long getSize(final RecordBatch batch) {
    long bytes = 0;
    for (VectorWrapper<?> v : batch) {
      bytes += v.getValueVector().getBufferSize();
    }
    return bytes;
  }

  public void build() throws SchemaChangeException {
    container.clear();
    if (queuedRightBatches.size() > Character.MAX_VALUE) {
      throw new SchemaChangeException("Join cannot work on more than %d batches at a time.", (int) Character.MAX_VALUE);
    }
    final DrillBuf drillBuf = allocationReservation.buffer();
    status.sv4 = new SelectionVector4(drillBuf, recordCount, Character.MAX_VALUE);
    final BatchSchema schema = queuedRightBatches.keySet().iterator().next();
    final List<RecordBatchData> data = queuedRightBatches.get(schema);

    // now we're going to generate the sv4 pointers
    switch (schema.getSelectionVectorMode()) {
      case NONE: {
        int index = 0;
        int recordBatchId = 0;
        for (RecordBatchData d : data) {
          for (int i =0; i < d.getRecordCount(); i++, index++) {
            status.sv4.set(index, recordBatchId, i);
          }
          recordBatchId++;
        }
        break;
      }
      case TWO_BYTE: {
        int index = 0;
        int recordBatchId = 0;
        for (RecordBatchData d : data) {
          for (int i =0; i < d.getRecordCount(); i++, index++) {
            status.sv4.set(index, recordBatchId, (int) d.getSv2().getIndex(i));
          }
          // might as well drop the selection vector since we'll stop using it now.
          d.getSv2().clear();
          recordBatchId++;
        }
        break;
      }
      default:
        throw new UnsupportedOperationException();
    }

    // next, we'll create lists of each of the vector types.
    final ArrayListMultimap<MaterializedField, ValueVector> vectors = ArrayListMultimap.create();
    for (RecordBatchData rbd : queuedRightBatches.values()) {
      for (ValueVector v : rbd.getVectors()) {
        vectors.put(v.getField(), v);
      }
    }

    for (MaterializedField f : vectors.keySet()) {
      final List<ValueVector> v = vectors.get(f);
      container.addHyperList(v);
    }

    container.buildSchema(BatchSchema.SelectionVectorMode.FOUR_BYTE);
  }

  @Override
  public void close() throws Exception {
    allocationReservation.close();
  }
}
