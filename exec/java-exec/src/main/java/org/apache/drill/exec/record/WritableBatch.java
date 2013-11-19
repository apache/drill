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
package org.apache.drill.exec.record;

import io.netty.buffer.ByteBuf;

import java.util.List;

import io.netty.buffer.CompositeByteBuf;
import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;
import org.apache.drill.exec.proto.UserBitShared.RecordBatchDef;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * A specialized version of record batch that can moves out buffers and preps them for writing.
 */
public class WritableBatch {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WritableBatch.class);

  private final RecordBatchDef def;
  private final ByteBuf[] buffers;
  private boolean cleared = false;

  private WritableBatch(RecordBatchDef def, List<ByteBuf> buffers) {
    logger.debug("Created new writable batch with def {} and buffers {}", def, buffers);
    this.def = def;
    this.buffers = buffers.toArray(new ByteBuf[buffers.size()]);
  }

  private WritableBatch(RecordBatchDef def, ByteBuf[] buffers) {
    super();
    this.def = def;
    this.buffers = buffers;
  }

  public RecordBatchDef getDef() {
    return def;
  }

  public ByteBuf[] getBuffers() {
    return buffers;
  }

  public void reconstructContainer(VectorContainer container) {
    Preconditions.checkState(!cleared,
        "Attempted to reconstruct a container from a WritableBatch after it had been cleared");
    if (buffers.length > 0) { /* If we have ByteBuf's associated with value vectors */
      
      CompositeByteBuf cbb = new CompositeByteBuf(buffers[0].alloc(), true, buffers.length);

      /* Copy data from each buffer into the compound buffer */
      for (ByteBuf buf : buffers) {
        cbb.addComponent(buf);
      }

      List<FieldMetadata> fields = def.getFieldList();

      int bufferOffset = 0;

      /*
       * For each value vector slice up the appropriate size from the compound buffer and load it into the value vector
       */
      int vectorIndex = 0;

      for (VectorWrapper<?> vv : container) {
        FieldMetadata fmd = fields.get(vectorIndex);
        ValueVector v = vv.getValueVector();
        v.load(fmd, cbb.slice(bufferOffset, fmd.getBufferLength()));
        vectorIndex++;
        bufferOffset += fmd.getBufferLength();
      }
    }

    SelectionVectorMode svMode;
    if (def.hasIsSelectionVector2() && def.getIsSelectionVector2()) {
      svMode = SelectionVectorMode.TWO_BYTE;
    } else {
      svMode = SelectionVectorMode.NONE;
    }
    container.buildSchema(svMode);

    /* Set the record count in the value vector */
    for (VectorWrapper<?> v : container) {
      ValueVector.Mutator m = v.getValueVector().getMutator();
      m.setValueCount(def.getRecordCount());
    }
  }

  public void clear() {
    for (ByteBuf buf : buffers) {
      buf.release();
    }
    cleared = true;
  }

  public static WritableBatch getBatchNoHVWrap(int recordCount, Iterable<VectorWrapper<?>> vws, boolean isSV2) {
    List<ValueVector> vectors = Lists.newArrayList();
    for (VectorWrapper<?> vw : vws) {
      Preconditions.checkArgument(!vw.isHyper());
      vectors.add(vw.getValueVector());
    }
    return getBatchNoHV(recordCount, vectors, isSV2);
  }

  public static WritableBatch getBatchNoHV(int recordCount, Iterable<ValueVector> vectors, boolean isSV2) {
    List<ByteBuf> buffers = Lists.newArrayList();
    List<FieldMetadata> metadata = Lists.newArrayList();

    for (ValueVector vv : vectors) {
      metadata.add(vv.getMetadata());

      // don't try to get the buffers if we don't have any records. It is possible the buffers are dead buffers.
      if (recordCount == 0)
        continue;

      for (ByteBuf b : vv.getBuffers()) {
        buffers.add(b);
      }
      // remove vv access to buffers.
      vv.clear();
    }

    RecordBatchDef batchDef = RecordBatchDef.newBuilder().addAllField(metadata).setRecordCount(recordCount)
        .setIsSelectionVector2(isSV2).build();
    WritableBatch b = new WritableBatch(batchDef, buffers);
    return b;
  }

  public static WritableBatch get(RecordBatch batch) {
    if (batch.getSchema() != null && batch.getSchema().getSelectionVectorMode() == SelectionVectorMode.FOUR_BYTE)
      throw new UnsupportedOperationException("Only batches without hyper selections vectors are writable.");

    boolean sv2 = (batch.getSchema().getSelectionVectorMode() == SelectionVectorMode.TWO_BYTE);
    return getBatchNoHVWrap(batch.getRecordCount(), batch, sv2);
  }

  public void retainBuffers() {
    for (ByteBuf buf : buffers) {
      buf.retain();
    }
  }
}
