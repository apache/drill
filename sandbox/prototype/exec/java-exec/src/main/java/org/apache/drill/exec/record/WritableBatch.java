/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.record;

import io.netty.buffer.ByteBuf;

import java.util.List;

import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;
import org.apache.drill.exec.proto.UserBitShared.RecordBatchDef;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.collect.Lists;

/**
 * A specialized version of record batch that can moves out buffers and preps them for writing.
 */
public class WritableBatch {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WritableBatch.class);

  private final RecordBatchDef def;
  private final ByteBuf[] buffers;

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

  public static WritableBatch getBatchNoSV(int recordCount, Iterable<ValueVector> vectors) {
    List<ByteBuf> buffers = Lists.newArrayList();
    List<FieldMetadata> metadata = Lists.newArrayList();

    for (ValueVector vv : vectors) {
      metadata.add(vv.getMetadata());
      for (ByteBuf b : vv.getBuffers()) {
        buffers.add(b);
        b.retain();
      }
      // remove vv access to buffers.
      vv.clear();
    }

    RecordBatchDef batchDef = RecordBatchDef.newBuilder().addAllField(metadata).setRecordCount(recordCount).build();
    WritableBatch b = new WritableBatch(batchDef, buffers);
    return b;
  }
  
  public static WritableBatch get(RecordBatch batch) {
    if(batch.getSchema() != null && batch.getSchema().getSelectionVector() != SelectionVectorMode.NONE) throw new UnsupportedOperationException("Only batches without selections vectors are writable.");
    return getBatchNoSV(batch.getRecordCount(), batch);
  }

}
