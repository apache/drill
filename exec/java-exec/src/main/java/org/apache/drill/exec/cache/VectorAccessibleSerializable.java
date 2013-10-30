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
package org.apache.drill.exec.cache;

import com.google.common.collect.Lists;
import com.yammer.metrics.MetricRegistry;
import com.yammer.metrics.Timer;
import io.netty.buffer.ByteBuf;
import org.apache.drill.common.util.DataInputInputStream;
import org.apache.drill.common.util.DataOutputOutputStream;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;

import java.io.*;
import java.util.List;

public class VectorAccessibleSerializable implements DrillSerializable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VectorAccessibleSerializable.class);
  static final MetricRegistry metrics = DrillMetrics.getInstance();
  static final String WRITER_TIMER = MetricRegistry.name(VectorAccessibleSerializable.class, "writerTime");

  private VectorAccessible va;
  private BufferAllocator allocator;
  private int recordCount = -1;
  private BatchSchema.SelectionVectorMode svMode = BatchSchema.SelectionVectorMode.NONE;
  private SelectionVector2 sv2;

  /**
   *
   * @param va
   */
  public VectorAccessibleSerializable(VectorAccessible va, BufferAllocator allocator){
    this.va = va;
    this.allocator = allocator;
  }

  public VectorAccessibleSerializable(VectorAccessible va, SelectionVector2 sv2, BufferAllocator allocator) {
    this.va = va;
    this.allocator = allocator;
    this.sv2 = sv2;
    if (sv2 != null) this.svMode = BatchSchema.SelectionVectorMode.TWO_BYTE;
  }

  public VectorAccessibleSerializable(BufferAllocator allocator) {
    this.va = new VectorContainer();
    this.allocator = allocator;
  }

  @Override
  public void read(DataInput input) throws IOException {
    readFromStream(DataInputInputStream.constructInputStream(input));
  }
  
  @Override
  public void readFromStream(InputStream input) throws IOException {
    VectorContainer container = new VectorContainer();
    UserBitShared.RecordBatchDef batchDef = UserBitShared.RecordBatchDef.parseDelimitedFrom(input);
    recordCount = batchDef.getRecordCount();
    if (batchDef.hasIsSelectionVector2() && batchDef.getIsSelectionVector2()) {
      sv2.allocateNew(recordCount * 2);
      sv2.getBuffer().setBytes(0, input, recordCount * 2);
      svMode = BatchSchema.SelectionVectorMode.TWO_BYTE;
    }
    List<ValueVector> vectorList = Lists.newArrayList();
    List<FieldMetadata> fieldList = batchDef.getFieldList();
    for (FieldMetadata metaData : fieldList) {
      int dataLength = metaData.getBufferLength();
      MaterializedField field = MaterializedField.create(metaData.getDef());
      ByteBuf buf = allocator.buffer(dataLength);
      buf.writeBytes(input, dataLength);
      ValueVector vector = TypeHelper.getNewVector(field, allocator);
      vector.load(metaData, buf);
      buf.release();
      vectorList.add(vector);
    }
    container.addCollection(vectorList);
    container.buildSchema(svMode);
    container.setRecordCount(recordCount);
    va = container;
  }

  @Override
  public void write(DataOutput output) throws IOException {
    writeToStream(DataOutputOutputStream.constructOutputStream(output));
  }

  @Override
  public void writeToStream(OutputStream output) throws IOException {
    final Timer.Context context = metrics.timer(WRITER_TIMER).time();
    WritableBatch batch = WritableBatch.getBatchNoHVWrap(va.getRecordCount(),va,false);

    ByteBuf[] incomingBuffers = batch.getBuffers();
    UserBitShared.RecordBatchDef batchDef = batch.getDef();

        /* ByteBuf associated with the selection vector */
    ByteBuf svBuf = null;

        /* Size of the selection vector */
    int svCount = 0;

    if (svMode == BatchSchema.SelectionVectorMode.TWO_BYTE)
    {
      svCount = sv2.getCount();
      svBuf = sv2.getBuffer();
    }

    int totalBufferLength = 0;

    try
    {
            /* Write the metadata to the file */
      batchDef.writeDelimitedTo(output);

            /* If we have a selection vector, dump it to file first */
      if (svBuf != null)
      {

                /* For writing to the selection vectors we use
                 * setChar() method which does not modify the
                 * reader and writer index. To copy the entire buffer
                 * without having to get each byte individually we need
                 * to set the writer index
                 */
        svBuf.writerIndex(svCount * SelectionVector2.RECORD_SIZE);

//        fc.write(svBuf.nioBuffers());
        svBuf.getBytes(0, output, svBuf.readableBytes());
        svBuf.release();
      }

            /* Dump the array of ByteBuf's associated with the value vectors */
      for (ByteBuf buf : incomingBuffers)
      {
                /* dump the buffer into the file channel */
        int bufLength = buf.readableBytes();
        buf.getBytes(0, output, bufLength);

                /* compute total length of buffer, will be used when
                 * we create a compound buffer
                 */
        totalBufferLength += buf.readableBytes();
        buf.release();
      }

      output.flush();
      context.stop();
    } catch (IOException e)
    {
      throw new RuntimeException(e);
    } finally {
      clear();
    }
  }

  private void clear() {
  }

  public VectorAccessible get() {
    return va;
  }
}
