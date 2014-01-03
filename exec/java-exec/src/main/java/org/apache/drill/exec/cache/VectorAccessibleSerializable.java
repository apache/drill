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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.netty.buffer.ByteBuf;
import org.apache.drill.common.util.DataInputInputStream;
import org.apache.drill.common.util.DataOutputOutputStream;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;

import java.io.*;
import java.util.List;

/**
 * A wrapper around a VectorAccessible. Will serialize a VectorAccessible and write to an OutputStream, or can read
 * from an InputStream and construct a new VectorContainer.
 */
public class VectorAccessibleSerializable implements DrillSerializable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VectorAccessibleSerializable.class);
  static final MetricRegistry metrics = DrillMetrics.getInstance();
  static final String WRITER_TIMER = MetricRegistry.name(VectorAccessibleSerializable.class, "writerTime");

  private VectorAccessible va;
  private WritableBatch batch;
  private BufferAllocator allocator;
  private int recordCount = -1;
  private BatchSchema.SelectionVectorMode svMode = BatchSchema.SelectionVectorMode.NONE;
  private SelectionVector2 sv2;

  private boolean retain = false;

  public VectorAccessibleSerializable(BufferAllocator allocator) {
    this.allocator = allocator;
    this.va = new VectorContainer();
  }

  public VectorAccessibleSerializable(WritableBatch batch, BufferAllocator allocator){
    this(batch, null, allocator);
  }

  /**
   * Creates a wrapper around batch and sv2 for writing to a stream. sv2 will never be released by this class, and ownership
   * is maintained by caller.
   * @param batch
   * @param sv2
   * @param allocator
   */
  public VectorAccessibleSerializable(WritableBatch batch, SelectionVector2 sv2, BufferAllocator allocator) {
    this.allocator = allocator;
    if (batch != null) {
      this.batch = batch;
    }
    if (sv2 != null) {
      this.sv2 = sv2;
      this.svMode = BatchSchema.SelectionVectorMode.TWO_BYTE;
    }
  }

  @Override
  public void read(DataInput input) throws IOException {
    readFromStream(DataInputInputStream.constructInputStream(input));
  }

  /**
   * Reads from an InputStream and parses a RecordBatchDef. From this, we construct a SelectionVector2 if it exits
   * and construct the vectors and add them to a vector container
   * @param input the InputStream to read from
   * @throws IOException
   */
  @Override
  public void readFromStream(InputStream input) throws IOException {
    VectorContainer container = new VectorContainer();
    UserBitShared.RecordBatchDef batchDef = UserBitShared.RecordBatchDef.parseDelimitedFrom(input);
    recordCount = batchDef.getRecordCount();
    if (batchDef.hasIsSelectionVector2() && batchDef.getIsSelectionVector2()) {

      if (sv2 == null) {
        sv2 = new SelectionVector2(allocator);
      }
      sv2.allocateNew(recordCount * SelectionVector2.RECORD_SIZE);
      sv2.getBuffer().setBytes(0, input, recordCount * SelectionVector2.RECORD_SIZE);
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

  public void writeToStreamAndRetain(OutputStream output) throws IOException {
    retain = true;
    writeToStream(output);
  }
  

  /**
   * Serializes the VectorAccessible va and writes it to an output stream
   * @param output the OutputStream to write to
   * @throws IOException
   */
  @Override
  public void writeToStream(OutputStream output) throws IOException {
    Preconditions.checkNotNull(output);
    final Timer.Context timerContext = metrics.timer(WRITER_TIMER).time();

    ByteBuf[] incomingBuffers = batch.getBuffers();
    UserBitShared.RecordBatchDef batchDef = batch.getDef();

        /* ByteBuf associated with the selection vector */
    ByteBuf svBuf = null;
    Integer svCount =  null;

    if (svMode == BatchSchema.SelectionVectorMode.TWO_BYTE)
    {
      svCount = sv2.getCount();
      svBuf = sv2.getBuffer();
    }

    try
    {
            /* Write the metadata to the file */
      batchDef.writeDelimitedTo(output);

            /* If we have a selection vector, dump it to file first */
      if (svBuf != null)
      {
        svBuf.getBytes(0, output, svBuf.readableBytes());
        sv2.setBuffer(svBuf);
        sv2.setRecordCount(svCount);
      }

            /* Dump the array of ByteBuf's associated with the value vectors */
      for (ByteBuf buf : incomingBuffers)
      {
                /* dump the buffer into the OutputStream */
        int bufLength = buf.readableBytes();
        buf.getBytes(0, output, bufLength);
      }

      output.flush();

      timerContext.stop();
    } catch (IOException e)
    {
      throw new RuntimeException(e);
    } finally {
      clear();
    }
  }

  public void clear() {
    if (!retain) {
      batch.clear();
      if(sv2 != null) sv2.clear();
    }
  }

  public VectorAccessible get() {
    return va;
  }
  
  public SelectionVector2 getSv2() {
    return sv2;
  }

}
