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

package org.apache.drill.exec.physical.impl.partitionsender;

import java.util.Iterator;

import com.google.common.base.Preconditions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HashPartitionSender;
import org.apache.drill.exec.proto.ExecProtos;
import org.apache.drill.exec.proto.GeneralRPCProtos;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.rpc.BaseRpcOutcomeListener;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.bit.BitTunnel;
import org.apache.drill.exec.vector.*;
import org.apache.drill.exec.work.foreman.ErrorHelper;

/**
 * OutgoingRecordBatch is a holder of value vectors which are to be sent to another host.  Thus,
 * next() will never be called on this object.  When a record batch is ready to send (e.g. nearing size
 * limit or schema change), call flush() to send the batch.
 */
public class OutgoingRecordBatch implements RecordBatch {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OutgoingRecordBatch.class);

  private BitTunnel tunnel;
  private HashPartitionSender operator;
  private volatile boolean ok = true;
  private boolean isLast = false;
  private RecordBatch incoming;
  private FragmentContext context;
  private BatchSchema outSchema;
  private VectorContainer vectorContainer;
  private int recordCount;
  private int recordCapacity;

  public OutgoingRecordBatch(HashPartitionSender operator, BitTunnel tunnel, RecordBatch incoming, FragmentContext context) {
    this.incoming = incoming;
    this.context = context;
    this.operator = operator;
    this.tunnel = tunnel;
    initializeBatch();
  }

  public void flushIfNecessary() {
    if (recordCount == recordCapacity) logger.debug("Flush is necesary:  Count is " + recordCount + ", capacity is " + recordCapacity);
    try {
      if (recordCount == recordCapacity) flush();
    } catch (SchemaChangeException e) {
      // TODO:
      logger.error("Unable to flush outgoing record batch: " + e);
    }
  }

  public void incRecordCount() {
    ++recordCount;
  }
  
  public void flush() throws SchemaChangeException {
    if (recordCount == 0) {
      logger.warn("Attempted to flush an empty record batch");
    }
    logger.debug("Flushing record batch.  count is: " + recordCount + ", capacity is " + recordCapacity);
    final ExecProtos.FragmentHandle handle = context.getHandle();
    FragmentWritableBatch writableBatch = new FragmentWritableBatch(isLast,
                                                                    handle.getQueryId(),
                                                                    handle.getMajorFragmentId(),
                                                                    handle.getMinorFragmentId(),
                                                                    operator.getOppositeMajorFragmentId(),
                                                                    0,
                                                                    getWritableBatch());
     tunnel.sendRecordBatch(statusHandler, context, writableBatch);

    // reset values and reallocate the buffer for each value vector.  NOTE: the value vector is directly
    // referenced by generated code and must not be replaced.
    recordCount = 0;
    for (VectorWrapper v : vectorContainer) {
      logger.debug("Reallocating vv to capacity " + recordCapacity + " after flush. " + v.getValueVector());
      getAllocator(v.getValueVector(),
                   TypeHelper.getNewVector(v.getField(), context.getAllocator())).alloc(recordCapacity);
    }
    if (!ok) { throw new SchemaChangeException("Flush ended NOT OK!"); }
  }


  /**
   * Create a new output schema and allocate space for value vectors based on the incoming record batch.
   */
  public void initializeBatch() {
    isLast = false;
    recordCapacity = incoming.getRecordCount();
    vectorContainer = new VectorContainer();

    SchemaBuilder bldr = BatchSchema.newBuilder().setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE);
    for (VectorWrapper v : incoming) {

      // add field to the output schema
      bldr.addField(v.getField());

      // allocate a new value vector
      ValueVector outgoingVector = TypeHelper.getNewVector(v.getField(), context.getAllocator());
      getAllocator(v.getValueVector(), outgoingVector).alloc(recordCapacity);
      vectorContainer.add(outgoingVector);
      logger.debug("Reallocating to cap " + recordCapacity + " because of newly init'd vector : " + v.getValueVector());
    }
    outSchema = bldr.build();
    logger.debug("Initialized OutgoingRecordBatch.  RecordCount: " + recordCount + ", cap: " + recordCapacity + " Schema: " + outSchema);
  }

  /**
   * Free any existing value vectors, create new output schema, and allocate value vectors based
   * on the incoming record batch.
   */
  public void resetBatch() {
    isLast = false;
    recordCount = 0;
    recordCapacity = 0;
    for (VectorWrapper v : vectorContainer)
      v.getValueVector().clear();
    initializeBatch();
  }

  public void setIsLast() {
    isLast = true;
  }

  @Override
  public IterOutcome next() {
    assert false;
    return IterOutcome.STOP;
  }

  @Override
  public FragmentContext getContext() {
    return context;
  }

  @Override
  public BatchSchema getSchema() {
    Preconditions.checkNotNull(outSchema);
    return outSchema;
  }

  @Override
  public int getRecordCount() {
    return recordCount;
  }

  @Override
  public void kill() {
    incoming.kill();
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypedFieldId getValueVectorId(SchemaPath path) {
    return vectorContainer.getValueVector(path);
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(int fieldId, Class<?> clazz) {
    return vectorContainer.getVectorAccessor(fieldId, clazz);
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return vectorContainer.iterator();
  }

  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }

  private VectorAllocator getAllocator(ValueVector in, ValueVector outgoing){
    if(outgoing instanceof FixedWidthVector){
      return new FixedVectorAllocator((FixedWidthVector) outgoing);
    }else if(outgoing instanceof VariableWidthVector && in instanceof VariableWidthVector){
      return new VariableVectorAllocator( (VariableWidthVector) in, (VariableWidthVector) outgoing);
    }else{
      throw new UnsupportedOperationException();
    }
  }

  private class FixedVectorAllocator implements VectorAllocator{
    FixedWidthVector out;

    public FixedVectorAllocator(FixedWidthVector out) {
      super();
      this.out = out;
    }

    public void alloc(int recordCount){
      out.allocateNew(recordCount);
      out.getMutator().setValueCount(recordCount);
    }
  }

  private class VariableVectorAllocator implements VectorAllocator{
    VariableWidthVector in;
    VariableWidthVector out;

    public VariableVectorAllocator(VariableWidthVector in, VariableWidthVector out) {
      super();
      this.in = in;
      this.out = out;
    }

    public void alloc(int recordCount){
      out.allocateNew(in.getByteCapacity(), recordCount);
      out.getMutator().setValueCount(recordCount);
    }
  }

  public interface VectorAllocator{
    public void alloc(int recordCount);
  }  
  
  private StatusHandler statusHandler = new StatusHandler();
  private class StatusHandler extends BaseRpcOutcomeListener<GeneralRPCProtos.Ack> {
    RpcException ex;

    @Override
    public void failed(RpcException ex) {
      logger.error("Failure while sending data to user.", ex);
      ErrorHelper.logAndConvertError(context.getIdentity(), "Failure while sending fragment to client.", ex, logger);
      ok = false;
      this.ex = ex;
    }

  }

}
