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
package org.apache.drill.exec.physical.impl.partitionsender;

import java.util.Iterator;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HashPartitionSender;
import org.apache.drill.exec.proto.ExecProtos;
import org.apache.drill.exec.proto.GeneralRPCProtos;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.SchemaBuilder;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.rpc.BaseRpcOutcomeListener;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.bit.BitTunnel;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.allocator.VectorAllocator;
import org.apache.drill.exec.work.foreman.ErrorHelper;

import com.google.common.base.Preconditions;

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
  private int oppositeMinorFragmentId;

  public OutgoingRecordBatch(HashPartitionSender operator, BitTunnel tunnel, RecordBatch incoming, FragmentContext context, int oppositeMinorFragmentId) {
    this.incoming = incoming;
    this.context = context;
    this.operator = operator;
    this.tunnel = tunnel;
    this.oppositeMinorFragmentId = oppositeMinorFragmentId;
    initializeBatch();
  }

  public void flushIfNecessary() {
    if (recordCount == recordCapacity) logger.debug("Flush is necesary:  Count is " + recordCount + ", capacity is " + recordCapacity);
    try {
      if (recordCount == recordCapacity) flush();
    } catch (SchemaChangeException e) {
      incoming.kill();
      logger.error("Error flushing outgoing batches", e);
      context.fail(e);
    }
  }

  public void incRecordCount() {
    ++recordCount;
  }

  /**
   * Send the record batch to the target node, then reset the value vectors
   * 
   * @return true if a flush was needed; otherwise false
   * @throws SchemaChangeException
   */
  public boolean flush() throws SchemaChangeException {
    final ExecProtos.FragmentHandle handle = context.getHandle();

    if (recordCount != 0) {
      
      for(VectorWrapper<?> w : vectorContainer){
        w.getValueVector().getMutator().setValueCount(recordCount);
      }
      
      FragmentWritableBatch writableBatch = new FragmentWritableBatch(isLast,
                                                                      handle.getQueryId(),
                                                                      handle.getMajorFragmentId(),
                                                                      handle.getMinorFragmentId(),
                                                                      operator.getOppositeMajorFragmentId(),
                                                                      oppositeMinorFragmentId,
                                                                      getWritableBatch());

      tunnel.sendRecordBatch(statusHandler, context, writableBatch);
    } else {
      logger.debug("Flush requested on an empty outgoing record batch" + (isLast ? " (last batch)" : ""));
      if (isLast) {
        // send final (empty) batch
        FragmentWritableBatch writableBatch = new FragmentWritableBatch(isLast,
                                                                        handle.getQueryId(),
                                                                        handle.getMajorFragmentId(),
                                                                        handle.getMinorFragmentId(),
                                                                        operator.getOppositeMajorFragmentId(),
                                                                        oppositeMinorFragmentId,
                                                                        getWritableBatch());
        tunnel.sendRecordBatch(statusHandler, context, writableBatch);
        vectorContainer.clear();
        return true;
      }
    }

    // reset values and reallocate the buffer for each value vector based on the incoming batch.
    // NOTE: the value vector is directly referenced by generated code; therefore references
    // must remain valid.
    recordCount = 0;
    for (VectorWrapper<?> v : vectorContainer) {
      logger.debug("Reallocating vv to capacity " + incoming.getRecordCount() + " after flush.");
      VectorAllocator.getAllocator(v.getValueVector(), v.getValueVector()).alloc(incoming.getRecordCount());
    }
    if (!ok) { throw new SchemaChangeException("Flush ended NOT OK!"); }
    return true;
  }


  /**
   * Create a new output schema and allocate space for value vectors based on the incoming record batch.
   */
  public void initializeBatch() {
    isLast = false;
    recordCapacity = incoming.getRecordCount();
    vectorContainer = new VectorContainer();

    SchemaBuilder bldr = BatchSchema.newBuilder().setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE);
    for (VectorWrapper<?> v : incoming) {

      // add field to the output schema
      bldr.addField(v.getField());

      // allocate a new value vector
      ValueVector outgoingVector = TypeHelper.getNewVector(v.getField(), context.getAllocator());
      VectorAllocator.getAllocator(v.getValueVector(), outgoingVector).alloc(recordCapacity);
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
    for (VectorWrapper<?> v : vectorContainer)
      v.getValueVector().clear();
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
    return vectorContainer.getValueAccessorById(fieldId, clazz);
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return vectorContainer.iterator();
  }

  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
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
