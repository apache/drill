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

import java.util.Iterator;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

public abstract class AbstractRecordBatch<T extends PhysicalOperator> implements RecordBatch{
  final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(this.getClass());

  protected final VectorContainer container; //= new VectorContainer();
  protected final T popConfig;
  protected final FragmentContext context;
  protected final OperatorContext oContext;
  protected final OperatorStats stats;

  protected BatchState state;

  protected AbstractRecordBatch(T popConfig, FragmentContext context) throws OutOfMemoryException {
    this(popConfig, context, true, new OperatorContext(popConfig, context, true));
  }

  protected AbstractRecordBatch(T popConfig, FragmentContext context, boolean buildSchema) throws OutOfMemoryException {
    this(popConfig, context, buildSchema, new OperatorContext(popConfig, context, true));
  }

  protected AbstractRecordBatch(T popConfig, FragmentContext context, boolean buildSchema, OperatorContext oContext) throws OutOfMemoryException {
    super();
    this.context = context;
    this.popConfig = popConfig;
    this.oContext = oContext;
    this.stats = oContext.getStats();
    this.container = new VectorContainer(this.oContext);
    if (buildSchema) {
      state = BatchState.BUILD_SCHEMA;
    } else {
      state = BatchState.FIRST;
    }
  }

  protected static enum BatchState {
    BUILD_SCHEMA, // Need to build schema and return
    FIRST, // This is still the first data batch
    NOT_FIRST, // The first data batch has alread been returned
    DONE // All work is done, no more data to be sent
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return container.iterator();
  }

  @Override
  public FragmentContext getContext() {
    return context;
  }

  public PhysicalOperator getPopConfig() {
    return popConfig;
  }

  public final IterOutcome next(RecordBatch b) {

    return next(0, b);
  }

  public final IterOutcome next(int inputIndex, RecordBatch b){
    IterOutcome next = null;
    stats.stopProcessing();
    try{
      if (context.isCancelled()) {
        return IterOutcome.STOP;
      }
      next = b.next();
    }finally{
      stats.startProcessing();
    }

    switch(next){
    case OK_NEW_SCHEMA:
      stats.batchReceived(inputIndex, b.getRecordCount(), true);
      break;
    case OK:
      stats.batchReceived(inputIndex, b.getRecordCount(), false);
      break;
    }

    return next;
  }

  public final IterOutcome next() {
    try {
      stats.startProcessing();
//      if (state == BatchState.BUILD_SCHEMA) {
//        buildSchema();
//        if (state == BatchState.BUILD_SCHEMA.DONE) {
//          return IterOutcome.NONE;
//        } else {
//          state = BatchState.FIRST;
//          return IterOutcome.OK_NEW_SCHEMA;
//        }
//      }
      switch (state) {
        case BUILD_SCHEMA: {
          buildSchema();
          if (state == BatchState.DONE) {
            return IterOutcome.NONE;
          } else {
            state = BatchState.FIRST;
            return IterOutcome.OK_NEW_SCHEMA;
          }
        }
        case DONE: {
          return IterOutcome.NONE;
        }
        default:
          return innerNext();
      }
    } catch (SchemaChangeException e) {
      throw new DrillRuntimeException(e);
    } finally {
      stats.stopProcessing();
    }
  }

  public abstract IterOutcome innerNext();

  @Override
  public BatchSchema getSchema() {
    return container.getSchema();
  }

  protected void buildSchema() throws SchemaChangeException {
  }

  @Override
  public void kill(boolean sendUpstream) {
    killIncoming(sendUpstream);
  }

  protected abstract void killIncoming(boolean sendUpstream);

  public void cleanup(){
    container.clear();
    oContext.close();
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
    return container.getValueVectorId(path);
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids) {
    return container.getValueAccessorById(clazz, ids);
  }


  @Override
  public WritableBatch getWritableBatch() {
//    logger.debug("Getting writable batch.");
    WritableBatch batch = WritableBatch.get(this);
    return batch;

  }

  @Override
  public VectorContainer getOutgoingContainer() {
    throw new UnsupportedOperationException(String.format(" You should not call getOutgoingContainer() for class %s", this.getClass().getCanonicalName()));
  }

}
