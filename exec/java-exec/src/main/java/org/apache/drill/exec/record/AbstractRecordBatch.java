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
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

public abstract class AbstractRecordBatch<T extends PhysicalOperator> implements CloseableRecordBatch {
  final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(this.getClass());

  protected final VectorContainer container;
  protected final T popConfig;
  protected final FragmentContext context;
  protected final OperatorContext oContext;
  protected final OperatorStats stats;

  protected BatchState state;

  protected AbstractRecordBatch(final T popConfig, final FragmentContext context) throws OutOfMemoryException {
    this(popConfig, context, true, context.newOperatorContext(popConfig, true));
  }

  protected AbstractRecordBatch(final T popConfig, final FragmentContext context, final boolean buildSchema) throws OutOfMemoryException {
    this(popConfig, context, buildSchema, context.newOperatorContext(popConfig, true));
  }

  protected AbstractRecordBatch(final T popConfig, final FragmentContext context, final boolean buildSchema,
      final OperatorContext oContext) throws OutOfMemoryException {
    super();
    this.context = context;
    this.popConfig = popConfig;
    this.oContext = oContext;
    stats = oContext.getStats();
    container = new VectorContainer(this.oContext);
    if (buildSchema) {
      state = BatchState.BUILD_SCHEMA;
    } else {
      state = BatchState.FIRST;
    }
  }

  protected static enum BatchState {
    BUILD_SCHEMA, // Need to build schema and return
    FIRST, // This is still the first data batch
    NOT_FIRST, // The first data batch has already been returned
    STOP, // The query most likely failed, we need to propagate STOP to the root
    OUT_OF_MEMORY, // Out of Memory while building the Schema...Ouch!
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

  public final IterOutcome next(final RecordBatch b) {
    if(!context.shouldContinue()) {
      return IterOutcome.STOP;
    }
    return next(0, b);
  }

  public final IterOutcome next(final int inputIndex, final RecordBatch b){
    IterOutcome next = null;
    stats.stopProcessing();
    try{
      if (!context.shouldContinue()) {
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
      switch (state) {
        case BUILD_SCHEMA: {
          buildSchema();
          switch (state) {
            case DONE:
              return IterOutcome.NONE;
            case OUT_OF_MEMORY:
              // because we don't support schema changes, it is safe to fail the query right away
              context.fail(UserException.memoryError().build());
              // FALL-THROUGH
            case STOP:
              return IterOutcome.STOP;
            default:
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
    } catch (final SchemaChangeException e) {
      throw new DrillRuntimeException(e);
    } finally {
      stats.stopProcessing();
    }
  }

  public abstract IterOutcome innerNext();

  @Override
  public BatchSchema getSchema() {
    if (container.hasSchema()) {
      return container.getSchema();
    } else {
      return null;
    }
  }

  protected void buildSchema() throws SchemaChangeException {
  }

  @Override
  public void kill(final boolean sendUpstream) {
    killIncoming(sendUpstream);
  }

  protected abstract void killIncoming(boolean sendUpstream);

  public void close(){
    container.clear();
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
  public TypedFieldId getValueVectorId(final SchemaPath path) {
    return container.getValueVectorId(path);
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(final Class<?> clazz, final int... ids) {
    return container.getValueAccessorById(clazz, ids);
  }


  @Override
  public WritableBatch getWritableBatch() {
//    logger.debug("Getting writable batch.");
    final WritableBatch batch = WritableBatch.get(this);
    return batch;

  }

  @Override
  public VectorContainer getOutgoingContainer() {
    throw new UnsupportedOperationException(String.format(" You should not call getOutgoingContainer() for class %s", this.getClass().getCanonicalName()));
  }

}
