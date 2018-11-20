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

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.impl.aggregate.SpilledRecordbatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.util.record.RecordBatchStats.RecordBatchStatsContext;

public abstract class AbstractRecordBatch<T extends PhysicalOperator> implements CloseableRecordBatch {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(new Object() {}.getClass().getEnclosingClass());

  protected final VectorContainer container;
  protected final T popConfig;
  protected final FragmentContext context;
  protected final OperatorContext oContext;
  protected final RecordBatchStatsContext batchStatsContext;
  protected final OperatorStats stats;
  protected final boolean unionTypeEnabled;
  protected BatchState state;

  // Represents last outcome of next(). If an Exception is thrown
  // during the method's execution a value IterOutcome.STOP will be assigned.
  private IterOutcome lastOutcome;

  protected AbstractRecordBatch(final T popConfig, final FragmentContext context) throws OutOfMemoryException {
    this(popConfig, context, true, context.newOperatorContext(popConfig));
  }

  protected AbstractRecordBatch(final T popConfig, final FragmentContext context, final boolean buildSchema) throws OutOfMemoryException {
    this(popConfig, context, buildSchema, context.newOperatorContext(popConfig));
  }

  protected AbstractRecordBatch(final T popConfig, final FragmentContext context, final boolean buildSchema,
      final OperatorContext oContext) {
    this.context = context;
    this.popConfig = popConfig;
    this.oContext = oContext;
    this.batchStatsContext = new RecordBatchStatsContext(context, oContext);
    stats = oContext.getStats();
    container = new VectorContainer(this.oContext.getAllocator());
    if (buildSchema) {
      state = BatchState.BUILD_SCHEMA;
    } else {
      state = BatchState.FIRST;
    }
    OptionValue option = context.getOptions().getOption(ExecConstants.ENABLE_UNION_TYPE.getOptionName());
    if (option != null) {
      unionTypeEnabled = option.bool_val;
    } else {
      unionTypeEnabled = false;
    }
  }

  public static enum BatchState {
    /** Need to build schema and return. */
    BUILD_SCHEMA,
    /** This is still the first data batch. */
    FIRST,
    /** The first data batch has already been returned. */
    NOT_FIRST,
    /** The query most likely failed, we need to propagate STOP to the root. */
    STOP,
    /** Out of Memory while building the Schema...Ouch! */
    OUT_OF_MEMORY,
    /** All work is done, no more data to be sent. */
    DONE
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
    if(!context.getExecutorState().shouldContinue()) {
      return IterOutcome.STOP;
    }
    return next(0, b);
  }

  public final IterOutcome next(final int inputIndex, final RecordBatch b){
    IterOutcome next;
    stats.stopProcessing();
    try{
      if (!context.getExecutorState().shouldContinue()) {
        return IterOutcome.STOP;
      }
      next = b.next();
    } finally {
      stats.startProcessing();
    }

    if (b instanceof SpilledRecordbatch) {
      // Don't double count records which were already read and spilled.
      // TODO evaluate whether swapping out upstream record batch with a SpilledRecordBatch
      // is the right thing to do.
      return next;
    }

    boolean isNewSchema = false;
    logger.debug("Received next batch for index: {} with outcome: {}", inputIndex, next);
    switch (next) {
      case OK_NEW_SCHEMA:
        isNewSchema = true;
      case OK:
      case EMIT:
        stats.batchReceived(inputIndex, b.getRecordCount(), isNewSchema);
        logger.debug("Number of records in received batch: {}", b.getRecordCount());
        break;
      default:
        break;
    }

    return next;
  }

  @Override
  public final IterOutcome next() {
    try {
      stats.startProcessing();
      switch (state) {
        case BUILD_SCHEMA: {
          buildSchema();
          switch (state) {
            case DONE:
              lastOutcome = IterOutcome.NONE;
              break;
            case OUT_OF_MEMORY:
              // because we don't support schema changes, it is safe to fail the query right away
              context.getExecutorState().fail(UserException.memoryError()
                .build(logger));
              // FALL-THROUGH
            case STOP:
              lastOutcome = IterOutcome.STOP;
              break;
            default:
              state = BatchState.FIRST;
              lastOutcome = IterOutcome.OK_NEW_SCHEMA;
              break;
          }
          break;
        }
        case DONE: {
          lastOutcome = IterOutcome.NONE;
          break;
        }
        default:
          lastOutcome = innerNext();
          break;
      }
      return lastOutcome;
    } catch (final SchemaChangeException e) {
      lastOutcome = IterOutcome.STOP;
      throw new DrillRuntimeException(e);
    } catch (Exception e) {
      lastOutcome = IterOutcome.STOP;
      throw e;
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

  @Override
  public void close() {
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

  @Override
  public VectorContainer getContainer() {
    return  container;
  }

  @Override
  public boolean hasFailed() {
    return lastOutcome == IterOutcome.STOP;
  }

  public RecordBatchStatsContext getRecordBatchStatsContext() {
    return batchStatsContext;
  }

  public boolean isRecordBatchStatsLoggingEnabled() {
    return batchStatsContext.isEnableBatchSzLogging();
  }
}
