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
package org.apache.drill.exec.store.drill.plugin;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.rpc.user.BlockingResultsListener;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.testing.ControlsInjector;
import org.apache.drill.exec.testing.ControlsInjectorFactory;
import org.apache.drill.exec.util.ImpersonationUtil;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLTimeoutException;
import java.util.Iterator;
import java.util.Optional;

public class DrillRecordReader implements CloseableRecordBatch {
  private static final Logger logger = LoggerFactory.getLogger(DrillRecordReader.class);

  private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(DrillRecordReader.class);

  private final DrillClient drillClient;
  private final RecordBatchLoader batchLoader;
  private final FragmentContext context;

  private final BlockingResultsListener resultsListener;
  private final UserBitShared.QueryId id;
  private BatchSchema schema;
  private boolean first = true;
  private final OperatorContext oContext;

  public DrillRecordReader(ExecutorFragmentContext context, DrillSubScan config)
      throws OutOfMemoryException, ExecutionSetupException {
    this.context = context;
    this.oContext = context.newOperatorContext(config);
    this.batchLoader = new RecordBatchLoader(oContext.getAllocator());

    String userName = Optional.ofNullable(config.getUserName()).orElse(ImpersonationUtil.getProcessUserName());
    this.drillClient = config.getPluginConfig().getDrillClient(userName, oContext.getAllocator());
    long queryTimeout = drillClient.getConfig().getLong(ExecConstants.JDBC_QUERY_TIMEOUT);
    int batchQueueThrottlingThreshold = drillClient.getConfig()
      .getInt(ExecConstants.JDBC_BATCH_QUEUE_THROTTLING_THRESHOLD);
    Stopwatch stopwatch = Stopwatch.createStarted();
    this.resultsListener =
      new BlockingResultsListener(() -> stopwatch, () -> queryTimeout, batchQueueThrottlingThreshold);
    this.drillClient.runQuery(QueryType.SQL, config.getQuery(), resultsListener);
    this.id = resultsListener.getQueryId();
    try {
      resultsListener.awaitFirstMessage();
    } catch (InterruptedException | SQLTimeoutException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public FragmentContext getContext() {
    return context;
  }

  @Override
  public BatchSchema getSchema() {
    return schema;
  }

  @Override
  public int getRecordCount() {
    return batchLoader.getRecordCount();
  }

  @Override
  public void cancel() {
    drillClient.cancelQuery(id);
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return batchLoader.iterator();
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
    return batchLoader.getValueVectorId(path);
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids) {
    return batchLoader.getValueAccessorById(clazz, ids);
  }

  private QueryDataBatch getNextBatch() {
    try {
      injector.injectChecked(context.getExecutionControls(), "next-allocate", OutOfMemoryException.class);
      return resultsListener.getNext();
    } catch(InterruptedException e) {
      // Preserve evidence that the interruption occurred so that code higher up
      // on the call stack can learn of the
      // interruption and respond to it if it wants to.
      Thread.currentThread().interrupt();

      return null;
    } catch (Exception e) {
      throw UserException.dataReadError(e)
        .addContext("Failure when reading incoming batch")
        .build(logger);
    }
  }

  @Override
  public RecordBatch.IterOutcome next() {
    batchLoader.resetRecordCount();
    oContext.getStats().startProcessing();
    try {
      QueryDataBatch batch;
      try {
        oContext.getStats().startWait();
        batch = getNextBatch();

        // skip over empty batches. we do this since these are basically control messages.
        while (batch != null && batch.getHeader().getDef().getRecordCount() == 0
          && (!first || batch.getHeader().getDef().getFieldCount() == 0)) {
          batch = getNextBatch();
        }
      } finally {
        oContext.getStats().stopWait();
      }

      first = false;

      if (batch == null) {
        // Represents last outcome of next(). If an Exception is thrown
        // during the method's execution a value IterOutcome.STOP will be assigned.
        IterOutcome lastOutcome = IterOutcome.NONE;
        batchLoader.zero();
        context.getExecutorState().checkContinue();
        return lastOutcome;
      }

      if (context.getAllocator().isOverLimit()) {
        context.requestMemory(this);
        if (context.getAllocator().isOverLimit()) {
          throw new OutOfMemoryException("Allocator over limit");
        }
      }

      UserBitShared.RecordBatchDef rbd = batch.getHeader().getDef();
      boolean schemaChanged = batchLoader.load(rbd, batch.getData());

      batch.release();
      if (schemaChanged) {
        this.schema = batchLoader.getSchema();
        oContext.getStats().batchReceived(0, rbd.getRecordCount(), true);
        return RecordBatch.IterOutcome.OK_NEW_SCHEMA;
      } else {
        oContext.getStats().batchReceived(0, rbd.getRecordCount(), false);
        return RecordBatch.IterOutcome.OK;
      }
    } finally {
      oContext.getStats().stopProcessing();
    }
  }

  @Override
  public WritableBatch getWritableBatch() {
    return batchLoader.getWritableBatch();
  }

  @Override
  public void close() {
    logger.debug("Closing {}", getClass().getCanonicalName());
    batchLoader.clear();
    resultsListener.close();
    drillClient.close();
  }

  @Override
  public VectorContainer getOutgoingContainer() {
    throw new UnsupportedOperationException(
      String.format("You should not call getOutgoingContainer() for class %s",
        getClass().getCanonicalName()));
  }

  @Override
  public VectorContainer getContainer() {
    return batchLoader.getContainer();
  }

  @Override
  public void dump() {
    logger.error("DrillRecordReader[batchLoader={}, schema={}]", batchLoader, schema);
  }
}
