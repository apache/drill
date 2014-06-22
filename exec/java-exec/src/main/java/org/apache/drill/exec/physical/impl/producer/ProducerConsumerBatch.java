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
package org.apache.drill.exec.physical.impl.producer;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.ProducerConsumer;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.vector.ValueVector;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class ProducerConsumerBatch extends AbstractRecordBatch {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProducerConsumerBatch.class);

  private RecordBatch incoming;
  private Thread producer = new Thread(new Producer(), Thread.currentThread().getName() + " - Producer Thread");
  private boolean running = false;
  private BlockingDeque<RecordBatchDataWrapper> queue;
  private int recordCount;
  private BatchSchema schema;
  private boolean stop = false;

  protected ProducerConsumerBatch(ProducerConsumer popConfig, FragmentContext context, RecordBatch incoming) throws OutOfMemoryException {
    super(popConfig, context);
    this.incoming = incoming;
    this.queue = new LinkedBlockingDeque<>(popConfig.getSize());
  }

  @Override
  public IterOutcome innerNext() {
    if (!running) {
      producer.start();
      running = true;
    }
    RecordBatchDataWrapper wrapper;
    try {
      stats.startWait();
      wrapper = queue.take();
      logger.debug("Got batch from queue");
    } catch (InterruptedException e) {
      context.fail(e);
      return IterOutcome.STOP;
    } finally {
      stats.stopWait();
    }
    if (wrapper.finished) {
      return IterOutcome.NONE;
    }

    recordCount = wrapper.batch.getRecordCount();
    boolean newSchema = load(wrapper.batch);

    return newSchema ? IterOutcome.OK_NEW_SCHEMA : IterOutcome.OK;
  }

  private boolean load(RecordBatchData batch) {
    VectorContainer newContainer = batch.getContainer();
    if (schema != null && newContainer.getSchema().equals(schema)) {
      container.zeroVectors();
      BatchSchema schema = container.getSchema();
      for (int i = 0; i < container.getNumberOfColumns(); i++) {
        MaterializedField field = schema.getColumn(i);
        MajorType type = field.getType();
        ValueVector vOut = container.getValueAccessorById(TypeHelper.getValueVectorClass(type.getMinorType(), type.getMode()),
                container.getValueVectorId(field.getPath()).getFieldIds()).getValueVector();
        ValueVector vIn = newContainer.getValueAccessorById(TypeHelper.getValueVectorClass(type.getMinorType(), type.getMode()),
                newContainer.getValueVectorId(field.getPath()).getFieldIds()).getValueVector();
        TransferPair tp = vIn.makeTransferPair(vOut);
        tp.transfer();
      }
      return false;
    } else {
      container.clear();
      for (VectorWrapper w : newContainer) {
        container.add(w.getValueVector());
      }
      container.buildSchema(SelectionVectorMode.NONE);
      schema = container.getSchema();
      return true;
    }
  }

  private class Producer implements Runnable {

    @Override
    public void run() {
      if (stop) return;
      outer: while (true) {
        IterOutcome upstream = incoming.next();
        switch (upstream) {
          case NONE:
            break outer;
          case STOP:
            try {
              queue.putFirst(new RecordBatchDataWrapper(null, false, true));
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          case OK_NEW_SCHEMA:
          case OK:
            try {
              if (!stop) queue.put(new RecordBatchDataWrapper(new RecordBatchData(incoming), false, false));
            } catch (InterruptedException e) {
              context.fail(e);
              try {
                queue.putFirst(new RecordBatchDataWrapper(null, false, true));
              } catch (InterruptedException e1) {
                throw new RuntimeException(e1);
              }
            }
            break;
          default:
            throw new UnsupportedOperationException();
        }
      }
      try {
        queue.put(new RecordBatchDataWrapper(null, true, false));
      } catch (InterruptedException e) {
        context.fail(e);
        try {
          queue.putFirst(new RecordBatchDataWrapper(null, false, true));
        } catch (InterruptedException e1) {
          throw new RuntimeException(e1);
        }
      }
      logger.debug("Producer thread finished");
    }
  }

  private void clearQueue() {
    RecordBatchDataWrapper wrapper;
    while ((wrapper = queue.poll()) != null) {
      wrapper.batch.getContainer().clear();
    }
  }

  @Override
  protected void killIncoming() {
    incoming.kill();
  }

  @Override
  public void cleanup() {
    stop = true;
    clearQueue();
    super.cleanup();
    incoming.cleanup();
  }

  @Override
  public int getRecordCount() {
    return recordCount;
  }

  private static class RecordBatchDataWrapper {
    RecordBatchData batch;
    boolean finished;
    boolean failed;

    RecordBatchDataWrapper(RecordBatchData batch, boolean finished, boolean failed) {
      this.batch = batch;
      this.finished = finished;
      this.failed = failed;
    }
  }
}
