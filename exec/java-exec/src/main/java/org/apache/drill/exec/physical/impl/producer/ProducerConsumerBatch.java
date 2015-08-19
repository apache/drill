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

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.memory.OutOfMemoryRuntimeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.ProducerConsumer;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;

public class ProducerConsumerBatch extends AbstractRecordBatch {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProducerConsumerBatch.class);

  private final RecordBatch incoming;
  private final Thread producer = new Thread(new Producer(), Thread.currentThread().getName() + " - Producer Thread");
  private boolean running = false;
  private final BlockingDeque<RecordBatchDataWrapper> queue;
  private int recordCount;
  private BatchSchema schema;
  private boolean stop = false;
  private final CountDownLatch cleanUpLatch = new CountDownLatch(1); // used to wait producer to clean up

  protected ProducerConsumerBatch(final ProducerConsumer popConfig, final FragmentContext context, final RecordBatch incoming) throws OutOfMemoryException {
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
    } catch (final InterruptedException e) {
      if (context.shouldContinue()) {
        context.fail(e);
      }
      return IterOutcome.STOP;
      // TODO InterruptedException
    } finally {
      stats.stopWait();
    }
    if (wrapper.finished) {
      return IterOutcome.NONE;
    } else if (wrapper.failed) {
      return IterOutcome.STOP;
    } else if (wrapper.outOfMemory) {
      throw new OutOfMemoryRuntimeException();
    }

    recordCount = wrapper.batch.getRecordCount();
    final boolean newSchema = load(wrapper.batch);

    return newSchema ? IterOutcome.OK_NEW_SCHEMA : IterOutcome.OK;
  }

  private boolean load(final RecordBatchData batch) {
    final VectorContainer newContainer = batch.getContainer();
    if (schema != null && newContainer.getSchema().equals(schema)) {
      container.zeroVectors();
      final BatchSchema schema = container.getSchema();
      for (int i = 0; i < container.getNumberOfColumns(); i++) {
        final MaterializedField field = schema.getColumn(i);
        final MajorType type = field.getType();
        final ValueVector vOut = container.getValueAccessorById(TypeHelper.getValueVectorClass(type.getMinorType(), type.getMode()),
                container.getValueVectorId(field.getPath()).getFieldIds()).getValueVector();
        final ValueVector vIn = newContainer.getValueAccessorById(TypeHelper.getValueVectorClass(type.getMinorType(), type.getMode()),
                newContainer.getValueVectorId(field.getPath()).getFieldIds()).getValueVector();
        final TransferPair tp = vIn.makeTransferPair(vOut);
        tp.transfer();
      }
      return false;
    } else {
      container.clear();
      for (final VectorWrapper<?> w : newContainer) {
        container.add(w.getValueVector());
      }
      container.buildSchema(SelectionVectorMode.NONE);
      schema = container.getSchema();
      return true;
    }
  }

  private class Producer implements Runnable {
    RecordBatchDataWrapper wrapper;

    @Override
    public void run() {
      try {
        if (stop) {
          return;
        }
        outer:
        while (true) {
          final IterOutcome upstream = incoming.next();
          switch (upstream) {
            case NONE:
              stop = true;
              break outer;
            case OUT_OF_MEMORY:
              queue.putFirst(RecordBatchDataWrapper.outOfMemory());
              return;
            case STOP:
              queue.putFirst(RecordBatchDataWrapper.failed());
              return;
            case OK_NEW_SCHEMA:
            case OK:
              wrapper = RecordBatchDataWrapper.batch(new RecordBatchData(incoming));
              queue.put(wrapper);
              wrapper = null;
              break;
            default:
              throw new UnsupportedOperationException();
          }
        }
      } catch (final OutOfMemoryRuntimeException e) {
        try {
          queue.putFirst(RecordBatchDataWrapper.outOfMemory());
        } catch (final InterruptedException ex) {
          logger.error("Unable to enqueue the last batch indicator. Something is broken.", ex);
          // TODO InterruptedException
        }
      } catch (final InterruptedException e) {
        logger.warn("Producer thread is interrupted.", e);
        // TODO InterruptedException
      } finally {
        if (stop) {
          try {
            clearQueue();
            queue.put(RecordBatchDataWrapper.finished());
          } catch (final InterruptedException e) {
            logger.error("Unable to enqueue the last batch indicator. Something is broken.", e);
            // TODO InterruptedException
          }
        }
        if (wrapper!=null) {
          wrapper.batch.clear();
        }
        cleanUpLatch.countDown();
      }
    }
  }

  private void clearQueue() {
    RecordBatchDataWrapper wrapper;
    while ((wrapper = queue.poll()) != null) {
      if (wrapper.batch != null) {
        wrapper.batch.getContainer().clear();
      }
    }
  }

  @Override
  protected void killIncoming(final boolean sendUpstream) {
    stop = true;
    producer.interrupt();
    try {
      producer.join();
    } catch (final InterruptedException e) {
      logger.warn("Interrupted while waiting for producer thread");
      // TODO InterruptedException
    }
  }

  @Override
  public void close() throws Exception {
    stop = true;
    try {
      cleanUpLatch.await();
    } catch (final InterruptedException e) {
      logger.warn("Interrupted while waiting for producer to clean up first. I will try to clean up now...", e);
      // TODO we should retry to wait for the latch
    } finally {
      super.close();
      clearQueue();
    }
  }

  @Override
  public int getRecordCount() {
    return recordCount;
  }

  private static class RecordBatchDataWrapper {
    final RecordBatchData batch;
    final boolean finished;
    final boolean failed;
    final boolean outOfMemory;

    RecordBatchDataWrapper(final RecordBatchData batch, final boolean finished, final boolean failed, final boolean outOfMemory) {
      this.batch = batch;
      this.finished = finished;
      this.failed = failed;
      this.outOfMemory = outOfMemory;
    }

    public static RecordBatchDataWrapper batch(final RecordBatchData batch) {
      return new RecordBatchDataWrapper(batch, false, false, false);
    }

    public static RecordBatchDataWrapper finished() {
      return new RecordBatchDataWrapper(null, true, false, false);
    }

    public static RecordBatchDataWrapper failed() {
      return new RecordBatchDataWrapper(null, false, true, false);
    }

    public static RecordBatchDataWrapper outOfMemory() {
      return new RecordBatchDataWrapper(null, false, false, true);
    }
  }

}
