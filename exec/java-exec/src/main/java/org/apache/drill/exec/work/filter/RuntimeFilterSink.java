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
package org.apache.drill.exec.work.filter;

import org.apache.drill.exec.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This sink receives the RuntimeFilters from the netty thread,
 * aggregates them in an async thread, supplies the aggregated
 * one to the fragment running thread.
 */
public class RuntimeFilterSink implements AutoCloseable {

  private AtomicInteger currentBookId = new AtomicInteger(0);

  private int staleBookId = 0;

  /**
   * RuntimeFilterWritable holding the aggregated version of all the received filter
   */
  private RuntimeFilterWritable aggregated = null;

  private BlockingQueue<RuntimeFilterWritable> rfQueue = new LinkedBlockingQueue<>();

  /**
   * Flag used by Minor Fragment thread to indicate it has encountered error
   */
  private AtomicBoolean running = new AtomicBoolean(true);

  /**
   * Lock used to synchronize between producer (Netty Thread) and consumer (AsyncAggregateThread) of elements of this
   * queue. This is needed because in error condition running flag can be consumed by producer and consumer thread at
   * different times. Whoever sees it first will take this lock and clear all elements and set the queue to null to
   * indicate producer not to put any new elements in it.
   */
  private ReentrantLock queueLock = new ReentrantLock();

  private Condition notEmpty = queueLock.newCondition();

  private ReentrantLock aggregatedRFLock = new ReentrantLock();

  private BufferAllocator bufferAllocator;

  private Future future;

  private static final Logger logger = LoggerFactory.getLogger(RuntimeFilterSink.class);


  public RuntimeFilterSink(BufferAllocator bufferAllocator, ExecutorService executorService) {
    this.bufferAllocator = bufferAllocator;
    AsyncAggregateWorker asyncAggregateWorker = new AsyncAggregateWorker();
    future = executorService.submit(asyncAggregateWorker);
  }

  public void aggregate(RuntimeFilterWritable runtimeFilterWritable) {
    if (running.get()) {
      try {
        aggregatedRFLock.lock();
        if (containOne()) {
          boolean same = aggregated.equals(runtimeFilterWritable);
          if (!same) {
            // This is to solve the only one fragment case that two RuntimeFilterRecordBatchs
            // share the same FragmentContext.
            aggregated.close();
            currentBookId.set(0);
            staleBookId = 0;
            clearQueued(false);
          }
        }
      } finally {
        aggregatedRFLock.unlock();
      }

      try {
        queueLock.lock();
        if (rfQueue != null) {
          rfQueue.add(runtimeFilterWritable);
          notEmpty.signal();
        } else {
          runtimeFilterWritable.close();
        }
      } finally {
        queueLock.unlock();
      }
    } else {
      runtimeFilterWritable.close();
    }
  }

  public RuntimeFilterWritable fetchLatestDuplicatedAggregatedOne() {
    try {
      aggregatedRFLock.lock();
      return aggregated.duplicate(bufferAllocator);
    } finally {
      aggregatedRFLock.unlock();
    }
  }

  /**
   * whether there's a fresh aggregated RuntimeFilter
   *
   * @return
   */
  public boolean hasFreshOne() {
    if (currentBookId.get() > staleBookId) {
      staleBookId = currentBookId.get();
      return true;
    }
    return false;
  }

  /**
   * whether there's a usable RuntimeFilter.
   *
   * @return
   */
  public boolean containOne() {
    return aggregated != null;
  }

  private void doCleanup() {
    running.compareAndSet(true, false);
    try {
      aggregatedRFLock.lock();
      if (containOne()) {
        aggregated.close();
        aggregated = null;
      }
    } finally {
      aggregatedRFLock.unlock();
    }
  }

  @Override
  public void close() throws Exception {
    future.cancel(true);
    doCleanup();
  }

  private void clearQueued(boolean setToNull) {
    RuntimeFilterWritable toClear;
    try {
      queueLock.lock();
      while (rfQueue != null && (toClear = rfQueue.poll()) != null) {
        toClear.close();
      }
      rfQueue = (setToNull) ? null : rfQueue;
    } finally {
      queueLock.unlock();
    }
  }

  private class AsyncAggregateWorker implements Runnable {

    @Override
    public void run() {
      try {
        RuntimeFilterWritable toAggregate = null;
        while (running.get()) {
          try {
            queueLock.lock();
            toAggregate = (rfQueue != null) ? rfQueue.poll() :  null;
            if (toAggregate == null) {
              notEmpty.await();
              continue;
            }
          } finally {
            queueLock.unlock();
          }

          try {
            aggregatedRFLock.lock();
            if (containOne()) {
              aggregated.aggregate(toAggregate);

              // Release the byteBuf referenced by toAggregate since aggregate will not do it
              toAggregate.close();
            } else {
              aggregated = toAggregate;
            }
          } finally {
            aggregatedRFLock.unlock();
          }
          currentBookId.incrementAndGet();
        }
      } catch (InterruptedException e) {
        logger.info("RFAggregating Thread : {} was interrupted.", Thread.currentThread().getName());
        Thread.currentThread().interrupt();
      } finally {
        doCleanup();
        clearQueued(true);
      }
    }
  }
}


