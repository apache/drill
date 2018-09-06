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
import org.apache.drill.exec.rpc.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This sink receives the RuntimeFilters from the netty thread,
 * aggregates them in an async thread, supplies the aggregated
 * one to the fragment running thread.
 */
public class RuntimeFilterSink implements AutoCloseable {

  private AtomicInteger currentBookId = new AtomicInteger(0);

  private int staleBookId = 0;

  private RuntimeFilterWritable aggregated = null;

  private BlockingQueue<RuntimeFilterWritable> rfQueue = new LinkedBlockingQueue<>();

  private AtomicBoolean running = new AtomicBoolean(true);

  private ReentrantLock aggregatedRFLock = new ReentrantLock();

  private Thread asyncAggregateThread;

  private BufferAllocator bufferAllocator;

  private static final Logger logger = LoggerFactory.getLogger(RuntimeFilterSink.class);


  public RuntimeFilterSink(BufferAllocator bufferAllocator) {
    this.bufferAllocator = bufferAllocator;
    AsyncAggregateWorker asyncAggregateWorker = new AsyncAggregateWorker();
    asyncAggregateThread = new NamedThreadFactory("RFAggregating-").newThread(asyncAggregateWorker);
    asyncAggregateThread.start();
  }

  public void aggregate(RuntimeFilterWritable runtimeFilterWritable) {
    if (running.get()) {
      if (containOne()) {
        boolean same = aggregated.same(runtimeFilterWritable);
        if (!same) {
          //This is to solve the only one fragment case that two RuntimeFilterRecordBatchs
          //share the same FragmentContext.
          try {
            aggregatedRFLock.lock();
            aggregated.close();
            aggregated = null;
          } finally {
            aggregatedRFLock.unlock();
          }
          currentBookId.set(0);
          staleBookId = 0;
          clearQueued();
        }
      }
      rfQueue.add(runtimeFilterWritable);
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

  @Override
  public void close() throws Exception {
    running.compareAndSet(true, false);
    asyncAggregateThread.interrupt();
    if (containOne()) {
      try {
        aggregatedRFLock.lock();
        aggregated.close();
      } finally {
        aggregatedRFLock.unlock();
      }
    }
    clearQueued();
  }

  private void clearQueued() {
    RuntimeFilterWritable toClear;
    while ((toClear = rfQueue.poll()) != null) {
      toClear.close();
    }
  }

  class AsyncAggregateWorker implements Runnable {

    @Override
    public void run() {
      try {
        while (running.get()) {
          RuntimeFilterWritable toAggregate = rfQueue.take();
          if (!running.get()) {
            toAggregate.close();
            return;
          }
          if (containOne()) {
            try {
              aggregatedRFLock.lock();
              aggregated.aggregate(toAggregate);
            } finally {
              aggregatedRFLock.unlock();
            }
          } else {
            aggregated = toAggregate;
          }
          currentBookId.incrementAndGet();
        }
      } catch (InterruptedException e) {
        logger.info("Thread : {} was interrupted.", asyncAggregateThread.getName(), e);
      }
    }
  }
}


