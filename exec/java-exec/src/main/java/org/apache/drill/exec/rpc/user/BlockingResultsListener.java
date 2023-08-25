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
package org.apache.drill.exec.rpc.user;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.rpc.ConnectionThrottle;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Queues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLTimeoutException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class BlockingResultsListener implements UserResultsListener {
  private static final Logger logger = LoggerFactory.getLogger(BlockingResultsListener.class);

  private static final AtomicInteger NEXT_INSTANCE_ID = new AtomicInteger(1);

  private final int instanceId;

  private final int batchQueueThrottlingThreshold;

  private volatile UserBitShared.QueryId queryId;

  private int lastReceivedBatchNumber;

  private int lastDequeuedBatchNumber;

  private volatile UserException executionFailureException;

  private volatile boolean completed;

  /**
   * Whether throttling of incoming data is active.
   */
  private final AtomicBoolean throttled = new AtomicBoolean(false);

  private volatile ConnectionThrottle throttle;

  private volatile boolean closed;

  private final CountDownLatch firstMessageReceived = new CountDownLatch(1);

  private final LinkedBlockingDeque<QueryDataBatch> batchQueue =
    Queues.newLinkedBlockingDeque();

  private final Supplier<Stopwatch> elapsedTimer;

  private final Supplier<Long> timeoutInMilliseconds;

  public BlockingResultsListener(Supplier<Stopwatch> elapsedTimer, Supplier<Long> timeoutInMilliseconds,
    int batchQueueThrottlingThreshold) {
    this.elapsedTimer = elapsedTimer;
    this.timeoutInMilliseconds = timeoutInMilliseconds;
    this.instanceId = NEXT_INSTANCE_ID.getAndIncrement();
    this.batchQueueThrottlingThreshold = batchQueueThrottlingThreshold;
    logger.debug("[#{}] Query listener created.", instanceId);
  }

  /**
   * Starts throttling if not currently throttling.
   *
   * @param throttle the "throttlable" object to throttle
   * @return true if actually started (wasn't throttling already)
   */
  private boolean startThrottlingIfNot(ConnectionThrottle throttle) {
    final boolean started = throttled.compareAndSet(false, true);
    if (started) {
      this.throttle = throttle;
      throttle.setAutoRead(false);
    }
    return started;
  }

  /**
   * Stops throttling if currently throttling.
   *
   * @return true if actually stopped (was throttling)
   */
  private boolean stopThrottlingIfSo() {
    final boolean stopped = throttled.compareAndSet(true, false);
    if (stopped) {
      throttle.setAutoRead(true);
      throttle = null;
    }
    return stopped;
  }

  public void awaitFirstMessage() throws InterruptedException, SQLTimeoutException {
    //Check if a non-zero timeout has been set
    if (timeoutInMilliseconds.get() > 0) {
      //Identifying remaining in milliseconds to maintain a granularity close to integer value of
      // timeout
      long timeToTimeout =
        timeoutInMilliseconds.get() - elapsedTimer.get().elapsed(TimeUnit.MILLISECONDS);
      if (timeToTimeout <= 0 || !firstMessageReceived.await(timeToTimeout, TimeUnit.MILLISECONDS)) {
        throw new SQLTimeoutException("Query timed out in "+ TimeUnit.MILLISECONDS.toSeconds(timeoutInMilliseconds.get()) + " seconds");
      }
    } else {
      firstMessageReceived.await();
    }
  }

  private void releaseIfFirst() {
    firstMessageReceived.countDown();
  }

  @Override
  public void queryIdArrived(UserBitShared.QueryId queryId) {
    logger.debug("[#{}] Received query ID: {}.",
      instanceId, QueryIdHelper.getQueryId(queryId));
    this.queryId = queryId;
  }

  @Override
  public void submissionFailed(UserException ex) {
    logger.debug("Received query failure: {} {}", instanceId, ex);
    this.executionFailureException = ex;
    this.completed = true;
    close();
    logger.info("[#{}] Query failed: ", instanceId, ex);
  }

  @Override
  public void dataArrived(QueryDataBatch result, ConnectionThrottle throttle) {
    lastReceivedBatchNumber++;
    logger.debug("[#{}] Received query data batch #{}: {}.",
      instanceId, lastReceivedBatchNumber, result);

    // If we're in a closed state, just release the message.
    if (closed) {
      result.release();
      completed = true;
      return;
    }

    // We're active; let's add to the queue.
    batchQueue.add(result);

    // Throttle server if queue size has exceed threshold.
    if (batchQueue.size() > batchQueueThrottlingThreshold) {
      if (startThrottlingIfNot(throttle)) {
        logger.debug("[#{}] Throttling started at queue size {}.",
          instanceId, batchQueue.size());
      }
    }

    releaseIfFirst();
  }

  @Override
  public void queryCompleted(UserBitShared.QueryResult.QueryState state) {
    logger.debug("[#{}] Received query completion: {}.", instanceId, state);
    releaseIfFirst();
    completed = true;
  }

  public UserBitShared.QueryId getQueryId() {
    return queryId;
  }

  /**
   * Gets the next batch of query results from the queue.
   *
   * @return the next batch, or {@code null} after last batch has been returned
   * @throws UserException        if the query failed
   * @throws InterruptedException if waiting on the queue was interrupted
   */
  public QueryDataBatch getNext() throws UserException, InterruptedException, SQLTimeoutException {
    while (true) {
      if (executionFailureException != null) {
        logger.debug("[#{}] Dequeued query failure exception: {}.",
          instanceId, executionFailureException);
        throw executionFailureException;
      }
      if (completed && batchQueue.isEmpty()) {
        return null;
      } else {
        QueryDataBatch qdb = batchQueue.poll(50, TimeUnit.MILLISECONDS);
        if (qdb != null) {
          lastDequeuedBatchNumber++;
          logger.debug("[#{}] Dequeued query data batch #{}: {}.",
            instanceId, lastDequeuedBatchNumber, qdb);

          // Unthrottle server if queue size has dropped enough below threshold:
          if (batchQueue.size() < batchQueueThrottlingThreshold / 2
            || batchQueue.size() == 0  // (in case threshold < 2)
          ) {
            if (stopThrottlingIfSo()) {
              logger.debug("[#{}] Throttling stopped at queue size {}.",
                instanceId, batchQueue.size());
            }
          }
          return qdb;
        }

        // Check and throw SQLTimeoutException
        if (timeoutInMilliseconds.get() > 0 && elapsedTimer.get().elapsed(TimeUnit.MILLISECONDS) >= timeoutInMilliseconds.get()) {
          throw new SQLTimeoutException("Query timed out in "+ TimeUnit.MILLISECONDS.toSeconds(timeoutInMilliseconds.get()) + " seconds");
        }
      }
    }
  }

  public void close() {
    logger.debug("[#{}] Query listener closing.", instanceId);
    closed = true;
    if (stopThrottlingIfSo()) {
      logger.debug("[#{}] Throttling stopped at close() (at queue size {}).",
        instanceId, batchQueue.size());
    }
    while (!batchQueue.isEmpty()) {
      // Don't bother with query timeout, we're closing the cursor
      QueryDataBatch qdb = batchQueue.poll();
      if (qdb != null && qdb.getData() != null) {
        qdb.getData().release();
      }
    }
    // Close may be called before the first result is received and therefore
    // when the main thread is blocked waiting for the result.  In that case
    // we want to unblock the main thread.
    releaseIfFirst();
    completed = true;
  }

  public boolean isCompleted() {
    return completed;
  }
}
