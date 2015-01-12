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
package org.apache.drill.exec.work.batch;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.BitData.FragmentRecordBatch;
import org.apache.drill.exec.record.RawFragmentBatch;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Queues;

public class UnlimitedRawBatchBuffer implements RawBatchBuffer{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnlimitedRawBatchBuffer.class);

  private static enum BufferState {
    INIT,
    FINISHED,
    KILLED
  }

  private final LinkedBlockingDeque<RawFragmentBatch> buffer;
  private volatile BufferState state = BufferState.INIT;
  private final int softlimit;
  private final int startlimit;
  private final int bufferSizePerSocket;
  private final AtomicBoolean overlimit = new AtomicBoolean(false);
  private final AtomicBoolean outOfMemory = new AtomicBoolean(false);
  private final ResponseSenderQueue readController = new ResponseSenderQueue();
  private int streamCounter;
  private int fragmentCount;
  private FragmentContext context;

  public UnlimitedRawBatchBuffer(FragmentContext context, int fragmentCount) {
    bufferSizePerSocket = context.getConfig().getInt(ExecConstants.INCOMING_BUFFER_SIZE);

    this.softlimit = bufferSizePerSocket * fragmentCount;
    this.startlimit = Math.max(softlimit/2, 1);
    logger.debug("softLimit: {}, startLimit: {}", softlimit, startlimit);
    this.buffer = Queues.newLinkedBlockingDeque();
    this.fragmentCount = fragmentCount;
    this.streamCounter = fragmentCount;
    this.context = context;
  }

  @Override
  public void enqueue(RawFragmentBatch batch) throws IOException {
    if (isFinished()) {
      if (state == BufferState.KILLED) {
        // do not even enqueue just release and send ack back
        batch.release();
        batch.sendOk();
        return;
      } else {
        throw new IOException("Attempted to enqueue batch after finished");
      }
    }
    if (batch.getHeader().getIsOutOfMemory()) {
      logger.debug("Setting autoread false");
      RawFragmentBatch firstBatch = buffer.peekFirst();
      FragmentRecordBatch header = firstBatch == null ? null :firstBatch.getHeader();
      if (!outOfMemory.get() && !(header == null) && header.getIsOutOfMemory()) {
        buffer.addFirst(batch);
      }
      outOfMemory.set(true);
      return;
    }
    buffer.add(batch);
    if (buffer.size() >= softlimit) {
      logger.debug("buffer.size: {}", buffer.size());
      overlimit.set(true);
      readController.enqueueResponse(batch.getSender());
    } else {
      batch.sendOk();
    }
  }

  @Override
  public void cleanup() {
    if (!isFinished() && !context.isCancelled()) {
      String msg = String.format("Cleanup before finished. " + (fragmentCount - streamCounter) + " out of " + fragmentCount + " streams have finished.");
      logger.error(msg);
      IllegalStateException e = new IllegalStateException(msg);
      context.fail(e);
      throw e;
    }

    if (!buffer.isEmpty()) {
      if (!context.isFailed() && !context.isCancelled()) {
        context.fail(new IllegalStateException("Batches still in queue during cleanup"));
        logger.error("{} Batches in queue.", buffer.size());
      }
      clearBufferWithBody();
    }
  }

  @Override
  public void kill(FragmentContext context) {
    state = BufferState.KILLED;
    clearBufferWithBody();
  }

  /**
   * Helper method to clear buffer with request bodies release
   * also flushes ack queue - in case there are still responses pending
   */
  private void clearBufferWithBody() {
    while (!buffer.isEmpty()) {
      RawFragmentBatch batch = buffer.poll();
      if (batch.getBody() != null) {
        batch.getBody().release();
      }
    }
    readController.flushResponses();
  }

  @Override
  public void finished() {
    if (state != BufferState.KILLED) {
      state = BufferState.FINISHED;
    }
    if (!buffer.isEmpty()) {
      throw new IllegalStateException("buffer not empty when finished");
    }
  }

  @Override
  public RawFragmentBatch getNext() {

    if (outOfMemory.get() && buffer.size() < 10) {
      logger.debug("Setting autoread true");
      outOfMemory.set(false);
      readController.flushResponses();
    }

    RawFragmentBatch b = null;

    b = buffer.poll();

    // if we didn't get a buffer, block on waiting for buffer.
    if (b == null && (!isFinished() || !buffer.isEmpty())) {
      try {
        b = buffer.take();
      } catch (InterruptedException e) {
        return null;
      }
    }

    if (b != null && b.getHeader().getIsOutOfMemory()) {
      outOfMemory.set(true);
      return b;
    }


    // try to flush the difference between softlimit and queue size, so every flush we are reducing backlog
    // when queue size is lower then softlimit - the bigger the difference the more we can flush
    if (!isFinished() && overlimit.get()) {
      int flushCount = softlimit - buffer.size();
      if ( flushCount > 0 ) {
        int flushed = readController.flushResponses(flushCount);
        logger.debug("flush {} entries, flushed {} entries ", flushCount, flushed);
        if ( flushed == 0 ) {
          // queue is empty - nothing to do for now
          overlimit.set(false);
        }
      }
    }

    if (b != null && b.getHeader().getIsLastBatch()) {
      streamCounter--;
      if (streamCounter == 0) {
        finished();
      }
    }

    if (b == null && buffer.size() > 0) {
      throw new IllegalStateException("Returning null when there are batches left in queue");
    }
    if (b == null && !isFinished()) {
      throw new IllegalStateException("Returning null when not finished");
    }
    return b;

  }

  private boolean isFinished() {
    return (state == BufferState.KILLED || state == BufferState.FINISHED);
  }

  @VisibleForTesting
  ResponseSenderQueue getReadController() {
    return readController;
  }

  @VisibleForTesting
  boolean isBufferEmpty() {
    return buffer.isEmpty();
  }
}
