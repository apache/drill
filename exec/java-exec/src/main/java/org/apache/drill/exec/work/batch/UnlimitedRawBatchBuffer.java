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

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.BitData.FragmentRecordBatch;
import org.apache.drill.exec.record.RawFragmentBatch;
import org.apache.drill.exec.rpc.ResponseSender;
import org.apache.drill.exec.rpc.data.DataRpcConfig;

import com.google.common.collect.Queues;

public class UnlimitedRawBatchBuffer implements RawBatchBuffer{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnlimitedRawBatchBuffer.class);

  private final LinkedBlockingDeque<RawFragmentBatch> buffer;
  private volatile boolean finished = false;
  private final int softlimit;
  private final int startlimit;
  private final AtomicBoolean overlimit = new AtomicBoolean(false);
  private final AtomicBoolean outOfMemory = new AtomicBoolean(false);
  private final ResponseSenderQueue readController = new ResponseSenderQueue();
  private int streamCounter;
  private FragmentContext context;

  public UnlimitedRawBatchBuffer(FragmentContext context, int fragmentCount) {
    int bufferSizePerSocket = context.getConfig().getInt(ExecConstants.INCOMING_BUFFER_SIZE);

    this.softlimit = bufferSizePerSocket * fragmentCount;
    this.startlimit = Math.max(softlimit/2, 1);
    this.buffer = Queues.newLinkedBlockingDeque();
    this.streamCounter = fragmentCount;
    this.context = context;
  }

  @Override
  public void enqueue(RawFragmentBatch batch) {
    if (finished) {
      throw new RuntimeException("Attempted to enqueue batch after finished");
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
    if(buffer.size() == softlimit){
      overlimit.set(true);
      readController.enqueueResponse(batch.getSender());
    }else{
      batch.sendOk();
    }
  }

  @Override
  public void cleanup() {
    if (!finished && !context.isCancelled()) {
      IllegalStateException e = new IllegalStateException("Cleanup before finished");
      context.fail(e);
      throw e;
    }

    if (!buffer.isEmpty()) {
      if (!context.isFailed() && !context.isCancelled()) {
        context.fail(new IllegalStateException("Batches still in queue during cleanup"));
        logger.error("{} Batches in queue.", buffer.size());
        RawFragmentBatch batch;
        while ((batch = buffer.poll()) != null) {
          logger.error("Batch left in queue: {}", batch);
        }
      }
      RawFragmentBatch batch;
      while ((batch = buffer.poll()) != null) {
        if (batch.getBody() != null) batch.getBody().release();
      }
    }
  }

  @Override
  public void kill(FragmentContext context) {
    while(!buffer.isEmpty()){
      RawFragmentBatch batch = buffer.poll();
      batch.getBody().release();
    }
  }

  @Override
  public void finished() {
    finished = true;
    if (!buffer.isEmpty()) {
      throw new IllegalStateException("buffer not empty when finished");
    }
  }

  @Override
  public RawFragmentBatch getNext(){

    if (outOfMemory.get() && buffer.size() < 10) {
      logger.debug("Setting autoread true");
      outOfMemory.set(false);
      readController.flushResponses();
    }

    RawFragmentBatch b = null;

    b = buffer.poll();

    // if we didn't get a buffer, block on waiting for buffer.
    if(b == null && (!finished || !buffer.isEmpty())){
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


    // if we are in the overlimit condition and aren't finished, check if we've passed the start limit.  If so, turn off the overlimit condition and set auto read to true (start reading from socket again).
    if(!finished && overlimit.get()){
      if(buffer.size() == startlimit){
        overlimit.set(false);
        readController.flushResponses();
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
    if (b == null && !finished) {
      throw new IllegalStateException("Returning null when not finished");
    }
    return b;

  }


}
