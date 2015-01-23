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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.MinorFragmentEndpoint;
import org.apache.drill.exec.physical.base.Receiver;
import org.apache.drill.exec.record.RawFragmentBatch;

import com.google.common.base.Preconditions;
import org.apache.drill.exec.util.ArrayWrappedIntIntMap;

public abstract class AbstractDataCollector implements DataCollector{

  private final List<MinorFragmentEndpoint> incoming;
  private final int oppositeMajorFragmentId;
  private final AtomicIntegerArray remainders;
  private final AtomicInteger remainingRequired;
  private final AtomicInteger parentAccounter;

  protected final RawBatchBuffer[] buffers;
  protected final ArrayWrappedIntIntMap fragmentMap;

  /**
   * @param parentAccounter
   * @param receiver
   * @param numBuffers Number of RawBatchBuffer inputs required to store the incoming data
   * @param bufferCapacity Capacity of each RawBatchBuffer.
   * @param context
   */
  public AbstractDataCollector(AtomicInteger parentAccounter, Receiver receiver,
      final int numBuffers, final int bufferCapacity, FragmentContext context) {
    Preconditions.checkNotNull(receiver);
    Preconditions.checkNotNull(parentAccounter);

    this.parentAccounter = parentAccounter;
    this.incoming = receiver.getProvidingEndpoints();
    this.remainders = new AtomicIntegerArray(incoming.size());
    this.oppositeMajorFragmentId = receiver.getOppositeMajorFragmentId();

    // Create fragmentId to index that is within the range [0, incoming.size()-1]
    // We use this mapping to find objects belonging to the fragment in buffers and remainders arrays.
    fragmentMap = new ArrayWrappedIntIntMap();
    int index = 0;
    for(MinorFragmentEndpoint endpoint : incoming) {
      fragmentMap.put(endpoint.getId(), index);
      index++;
    }

    buffers = new RawBatchBuffer[numBuffers];
    remainingRequired = new AtomicInteger(numBuffers);

    try {
      String bufferClassName = context.getConfig().getString(ExecConstants.INCOMING_BUFFER_IMPL);
      Constructor<?> bufferConstructor = Class.forName(bufferClassName).getConstructor(FragmentContext.class, int.class);

      for(int i=0; i<numBuffers; i++) {
        buffers[i] = (RawBatchBuffer) bufferConstructor.newInstance(context, bufferCapacity);
      }
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
            NoSuchMethodException | ClassNotFoundException e) {
      context.fail(e);
    }
  }

  @Override
  public int getOppositeMajorFragmentId() {
    return oppositeMajorFragmentId;
  }

  @Override
  public RawBatchBuffer[] getBuffers(){
    return buffers;
  }

  @Override
  public boolean batchArrived(int minorFragmentId, RawFragmentBatch batch)  throws IOException {

    // if we received an out of memory, add an item to all the buffer queues.
    if (batch.getHeader().getIsOutOfMemory()) {
      for (RawBatchBuffer buffer : buffers) {
        buffer.enqueue(batch);
      }
    }

    // check to see if we have enough fragments reporting to proceed.
    boolean decremented = false;
    if (remainders.compareAndSet(fragmentMap.get(minorFragmentId), 0, 1)) {
      int rem = remainingRequired.decrementAndGet();
      if (rem == 0) {
        parentAccounter.decrementAndGet();
        decremented = true;
      }
    }

    getBuffer(minorFragmentId).enqueue(batch);

    return decremented;
  }


  @Override
  public int getTotalIncomingFragments() {
    return incoming.size();
  }

  protected abstract RawBatchBuffer getBuffer(int minorFragmentId);

  @Override
  public void close() {
  }

}