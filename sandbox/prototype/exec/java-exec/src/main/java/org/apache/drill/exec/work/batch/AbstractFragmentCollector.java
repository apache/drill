/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.work.batch;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.apache.drill.exec.physical.base.Receiver;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.record.RawFragmentBatch;
import org.apache.drill.exec.rpc.RemoteConnection.ConnectionThrottle;

import com.google.common.base.Preconditions;

public abstract class AbstractFragmentCollector implements BatchCollector{

  private final List<DrillbitEndpoint> incoming;
  private final int oppositeMajorFragmentId;
  private final AtomicIntegerArray remainders;
  private final AtomicInteger remainingRequired;
  protected final RawBatchBuffer[] buffers;
  private final AtomicInteger parentAccounter;
  private final AtomicInteger finishedStreams = new AtomicInteger();
  
  public AbstractFragmentCollector(AtomicInteger parentAccounter, Receiver receiver, int minInputsRequired) {
    Preconditions.checkArgument(minInputsRequired > 0);
    Preconditions.checkNotNull(receiver);
    Preconditions.checkNotNull(parentAccounter);

    this.parentAccounter = parentAccounter;
    this.incoming = receiver.getProvidingEndpoints();
    this.remainders = new AtomicIntegerArray(incoming.size());
    this.oppositeMajorFragmentId = receiver.getOppositeMajorFragmentId();
    this.buffers = new RawBatchBuffer[minInputsRequired];
    for(int i = 0; i < buffers.length; i++){
      buffers[i] = new UnlmitedRawBatchBuffer();
    }
    if (receiver.supportsOutOfOrderExchange()) {
      this.remainingRequired = new AtomicInteger(1);
    } else {
      this.remainingRequired = new AtomicInteger(minInputsRequired);
    }
  }

  public int getOppositeMajorFragmentId() {
    return oppositeMajorFragmentId;
  }

  public RawBatchBuffer[] getBuffers(){
    return buffers;
  }
  
  public abstract void streamFinished(int minorFragmentId);
  
  public boolean batchArrived(ConnectionThrottle throttle, int minorFragmentId, RawFragmentBatch batch) {
    boolean decremented = false;
    if (remainders.compareAndSet(minorFragmentId, 0, 1)) {
      int rem = remainingRequired.decrementAndGet();
      if (rem == 0) {
        parentAccounter.decrementAndGet();
        decremented = true;
      }
    }
    if(batch.getHeader().getIsLastBatch()){
      streamFinished(minorFragmentId);
    }
    getBuffer(minorFragmentId).enqueue(throttle, batch);
    return decremented;
  }


  @Override
  public int getTotalIncomingFragments() {
    return incoming.size();
  }

  protected abstract RawBatchBuffer getBuffer(int minorFragmentId);
}