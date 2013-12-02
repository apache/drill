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

import java.util.Iterator;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RawFragmentBatch;
import org.apache.drill.exec.rpc.RemoteConnection.ConnectionThrottle;

import com.google.common.collect.Queues;

public class UnlimitedRawBatchBuffer implements RawBatchBuffer{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnlimitedRawBatchBuffer.class);

  private final LinkedBlockingDeque<RawFragmentBatch> buffer = Queues.newLinkedBlockingDeque();
  private volatile boolean finished = false;

  public UnlimitedRawBatchBuffer(FragmentContext context) {

  }
  
  @Override
  public void enqueue(ConnectionThrottle throttle, RawFragmentBatch batch) {
    buffer.add(batch);
  }

//  @Override
//  public RawFragmentBatch dequeue() {
//    return buffer.poll();
//  }

  @Override
  public void kill(FragmentContext context) {
    // TODO: Pass back or kill handler?
  }

  
  @Override
  public void finished() {
    finished = true;
  }

  @Override
  public RawFragmentBatch getNext(){
    
    RawFragmentBatch b = buffer.poll();
    if(b == null && !finished){
      try {
        return buffer.take();
      } catch (InterruptedException e) {
        return null;
      }
    }
    
    return b;
    
  }

  
}
