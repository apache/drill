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

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RawFragmentBatch;
import org.apache.drill.exec.rpc.RemoteConnection;

import com.google.common.collect.Queues;

public class UnlimitedRawBatchBuffer implements RawBatchBuffer{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnlimitedRawBatchBuffer.class);

  private final LinkedBlockingDeque<RawFragmentBatch> buffer;
  private volatile boolean finished = false;
  private int softlimit;
  private int startlimit;
  private AtomicBoolean overlimit = new AtomicBoolean(false);

  public UnlimitedRawBatchBuffer(FragmentContext context) {
    softlimit = context.getConfig().getInt(ExecConstants.INCOMING_BUFFER_SIZE);
    startlimit = softlimit/2;
    buffer = Queues.newLinkedBlockingDeque();
  }
  
  @Override
  public void enqueue(RawFragmentBatch batch) {
    buffer.add(batch);
    if(buffer.size() == softlimit){
      overlimit.set(true);
      batch.getConnection().setAutoRead(false);
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
  }

  @Override
  public RawFragmentBatch getNext(){
    
    RawFragmentBatch b = null;
    
    b = buffer.poll();
    
    // if we didn't get a buffer, block on waiting for buffer.
    if(b == null && !finished){
      try {
        b = buffer.take();
      } catch (InterruptedException e) {
        return null;
      }
    }
    
    // if we are in the overlimit condition and aren't finished, check if we've passed the start limit.  If so, turn off the overlimit condition and set auto read to true (start reading from socket again).
    if(!finished && overlimit.get()){
      if(buffer.size() == startlimit){
        overlimit.set(false);
        b.getConnection().setAutoRead(true);
      }
    }
    
    return b;
    
  }

  
}
