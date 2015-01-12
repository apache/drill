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

import java.util.Queue;

import org.apache.drill.exec.rpc.ResponseSender;
import org.apache.drill.exec.rpc.data.DataRpcConfig;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Queues;

public class ResponseSenderQueue {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ResponseSenderQueue.class);

  private Queue<ResponseSender> q = Queues.newConcurrentLinkedQueue();

  public void enqueueResponse(ResponseSender sender){
    q.add(sender);
  }

  public void flushResponses(){
    flushResponses(Integer.MAX_VALUE);
  }

  /**
   * Flush only up to a count responses
   * @param count
   * @return
   */
  public int flushResponses(int count){
    logger.debug("queue.size: {}, count: {}", q.size(), count);
    int i = 0;
    while(!q.isEmpty() && i < count){
      ResponseSender s = q.poll();
      if(s != null){
        s.send(DataRpcConfig.OK);
      }
      i++;
    }
    return i;
  }

  @VisibleForTesting
  boolean isEmpty() {
    return q.isEmpty();
  }
}
