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
package org.apache.drill.sql.client.full;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;

public class BatchListener implements UserResultsListener {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BatchListener.class);

  private volatile RpcException ex;
  private volatile boolean completed = false;
  
  final BlockingQueue<QueryResultBatch> queue = new ArrayBlockingQueue<>(100);

  @Override
  public void submissionFailed(RpcException ex) {
    this.ex = ex;
    completed = true;
    System.out.println("Query failed: " + ex);
  }

  @Override
  public void resultArrived(QueryResultBatch result) {
    logger.debug("Result arrived {}", result);
    queue.add(result);
    if(result.getHeader().getIsLastChunk()){
      completed = true;
    }
    if (result.getHeader().getErrorCount() > 0) {
      submissionFailed(new RpcException(String.format("%s", result.getHeader().getErrorList())));
    }
  }

  public boolean completed(){
    return completed;
  }

  public QueryResultBatch getNext() throws RpcException, InterruptedException{
    while(true){
      if(ex != null) throw ex;
      if(completed && queue.isEmpty()){
        return null;
      }else{
        QueryResultBatch q = queue.poll(50, TimeUnit.MILLISECONDS);
        if(q != null) return q;
      }
      
    }
  }

  @Override
  public void queryIdArrived(QueryId queryId) {
  }

  
}
