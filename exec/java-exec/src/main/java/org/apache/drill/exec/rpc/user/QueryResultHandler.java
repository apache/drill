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
package org.apache.drill.exec.rpc.user;

import io.netty.buffer.ByteBuf;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserProtos.QueryResult;
import org.apache.drill.exec.proto.UserProtos.QueryResult.QueryState;
import org.apache.drill.exec.rpc.BaseRpcOutcomeListener;
import org.apache.drill.exec.rpc.RpcBus;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

/**
 * Encapsulates the future management of query submissions. This entails a potential race condition. Normal ordering is:
 * 1. Submit query to be executed. 2. Receive QueryHandle for buffer management 3. Start receiving results batches for
 * query.
 * 
 * However, 3 could potentially occur before 2. As such, we need to handle this case and then do a switcheroo.
 * 
 */
public class QueryResultHandler {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryResultHandler.class);

  private ConcurrentMap<QueryId, UserResultsListener> resultsListener = Maps.newConcurrentMap();

  
  public RpcOutcomeListener<QueryId> getWrappedListener(UserResultsListener listener){
    return new SubmissionListener(listener);
  }
  
  public void batchArrived(ConnectionThrottle throttle, ByteBuf pBody, ByteBuf dBody) throws RpcException {
    final QueryResult result = RpcBus.get(pBody, QueryResult.PARSER);
    final QueryResultBatch batch = new QueryResultBatch(result, dBody);
    UserResultsListener l = resultsListener.get(result.getQueryId());
    
    boolean failed = batch.getHeader().getQueryState() == QueryState.FAILED;
    // logger.debug("For QueryId [{}], retrieved result listener {}", result.getQueryId(), l);
    if (l == null) {
      BufferingListener bl = new BufferingListener();
      l = resultsListener.putIfAbsent(result.getQueryId(), bl);
      // if we had a succesful insert, use that reference.  Otherwise, just throw away the new bufering listener.
      if (l == null) l = bl;
      if (result.getQueryId().toString().equals("")) {
        failAll();
      }
    }
      
    if(failed){
      l.submissionFailed(new RpcException("Remote failure while running query." + batch.getHeader().getErrorList()));
      resultsListener.remove(result.getQueryId(), l);
    }else{
      l.resultArrived(batch, throttle);
    }
    
    if (
        (failed || result.getIsLastChunk())
        && 
        (!(l instanceof BufferingListener) || ((BufferingListener)l).output != null)
        ) {
      resultsListener.remove(result.getQueryId(), l);
    }


  }

  private void failAll() {
    for (UserResultsListener l : resultsListener.values()) {
      l.submissionFailed(new RpcException("Received result without QueryId"));
    }
  }

  
  
  private class BufferingListener implements UserResultsListener {

    private ConcurrentLinkedQueue<QueryResultBatch> results = Queues.newConcurrentLinkedQueue();
    private volatile boolean finished = false;
    private volatile RpcException ex;
    private volatile UserResultsListener output;
    private volatile ConnectionThrottle throttle;
    public boolean transferTo(UserResultsListener l) {
      synchronized (this) {
        output = l;
        boolean last = false;
        for (QueryResultBatch r : results) {
          l.resultArrived(r, throttle);
          last = r.getHeader().getIsLastChunk();
        }
        if(ex != null){
          l.submissionFailed(ex);
          return true;
        }
        return last;
      }
    }

    
    @Override
    public void resultArrived(QueryResultBatch result, ConnectionThrottle throttle) {
      this.throttle = throttle;
      if(result.getHeader().getIsLastChunk()) finished = true;
      
      synchronized (this) {
        if (output == null) {
          this.results.add(result);
        } else {
          output.resultArrived(result, throttle);
        }
      }
    }

    @Override
    public void submissionFailed(RpcException ex) {
      finished = true;
      synchronized (this) {
        if (output == null){
          this.ex = ex;
        } else{
          output.submissionFailed(ex);
        }
      }
    }
    
    public boolean isFinished(){
      return finished;
    }


    @Override
    public void queryIdArrived(QueryId queryId) {
    }

  }

  private class SubmissionListener extends BaseRpcOutcomeListener<QueryId> {
    private UserResultsListener listener;

    public SubmissionListener(UserResultsListener listener) {
      super();
      this.listener = listener;
    }

    @Override
    public void failed(RpcException ex) {
      listener.submissionFailed(ex);
    }

    @Override
    public void success(QueryId queryId, ByteBuf buf) {
      listener.queryIdArrived(queryId);
      logger.debug("Received QueryId {} succesfully.  Adding listener {}", queryId, listener);
      UserResultsListener oldListener = resultsListener.putIfAbsent(queryId, listener);

      // we need to deal with the situation where we already received results by the time we got the query id back. In
      // that case, we'll need to transfer the buffering listener over, grabbing a lock against reception of additional
      // results during the transition
      if (oldListener != null) {
        logger.debug("Unable to place user results listener, buffering listener was already in place.");
        if (oldListener instanceof BufferingListener) {
          resultsListener.remove(oldListener);
          boolean all = ((BufferingListener) oldListener).transferTo(this.listener);
          // simply remove the buffering listener if we already have the last response.
          if (all) {
            resultsListener.remove(oldListener);
          } else {
            boolean replaced = resultsListener.replace(queryId, oldListener, listener);
            if (!replaced) throw new IllegalStateException();
          }
        } else {
          throw new IllegalStateException("Trying to replace a non-buffering User Results listener.");
        }
      }

    }

  }
}
