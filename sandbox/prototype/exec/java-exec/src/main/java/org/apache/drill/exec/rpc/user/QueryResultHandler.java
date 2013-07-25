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
package org.apache.drill.exec.rpc.user;

import io.netty.buffer.ByteBuf;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserProtos.QueryResult;
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
  
  public void batchArrived(ByteBuf pBody, ByteBuf dBody) throws RpcException {
    final QueryResult result = RpcBus.get(pBody, QueryResult.PARSER);
    final QueryResultBatch batch = new QueryResultBatch(result, dBody);
    UserResultsListener l = resultsListener.get(result.getQueryId());
    // logger.debug("For QueryId [{}], retrieved result listener {}", result.getQueryId(), l);
    if (l != null) {
      // logger.debug("Results listener available, using existing.");
      l.resultArrived(batch);
      if (result.getIsLastChunk()) {
        resultsListener.remove(result.getQueryId(), l);
      }
    } else {
      logger.debug("Results listener not available, creating a buffering listener.");
      // manage race condition where we start getting results before we receive the queryid back.
      BufferingListener bl = new BufferingListener();
      l = resultsListener.putIfAbsent(result.getQueryId(), bl);
      if (l != null) {
        l.resultArrived(batch);
      } else {
        bl.resultArrived(batch);
      }
    }
  }

  private class BufferingListener implements UserResultsListener {

    private ConcurrentLinkedQueue<QueryResultBatch> results = Queues.newConcurrentLinkedQueue();
    private volatile UserResultsListener output;

    public boolean transferTo(UserResultsListener l) {
      synchronized (this) {
        output = l;
        boolean last = false;
        for (QueryResultBatch r : results) {
          l.resultArrived(r);
          last = r.getHeader().getIsLastChunk();
        }
        return last;
      }
    }

    @Override
    public void resultArrived(QueryResultBatch result) {
      synchronized (this) {
        if (output == null) {
          this.results.add(result);
        } else {
          output.resultArrived(result);
        }
      }
    }

    @Override
    public void submissionFailed(RpcException ex) {
      throw new UnsupportedOperationException("You cannot report failed submissions to a buffering listener.");
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
