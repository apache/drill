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
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoopGroup;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserProtos.BitToUserHandshake;
import org.apache.drill.exec.proto.UserProtos.QueryResult;
import org.apache.drill.exec.proto.UserProtos.RpcType;
import org.apache.drill.exec.proto.UserProtos.RunQuery;
import org.apache.drill.exec.proto.UserProtos.UserToBitHandshake;
import org.apache.drill.exec.rpc.BasicClientWithConnection;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.protobuf.MessageLite;

public class UserClient extends BasicClientWithConnection<RpcType> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserClient.class);

  private ConcurrentMap<QueryId, UserResultsListener> resultsListener = Maps.newConcurrentMap();

  public UserClient(ByteBufAllocator alloc, EventLoopGroup eventLoopGroup) {
    super(UserRpcConfig.MAPPING, alloc, eventLoopGroup);
  }

  public Future<Void> submitQuery(RunQuery query, UserResultsListener resultsListener) throws RpcException {
    this.send(RpcType.RUN_QUERY, query, QueryId.class).addLightListener(new SubmissionListener(resultsListener));
    return resultsListener.getFuture();
  }

  public BitToUserHandshake connect(DrillbitEndpoint endpoint) throws RpcException, InterruptedException{
    return this.connectAsClient(RpcType.HANDSHAKE, UserToBitHandshake.newBuilder().setRpcVersion(UserRpcConfig.RPC_VERSION).build(), endpoint.getAddress(), endpoint.getUserPort(), BitToUserHandshake.class);
  }
  
  private class BufferingListener extends UserResultsListener {

    private ConcurrentLinkedQueue<QueryResultBatch> results = Queues.newConcurrentLinkedQueue();
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile UserResultsListener output;

    public boolean transferTo(UserResultsListener l) {
      lock.writeLock().lock();
      output = l;
      boolean last = false;
      for (QueryResultBatch r : results) {
        l.resultArrived(r);
        last = r.getHeader().getIsLastChunk();
      }
      if (future.isDone()) {
        l.set();
      }
      return last;
    }

    @Override
    public void resultArrived(QueryResultBatch result) {
      logger.debug("Result arrvied.");
      lock.readLock().lock();
      try {
        if (output == null) {
          this.results.add(result);
        } else {
          output.resultArrived(result);
        }

      } finally {
        lock.readLock().unlock();
      }

    }

    @Override
    public void submissionFailed(RpcException ex) {
      throw new UnsupportedOperationException("You cannot report failed submissions to a buffering listener.");
    }

  }

  private class SubmissionListener extends RpcOutcomeListener<QueryId> {
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
    public void success(QueryId queryId) {
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

  @Override
  protected MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
    switch (rpcType) {
    case RpcType.ACK_VALUE:
      return Ack.getDefaultInstance();
    case RpcType.HANDSHAKE_VALUE:
      return BitToUserHandshake.getDefaultInstance();
    case RpcType.QUERY_HANDLE_VALUE:
      return QueryId.getDefaultInstance();
    case RpcType.QUERY_RESULT_VALUE:
      return QueryResult.getDefaultInstance();
    }
    throw new RpcException(String.format("Unable to deal with RpcType of %d", rpcType));
  }

  protected Response handle(int rpcType, ByteBuf pBody, ByteBuf dBody) throws RpcException {
    switch (rpcType) {
    case RpcType.QUERY_RESULT_VALUE:
      final QueryResult result = get(pBody, QueryResult.PARSER);
      final QueryResultBatch batch = new QueryResultBatch(result, dBody);
      UserResultsListener l = resultsListener.get(result.getQueryId());
//      logger.debug("For QueryId [{}], retrieved result listener {}", result.getQueryId(), l);
      if (l != null) {
//        logger.debug("Results listener available, using existing.");
        l.resultArrived(batch);
        if (result.getIsLastChunk()) {
          resultsListener.remove(result.getQueryId(), l);
          l.set();
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

      return new Response(RpcType.ACK, Ack.getDefaultInstance());
    default:
      throw new RpcException(String.format("Unknown Rpc Type %d. ", rpcType));
    }

  }

  @Override
  protected ClientHandshakeHandler<BitToUserHandshake> getHandshakeHandler() {
    return new ClientHandshakeHandler<BitToUserHandshake>(RpcType.HANDSHAKE, BitToUserHandshake.class, BitToUserHandshake.PARSER) {

      @Override
      protected void validateHandshake(BitToUserHandshake inbound) throws Exception {
        logger.debug("Handling handshake from bit to user. {}", inbound);
        if (inbound.getRpcVersion() != UserRpcConfig.RPC_VERSION)
          throw new RpcException(String.format("Invalid rpc version.  Expected %d, actual %d.",
              inbound.getRpcVersion(), UserRpcConfig.RPC_VERSION));
      }

    };
  }

}
