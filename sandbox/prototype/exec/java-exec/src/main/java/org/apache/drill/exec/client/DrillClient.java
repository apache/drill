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
package org.apache.drill.exec.client;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.drill.exec.proto.UserProtos.QueryResultsMode.STREAM_FULL;
import static org.apache.drill.exec.proto.UserProtos.RunQuery.newBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocatorL;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Vector;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.ZKClusterCoordinator;
import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserProtos.QueryType;
import org.apache.drill.exec.rpc.BasicClientWithConnection.ServerConnection;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.NamedThreadFactory;
import org.apache.drill.exec.rpc.RpcConnectionHandler;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserClient;
import org.apache.drill.exec.rpc.user.UserResultsListener;

import com.google.common.util.concurrent.AbstractCheckedFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Thin wrapper around a UserClient that handles connect/close and transforms String into ByteBuf
 */
public class DrillClient implements Closeable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillClient.class);
  
  DrillConfig config;
  private UserClient client;
  private volatile ClusterCoordinator clusterCoordinator;
  private volatile boolean connected = false;
  private final DirectBufferAllocator allocator = new DirectBufferAllocator();
  
  public DrillClient() {
    this(DrillConfig.create());
  }

  public DrillClient(String fileName) {
    this(DrillConfig.create(fileName));
  }

  public DrillClient(DrillConfig config) {
    this(config, null);
  }
  
  public DrillClient(DrillConfig config, ClusterCoordinator coordinator){
    this.config = config;
    this.clusterCoordinator = coordinator;
  }
  
  public DrillConfig getConfig(){
    return config;
  }
  
  /**
   * Connects the client to a Drillbit server
   *
   * @throws IOException
   */
  public synchronized void connect() throws RpcException {
    if(connected) return;
    
    if(clusterCoordinator == null){
      try {
        this.clusterCoordinator = new ZKClusterCoordinator(this.config);
        this.clusterCoordinator.start(10000);
      } catch (Exception e) {
        throw new RpcException("Failure setting up ZK for client.", e);
      }
      
    }
    
    Collection<DrillbitEndpoint> endpoints = clusterCoordinator.getAvailableEndpoints();
    checkState(!endpoints.isEmpty(), "No DrillbitEndpoint can be found");
    // just use the first endpoint for now
    DrillbitEndpoint endpoint = endpoints.iterator().next();
    this.client = new UserClient(allocator.getUnderlyingAllocator(), new NioEventLoopGroup(1, new NamedThreadFactory("Client-")));
    try {
      logger.debug("Connecting to server {}:{}", endpoint.getAddress(), endpoint.getUserPort());
      FutureHandler f = new FutureHandler();
      this.client.connect(f, endpoint);
      f.checkedGet();
      connected = true;
    } catch (InterruptedException e) {
      throw new RpcException(e);
    }
  }

  
  
  public DirectBufferAllocator getAllocator() {
    return allocator;
  }

  /**
   * Closes this client's connection to the server
   */
  public void close(){
    this.client.close();
    connected = false;
  }

  /**
   * Submits a Logical plan for direct execution (bypasses parsing)
   *
   * @param plan the plan to execute
   * @return a handle for the query result
   * @throws RpcException
   */
  public List<QueryResultBatch> runQuery(QueryType type, String plan) throws RpcException {
    ListHoldingResultsListener listener = new ListHoldingResultsListener();
    client.submitQuery(listener, newBuilder().setResultsMode(STREAM_FULL).setType(type).setPlan(plan).build());
    return listener.getResults();
  }
  
  /**
   * Submits a Logical plan for direct execution (bypasses parsing)
   *
   * @param plan the plan to execute
   * @return a handle for the query result
   * @throws RpcException
   */
  public void runQuery(QueryType type, String plan, UserResultsListener resultsListener){
    client.submitQuery(resultsListener, newBuilder().setResultsMode(STREAM_FULL).setType(type).setPlan(plan).build());
  }
  
  private class ListHoldingResultsListener implements UserResultsListener {
    private Vector<QueryResultBatch> results = new Vector<QueryResultBatch>();
    private SettableFuture<List<QueryResultBatch>> future = SettableFuture.create();
    
    @Override
    public void submissionFailed(RpcException ex) {
      logger.debug("Submission failed.", ex);
      future.setException(ex);
    }

    @Override
    public void resultArrived(QueryResultBatch result) {
      logger.debug("Result arrived.  Is Last Chunk: {}.  Full Result: {}", result.getHeader().getIsLastChunk(), result);
      results.add(result);
      if(result.getHeader().getIsLastChunk()){
        future.set(results);
      }
    }
  
    public List<QueryResultBatch> getResults() throws RpcException{
      try{
        return future.get();
      }catch(Throwable t){
        throw RpcException.mapException(t);
      }
    }
  }
  
  private class FutureHandler extends AbstractCheckedFuture<Void, RpcException> implements RpcConnectionHandler<ServerConnection>, DrillRpcFuture<Void>{

    protected FutureHandler() {
      super( SettableFuture.<Void>create());
    }

    @Override
    public void connectionSucceeded(ServerConnection connection) {
      getInner().set(null);
    }

    @Override
    public void connectionFailed(FailureType type, Throwable t) {
      getInner().setException(new RpcException(String.format("Failure connecting to server. Failure of type %s.", type.name()), t));
    }

    private SettableFuture<Void> getInner(){
      return (SettableFuture<Void>) delegate();
    }
    
    @Override
    protected RpcException mapException(Exception e) {
      return RpcException.mapException(e);
    }

    @Override
    public ByteBuf getBuffer() {
      return null;
    }
    
  }

}
