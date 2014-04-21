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
package org.apache.drill.exec.client;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.drill.exec.proto.UserProtos.QueryResultsMode.STREAM_FULL;
import static org.apache.drill.exec.proto.UserProtos.RunQuery.newBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.ZKClusterCoordinator;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.proto.UserProtos.Property;
import org.apache.drill.exec.proto.UserProtos.QueryType;
import org.apache.drill.exec.proto.UserProtos.RpcType;
import org.apache.drill.exec.proto.UserProtos.UserProperties;
import org.apache.drill.exec.rpc.BasicClientWithConnection.ServerConnection;
import org.apache.drill.exec.rpc.ChannelClosedException;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.NamedThreadFactory;
import org.apache.drill.exec.rpc.RpcConnectionHandler;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserClient;
import org.apache.drill.exec.rpc.user.UserResultsListener;

import com.google.common.util.concurrent.AbstractCheckedFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Thin wrapper around a UserClient that handles connect/close and transforms String into ByteBuf
 */
public class DrillClient implements Closeable, ConnectionThrottle{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillClient.class);

  DrillConfig config;
  private UserClient client;
  private UserProperties props = null;
  private volatile ClusterCoordinator clusterCoordinator;
  private volatile boolean connected = false;
  private final TopLevelAllocator allocator = new TopLevelAllocator(Long.MAX_VALUE);
  private int reconnectTimes;
  private int reconnectDelay;
  private final boolean ownsZkConnection;

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
    this.ownsZkConnection = coordinator == null;
    this.config = config;
    this.clusterCoordinator = coordinator;
    this.reconnectTimes = config.getInt(ExecConstants.BIT_RETRY_TIMES);
    this.reconnectDelay = config.getInt(ExecConstants.BIT_RETRY_DELAY);
  }

  public DrillConfig getConfig(){
    return config;
  }

  @Override
  public void setAutoRead(boolean enableAutoRead) {
    client.setAutoRead(enableAutoRead);
  }



  /**
   * Connects the client to a Drillbit server
   *
   * @throws IOException
   */
  public void connect() throws RpcException {
    connect(null, new Properties());
  }

  public void connect(Properties props) throws RpcException {
    connect(null, props);
  }

  public synchronized void connect(String connect, Properties props) throws RpcException {
    if (connected) return;

    if (ownsZkConnection) {
      try {
        this.clusterCoordinator = new ZKClusterCoordinator(this.config, connect);
        this.clusterCoordinator.start(10000);
      } catch (Exception e) {
        throw new RpcException("Failure setting up ZK for client.", e);
      }
    }

    if (props != null) {
      UserProperties.Builder upBuilder = UserProperties.newBuilder();
      for(String key : props.stringPropertyNames())
        upBuilder.addProperties(Property.newBuilder().setKey(key).setValue(props.getProperty(key)));

      this.props = upBuilder.build();
    }

    Collection<DrillbitEndpoint> endpoints = clusterCoordinator.getAvailableEndpoints();
    checkState(!endpoints.isEmpty(), "No DrillbitEndpoint can be found");
    // just use the first endpoint for now
    DrillbitEndpoint endpoint = endpoints.iterator().next();
    this.client = new UserClient(allocator, new NioEventLoopGroup(config.getInt(ExecConstants.CLIENT_RPC_THREADS), new NamedThreadFactory("Client-")));
    logger.debug("Connecting to server {}:{}", endpoint.getAddress(), endpoint.getUserPort());
    connect(endpoint);
    connected = true;
  }


  public synchronized boolean reconnect() {
    if (client.isActive()) {
      return true;
    }
    int retry = reconnectTimes;
    while (retry > 0) {
      retry--;
      try {
        Thread.sleep(this.reconnectDelay);
        Collection<DrillbitEndpoint> endpoints = clusterCoordinator.getAvailableEndpoints();
        if (endpoints.isEmpty()) {
          continue;
        }
        client.close();
        connect(endpoints.iterator().next());
        return true;
      } catch (Exception e) {
      }
    }
    return false;
  }

  private void connect(DrillbitEndpoint endpoint) throws RpcException {
    FutureHandler f = new FutureHandler();
    try {
      client.connect(f, endpoint, props);
      f.checkedGet();
    } catch (InterruptedException e) {
      throw new RpcException(e);
    }
  }

  public TopLevelAllocator getAllocator() {
    return allocator;
  }

  /**
   * Closes this client's connection to the server
   */
  public void close(){
    this.client.close();
    if(ownsZkConnection){
      try {
        this.clusterCoordinator.close();
      } catch (IOException e) {
        logger.warn("Error while closing Cluster Coordinator.", e);
      }
    }

    // TODO: fix tests that fail when this is called.
    //allocator.close();
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
    UserProtos.RunQuery query = newBuilder().setResultsMode(STREAM_FULL).setType(type).setPlan(plan).build() ;
    ListHoldingResultsListener listener = new ListHoldingResultsListener(query);
    client.submitQuery(listener,query);
    return listener.getResults();
  }

  public void cancelQuery(QueryId id){
    client.send(RpcType.CANCEL_QUERY, id, Ack.class);
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
    private UserProtos.RunQuery query ;

    public ListHoldingResultsListener(UserProtos.RunQuery query) {
      this.query = query;
    }

    @Override
    public void submissionFailed(RpcException ex) {
      // or  !client.isActive()
      if (ex instanceof ChannelClosedException) {
        if (reconnect()) {
          try {
            client.submitQuery(this, query);
          } catch (Exception e) {
            fail(e);
          }
        } else {
          fail(ex);
        }
      } else {
        fail(ex);
      }
    }

    private void fail(Exception ex) {
      logger.debug("Submission failed.", ex);
      future.setException(ex);
      future.set(results);
    }

    @Override
    public void resultArrived(QueryResultBatch result, ConnectionThrottle throttle) {
//      logger.debug("Result arrived.  Is Last Chunk: {}.  Full Result: {}", result.getHeader().getIsLastChunk(), result);
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

    @Override
    public void queryIdArrived(QueryId queryId) {
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
