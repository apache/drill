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
import io.netty.buffer.DrillBuf;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import io.netty.channel.EventLoopGroup;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.zk.ZKClusterCoordinator;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.proto.UserProtos.Property;
import org.apache.drill.exec.proto.UserProtos.RpcType;
import org.apache.drill.exec.proto.UserProtos.UserProperties;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.rpc.BasicClientWithConnection.ServerConnection;
import org.apache.drill.exec.rpc.ChannelClosedException;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.RpcConnectionHandler;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.TransportCheck;
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
  private final BufferAllocator allocator;
  private int reconnectTimes;
  private int reconnectDelay;
  private boolean supportComplexTypes;
  private final boolean ownsZkConnection;
  private final boolean ownsAllocator;
  private EventLoopGroup eventLoopGroup;

  public DrillClient() {
    this(DrillConfig.create());
  }

  public DrillClient(String fileName) {
    this(DrillConfig.create(fileName));
  }

  public DrillClient(DrillConfig config) {
    this(config, null);
  }

  public DrillClient(DrillConfig config, ClusterCoordinator coordinator) {
    this(config, coordinator, null);
  }

  public DrillClient(DrillConfig config, ClusterCoordinator coordinator, BufferAllocator allocator) {
    this.ownsZkConnection = coordinator == null;
    this.ownsAllocator = allocator == null;
    this.allocator = allocator == null ? new TopLevelAllocator(config) : allocator;
    this.config = config;
    this.clusterCoordinator = coordinator;
    this.reconnectTimes = config.getInt(ExecConstants.BIT_RETRY_TIMES);
    this.reconnectDelay = config.getInt(ExecConstants.BIT_RETRY_DELAY);
    this.supportComplexTypes = config.getBoolean(ExecConstants.CLIENT_SUPPORT_COMPLEX_TYPES);
  }

  public DrillConfig getConfig() {
    return config;
  }

  @Override
  public void setAutoRead(boolean enableAutoRead) {
    client.setAutoRead(enableAutoRead);
  }

  /**
   * Sets whether the application is willing to accept complex types (Map, Arrays) in the returned result set.
   * Default is {@code true}. If set to {@code false}, the complex types are returned as JSON encoded VARCHAR type.
   *
   * @throws IllegalStateException if called after a connection has been established.
   */
  public void setSupportComplexTypes(boolean supportComplexTypes) {
    if (connected) {
      throw new IllegalStateException("Attempted to modify client connection property after connection has been established.");
    }
    this.supportComplexTypes = supportComplexTypes;
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
    if (connected) {
      return;
    }

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
      for (String key : props.stringPropertyNames()) {
        upBuilder.addProperties(Property.newBuilder().setKey(key).setValue(props.getProperty(key)));
      }

      this.props = upBuilder.build();
    }

    Collection<DrillbitEndpoint> endpoints = clusterCoordinator.getAvailableEndpoints();
    checkState(!endpoints.isEmpty(), "No DrillbitEndpoint can be found");
    // just use the first endpoint for now
    DrillbitEndpoint endpoint = endpoints.iterator().next();

    eventLoopGroup = createEventLoop(config.getInt(ExecConstants.CLIENT_RPC_THREADS), "Client-");
    client = new UserClient(supportComplexTypes, allocator, eventLoopGroup);
    logger.debug("Connecting to server {}:{}", endpoint.getAddress(), endpoint.getUserPort());
    connect(endpoint);
    connected = true;
  }

  protected EventLoopGroup createEventLoop(int size, String prefix) {
    return TransportCheck.createEventLoopGroup(size, prefix);
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
      client.connect(f, endpoint, props, getUserCredentials());
      f.checkedGet();
    } catch (InterruptedException e) {
      throw new RpcException(e);
    }
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  /**
   * Closes this client's connection to the server
   */
  public void close() {
    if (this.client != null) {
      this.client.close();
    }
    if (this.ownsAllocator && allocator != null) {
      allocator.close();
    }
    if(ownsZkConnection) {
      try {
        this.clusterCoordinator.close();
      } catch (IOException e) {
        logger.warn("Error while closing Cluster Coordinator.", e);
      }
    }
    if (eventLoopGroup != null) {
      eventLoopGroup.shutdownGracefully();
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


  /*
   * Helper method to generate the UserCredentials message from the properties.
   */
  private UserBitShared.UserCredentials getUserCredentials() {
    // If username is not propagated as one of the properties
    String userName = "anonymous";

    if (props != null) {
      for (Property property: props.getPropertiesList()) {
        if (property.getKey().equalsIgnoreCase("user")) {
          userName = property.getValue();
          break;
        }
      }
    }

    return UserBitShared.UserCredentials.newBuilder().setUserName(userName).build();
  }

  public DrillRpcFuture<Ack> cancelQuery(QueryId id) {
    logger.debug("Cancelling query {}", QueryIdHelper.getQueryId(id));
    return client.send(RpcType.CANCEL_QUERY, id, Ack.class);
  }


  /**
   * Submits a Logical plan for direct execution (bypasses parsing)
   *
   * @param plan the plan to execute
   * @return a handle for the query result
   * @throws RpcException
   */
  public void runQuery(QueryType type, String plan, UserResultsListener resultsListener) {
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
      if (result.getHeader().getIsLastChunk()) {
        future.set(results);
      }
    }

    public List<QueryResultBatch> getResults() throws RpcException{
      try {
        return future.get();
      } catch (Throwable t) {
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

    private SettableFuture<Void> getInner() {
      return (SettableFuture<Void>) delegate();
    }

    @Override
    protected RpcException mapException(Exception e) {
      return RpcException.mapException(e);
    }

    @Override
    public DrillBuf getBuffer() {
      return null;
    }

  }
}
