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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import io.netty.channel.EventLoopGroup;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.zk.ZKClusterCoordinator;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
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
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.user.UserClient;
import org.apache.drill.exec.rpc.user.UserResultsListener;

import com.google.common.util.concurrent.AbstractCheckedFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Thin wrapper around a UserClient that handles connect/close and transforms
 * String into ByteBuf.
 */
public class DrillClient implements Closeable, ConnectionThrottle {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillClient.class);

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
  private final boolean isDirectConnection; // true if the connection bypasses zookeeper and connects directly to a drillbit
  private EventLoopGroup eventLoopGroup;

  public DrillClient() {
    this(DrillConfig.create(), false);
  }

  public DrillClient(boolean isDirect) {
    this(DrillConfig.create(), isDirect);
  }

  public DrillClient(String fileName) {
    this(DrillConfig.create(fileName), false);
  }

  public DrillClient(DrillConfig config) {
    this(config, null, false);
  }

  public DrillClient(DrillConfig config, boolean isDirect) {
    this(config, null, isDirect);
  }

  public DrillClient(DrillConfig config, ClusterCoordinator coordinator) {
    this(config, coordinator, null, false);
  }

  public DrillClient(DrillConfig config, ClusterCoordinator coordinator, boolean isDirect) {
    this(config, coordinator, null, isDirect);
  }

  public DrillClient(DrillConfig config, ClusterCoordinator coordinator, BufferAllocator allocator) {
    this(config, coordinator, allocator, false);
  }

  public DrillClient(DrillConfig config, ClusterCoordinator coordinator, BufferAllocator allocator, boolean isDirect) {
    // if isDirect is true, the client will connect directly to the drillbit instead of
    // going thru the zookeeper
    this.isDirectConnection = isDirect;
    this.ownsZkConnection = coordinator == null;
    this.ownsAllocator = allocator == null;
    this.allocator = ownsAllocator ? new TopLevelAllocator(config) : allocator;
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
   * @throws RpcException
   */
  public void connect() throws RpcException {
    connect(null, null);
  }

  public void connect(Properties props) throws RpcException {
    connect(null, props);
  }

  public synchronized void connect(String connect, Properties props) throws RpcException {
    if (connected) {
      return;
    }

    final DrillbitEndpoint endpoint;
    if (isDirectConnection) {
      String[] connectInfo = props.getProperty("drillbit").split(":");
      endpoint = DrillbitEndpoint.newBuilder()
              .setAddress(connectInfo[0])
              .setUserPort(Integer.parseInt(connectInfo[1]))
              .build();
    } else {
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

      ArrayList<DrillbitEndpoint> endpoints = new ArrayList<>(clusterCoordinator.getAvailableEndpoints());
      checkState(!endpoints.isEmpty(), "No DrillbitEndpoint can be found");
      // shuffle the collection then get the first endpoint
      Collections.shuffle(endpoints);
      endpoint = endpoints.iterator().next();
    }

    eventLoopGroup = createEventLoop(config.getInt(ExecConstants.CLIENT_RPC_THREADS), "Client-");
    client = new UserClient(supportComplexTypes, allocator, eventLoopGroup);
    logger.debug("Connecting to server {}:{}", endpoint.getAddress(), endpoint.getUserPort());
    connect(endpoint);
    connected = true;
  }

  protected static EventLoopGroup createEventLoop(int size, String prefix) {
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
        ArrayList<DrillbitEndpoint> endpoints = new ArrayList<>(clusterCoordinator.getAvailableEndpoints());
        if (endpoints.isEmpty()) {
          continue;
        }
        client.close();
        Collections.shuffle(endpoints);
        connect(endpoints.iterator().next());
        return true;
      } catch (Exception e) {
      }
    }
    return false;
  }

  private void connect(DrillbitEndpoint endpoint) throws RpcException {
    FutureHandler f = new FutureHandler();
    client.connect(f, endpoint, props, getUserCredentials());
    f.checkedGet();
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  /**
   * Closes this client's connection to the server
   */
  @Override
  public void close() {
    if (this.client != null) {
      this.client.close();
    }
    if (this.ownsAllocator && allocator != null) {
      allocator.close();
    }
    if (ownsZkConnection) {
      try {
        this.clusterCoordinator.close();
      } catch (IOException e) {
        logger.warn("Error while closing Cluster Coordinator.", e);
      }
    }
    if (eventLoopGroup != null) {
      eventLoopGroup.shutdownGracefully();
    }

    // TODO:  Did DRILL-1735 changes cover this TODO?:
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
  public List<QueryDataBatch> runQuery(QueryType type, String plan) throws RpcException {
    UserProtos.RunQuery query = newBuilder().setResultsMode(STREAM_FULL).setType(type).setPlan(plan).build();
    ListHoldingResultsListener listener = new ListHoldingResultsListener(query);
    client.submitQuery(listener, query);
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
   * @param  plan  the plan to execute
   * @return a handle for the query result
   * @throws RpcException
   */
  public void runQuery(QueryType type, String plan, UserResultsListener resultsListener) {
    client.submitQuery(resultsListener, newBuilder().setResultsMode(STREAM_FULL).setType(type).setPlan(plan).build());
  }

  private class ListHoldingResultsListener implements UserResultsListener {
    private Vector<QueryDataBatch> results = new Vector<>();
    private SettableFuture<List<QueryDataBatch>> future = SettableFuture.create();
    private UserProtos.RunQuery query ;

    public ListHoldingResultsListener(UserProtos.RunQuery query) {
      logger.debug( "Listener created for query \"\"\"{}\"\"\"", query );
      this.query = query;
    }

    @Override
    public void submissionFailed(UserException ex) {
      // or  !client.isActive()
      if (ex.getCause() instanceof ChannelClosedException) {
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

    @Override
    public void queryCompleted(QueryState state) {
      future.set(results);
    }

    private void fail(Exception ex) {
      logger.debug("Submission failed.", ex);
      future.setException(ex);
      future.set(results);
    }

    @Override
    public void dataArrived(QueryDataBatch result, ConnectionThrottle throttle) {
      logger.debug("Result arrived:  Result: {}", result );
      results.add(result);
    }

    public List<QueryDataBatch> getResults() throws RpcException{
      try {
        return future.get();
      } catch (Throwable t) {
        throw RpcException.mapException(t);
      }
    }

    @Override
    public void queryIdArrived(QueryId queryId) {
      if (logger.isDebugEnabled()) {
        logger.debug("Query ID arrived: {}", QueryIdHelper.getQueryId(queryId));
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
      getInner().setException(new RpcException(String.format("%s : %s", type.name(), t.getMessage()), t));
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
