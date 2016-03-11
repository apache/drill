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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.AbstractCheckedFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;

import io.netty.buffer.DrillBuf;
import io.netty.channel.EventLoopGroup;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.zk.ZKClusterCoordinator;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
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
import org.apache.drill.exec.rpc.ConnectionThrottle;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.NamedThreadFactory;
import org.apache.drill.exec.rpc.RpcConnectionHandler;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.TransportCheck;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.user.UserClient;
import org.apache.drill.exec.rpc.user.UserResultsListener;

/**
 * Thin wrapper around a UserClient that handles connect/close and transforms
 * String into ByteBuf.
 *
 * To create non-default objects, use {@link DrillClient.Builder the builder class}.
 * E.g.
 * <code>
 *   DrillClient client = DrillClient.newBuilder()
 *       .setConfig(...)
 *       .setIsDirectConnection(true)
 *       .build();
 * </code>
 *
 * Except for {@link #runQuery} and {@link #cancelQuery}, this class is generally not thread safe.
 */
public class DrillClient implements Closeable, ConnectionThrottle {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillClient.class);

  private final DrillConfig config;
  private final BufferAllocator allocator;
  private final EventLoopGroup eventLoopGroup;
  private final ExecutorService executor;
  private final boolean isDirectConnection;
  private final int reconnectTimes;
  private final int reconnectDelay;

  // TODO: This should be initialized in the constructor. Currently, initialization is tightly coupled with #connect.
  private ClusterCoordinator clusterCoordinator;

  // checks if this client owns these resources (used when closing)
  private final boolean ownsAllocator;
  private final boolean ownsZkConnection;
  private final boolean ownsEventLoopGroup;
  private final boolean ownsExecutor;

  // once #setSupportComplexTypes() is removed, make this final
  private boolean supportComplexTypes;

  private UserClient client;
  private UserProperties props;
  private boolean connected;

  public DrillClient() {
    this(newBuilder());
  }

  /**
   * @deprecated Create a DrillClient using {@link DrillClient.Builder}.
   */
  @Deprecated
  public DrillClient(boolean isDirect) {
    this(newBuilder()
        .setDirectConnection(isDirect));
  }

  /**
   * @deprecated Create a DrillClient using {@link DrillClient.Builder}.
   */
  @Deprecated
  public DrillClient(String fileName) {
    this(newBuilder()
        .setConfigFromFile(fileName));
  }

  /**
   * @deprecated Create a DrillClient using {@link DrillClient.Builder}.
   */
  @Deprecated
  public DrillClient(DrillConfig config) {
    this(newBuilder()
        .setConfig(config));
  }

  /**
   * @deprecated Create a DrillClient using {@link DrillClient.Builder}.
   */
  @Deprecated
  public DrillClient(DrillConfig config, boolean isDirect) {
    this(newBuilder()
        .setConfig(config)
        .setDirectConnection(isDirect));
  }

  /**
   * @deprecated Create a DrillClient using {@link DrillClient.Builder}.
   */
  @Deprecated
  public DrillClient(DrillConfig config, ClusterCoordinator coordinator) {
    this(newBuilder()
        .setConfig(config)
        .setClusterCoordinator(coordinator));
  }

  /**
   * @deprecated Create a DrillClient using {@link DrillClient.Builder}.
   */
  @Deprecated
  public DrillClient(DrillConfig config, ClusterCoordinator coordinator, boolean isDirect) {
    this(newBuilder()
        .setConfig(config)
        .setClusterCoordinator(coordinator)
        .setDirectConnection(isDirect));
  }

  /**
   * @deprecated Create a DrillClient using {@link DrillClient.Builder}.
   */
  @Deprecated
  public DrillClient(DrillConfig config, ClusterCoordinator coordinator, BufferAllocator allocator) {
    this(newBuilder()
        .setConfig(config)
        .setClusterCoordinator(coordinator)
        .setAllocator(allocator));
  }

  /**
   * @deprecated Create a DrillClient using {@link DrillClient.Builder}.
   */
  @Deprecated
  public DrillClient(DrillConfig config, ClusterCoordinator coordinator, BufferAllocator allocator, boolean isDirect) {
    this(newBuilder()
        .setConfig(config)
        .setClusterCoordinator(coordinator)
        .setAllocator(allocator)
        .setDirectConnection(isDirect));
  }

  // used by DrillClient.Builder
  private DrillClient(final Builder builder) {
    this.config = builder.config != null ? builder.config : DrillConfig.create();

    this.ownsAllocator = builder.allocator == null;
    this.allocator = !ownsAllocator ? builder.allocator : RootAllocatorFactory.newRoot(config);

    this.isDirectConnection = builder.isDirectConnection;
    this.ownsZkConnection = builder.clusterCoordinator == null && !isDirectConnection;
    this.clusterCoordinator = builder.clusterCoordinator; // could be null

    this.ownsEventLoopGroup = builder.eventLoopGroup == null;
    this.eventLoopGroup = !ownsEventLoopGroup ? builder.eventLoopGroup :
        TransportCheck.createEventLoopGroup(config.getInt(ExecConstants.CLIENT_RPC_THREADS), "Client-");

    this.ownsExecutor = builder.executor == null;
    this.executor = !ownsExecutor ? builder.executor :
        new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
            new SynchronousQueue<Runnable>(),
            new NamedThreadFactory("drill-client-executor-")) {
          @Override
          protected void afterExecute(final Runnable r, final Throwable t) {
            if (t != null) {
              logger.error("{}.run() leaked an exception.", r.getClass().getName(), t);
            }
            super.afterExecute(r, t);
          }
        };

    this.supportComplexTypes = builder.supportComplexTypes;

    // These are currently not exposed to the users of this class through {@link DrillClient.Builder}.
    // Reconnection is only done when a cluster coordinator is used (and not when a direct connection is made), and
    // these are used only in reconnection.
    this.reconnectTimes = config.getInt(ExecConstants.BIT_RETRY_TIMES);
    this.reconnectDelay = config.getInt(ExecConstants.BIT_RETRY_DELAY);
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
   * @deprecated use {@link Builder#setSupportsComplexTypes} while building the client.
   */
  @Deprecated
  public void setSupportComplexTypes(boolean supportComplexTypes) {
    if (connected) {
      throw new IllegalStateException("Attempted to modify a property after connection has been established.");
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

  public void connect(String connect, Properties props) throws RpcException {
    if (connected) {
      return;
    }

    final DrillbitEndpoint endpoint;
    if (isDirectConnection) {
      final String[] connectInfo = props.getProperty("drillbit").split(":");
      final String port = connectInfo.length==2?connectInfo[1]:config.getString(ExecConstants.INITIAL_USER_PORT);
      endpoint = DrillbitEndpoint.newBuilder()
              .setAddress(connectInfo[0])
              .setUserPort(Integer.parseInt(port))
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

      final ArrayList<DrillbitEndpoint> endpoints = new ArrayList<>(clusterCoordinator.getAvailableEndpoints());
      checkState(!endpoints.isEmpty(), "No DrillbitEndpoint can be found");
      // shuffle the collection then get the first endpoint
      Collections.shuffle(endpoints);
      endpoint = endpoints.iterator().next();
    }

    if (props != null) {
      final UserProperties.Builder upBuilder = UserProperties.newBuilder();
      for (final String key : props.stringPropertyNames()) {
        upBuilder.addProperties(Property.newBuilder()
            .setKey(key)
            .setValue(props.getProperty(key)));
      }

      this.props = upBuilder.build();
    }

    client = new UserClient(config, supportComplexTypes, allocator, eventLoopGroup, executor);
    logger.debug("Connecting to server {}:{}", endpoint.getAddress(), endpoint.getUserPort());
    connect(endpoint);
    connected = true;
  }

  /**
   * @deprecated use {@link TransportCheck#createEventLoopGroup} directly.
   */
  @Deprecated
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
        final ArrayList<DrillbitEndpoint> endpoints = new ArrayList<>(clusterCoordinator.getAvailableEndpoints());
        if (endpoints.isEmpty()) {
          continue;
        }
        client.close();
        Collections.shuffle(endpoints);
        connect(endpoints.iterator().next());
        return true;
      } catch (Exception e) {
        logger.info(String.format("Trying to reconnect (#%s).", retry));
      }
    }
    return false;
  }

  private void connect(DrillbitEndpoint endpoint) throws RpcException {
    final FutureHandler f = new FutureHandler();
    client.connect(f, endpoint, props, getUserCredentials());
    f.checkedGet();
  }

  private UserBitShared.UserCredentials getUserCredentials() {
    // If username is not propagated as one of the properties
    String userName = "anonymous";

    if (props != null) {
      for (Property property: props.getPropertiesList()) {
        if (property.getKey().equalsIgnoreCase("user") &&
            !Strings.isNullOrEmpty(property.getValue())) {
          userName = property.getValue();
          break;
        }
      }
    }

    return UserBitShared.UserCredentials.newBuilder()
        .setUserName(userName)
        .build();
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
    if (ownsAllocator && allocator != null) {
      AutoCloseables.closeNoChecked(allocator);
    }
    if (ownsZkConnection) {
      if (clusterCoordinator != null) {
        try {
          clusterCoordinator.close();
          clusterCoordinator = null;
        } catch (Exception e) {
          logger.warn("Error while closing Cluster Coordinator.", e);
        }
      }
    }

    if (ownsEventLoopGroup && eventLoopGroup != null) {
      TransportCheck.shutDownEventLoopGroup(eventLoopGroup, "client loop group", logger);
    }

    if (ownsExecutor && executor != null) {
      MoreExecutors.shutdownAndAwaitTermination(executor, 1, TimeUnit.SECONDS);
    }

    connected = false;
  }

  /**
   * Runs the given plan of the given {@link QueryType type}.
   *
   * @param type query type
   * @param plan query plan
   * @return results as a list of {@link QueryDataBatch data batches}
   * @throws RpcException if the query failed
   */
  public List<QueryDataBatch> runQuery(QueryType type, String plan) throws RpcException {
    final UserProtos.RunQuery query = UserProtos.RunQuery.newBuilder()
        .setResultsMode(UserProtos.QueryResultsMode.STREAM_FULL)
        .setType(type)
        .setPlan(plan)
        .build();
    final ListHoldingResultsListener listener = new ListHoldingResultsListener(query);
    client.submitQuery(listener, query);
    return listener.getResults();
  }

  /**
   * Runs the given plan of the given {@link QueryType type}. The {@link UserResultsListener listener} is notified
   * if results arrive, query completed, etc.
   *
   * @param type query type
   * @param plan query plan
   * @param resultsListener results listener
   */
  public void runQuery(QueryType type, String plan, UserResultsListener resultsListener) {
    client.submitQuery(resultsListener,
        UserProtos.RunQuery.newBuilder()
            .setResultsMode(UserProtos.QueryResultsMode.STREAM_FULL)
            .setType(type)
            .setPlan(plan)
            .build());
  }

  /**
   * Cancels the given query
   *
   * @param id query id, received through {@link UserResultsListener#queryIdArrived}.
   * @return a future that acknowledges cancellation
   */
  public DrillRpcFuture<Ack> cancelQuery(QueryId id) {
    if(logger.isDebugEnabled()) {
      logger.debug("Cancelling query {}", QueryIdHelper.getQueryId(id));
    }
    return client.send(RpcType.CANCEL_QUERY, id, Ack.class);
  }

  @VisibleForTesting
  public DrillRpcFuture<Ack> resumeQuery(final QueryId queryId) {
    if(logger.isDebugEnabled()) {
      logger.debug("Resuming query {}", QueryIdHelper.getQueryId(queryId));
    }
    return client.send(RpcType.RESUME_PAUSED_QUERY, queryId, Ack.class);
  }

  private class ListHoldingResultsListener implements UserResultsListener {
    private final Vector<QueryDataBatch> results = new Vector<>();
    private final SettableFuture<List<QueryDataBatch>> future = SettableFuture.create();
    private final UserProtos.RunQuery query ;

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
        /*
         * Since we're not going to return the result to the caller
         * to clean up, we have to do it.
         */
        for(final QueryDataBatch queryDataBatch : results) {
          queryDataBatch.release();
        }

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

  private static class FutureHandler extends AbstractCheckedFuture<Void, RpcException>
      implements RpcConnectionHandler<ServerConnection>, DrillRpcFuture<Void> {
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

  /**
   * Return a new {@link DrillClient.Builder Drill client builder}.
   * @return a new builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Helper class to construct a {@link DrillClient Drill client}.
   */
  public static class Builder {

    private DrillConfig config;
    private BufferAllocator allocator;
    private ClusterCoordinator clusterCoordinator;
    private EventLoopGroup eventLoopGroup;
    private ExecutorService executor;

    // defaults
    private boolean supportComplexTypes = true;
    private boolean isDirectConnection = false;

    /**
     * Sets the {@link DrillConfig configuration} for this client.
     *
     * @param drillConfig drill configuration
     * @return this builder
     */
    public Builder setConfig(DrillConfig drillConfig) {
      this.config = drillConfig;
      return this;
    }

    /**
     * Sets the {@link DrillConfig configuration} for this client based on the given file.
     *
     * @param fileName configuration file name
     * @return this builder
     */
    public Builder setConfigFromFile(final String fileName) {
      this.config = DrillConfig.create(fileName);
      return this;
    }

    /**
     * Sets the {@link BufferAllocator buffer allocator} to be used by this client. If this is not set, an allocator
     * will be created based on the configuration.
     *
     * If this is set, the caller is responsible for closing the given allocator.
     *
     * @param allocator buffer allocator
     * @return this builder
     */
    public Builder setAllocator(final BufferAllocator allocator) {
      this.allocator = allocator;
      return this;
    }

    /**
     * Sets the {@link ClusterCoordinator cluster coordinator} that this client registers with. If this is not set
     * and the this client does not use a {@link #setDirectConnection direct connection}, a cluster coordinator will be
     * created based on the configuration.
     *
     * If this is set, the caller is responsible for closing the given coordinator.
     *
     * @param clusterCoordinator cluster coordinator
     * @return this builder
     */
    public Builder setClusterCoordinator(final ClusterCoordinator clusterCoordinator) {
      this.clusterCoordinator = clusterCoordinator;
      return this;
    }

    /**
     * Sets the event loop group that to be used by the client. If this is not set, an event loop group will be
     * created based on the configuration.
     *
     * If this is set, the caller is responsible for closing the given event loop group.
     *
     * @param eventLoopGroup event loop group
     * @return this builder
     */
    public Builder setEventLoopGroup(final EventLoopGroup eventLoopGroup) {
      this.eventLoopGroup = eventLoopGroup;
      return this;
    }

    /**
     * Sets the executor service to be used by the client. If this is not set, an executor will be created based on
     * the configuration.
     *
     * If this is set, the caller is responsible for closing the given executor.
     *
     * @param executor executor service
     * @return this builder
     */
    public Builder setExecutorService(final ExecutorService executor) {
      this.executor = executor;
      return this;
    }

    /**
     * Sets whether the application is willing to accept complex types (Map, Arrays) in the returned result set.
     * Default is {@code true}. If set to {@code false}, the complex types are returned as JSON encoded VARCHAR type.
     *
     * @param supportComplexTypes if client accepts complex types
     * @return this builder
     */
    public Builder setSupportsComplexTypes(final boolean supportComplexTypes) {
      this.supportComplexTypes = supportComplexTypes;
      return this;
    }

    /**
     * Sets whether the client will connect directly to the drillbit instead of going through the cluster coordinator
     * (zookeeper).
     *
     * @param isDirectConnection is direct connection
     * @return this builder
     */
    public Builder setDirectConnection(final boolean isDirectConnection) {
      this.isDirectConnection = isDirectConnection;
      return this;
    }

    /**
     * Builds the drill client.
     *
     * @return a new drill client
     */
    public DrillClient build() {
      return new DrillClient(Builder.this);
    }
  }
}
