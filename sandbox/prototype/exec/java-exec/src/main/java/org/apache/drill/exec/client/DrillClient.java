package org.apache.drill.exec.client;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.ZKClusterCoordinator;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.NamedThreadFactory;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.UserClient;

import java.io.IOException;
import java.util.Collection;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.get;
import static io.netty.buffer.Unpooled.EMPTY_BUFFER;
import static io.netty.buffer.Unpooled.copiedBuffer;
import static java.nio.charset.Charset.forName;
import static org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import static org.apache.drill.exec.proto.UserProtos.QueryHandle;
import static org.apache.drill.exec.proto.UserProtos.QueryResultsMode.STREAM_FULL;
import static org.apache.drill.exec.proto.UserProtos.RunQuery.newBuilder;

/**
 * Thin wrapper around a UserClient that handles connect/close and transforms String into ByteBuf
 */
public class DrillClient {

  DrillConfig config;
  private UserClient client;
  private ClusterCoordinator clusterCoordinator;

  public DrillClient() {
    this(DrillConfig.create());
  }

  public DrillClient(String fileName) {
    this(DrillConfig.create(fileName));
  }

  public DrillClient(DrillConfig config) {
    this.config = config;
  }

  /**
   * Connects the client to a Drillbit server
   *
   * @throws IOException
   */
  public void connect() throws Exception {
    this.clusterCoordinator = new ZKClusterCoordinator(this.config);
    this.clusterCoordinator.start();
    Thread.sleep(10000);
    Collection<DrillbitEndpoint> endpoints = clusterCoordinator.getAvailableEndpoints();
    checkState(!endpoints.isEmpty(), "No DrillbitEndpoint can be found");
    // just use the first endpoint for now
    DrillbitEndpoint endpoint = get(endpoints, 0);
    ByteBufAllocator bb = new PooledByteBufAllocator(true);
    this.client = new UserClient(bb, new NioEventLoopGroup(1, new NamedThreadFactory("Client-")));
    try {
      this.client.connectAsClient(endpoint.getAddress(), endpoint.getUserPort());
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  /**
   * Closes this client's connection to the server
   *
   * @throws IOException
   */
  public void close() throws IOException {
    this.client.close();
  }

  /**
   * Submits a Logical plan for direct execution (bypasses parsing)
   *
   * @param plan the plan to execute
   * @return a handle for the query result
   * @throws RpcException
   */
  public DrillRpcFuture<QueryHandle> submitPlan(String plan) throws RpcException {
    return this.client.submitQuery(newBuilder().setMode(STREAM_FULL).setPlan(plan).build(), EMPTY_BUFFER);
  }

  /**
   * Submits a Query for parsing and execution
   *
   * @param query the query to execute
   * @return a handle for the query result
   * @throws RpcException
   */
  public DrillRpcFuture<QueryHandle> submitQuery(String query) throws RpcException {
    return this.client.submitQuery(newBuilder().setMode(STREAM_FULL).build(), copiedBuffer(query, forName("UTF-8")));
  }
}
