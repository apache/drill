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
import static com.google.common.collect.Iterables.get;
import static org.apache.drill.exec.proto.UserProtos.QueryResultsMode.STREAM_FULL;
import static org.apache.drill.exec.proto.UserProtos.RunQuery.newBuilder;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.IOException;
import java.util.Collection;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.ZKClusterCoordinator;
import org.apache.drill.exec.proto.UserProtos.QueryHandle;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.NamedThreadFactory;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.UserClient;

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
    return this.client.submitQuery(newBuilder().setMode(STREAM_FULL).setPlan(plan).build(), null);
  }

}
