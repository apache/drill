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
package org.apache.drill.exec.rpc.data;

import java.io.Closeable;
import java.util.concurrent.ConcurrentMap;

import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.rpc.control.WorkEventBus;
import org.apache.drill.exec.server.BootStrapContext;

import com.google.common.collect.Maps;
import com.google.common.io.Closeables;

/**
 * Manages a connection for each endpoint.
 */
public class DataConnectionCreator implements Closeable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DataConnectionCreator.class);

  private volatile DataServer server;
  private final BootStrapContext context;
  private final WorkEventBus workBus;
  private final DataResponseHandler dataHandler;
  private final boolean allowPortHunting;
  private ConcurrentMap<DrillbitEndpoint, DataConnectionManager> connectionManager = Maps.newConcurrentMap();

  public DataConnectionCreator(BootStrapContext context, WorkEventBus workBus, DataResponseHandler dataHandler, boolean allowPortHunting) {
    super();
    this.context = context;
    this.workBus = workBus;
    this.dataHandler = dataHandler;
    this.allowPortHunting = allowPortHunting;
  }

  public DrillbitEndpoint start(DrillbitEndpoint partialEndpoint) throws DrillbitStartupException {
    server = new DataServer(context, workBus, dataHandler);
    int port = server.bind(partialEndpoint.getControlPort() + 1, allowPortHunting);
    DrillbitEndpoint completeEndpoint = partialEndpoint.toBuilder().setDataPort(port).build();
    return completeEndpoint;
  }

  public DataTunnel getTunnel(DrillbitEndpoint endpoint) {
    DataConnectionManager newManager = new DataConnectionManager(endpoint, context);
    DataConnectionManager oldManager = connectionManager.putIfAbsent(endpoint, newManager);
    if(oldManager != null){
      newManager = oldManager;
    }
    return new DataTunnel(newManager);
  }

  @Override
  public void close() {
    Closeables.closeQuietly(server);
  }

}
