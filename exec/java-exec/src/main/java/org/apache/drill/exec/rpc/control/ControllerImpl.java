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
package org.apache.drill.exec.rpc.control;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.work.batch.ControlMessageHandler;

import com.google.common.io.Closeables;

/**
 * Manages communication tunnels between nodes.
 */
public class ControllerImpl implements Controller {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ControllerImpl.class);

  private volatile ControlServer server;
  private final ControlMessageHandler handler;
  private final BootStrapContext context;
  private final ConnectionManagerRegistry connectionRegistry;
  private final boolean allowPortHunting;

  public ControllerImpl(BootStrapContext context, ControlMessageHandler handler, boolean allowPortHunting) {
    super();
    this.handler = handler;
    this.context = context;
    this.connectionRegistry = new ConnectionManagerRegistry(handler, context);
    this.allowPortHunting = allowPortHunting;
  }

  @Override
  public DrillbitEndpoint start(DrillbitEndpoint partialEndpoint) throws InterruptedException, DrillbitStartupException {
    server = new ControlServer(handler, context, connectionRegistry);
    int port = context.getConfig().getInt(ExecConstants.INITIAL_BIT_PORT);
    port = server.bind(port, allowPortHunting);
    DrillbitEndpoint completeEndpoint = partialEndpoint.toBuilder().setControlPort(port).build();
    connectionRegistry.setEndpoint(completeEndpoint);
    return completeEndpoint;
  }

  @Override
  public ControlTunnel getTunnel(DrillbitEndpoint endpoint) {
    return new ControlTunnel(endpoint, connectionRegistry.getConnectionManager(endpoint));
  }

  public void close() {
    Closeables.closeQuietly(server);
    for (ControlConnectionManager bt : connectionRegistry) {
      bt.close();
    }
  }

}
