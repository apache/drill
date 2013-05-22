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
package org.apache.drill.exec.rpc.bit;

import java.util.concurrent.ConcurrentMap;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.work.batch.BitComHandler;
import org.apache.drill.exec.work.fragment.IncomingFragmentHandler;

import com.google.common.collect.Maps;
import com.google.common.io.Closeables;

/**
 * Manages communication tunnels between nodes.
 */
public class BitComImpl implements BitCom {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BitComImpl.class);

  private final ListenerPool listeners;
  private volatile BitServer server;
  private final BitComHandler handler;
  private final BootStrapContext context;
  private final ConnectionManagerRegistry connectionRegistry;

  public BitComImpl(BootStrapContext context, BitComHandler handler) {
    super();
    this.handler = handler;
    this.context = context;
    this.listeners = new ListenerPool(8);
    this.connectionRegistry = new ConnectionManagerRegistry(handler, context, listeners);
  }

  @Override
  public DrillbitEndpoint start(DrillbitEndpoint partialEndpoint) throws InterruptedException, DrillbitStartupException {
    server = new BitServer(handler, context, connectionRegistry, listeners);
    int port = context.getConfig().getInt(ExecConstants.INITIAL_BIT_PORT);
    port = server.bind(port);
    DrillbitEndpoint completeEndpoint = partialEndpoint.toBuilder().setBitPort(port).build();
    connectionRegistry.setEndpoint(completeEndpoint);
    return completeEndpoint;
  }

  
   
  public ListenerPool getListeners() {
    return listeners;
  }

  @Override
  public BitTunnel getTunnel(DrillbitEndpoint endpoint) {
    return new BitTunnel(endpoint, connectionRegistry.getConnectionManager(endpoint));
  }

  @Override
  public void registerIncomingBatchHandler(IncomingFragmentHandler handler) {
    this.handler.registerIncomingFragmentHandler(handler);
  }

  public void close() {
    Closeables.closeQuietly(server);
    for (BitConnectionManager bt : connectionRegistry) {
      bt.close();
    }
  }

}
