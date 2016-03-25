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

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.BitControl.BitControlHandshake;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.rpc.BasicClient;
import org.apache.drill.exec.rpc.ReconnectingConnection;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.work.batch.ControlMessageHandler;

/**
 * Maintains connection between two particular bits.
 */
public class ControlConnectionManager extends ReconnectingConnection<ControlConnection, BitControlHandshake> {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ControlConnectionManager.class);

  private final BufferAllocator allocator;
  private final DrillbitEndpoint remoteEndpoint;
  private final DrillbitEndpoint localEndpoint;
  private final ControlMessageHandler handler;
  private final BootStrapContext context;

  public ControlConnectionManager(BufferAllocator allocator, DrillbitEndpoint remoteEndpoint,
                                  DrillbitEndpoint localEndpoint, ControlMessageHandler handler,
                                  BootStrapContext context) {
    super(
        BitControlHandshake.newBuilder()
            .setRpcVersion(ControlRpcConfig.RPC_VERSION)
            .setEndpoint(localEndpoint)
            .build(),
        remoteEndpoint.getAddress(),
        remoteEndpoint.getControlPort());

    this.allocator = allocator;
    this.remoteEndpoint = remoteEndpoint;
    this.localEndpoint = localEndpoint;
    this.handler = handler;
    this.context = context;
  }

  @Override
  protected BasicClient<?, ControlConnection, BitControlHandshake, ?> getNewClient() {
    return new ControlClient(allocator, remoteEndpoint, localEndpoint, handler, context, new CloseHandlerCreator());
  }

}
