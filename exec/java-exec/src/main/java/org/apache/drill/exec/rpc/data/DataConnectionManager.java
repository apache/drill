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

import org.apache.drill.exec.proto.BitData.BitClientHandshake;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared.RpcChannel;
import org.apache.drill.exec.rpc.ReconnectingConnection;
import org.apache.drill.exec.server.BootStrapContext;

public class DataConnectionManager extends ReconnectingConnection<DataClientConnection, BitClientHandshake>{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DataConnectionManager.class);

  private final DrillbitEndpoint endpoint;
  private final BootStrapContext context;

  private final static BitClientHandshake HANDSHAKE = BitClientHandshake //
      .newBuilder() //
      .setRpcVersion(DataRpcConfig.RPC_VERSION) //
      .setChannel(RpcChannel.BIT_DATA) //
      .build();

  public DataConnectionManager(DrillbitEndpoint endpoint, BootStrapContext context) {
    super(HANDSHAKE, endpoint.getAddress(), endpoint.getDataPort());
    this.endpoint = endpoint;
    this.context = context;
  }

  @Override
  protected DataClient getNewClient() {
    return new DataClient(endpoint, context, new CloseHandlerCreator());
  }

}