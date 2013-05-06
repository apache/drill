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
package org.apache.drill.exec.server;

import io.netty.buffer.ByteBuf;

import java.util.concurrent.ConcurrentMap;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.bit.BitClient;
import org.apache.drill.exec.rpc.bit.BitComImpl;
import org.apache.drill.exec.rpc.bit.BitConnection;
import org.apache.drill.exec.rpc.bit.BitRpcConfig;
import org.apache.drill.exec.rpc.bit.BitServer;
import org.apache.drill.exec.rpc.bit.ListenerPool;
import org.apache.drill.exec.work.batch.BitComHandler;
import org.apache.drill.exec.work.fragment.IncomingFragmentHandler;
import org.junit.Test;

import com.google.common.collect.Maps;

public class TestBitRpc {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestBitRpc.class);
  
  @Test
  public void testBasicConnectionAndHandshake() throws Exception{
    int port = 1234;
    BootStrapContext c = new BootStrapContext(DrillConfig.create());
    ConcurrentMap<DrillbitEndpoint, BitConnection> registry = Maps.newConcurrentMap();
    BitServer server = new BitServer(new BitComTestHandler(), c, registry, new ListenerPool(2));
    port = server.bind(port);
    for(int i =0; i < 10; i++){
      BitClient client = new BitClient(DrillbitEndpoint.newBuilder().setAddress("localhost").setBitPort(port).build(), null, new BitComTestHandler(), c, registry, new ListenerPool(2));
      client.connect();
      
    }
    System.out.println("connected");
  }
  
  
  
  private class BitComTestHandler implements BitComHandler{

    @Override
    public Response handle(BitConnection connection, int rpcType, ByteBuf pBody, ByteBuf dBody) throws RpcException {
      return BitRpcConfig.OK;
    }

    @Override
    public void startNewRemoteFragment(PlanFragment fragment) {
    }

    @Override
    public Ack cancelFragment(FragmentHandle handle) {
      return null;
    }

    @Override
    public void registerIncomingFragmentHandler(IncomingFragmentHandler handler) {
    }
    
  }
}
