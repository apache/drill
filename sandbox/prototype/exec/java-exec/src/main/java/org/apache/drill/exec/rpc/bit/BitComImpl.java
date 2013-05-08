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

import io.netty.channel.Channel;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.Future;

import java.util.Collection;
import java.util.Map;

import org.apache.drill.common.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.ExecProtos.FragmentStatus;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.RpcBus;
import org.apache.drill.exec.server.DrillbitContext;

import com.google.common.collect.Maps;
import com.google.common.io.Closeables;

public class BitComImpl implements BitCom {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BitComImpl.class);

  private Map<DrillbitEndpoint, BitTunnel> tunnels = Maps.newConcurrentMap();
  private Map<SocketChannel, DrillbitEndpoint> endpoints = Maps.newConcurrentMap();
  private Object lock = new Object();
  private BitServer server;
  private DrillbitContext context;

  public BitComImpl(DrillbitContext context) {
    this.context = context;
  }

  public int start() throws InterruptedException, DrillbitStartupException {
    server = new BitServer(new BitComHandler(modifier), context.getAllocator().getUnderlyingAllocator(), context.getBitLoopGroup(), context);
    int port = context.getConfig().getInt(ExecConstants.INITIAL_BIT_PORT);
    return server.bind(port);
  }

  private Future<BitTunnel> getNode(DrillbitEndpoint endpoint) {
    return null;
    
//    BitTunnel t = tunnels.get(endpoint);
//    if (t == null) {
//      synchronized (lock) {
//        t = tunnels.get(endpoint);
//        if (t != null) return t;
//        BitClient c = new BitClient(new BitComHandler(modifier), context.getAllocator().getUnderlyingAllocator(),
//            context.getBitLoopGroup(), context);
//
//        // need to figure what to do here with regards to waiting for handshake before returning. Probably need to add
//        // future registry so that new endpoint registration ping the registry.
//        throw new UnsupportedOperationException();
//        c.connectAsClient(endpoint.getAddress(), endpoint.getBitPort()).await();
//        t = new BitTunnel(c);
//        tunnels.put(endpoint, t);
//
//      }
//    }
//    return null;
  }

  

  @Override
  public DrillRpcFuture<FragmentHandle> sendFragment(FragmentContext context, DrillbitEndpoint node,
      PlanFragment fragment) {
    return null;
  }

  @Override
  public DrillRpcFuture<Ack> cancelFragment(FragmentContext context, DrillbitEndpoint node, FragmentHandle handle) {
    return null;
  }

  @Override
  public DrillRpcFuture<FragmentStatus> getFragmentStatus(FragmentContext context, DrillbitEndpoint node,
      FragmentHandle handle) {
    return null;
  }

  private final TunnelModifier modifier = new TunnelModifier();

  /**
   * Fully synchronized modifier. Contention should be low since endpoints shouldn't be constantly changing.
   */
  class TunnelModifier {
    public BitTunnel remove(Channel ch) {
      synchronized (this) {
        DrillbitEndpoint endpoint = endpoints.remove(ch);
        if (endpoint == null) {
          logger
              .warn("We attempted to find a endpoint from a provided channel and found none.  This suggests a race condition or memory leak problem.");
          return null;
        }

        BitTunnel tunnel = tunnels.remove(endpoint);
        return tunnel;
      }
    }

    public void create(SocketChannel channel, DrillbitEndpoint endpoint, RpcBus<?> bus) {
      synchronized (this) {
        endpoints.put(channel, endpoint);
        tunnels.put(endpoint, new BitTunnel(bus));
      }
    }
  }

  public void close() {
    Closeables.closeQuietly(server);
    for (BitTunnel bt : tunnels.values()) {
      bt.shutdownIfClient();
    }
  }


  @Override
  public DrillRpcFuture<Ack> sendRecordBatch(FragmentContext context, DrillbitEndpoint node, RecordBatch batch) {
    return null;
  }

  @Override
  public RecordBatch getReceivingRecordBatchHandle(int majorFragmentId, int minorFragmentId) {
    return null;
  }

  @Override
  public void startQuery(Collection<DrillbitEndpoint> firstNodes, long queryId) {
  }

}
