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

import io.netty.channel.ChannelFuture;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.Closeable;
import java.util.Collection;

import org.apache.drill.common.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.ExecProtos.FragmentStatus;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.RpcBus;

/**
 * Service that allows one Drillbit to communicate with another. Internally manages whether each particular bit is a server
 * or a client depending on who initially made the connection. If no connection exists, BitCom is
 * responsible for making a connection.  BitCom should automatically straight route local BitCommunication rather than connecting to its self.
 */
public interface BitCom extends Closeable{

  /**
   * Routes the output of a RecordBatch to another node.  The record batch
   * @param node The node id to send the record batch to.
   * @param batch The record batch to send.
   * @return A SendProgress object which can be used to monitor the sending of the batch.
   */
  public abstract DrillRpcFuture<Ack> sendRecordBatch(FragmentContext context, DrillbitEndpoint node, RecordBatch batch);

  
  /**
   * Requests an iterator to access an incoming record batch.  
   * @param fragmentId
   * @return
   */
  public RecordBatch getReceivingRecordBatchHandle(int majorFragmentId, int minorFragmentId);
  
  /**
   * Send a query PlanFragment to another bit.   
   * @param context
   * @param node
   * @param fragment
   * @return
   */
  public abstract DrillRpcFuture<FragmentHandle> sendFragment(FragmentContext context, DrillbitEndpoint node, PlanFragment fragment);

  public abstract void startQuery(Collection<DrillbitEndpoint> firstNodes, long queryId);
    
  
  public abstract DrillRpcFuture<Ack> cancelFragment(FragmentContext context, DrillbitEndpoint node, FragmentHandle handle);
  
  public abstract DrillRpcFuture<FragmentStatus> getFragmentStatus(FragmentContext context, DrillbitEndpoint node, FragmentHandle handle);
  
  
  public interface TunnelListener extends GenericFutureListener<ChannelFuture> {
    public void connectionEstablished(SocketChannel channel, DrillbitEndpoint endpoint, RpcBus<?> bus);
  }
  
  public interface SendManager{
    /**
     * Sender responsible for regularly checking this value to see whether it should continue to send or yield the process
     * @return
     */
    public boolean canContinue();
  }

}
