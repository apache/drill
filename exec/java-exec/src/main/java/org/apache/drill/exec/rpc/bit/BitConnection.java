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
package org.apache.drill.exec.rpc.bit;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.ExecProtos.FragmentStatus;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;
import org.apache.drill.exec.proto.ExecProtos.RpcType;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.RemoteConnection;
import org.apache.drill.exec.rpc.RpcBus;
import org.apache.drill.exec.rpc.RpcConnectionHandler;
import org.apache.drill.exec.rpc.RpcOutcomeListener;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.google.protobuf.MessageLite;

public class BitConnection extends RemoteConnection{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BitConnection.class); 
  
  private final RpcBus<RpcType, BitConnection> bus;
  private final ListenerPool listeners;
  private volatile DrillbitEndpoint endpoint;
  private volatile boolean active = false;
  private final UUID id;
  
  public BitConnection(Channel channel, RpcBus<RpcType, BitConnection> bus, ListenerPool listeners){
    super(channel);
    this.bus = bus;
    // we use a local listener pool unless a global one is provided.
    this.listeners = listeners != null ? listeners : new ListenerPool(2);
    this.id = UUID.randomUUID();
  }
  
  void setEndpoint(DrillbitEndpoint endpoint){
    assert this.endpoint == null : "Endpoint should only be set once (only in the case in incoming server requests).";
    this.endpoint = endpoint;
    active = true;
  }

  protected DrillbitEndpoint getEndpoint() {
    return endpoint;
  }

  public ListenerPool getListenerPool(){
    return listeners;
  }
  
  
  public <SEND extends MessageLite, RECEIVE extends MessageLite> void send(RpcOutcomeListener<RECEIVE> outcomeListener, RpcType rpcType,
      SEND protobufBody, Class<RECEIVE> clazz, ByteBuf... dataBodies){
    bus.send(outcomeListener, this, rpcType, protobufBody, clazz, dataBodies);
  }
  

  public void disable(){
    active = false;
  }
  
  public boolean isActive(){
    return active;
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((id == null) ? 0 : id.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    BitConnection other = (BitConnection) obj;
    if (id == null) {
      if (other.id != null) return false;
    } else if (!id.equals(other.id)) return false;
    return true;
  }


  
  public void shutdownIfClient(){
    if(bus.isClient()) Closeables.closeQuietly(bus);
  }
}