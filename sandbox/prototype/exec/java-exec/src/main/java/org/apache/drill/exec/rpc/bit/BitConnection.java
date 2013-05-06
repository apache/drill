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

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;

public class BitConnection extends RemoteConnection{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BitConnection.class); 
  
  private final RpcBus<RpcType, BitConnection> bus;
  private final ConcurrentMap<DrillbitEndpoint, BitConnection> registry;
  private final ListenerPool listeners;

  private final AvailabilityListener listener;
  private volatile DrillbitEndpoint endpoint;
  private volatile boolean active = false;
  private final UUID id;
  
  public BitConnection(AvailabilityListener listener, Channel channel, RpcBus<RpcType, BitConnection> bus, ConcurrentMap<DrillbitEndpoint, BitConnection> registry, ListenerPool listeners){
    super(channel);
    this.bus = bus;
    this.registry = registry;
    // we use a local listener pool unless a global one is provided.
    this.listeners = listeners != null ? listeners : new ListenerPool(2);
    this.listener = listener;
    this.id = UUID.randomUUID();
  }

  protected DrillbitEndpoint getEndpoint() {
    return endpoint;
  }

  public ListenerPool getListenerPool(){
    return listeners;
  }
  
  protected void setEndpoint(DrillbitEndpoint endpoint) {
    Preconditions.checkNotNull(endpoint);
    Preconditions.checkArgument(this.endpoint == null);
    
    this.endpoint = endpoint;
    BitServer.logger.debug("Adding new endpoint to available BitServer connections.  Endpoint: {}.", endpoint);
    synchronized(this){
      BitConnection c = registry.putIfAbsent(endpoint, this);
      
      if(c != null){ // the registry already has a connection like this
        
        // give the awaiting future an alternative connection.
        if(listener != null){
          listener.isAvailable(c);
        }
        
        // shut this down if this is a client as it won't be available in the registry.
        // otherwise we'll leave as, possibly allowing to bit coms to use different tunnels to talk to each other.  This shouldn't cause a problem.
        logger.debug("Shutting down connection to {} since the registry already has an active connection that endpoint.", endpoint);
        shutdownIfClient();
        
      }
      active = true;
      if(listener != null) listener.isAvailable(this);
    }
  }

  public DrillRpcFuture<Ack> sendRecordBatch(FragmentContext context, FragmentWritableBatch batch){
    return bus.send(this, RpcType.REQ_RECORD_BATCH, batch.getHeader(), Ack.class, batch.getBuffers());
  }
  
  public DrillRpcFuture<Ack> sendFragment(PlanFragment fragment){
    return bus.send(this, RpcType.REQ_INIATILIZE_FRAGMENT, fragment, Ack.class);
  }
  
  public DrillRpcFuture<Ack> cancelFragment(FragmentHandle handle){
    return bus.send(this,  RpcType.REQ_CANCEL_FRAGMENT, handle, Ack.class);
  }
  
  public DrillRpcFuture<Ack> sendFragmentStatus(FragmentStatus status){
    return bus.send(this,  RpcType.REQ_FRAGMENT_STATUS, status, Ack.class);
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

  public GenericFutureListener<ChannelFuture> getCloseHandler(GenericFutureListener<ChannelFuture> parent){
    return new CloseHandler(this, parent);
  }
  
  private class CloseHandler implements GenericFutureListener<ChannelFuture>{
    private BitConnection connection;
    private GenericFutureListener<ChannelFuture> parent;
    
    public CloseHandler(BitConnection connection, GenericFutureListener<ChannelFuture> parent) {
      super();
      this.connection = connection;
      this.parent = parent;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      if(connection.getEndpoint() != null) registry.remove(connection.getEndpoint(), connection);
      parent.operationComplete(future);
    }
    
  }
  
  public void shutdownIfClient(){
    if(bus.isClient()) Closeables.closeQuietly(bus);
  }
}