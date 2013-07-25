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
import io.netty.util.concurrent.GenericFutureListener;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.rpc.RpcConnectionHandler;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.work.batch.BitComHandler;

import com.google.protobuf.MessageLite;

/**
 * Manager all connections between two particular bits.
 */
public class BitConnectionManager implements Closeable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BitConnectionManager.class);
  
  private final DrillbitEndpoint endpoint;
  private final AtomicReference<BitConnection> connectionHolder;
  private final BitComHandler handler;
  private final BootStrapContext context;
  private final ListenerPool listenerPool;
  private final DrillbitEndpoint localIdentity;
  
  public BitConnectionManager(DrillbitEndpoint remoteEndpoint, DrillbitEndpoint localIdentity, BitComHandler handler, BootStrapContext context, ListenerPool listenerPool) {
    assert remoteEndpoint != null : "Endpoint cannot be null.";
    assert remoteEndpoint.getAddress() != null && !remoteEndpoint.getAddress().isEmpty(): "Endpoint address cannot be null.";
    assert remoteEndpoint.getBitPort() > 0 : String.format("Bit Port must be set to a port between 1 and 65k.  Was set to %d.", remoteEndpoint.getBitPort());
    
    this.connectionHolder =  new AtomicReference<BitConnection>();
    this.endpoint = remoteEndpoint;
    this.localIdentity = localIdentity;
    this.handler = handler;
    this.context = context;
    this.listenerPool = listenerPool;
  }
  
  public <R extends MessageLite> BitCommand<R> runCommand(BitCommand<R> cmd){
    logger.debug("Running command {}", cmd);
    BitConnection connection = connectionHolder.get();
    if(connection != null){
      if(connection.isActive()){
        cmd.connectionAvailable(connection);
        return cmd;
      }else{
        // remove the old connection. (don't worry if we fail since someone else should have done it.
        connectionHolder.compareAndSet(connection, null);
      }
    }
    
    /** We've arrived here without a connection, let's make sure only one of us makes a connection. (fyi, another endpoint could create a reverse connection **/
    synchronized(this){
      connection = connectionHolder.get();
      if(connection != null){
        cmd.connectionAvailable(connection);
      }else{
        BitClient client = new BitClient(endpoint, localIdentity, handler, context, new CloseHandlerCreator(), listenerPool);
        
        client.connect(new ConnectionListeningDecorator(cmd, !endpoint.equals(localIdentity)));
      }
      return cmd;
      
    }
  }
  
  CloseHandlerCreator getCloseHandlerCreator(){
    return new CloseHandlerCreator();
  }

  /** Factory for close handlers **/
  class CloseHandlerCreator{
    public GenericFutureListener<ChannelFuture> getHandler(BitConnection connection, GenericFutureListener<ChannelFuture> parent){
      return new CloseHandler(connection, parent);
    }
  }
  
  
  
  /**
   * Listens for connection closes and clears connection holder.
   */
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
      connectionHolder.compareAndSet(connection, null);
      parent.operationComplete(future);
    }
    
  } 
  
  /**
   * Decorate a connection creation so that we capture a success and keep it available for future requests.  If we have raced and another is already available... we return that one and close things down on this one.
   */
  private class ConnectionListeningDecorator implements RpcConnectionHandler<BitConnection>{

    private final RpcConnectionHandler<BitConnection> delegate;
    private final boolean closeOnDupe;
    
    public ConnectionListeningDecorator(RpcConnectionHandler<BitConnection> delegate,  boolean closeOnDupe) {
      this.delegate = delegate;
      this.closeOnDupe = closeOnDupe;
    }

    @Override
    public void connectionSucceeded(BitConnection incoming) {
      BitConnection connection = connectionHolder.get();
      while(true){
        boolean setted = connectionHolder.compareAndSet(null, incoming);
        if(setted){
          connection = incoming;
          break;
        }
        connection = connectionHolder.get();
        if(connection != null) break; 
      }
      
      
      if(connection == incoming){
        delegate.connectionSucceeded(connection);
      }else{

        if(closeOnDupe){
          // close the incoming because another channel was created in the mean time (unless this is a self connection).
          logger.debug("Closing incoming connection because a connection was already set.");
          incoming.getChannel().close();
        }
        delegate.connectionSucceeded(connection);
      }
    }

    @Override
    public void connectionFailed(org.apache.drill.exec.rpc.RpcConnectionHandler.FailureType type, Throwable t) {
      delegate.connectionFailed(type, t);
    }
    
  }

  public DrillbitEndpoint getEndpoint() {
    return endpoint;
  }
  
  public void addServerConnection(BitConnection connection){
    // if the connection holder is not set, set it to this incoming connection.
    logger.debug("Setting server connection.");
    this.connectionHolder.compareAndSet(null, connection);
  }

  @Override
  public void close() {
    BitConnection c = connectionHolder.getAndSet(null);
    if(c != null){
      c.getChannel().close();
    }
  }
  
  
  
}
