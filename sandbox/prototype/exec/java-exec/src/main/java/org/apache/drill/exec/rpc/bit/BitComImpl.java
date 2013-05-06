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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.exception.FragmentSetupException;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.BitHandshake;
import org.apache.drill.exec.proto.ExecProtos.RpcType;
import org.apache.drill.exec.rpc.NamedThreadFactory;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.work.batch.BitComHandler;
import org.apache.drill.exec.work.fragment.IncomingFragmentHandler;

import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.AbstractCheckedFuture;
import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Manages communication tunnels between nodes.   
 */
public class BitComImpl implements BitCom {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BitComImpl.class);

  private final ConcurrentMap<DrillbitEndpoint, BitConnection> registry = Maps.newConcurrentMap();
  private final ListenerPool listeners;
  private volatile BitServer server;
  private final BitComHandler handler;
  private final BootStrapContext context;
  
  // TODO: this executor should be removed.
  private final Executor exec = Executors.newCachedThreadPool(new NamedThreadFactory("BitComImpl execution pool: "));

  public BitComImpl(BootStrapContext context, BitComHandler handler) {
    super();
    this.handler = handler;
    this.context = context;
    this.listeners = new ListenerPool(8);
  }

  public int start() throws InterruptedException, DrillbitStartupException {
    server = new BitServer(handler, context, registry, listeners);
    int port = context.getConfig().getInt(ExecConstants.INITIAL_BIT_PORT);
    return server.bind(port);
  }

  private CheckedFuture<BitConnection, RpcException> getNode(final DrillbitEndpoint endpoint, boolean check) {
    
    
    SettableFuture<BitConnection> future = SettableFuture.create();
    BitComFuture<BitConnection> checkedFuture = new BitComFuture<BitConnection>(future);
    BitConnection t = null;

    if (check) {
      t = registry.get(endpoint);

      if (t != null) {
        future.set(t);
        return checkedFuture;
      }
    }
    
    try {
      AvailWatcher watcher = new AvailWatcher(future);
      BitClient c = new BitClient(endpoint, watcher, handler, context, registry, listeners);
      c.connect();
      return checkedFuture;
    } catch (InterruptedException | RpcException e) {
      future.setException(new FragmentSetupException("Unable to open connection"));
      return checkedFuture;
    }

  }

  private class AvailWatcher implements AvailabilityListener{
    final SettableFuture<BitConnection> future;
    
    public AvailWatcher(SettableFuture<BitConnection> future) {
      super();
      this.future = future;
    }

    @Override
    public void isAvailable(BitConnection connection) {
      future.set(connection);
    }
    
  }
  
  BitConnection getConnection(DrillbitEndpoint endpoint) throws RpcException {
    BitConnection t = registry.get(endpoint);
    if(t != null) return t;
    return this.getNode(endpoint, false).checkedGet();
  }

  
  CheckedFuture<BitConnection, RpcException> getConnectionAsync(DrillbitEndpoint endpoint) {
    return this.getNode(endpoint, true);
  }

  
  @Override
  public BitTunnel getTunnel(DrillbitEndpoint endpoint){
    BitConnection t = registry.get(endpoint);
    if(t == null){
      return new BitTunnel(exec, endpoint, this, t);
    }else{
      return new BitTunnel(exec, endpoint, this,  this.getNode(endpoint, false));
    }
  }


  /**
   * A future which remaps exceptions to a BitComException.
   * @param <T>
   */
  private class BitComFuture<T> extends AbstractCheckedFuture<T, RpcException>{

    protected BitComFuture(ListenableFuture<T> delegate) {
      super(delegate);
    }

    @Override
    protected RpcException mapException(Exception e) {
      Throwable t = e;
      if(e instanceof ExecutionException){
        t = e.getCause();
      }
      
      if(t instanceof RpcException) return (RpcException) t;
      return new RpcException(t);
    }
  }

  public void close() {
    Closeables.closeQuietly(server);
    for (BitConnection bt : registry.values()) {
      bt.shutdownIfClient();
    }
  }

  @Override
  public void registerIncomingBatchHandler(IncomingFragmentHandler handler) {
    this.handler.registerIncomingFragmentHandler(handler);
  }
  
  

}
