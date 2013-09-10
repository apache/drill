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

import io.netty.channel.Channel;

import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.work.batch.BitComHandler;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.CheckedFuture;

public class ConnectionManagerRegistry implements Iterable<BitConnectionManager>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ConnectionManagerRegistry.class);
  
  private final ConcurrentMap<DrillbitEndpoint, BitConnectionManager> registry = Maps.newConcurrentMap();
  
  private final BitComHandler handler;
  private final BootStrapContext context;
  private final ListenerPool listenerPool;
  private volatile DrillbitEndpoint localEndpoint;
  
  public ConnectionManagerRegistry(BitComHandler handler, BootStrapContext context, ListenerPool listenerPool) {
    super();
    this.handler = handler;
    this.context = context;
    this.listenerPool = listenerPool;
  }

  public BitConnectionManager getConnectionManager(DrillbitEndpoint endpoint){
    assert localEndpoint != null : "DrillbitEndpoint must be set before a connection manager can be retrieved";
    BitConnectionManager m = registry.get(endpoint);
    if(m == null){
      m = new BitConnectionManager(endpoint, localEndpoint, handler, context, listenerPool);
      BitConnectionManager m2 = registry.putIfAbsent(endpoint, m);
      if(m2 != null) m = m2;
    }
    
    return m;
  }

  @Override
  public Iterator<BitConnectionManager> iterator() {
    return registry.values().iterator();
  }
  
  public void setEndpoint(DrillbitEndpoint endpoint){
    this.localEndpoint = endpoint;
  }
  
}
