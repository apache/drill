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

import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.work.batch.ControlMessageHandler;

import com.google.common.collect.Maps;

/**
 * Registry of {@link ControlConnectionManager connection managers} that maintain control connections to other bits.
 */
public class ConnectionManagerRegistry implements Iterable<ControlConnectionManager> {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ConnectionManagerRegistry.class);

  private final ConcurrentMap<DrillbitEndpoint, ControlConnectionManager> registry = Maps.newConcurrentMap();

  private final ControlMessageHandler handler;
  private final BootStrapContext context;
  private volatile DrillbitEndpoint localEndpoint;
  private final BufferAllocator allocator;

  public ConnectionManagerRegistry(BufferAllocator allocator, ControlMessageHandler handler,
                                   BootStrapContext context) {
    this.handler = handler;
    this.context = context;
    this.allocator = allocator;
  }

  /**
   * Gets the connection manager of the control connection to the given remote endpoint, creates a new manager
   * if one does not exist.
   *
   * @param remoteEndpoint remote drillbit endpoint
   * @return connection manager
   */
  public ControlConnectionManager getOrCreateConnectionManager(DrillbitEndpoint remoteEndpoint) {
    assert localEndpoint != null : "DrillbitEndpoint must be set before a connection manager can be retrieved";
    ControlConnectionManager manager = registry.get(remoteEndpoint);
    if (manager == null) {
      manager = new ControlConnectionManager(allocator, remoteEndpoint, localEndpoint, handler, context);
      final ControlConnectionManager otherManager = registry.putIfAbsent(remoteEndpoint, manager);
      if (otherManager != null) {
        manager.close();
        manager = otherManager;
      }
    }

    return manager;
  }

  @Override
  public Iterator<ControlConnectionManager> iterator() {
    return registry.values().iterator();
  }

  public void setEndpoint(DrillbitEndpoint endpoint) {
    this.localEndpoint = endpoint;
  }

}
