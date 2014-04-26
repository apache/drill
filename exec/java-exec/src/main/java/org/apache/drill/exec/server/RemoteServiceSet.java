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
package org.apache.drill.exec.server;

import java.io.Closeable;
import java.io.IOException;

import org.apache.drill.exec.cache.DistributedCache;
import org.apache.drill.exec.cache.LocalCache;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.LocalClusterCoordinator;
import org.apache.drill.exec.exception.DrillbitStartupException;

public class RemoteServiceSet implements Closeable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RemoteServiceSet.class);
  
  private final DistributedCache cache;
  private final ClusterCoordinator coordinator;
  
  public RemoteServiceSet(DistributedCache cache, ClusterCoordinator coordinator) {
    super();
    this.cache = cache;
    this.coordinator = coordinator;
  }


  public DistributedCache getCache() {
    return cache;
  }

  public ClusterCoordinator getCoordinator() {
    return coordinator;
  }
  
  
  @Override
  public void close() throws IOException {
    cache.close();
    coordinator.close();
  }

  public static RemoteServiceSet getLocalServiceSet(){
    return new RemoteServiceSet(new LocalCache(), new LocalClusterCoordinator());
  }
  
}
