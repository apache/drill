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

import com.google.common.io.Closeables;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.BufferAllocator;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.cache.DistributedCache;
import org.apache.drill.exec.cache.HazelCache;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.ClusterCoordinator.RegistrationHandle;
import org.apache.drill.exec.coord.ZKClusterCoordinator;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.service.ServiceEngine;

import java.net.InetAddress;

public class Drillbit {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Drillbit.class);

  public static Drillbit start(StartupOptions options) throws DrillbitStartupException {
    Drillbit bit = null;
    try {
      logger.debug("Setting up Drillbit.");
      DrillConfig config = DrillConfig.create(options.getConfigLocation());
      bit = new Drillbit(config);
    } catch (Exception ex) {
      throw new DrillbitStartupException("Failure while initializing values in Drillbit.", ex);
    }


    try {
      logger.debug("Starting Drillbit.");
      bit.run();
    } catch (Exception e) {
      throw new DrillbitStartupException("Failure during initial startup of Drillbit.", e);
    }
    return bit;
  }

  public static void main(String[] cli) throws DrillbitStartupException {
    StartupOptions options = StartupOptions.parse(cli);
    start(options);
  }

  private final DrillbitContext context;
  final BufferAllocator pool;
  final ClusterCoordinator coord;
  final ServiceEngine engine;
  final DistributedCache cache;
  private RegistrationHandle handle;

  public Drillbit(DrillConfig config) throws Exception {
    final DrillbitContext context = new DrillbitContext(config, this);
    Runtime.getRuntime().addShutdownHook(new ShutdownThread(config));
    this.context = context;
    this.pool = BufferAllocator.getAllocator(context);
    this.coord = new ZKClusterCoordinator(config);
    this.engine = new ServiceEngine(context);
    this.cache = new HazelCache(context.getConfig());
  }

  public void run() throws Exception {
    coord.start();
    engine.start();
    
    DrillbitEndpoint md = DrillbitEndpoint.newBuilder().setAddress(InetAddress.getLocalHost().getHostAddress())
        .setBitPort(engine.getBitPort()).setUserPort(engine.getUserPort()).build();
    handle = coord.register(md);
    cache.run(md);
  }

  public void close() {
    if (coord != null) coord.unregister(handle);

    try {
      Thread.sleep(context.getConfig().getInt(ExecConstants.ZK_REFRESH) * 2);
    } catch (InterruptedException e) {
      logger.warn("Interrupted while sleeping during coordination deregistration.");
    }

    Closeables.closeQuietly(engine);
    Closeables.closeQuietly(coord);
    Closeables.closeQuietly(pool);
    logger.info("Shutdown completed.");
  }

  private class ShutdownThread extends Thread {
    ShutdownThread(DrillConfig config) {
      this.setName("ShutdownHook");
    }

    @Override
    public void run() {
      logger.info("Received shutdown request.");
      close();
    }

  }

}
