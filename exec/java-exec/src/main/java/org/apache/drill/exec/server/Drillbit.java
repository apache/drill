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

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.cache.DistributedCache;
import org.apache.drill.exec.cache.infinispan.ICache;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.ClusterCoordinator.RegistrationHandle;
import org.apache.drill.exec.coord.zk.ZKClusterCoordinator;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.server.rest.DrillRestServer;
import org.apache.drill.exec.service.ServiceEngine;
import org.apache.drill.exec.store.sys.TableProvider;
import org.apache.drill.exec.store.sys.local.LocalTableProvider;
import org.apache.drill.exec.store.sys.zk.ZkTableProvider;
import org.apache.drill.exec.work.WorkManager;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;

import com.google.common.io.Closeables;

/**
 * Starts, tracks and stops all the required services for a Drillbit daemon to work.
 */
public class Drillbit implements Closeable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Drillbit.class);

  public static Drillbit start(StartupOptions options) throws DrillbitStartupException {
    return start(DrillConfig.create(options.getConfigLocation()));
  }

  public static Drillbit start(DrillConfig config) throws DrillbitStartupException {
    Drillbit bit;
    try {
      logger.debug("Setting up Drillbit.");
      bit = new Drillbit(config, null);
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

  final ClusterCoordinator coord;
  final ServiceEngine engine;
  final DistributedCache cache;
  final TableProvider tableProvider;
  final WorkManager manager;
  final BootStrapContext context;
  final Server embeddedJetty;

  private volatile RegistrationHandle handle;

  public Drillbit(DrillConfig config, RemoteServiceSet serviceSet) throws Exception {
    boolean allowPortHunting = serviceSet != null;
    boolean enableHttp = config.getBoolean(ExecConstants.HTTP_ENABLE);
    this.context = new BootStrapContext(config);
    this.manager = new WorkManager(context);
    this.engine = new ServiceEngine(manager.getControlMessageHandler(), manager.getUserWorker(), context, manager.getWorkBus(), manager.getDataHandler(), allowPortHunting);

    if(enableHttp){
      this.embeddedJetty = new Server(config.getInt(ExecConstants.HTTP_PORT));
    }else{
      this.embeddedJetty = null;
    }


    if(serviceSet != null){
      this.coord = serviceSet.getCoordinator();
      this.cache = serviceSet.getCache();
      this.tableProvider = new LocalTableProvider(config);
    }else{
      Runtime.getRuntime().addShutdownHook(new ShutdownThread(config));
      this.coord = new ZKClusterCoordinator(config);
      this.tableProvider = new ZkTableProvider(((ZKClusterCoordinator)coord).getCurator());
      this.cache = new ICache(config, context.getAllocator(), false);
    }
  }

  private void startJetty() throws Exception{
    if(embeddedJetty == null) return;

    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.setContextPath("/");
    embeddedJetty.setHandler(context);
    ServletHolder h = new ServletHolder(new ServletContainer(new DrillRestServer(manager)));
//    h.setInitParameter(ServerProperties.PROVIDER_PACKAGES, "org.apache.drill.exec.server");
    h.setInitOrder(1);
    context.addServlet(h, "/*");
    embeddedJetty.start();
  }



  public void run() throws Exception {
    coord.start(10000);
    tableProvider.start();
    DrillbitEndpoint md = engine.start();
    manager.start(md, cache, engine.getController(), engine.getDataConnectionCreator(), coord, tableProvider);
    cache.run();
    manager.getContext().getStorage().init();
    manager.getContext().getOptionManager().init();
    handle = coord.register(md);
    startJetty();
  }

  public void close() {
    if (coord != null) coord.unregister(handle);

    try {
      Thread.sleep(context.getConfig().getInt(ExecConstants.ZK_REFRESH) * 2);
    } catch (InterruptedException e) {
      logger.warn("Interrupted while sleeping during coordination deregistration.");
    }
    try {
      if(embeddedJetty != null) embeddedJetty.stop();
    } catch (Exception e) {
      logger.warn("Failure while shutting down embedded jetty server.");
    }
    Closeables.closeQuietly(engine);
    Closeables.closeQuietly(tableProvider);
    Closeables.closeQuietly(coord);
    Closeables.closeQuietly(manager);
    Closeables.closeQuietly(context);
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
  public ClusterCoordinator getCoordinator(){
    return coord;
  }

  public DrillbitContext getContext(){
    return this.manager.getContext();
  }
}
