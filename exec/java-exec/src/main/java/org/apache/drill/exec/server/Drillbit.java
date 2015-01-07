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
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.ClusterCoordinator.RegistrationHandle;
import org.apache.drill.exec.coord.zk.ZKClusterCoordinator;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.server.rest.DrillRestServer;
import org.apache.drill.exec.service.ServiceEngine;
import org.apache.drill.exec.store.sys.CachingStoreProvider;
import org.apache.drill.exec.store.sys.PStoreProvider;
import org.apache.drill.exec.store.sys.PStoreRegistry;
import org.apache.drill.exec.store.sys.local.LocalPStoreProvider;
import org.apache.drill.exec.work.WorkManager;
import org.apache.zookeeper.Environment;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.glassfish.jersey.servlet.ServletContainer;

import com.codahale.metrics.servlets.MetricsServlet;
import com.codahale.metrics.servlets.ThreadDumpServlet;
import com.google.common.io.Closeables;

/**
 * Starts, tracks and stops all the required services for a Drillbit daemon to work.
 */
public class Drillbit implements Closeable{
  static final org.slf4j.Logger logger;
  static {
    logger = org.slf4j.LoggerFactory.getLogger(Drillbit.class);
    Environment.logEnv("Drillbit environment:.", logger);
  }

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
      bit.close();
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
  final PStoreProvider storeProvider;
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

    if(enableHttp) {
      this.embeddedJetty = new Server(config.getInt(ExecConstants.HTTP_PORT));
    } else {
      this.embeddedJetty = null;
    }

    if(serviceSet != null) {
      this.coord = serviceSet.getCoordinator();
      this.storeProvider = new CachingStoreProvider(new LocalPStoreProvider(config));
    } else {
      Runtime.getRuntime().addShutdownHook(new ShutdownThread(config));
      this.coord = new ZKClusterCoordinator(config);
      this.storeProvider = new PStoreRegistry(this.coord, config).newPStoreProvider();
    }
  }

  private void startJetty() throws Exception{
    if (embeddedJetty == null) {
      return;
    }

    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);

    ErrorHandler errorHandler = new ErrorHandler();
    errorHandler.setShowStacks(true);
    errorHandler.setShowMessageInTitle(true);
    context.setErrorHandler(errorHandler);
    context.setContextPath("/");
    embeddedJetty.setHandler(context);
    ServletHolder h = new ServletHolder(new ServletContainer(new DrillRestServer(manager)));
//    h.setInitParameter(ServerProperties.PROVIDER_PACKAGES, "org.apache.drill.exec.server");
    h.setInitOrder(1);
    context.addServlet(h, "/*");
    context.addServlet(new ServletHolder(new MetricsServlet(this.context.getMetrics())), "/status/metrics");
    context.addServlet(new ServletHolder(new ThreadDumpServlet()), "/status/threads");

    ServletHolder staticHolder = new ServletHolder("static", DefaultServlet.class);
    staticHolder.setInitParameter("resourceBase", Resource.newClassPathResource("/rest/static").toString());
    staticHolder.setInitParameter("dirAllowed","false");
    staticHolder.setInitParameter("pathInfoOnly","true");
    context.addServlet(staticHolder,"/static/*");

    embeddedJetty.start();

  }



  public void run() throws Exception {
    coord.start(10000);
    storeProvider.start();
    DrillbitEndpoint md = engine.start();
    manager.start(md, engine.getController(), engine.getDataConnectionCreator(), coord, storeProvider);
    manager.getContext().getStorage().init();
    manager.getContext().getOptionManager().init();
    handle = coord.register(md);
    startJetty();
  }

  public void close() {
    if (coord != null && handle != null) {
      coord.unregister(handle);
    }

    try {
      Thread.sleep(context.getConfig().getInt(ExecConstants.ZK_REFRESH) * 2);
    } catch (InterruptedException e) {
      logger.warn("Interrupted while sleeping during coordination deregistration.");
    }
    try {
      if (embeddedJetty != null) {
        embeddedJetty.stop();
      }
    } catch (Exception e) {
      logger.warn("Failure while shutting down embedded jetty server.");
    }
    Closeables.closeQuietly(engine);
    try{
      storeProvider.close();
    }catch(Exception e){
      logger.warn("Failure while closing store provider.", e);
    }
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
  public ClusterCoordinator getCoordinator() {
    return coord;
  }

  public DrillbitContext getContext() {
    return this.manager.getContext();
  }

}
