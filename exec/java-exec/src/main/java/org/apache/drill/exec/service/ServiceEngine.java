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
package org.apache.drill.exec.service;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.netty.channel.EventLoopGroup;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.rpc.TransportCheck;
import org.apache.drill.exec.rpc.control.Controller;
import org.apache.drill.exec.rpc.control.ControllerImpl;
import org.apache.drill.exec.rpc.control.WorkEventBus;
import org.apache.drill.exec.rpc.data.DataConnectionCreator;
import org.apache.drill.exec.rpc.data.DataResponseHandler;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.work.batch.ControlMessageHandler;
import org.apache.drill.exec.work.user.UserWorker;

import com.google.common.base.Stopwatch;
import com.google.common.io.Closeables;

public class ServiceEngine implements Closeable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ServiceEngine.class);

  private final UserServer userServer;
  private final Controller controller;
  private final DataConnectionCreator dataPool;
  private final DrillConfig config;
  boolean useIP = false;
  private final boolean allowPortHunting;

  public ServiceEngine(ControlMessageHandler controlMessageHandler, UserWorker userWorker, BootStrapContext context,
      WorkEventBus workBus, DataResponseHandler dataHandler, boolean allowPortHunting) throws DrillbitStartupException {
    final EventLoopGroup eventLoopGroup = TransportCheck.createEventLoopGroup(
        context.getConfig().getInt(ExecConstants.USER_SERVER_RPC_THREADS), "UserServer-");
    this.userServer = new UserServer(context.getConfig(), context.getClasspathScan(), context.getAllocator(), eventLoopGroup, userWorker);
    this.controller = new ControllerImpl(context, controlMessageHandler, allowPortHunting);
    this.dataPool = new DataConnectionCreator(context, workBus, dataHandler, allowPortHunting);
    this.config = context.getConfig();
    this.allowPortHunting = allowPortHunting;
  }

  public DrillbitEndpoint start() throws DrillbitStartupException, UnknownHostException{
    int userPort = userServer.bind(config.getInt(ExecConstants.INITIAL_USER_PORT), allowPortHunting);
    String address = useIP ?  InetAddress.getLocalHost().getHostAddress() : InetAddress.getLocalHost().getCanonicalHostName();
    DrillbitEndpoint partialEndpoint = DrillbitEndpoint.newBuilder()
        .setAddress(address)
        //.setAddress("localhost")
        .setUserPort(userPort)
        .build();

    partialEndpoint = controller.start(partialEndpoint);
    return dataPool.start(partialEndpoint);
  }

  public DataConnectionCreator getDataConnectionCreator(){
    return dataPool;
  }

  public Controller getController() {
    return controller;
  }

  private void submit(ExecutorService p, final String name, final Closeable c) {
    p.submit(new Runnable() {
      @Override
      public void run() {
        Stopwatch watch = new Stopwatch().start();
        Closeables.closeQuietly(c);
        long elapsed = watch.elapsed(MILLISECONDS);
        if (elapsed > 500) {
          logger.info("closed " + name + " in " + elapsed + " ms");
        }
      }
    });
  }

  @Override
  public void close() throws IOException {
    // this takes time so close them in parallel
    // Ideally though we fix this netty bug: https://github.com/netty/netty/issues/2545
    ExecutorService p = Executors.newFixedThreadPool(2);
    submit(p, "userServer", userServer);
    submit(p, "dataPool", dataPool);
    submit(p, "controller", controller);
    p.shutdown();
    try {
      p.awaitTermination(3, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
