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

import io.netty.buffer.PooledByteBufAllocatorL;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.rpc.control.Controller;
import org.apache.drill.exec.rpc.control.ControllerImpl;
import org.apache.drill.exec.work.WorkEventBus;
import org.apache.drill.exec.rpc.data.DataConnectionCreator;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.work.WorkManager.WorkerBee;
import org.apache.drill.exec.work.batch.ControlMessageHandler;
import org.apache.drill.exec.work.user.UserWorker;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

public class ServiceEngine implements AutoCloseable {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ServiceEngine.class);

  private final UserServer userServer;
  private final Controller controller;
  private final DataConnectionCreator dataPool;
  private final DrillConfig config;
  boolean useIP = false;
  private final boolean allowPortHunting;
  private final BufferAllocator userAllocator;
  private final BufferAllocator controlAllocator;
  private final BufferAllocator dataAllocator;
  private final Callable<Void> transportStopper;

  public ServiceEngine(ControlMessageHandler controlMessageHandler, UserWorker userWorker, BootStrapContext context,
      WorkEventBus workBus, WorkerBee bee, boolean allowPortHunting) throws DrillbitStartupException {
    userAllocator = newAllocator(context, "rpc:user",
        "drill.exec.rpc.user.server.memory.reservation", "drill.exec.rpc.user.server.memory.maximum");
    controlAllocator = newAllocator(context, "rpc:bit-control",
        "drill.exec.rpc.bit.server.memory.control.reservation", "drill.exec.rpc.bit.server.memory.control.maximum");
    dataAllocator = newAllocator(context, "rpc:bit-data",
        "drill.exec.rpc.bit.server.memory.data.reservation", "drill.exec.rpc.bit.server.memory.data.maximum");
    this.userServer = new UserServer(context, userAllocator, userWorker);
    this.controller = new ControllerImpl(context, controlMessageHandler, controlAllocator, allowPortHunting);
    this.dataPool = new DataConnectionCreator(context, dataAllocator, workBus, bee, allowPortHunting);
    this.config = context.getConfig();
    this.allowPortHunting = allowPortHunting;
    this.transportStopper = context.getTransportStopper();
    registerMetrics(context.getMetrics());
  }

  private void registerMetrics(final MetricRegistry registry) {
    final String prefix = PooledByteBufAllocatorL.METRIC_PREFIX + "rpc.";
    DrillMetrics.register(prefix + "user.current", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return userAllocator.getAllocatedMemory();
      }
    });
    DrillMetrics.register(prefix + "user.peak", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return userAllocator.getPeakMemoryAllocation();
      }
    });
    DrillMetrics.register(prefix + "bit.control.current", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return controlAllocator.getAllocatedMemory();
      }
    });
    DrillMetrics.register(prefix + "bit.control.peak", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return controlAllocator.getPeakMemoryAllocation();
      }
    });

    DrillMetrics.register(prefix + "bit.data.current", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return dataAllocator.getAllocatedMemory();
      }
    });
    DrillMetrics.register(prefix + "bit.data.peak", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return dataAllocator.getPeakMemoryAllocation();
      }
    });
  }

  private static BufferAllocator newAllocator(
      BootStrapContext context, String name, String initReservation, String maxAllocation) {
    return context.getAllocator().newChildAllocator(
        name, context.getConfig().getLong(initReservation), context.getConfig().getLong(maxAllocation));
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

  @Override
  public void close() throws Exception {
    transportStopper.call(); // stop handling events
    AutoCloseables.close(userServer, dataPool, controller,
        userAllocator, controlAllocator, dataAllocator);
  }
}
