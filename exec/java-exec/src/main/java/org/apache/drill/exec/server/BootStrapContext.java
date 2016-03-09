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

import com.google.common.util.concurrent.MoreExecutors;
import com.typesafe.config.ConfigException;
import io.netty.channel.EventLoopGroup;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.rpc.NamedThreadFactory;
import org.apache.drill.exec.rpc.TransportCheck;

import com.codahale.metrics.MetricRegistry;

public class BootStrapContext implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BootStrapContext.class);

  public static final String CONTROL_SERVER_RPC_THREADS = "drill.exec.rpc.bit.server.control.threads";
  public static final String DATA_SERVER_RPC_THREADS = "drill.exec.rpc.bit.server.data.threads";

  private final DrillConfig config;
  private final EventLoopGroup controlLoopGroup;
  private final EventLoopGroup dataLoopGroup;
  private final EventLoopGroup userLoopGroup;
  private final MetricRegistry metrics;
  private final BufferAllocator allocator;
  private final ScanResult classpathScan;
  private final ExecutorService executor;

  public BootStrapContext(DrillConfig config, ScanResult classpathScan) {
    this.config = config;
    this.classpathScan = classpathScan;

    // for backward compatibility
    int numControlThreads;
    try {
      numControlThreads = config.getInt(ExecConstants.BIT_SERVER_RPC_THREADS);
      logger.warn("Config option '" + ExecConstants.BIT_SERVER_RPC_THREADS + "' is deprecated, and will be removed." +
          " Please start using '" + CONTROL_SERVER_RPC_THREADS + "' to set the size of the control loop group.");
    } catch (ConfigException.Missing e) {
      numControlThreads = config.getInt(CONTROL_SERVER_RPC_THREADS);
    }
    this.controlLoopGroup = TransportCheck.createEventLoopGroup(numControlThreads, "control-loop-");

    // for backward compatibility
    int numDataThreads;
    try {
      numDataThreads = config.getInt(ExecConstants.BIT_SERVER_RPC_THREADS);
      logger.warn("Config option '" + ExecConstants.BIT_SERVER_RPC_THREADS + "' is deprecated, and will be removed." +
          " Please start using '" + DATA_SERVER_RPC_THREADS + "' to set the size of the data loop group.");
    } catch (ConfigException.Missing e) {
      numDataThreads = config.getInt(DATA_SERVER_RPC_THREADS);
    }
    this.dataLoopGroup = TransportCheck.createEventLoopGroup(numDataThreads, "data-loop-");

    this.userLoopGroup = TransportCheck.createEventLoopGroup(config.getInt(ExecConstants.USER_SERVER_RPC_THREADS),
        "user-loop-");

    this.metrics = DrillMetrics.getInstance();
    this.allocator = RootAllocatorFactory.newRoot(config);

    this.executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(),
        new NamedThreadFactory("drill-executor-")) {
      @Override
      protected void afterExecute(final Runnable r, final Throwable t) {
        if (t != null) {
          logger.error("{}.run() leaked an exception.", r.getClass().getName(), t);
        }
        super.afterExecute(r, t);
      }
    };
  }

  public ExecutorService getExecutor() {
    return executor;
  }

  public DrillConfig getConfig() {
    return config;
  }

  public EventLoopGroup getControlLoopGroup() {
    return controlLoopGroup;
  }

  public EventLoopGroup getDataLoopGroup() {
    return dataLoopGroup;
  }

  public EventLoopGroup getUserLoopGroup() {
    return userLoopGroup;
  }

  public MetricRegistry getMetrics() {
    return metrics;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public ScanResult getClasspathScan() {
    return classpathScan;
  }

  /**
   * The callable returned is meant to be {@link Callable#call called} once within
   * {@link org.apache.drill.exec.service.ServiceEngine#close}.
   *
   * @return a callable that can stop all transport
   */
  public Callable<Void> getTransportStopper() {
    return new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        shutDownLoopGroups();
        return null;
      }
    };
  }

  private void shutDownLoopGroups() throws Exception {
    // Shutting down in parallel as a {@link io.netty.channel.nio.NioEventLoopGroup#shutdownGracefully}
    // takes a second (Netty issue: https://github.com/netty/netty/issues/2545).
    final ExecutorService executor = Executors.newFixedThreadPool(3);
    shutDownGroup(executor, "data loop group", dataLoopGroup);
    shutDownGroup(executor, "control loop group", controlLoopGroup);
    shutDownGroup(executor, "user loop group", userLoopGroup);
    executor.shutdown();
    try {
      executor.awaitTermination(3, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.warn("Interrupted while closing event loop groups.", e);
    }
  }

  private static void shutDownGroup(final ExecutorService executor, final String name,
                                    final EventLoopGroup eventLoopGroup) {
    executor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          TransportCheck.shutDownEventLoopGroup(eventLoopGroup, name, logger);
        } catch (Exception e) {
          logger.warn("Failure while closing {}.", name, e);
        }
      }
    });
  }

  @Override
  public void close() {
    try {
      DrillMetrics.resetMetrics();
    } catch (Error | Exception e) {
      logger.warn("failure resetting metrics.", e);
    }

    if (executor != null && !MoreExecutors.shutdownAndAwaitTermination(executor, 2, TimeUnit.SECONDS)) {
      logger.error("Worker pool did not terminate.");
    }

    AutoCloseables.closeNoChecked(allocator);
  }
}
