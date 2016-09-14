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

import com.codahale.metrics.MetricRegistry;
import io.netty.channel.EventLoopGroup;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.SynchronousQueue;
import org.apache.drill.common.DrillAutoCloseables;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.rpc.NamedThreadFactory;
import org.apache.drill.exec.rpc.TransportCheck;

public class BootStrapContext implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BootStrapContext.class);
  private static final int MIN_SCAN_THREADPOOL_SIZE = 8; // Magic num

  private final DrillConfig config;
  private final EventLoopGroup loop;
  private final EventLoopGroup loop2;
  private final MetricRegistry metrics;
  private final BufferAllocator allocator;
  private final ScanResult classpathScan;
  private final ExecutorService executor;
  private final ExecutorService scanExecutor;
  private final ExecutorService scanDecodeExecutor;

  public BootStrapContext(DrillConfig config, ScanResult classpathScan) {
    this.config = config;
    this.classpathScan = classpathScan;
    this.loop = TransportCheck.createEventLoopGroup(config.getInt(ExecConstants.BIT_SERVER_RPC_THREADS), "BitServer-");
    this.loop2 = TransportCheck.createEventLoopGroup(config.getInt(ExecConstants.BIT_SERVER_RPC_THREADS), "BitClient-");
    // Note that metrics are stored in a static instance
    this.metrics = DrillMetrics.getRegistry();
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
    // Setup two threadpools one for reading raw data from disk and another for decoding the data
    // A good guideline is to have the number threads in the scan pool to be a multiple (fractional
    // numbers are ok) of the number of disks.
    // A good guideline is to have the number threads in the decode pool to be a small multiple (fractional
    // numbers are ok) of the number of cores.
    final int numCores = Runtime.getRuntime().availableProcessors();
    final int numScanThreads = (int) (config.getDouble(ExecConstants.SCAN_THREADPOOL_SIZE));
    final int numScanDecodeThreads = (int) config.getDouble(ExecConstants.SCAN_DECODE_THREADPOOL_SIZE);
    final int scanThreadPoolSize =
        MIN_SCAN_THREADPOOL_SIZE > numScanThreads ? MIN_SCAN_THREADPOOL_SIZE : numScanThreads;
    final int scanDecodeThreadPoolSize =
        (numCores + 1) / 2 > numScanDecodeThreads ? (numCores + 1) / 2 : numScanDecodeThreads;
    this.scanExecutor = Executors.newFixedThreadPool(scanThreadPoolSize, new NamedThreadFactory("scan-"));
    this.scanDecodeExecutor =
        Executors.newFixedThreadPool(scanDecodeThreadPoolSize, new NamedThreadFactory("scan-decode-"));
  }

  public ExecutorService getExecutor() {
    return executor;
  }

  public ExecutorService getScanExecutor() {
    return scanExecutor;
  }

  public ExecutorService getScanDecodeExecutor() {
    return scanDecodeExecutor;
  }

  public DrillConfig getConfig() {
    return config;
  }

  public EventLoopGroup getBitLoopGroup() {
    return loop;
  }

  public EventLoopGroup getBitClientLoopGroup() {
    return loop2;
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

  @Override
  public void close() {
    try {
      DrillMetrics.resetMetrics();
    } catch (Error | Exception e) {
      logger.warn("failure resetting metrics.", e);
    }

    if (executor != null) {
      executor.shutdown(); // Disable new tasks from being submitted
      try {
        // Wait a while for existing tasks to terminate
        if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
          executor.shutdownNow(); // Cancel currently executing tasks
          // Wait a while for tasks to respond to being cancelled
          if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
            logger.error("Pool did not terminate");
          }
        }
      } catch (InterruptedException ie) {
        logger.warn("Executor interrupted while awaiting termination");

        // (Re-)Cancel if current thread also interrupted
        executor.shutdownNow();
        // Preserve interrupt status
        Thread.currentThread().interrupt();
      }
    }

    DrillAutoCloseables.closeNoChecked(allocator);
  }
}
