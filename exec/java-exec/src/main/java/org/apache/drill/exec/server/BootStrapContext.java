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

import io.netty.channel.EventLoopGroup;

import org.apache.drill.common.DrillAutoCloseables;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.rpc.TransportCheck;

import com.codahale.metrics.MetricRegistry;

// TODO:  Doc.  What kind of context?  (For what aspects, RPC?  What kind of data?)
public class BootStrapContext implements AutoCloseable {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BootStrapContext.class);

  private final DrillConfig config;
  private final EventLoopGroup loop;
  private final EventLoopGroup loop2;
  private final MetricRegistry metrics;
  private final BufferAllocator allocator;

  public BootStrapContext(DrillConfig config) {
    this.config = config;
    loop = TransportCheck.createEventLoopGroup(config.getInt(ExecConstants.BIT_SERVER_RPC_THREADS), "BitServer-");
    loop2 = TransportCheck.createEventLoopGroup(config.getInt(ExecConstants.BIT_SERVER_RPC_THREADS), "BitClient-");
    metrics = DrillMetrics.getInstance();
    try {
      allocator = RootAllocatorFactory.newRoot(config);
    } catch(final Exception e) {
      throw new RuntimeException("Couldn't create root allocator", e);
    }
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

  @Override
  public void close() {
    DrillMetrics.resetMetrics();
    loop.shutdownGracefully();
    DrillAutoCloseables.closeNoChecked(allocator);
  }
}
