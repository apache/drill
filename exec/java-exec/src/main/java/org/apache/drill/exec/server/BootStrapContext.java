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

import io.netty.channel.nio.NioEventLoopGroup;

import java.io.Closeable;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.rpc.NamedThreadFactory;

import com.yammer.metrics.MetricRegistry;

public class BootStrapContext implements Closeable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BootStrapContext.class);
  
  private final DrillConfig config;
  private final NioEventLoopGroup loop;
  private final MetricRegistry metrics;
  private final BufferAllocator allocator;
  
  public BootStrapContext(DrillConfig config) {
    super();
    this.config = config;
    this.loop = new NioEventLoopGroup(config.getInt(ExecConstants.BIT_SERVER_RPC_THREADS), new NamedThreadFactory("BitServer-"));
    this.metrics = new MetricRegistry(config.getString(ExecConstants.METRICS_CONTEXT_NAME));
    this.allocator = BufferAllocator.getAllocator(config);
  }

  public DrillConfig getConfig() {
    return config;
  }

  public NioEventLoopGroup getBitLoopGroup() {
    return loop;
  }

  public MetricRegistry getMetrics() {
    return metrics;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public void close(){
    loop.shutdownGracefully();
    allocator.close();
  }
  
}
