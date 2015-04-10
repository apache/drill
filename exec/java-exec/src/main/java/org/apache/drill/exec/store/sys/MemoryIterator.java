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
package org.apache.drill.exec.store.sys;


import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.Iterator;

public class MemoryIterator implements Iterator<Object> {

  private boolean beforeFirst = true;
  private final FragmentContext context;

  public MemoryIterator(final FragmentContext context) {
    this.context = context;
  }

  @Override
  public boolean hasNext() {
    return beforeFirst;
  }

  @Override
  public Object next() {
    if (!beforeFirst) {
      throw new IllegalStateException();
    }
    beforeFirst = false;
    final MemoryInfo memoryInfo = new MemoryInfo();

    final DrillbitEndpoint endpoint = context.getIdentity();
    memoryInfo.hostname = endpoint.getAddress();
    memoryInfo.user_port = endpoint.getUserPort();

    final MemoryUsage heapMemoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    memoryInfo.heap_current = heapMemoryUsage.getUsed();
    memoryInfo.heap_max = heapMemoryUsage.getMax();

    memoryInfo.direct_current = context.getDrillbitContext().getAllocator().getAllocatedMemory();
    memoryInfo.direct_max = TopLevelAllocator.MAXIMUM_DIRECT_MEMORY;
    return memoryInfo;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  public static class MemoryInfo {
    public String hostname;
    public long user_port;
    public long heap_current;
    public long heap_max;
    public long direct_current;
    public long direct_max;
  }
}
