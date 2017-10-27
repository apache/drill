/*
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
package org.apache.drill.exec.ops;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.testing.ExecutionControls;

import io.netty.buffer.DrillBuf;

/**
 * Implementation of {@link OperatorExecContext} that provides services
 * needed by most run-time operators. Excludes services that need the
 * entire Drillbit. Allows easy testing of operator code that uses this
 * interface.
 */

public class AbstractOperatorExecContext implements OperatorExecContext {

  protected final BufferAllocator allocator;
  protected final ExecutionControls executionControls;
  protected final PhysicalOperator popConfig;
  protected final BufferManager manager;
  protected OperatorStatReceiver statsWriter;

  public AbstractOperatorExecContext(BufferAllocator allocator, PhysicalOperator popConfig,
                                     ExecutionControls executionControls,
                                     OperatorStatReceiver stats) {
    this.allocator = allocator;
    this.popConfig = popConfig;
    this.manager = new BufferManagerImpl(allocator);
    statsWriter = stats;

    this.executionControls = executionControls;
  }

  @Override
  public DrillBuf replace(DrillBuf old, int newSize) {
    return manager.replace(old, newSize);
  }

  @Override
  public DrillBuf getManagedBuffer() {
    return manager.getManagedBuffer();
  }

  @Override
  public DrillBuf getManagedBuffer(int size) {
    return manager.getManagedBuffer(size);
  }

  @Override
  public ExecutionControls getExecutionControls() {
    return executionControls;
  }

  @Override
  public BufferAllocator getAllocator() {
    if (allocator == null) {
      throw new UnsupportedOperationException("Operator context does not have an allocator");
    }
    return allocator;
  }

  @Override
  public void close() {
    try {
      manager.close();
    } finally {
      if (allocator != null) {
        allocator.close();
      }
    }
  }

  @Override
  public OperatorStatReceiver getStatsWriter() {
    return statsWriter;
  }
}
