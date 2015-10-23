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
package org.apache.drill.exec.ops;

import com.google.common.base.Preconditions;
import io.netty.buffer.DrillBuf;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.physical.base.PhysicalOperator;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import org.apache.drill.exec.testing.ExecutionControls;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

class OperatorContextImpl extends OperatorContext implements AutoCloseable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OperatorContextImpl.class);

  private final BufferAllocator allocator;
  private final ExecutionControls executionControls;
  private boolean closed = false;
  private PhysicalOperator popConfig;
  private OperatorStats stats;
  private LongObjectOpenHashMap<DrillBuf> managedBuffers = new LongObjectOpenHashMap<>();
  private final boolean applyFragmentLimit;
  private DrillFileSystem fs;

  public OperatorContextImpl(PhysicalOperator popConfig, FragmentContext context, boolean applyFragmentLimit) throws OutOfMemoryException {
    this.applyFragmentLimit=applyFragmentLimit;
    this.allocator = context.getNewChildAllocator(popConfig.getInitialAllocation(), popConfig.getMaxAllocation(), applyFragmentLimit);
    this.popConfig = popConfig;

    OpProfileDef def = new OpProfileDef(popConfig.getOperatorId(), popConfig.getOperatorType(), getChildCount(popConfig));
    stats = context.getStats().newOperatorStats(def, allocator);
    executionControls = context.getExecutionControls();
  }

  public OperatorContextImpl(PhysicalOperator popConfig, FragmentContext context, OperatorStats stats, boolean applyFragmentLimit) throws OutOfMemoryException {
    this.applyFragmentLimit=applyFragmentLimit;
    this.allocator = context.getNewChildAllocator(popConfig.getInitialAllocation(), popConfig.getMaxAllocation(), applyFragmentLimit);
    this.popConfig = popConfig;
    this.stats     = stats;
    executionControls = context.getExecutionControls();
  }

  public DrillBuf replace(DrillBuf old, int newSize) {
    if (managedBuffers.remove(old.memoryAddress()) == null) {
      throw new IllegalStateException("Tried to remove unmanaged buffer.");
    }
    old.release();
    return getManagedBuffer(newSize);
  }

  public DrillBuf getManagedBuffer() {
    return getManagedBuffer(256);
  }

  public DrillBuf getManagedBuffer(int size) {
    DrillBuf newBuf = allocator.buffer(size);
    managedBuffers.put(newBuf.memoryAddress(), newBuf);
    newBuf.setOperatorContext(this);
    return newBuf;
  }

  public ExecutionControls getExecutionControls() {
    return executionControls;
  }

  public BufferAllocator getAllocator() {
    if (allocator == null) {
      throw new UnsupportedOperationException("Operator context does not have an allocator");
    }
    return allocator;
  }

  public boolean isClosed() {
    return closed;
  }

  @Override
  public void close() {
    if (closed) {
      logger.debug("Attempted to close Operator context for {}, but context is already closed", popConfig != null ? popConfig.getClass().getName() : null);
      return;
    }
    logger.debug("Closing context for {}", popConfig != null ? popConfig.getClass().getName() : null);

    // release managed buffers.
    Object[] buffers = ((LongObjectOpenHashMap<Object>)(Object)managedBuffers).values;
    for (int i =0; i < buffers.length; i++) {
      if (managedBuffers.allocated[i]) {
        ((DrillBuf)buffers[i]).release();
      }
    }

    if (allocator != null) {
      allocator.close();
    }

    if (fs != null) {
      try {
        fs.close();
      } catch (IOException e) {
        throw new DrillRuntimeException(e);
      }
    }
    closed = true;
  }

  public OperatorStats getStats() {
    return stats;
  }

  @Override
  public DrillFileSystem newFileSystem(Configuration conf) throws IOException {
    Preconditions.checkState(fs == null, "Tried to create a second FileSystem. Can only be called once per OperatorContext");
    fs = new DrillFileSystem(conf, getStats());
    return fs;
  }

}
