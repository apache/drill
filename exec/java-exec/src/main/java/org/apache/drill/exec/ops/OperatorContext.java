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

import java.util.Iterator;

import org.apache.drill.common.util.Hook.Closeable;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.physical.base.PhysicalOperator;

public class OperatorContext implements Closeable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OperatorContext.class);

  private final BufferAllocator allocator;
  private boolean closed = false;
  private PhysicalOperator popConfig;
  private OperatorStats stats;

  public OperatorContext(PhysicalOperator popConfig, FragmentContext context) throws OutOfMemoryException {
    this.allocator = context.getNewChildAllocator(popConfig.getInitialAllocation(), popConfig.getMaxAllocation());
    this.popConfig = popConfig;

    OpProfileDef def = new OpProfileDef(popConfig.getOperatorId(), popConfig.getOperatorType(), getChildCount(popConfig));
    this.stats = context.getStats().getOperatorStats(def, allocator);
  }

  public OperatorContext(PhysicalOperator popConfig, FragmentContext context, OperatorStats stats) throws OutOfMemoryException {
    this.allocator = context.getNewChildAllocator(popConfig.getInitialAllocation(), popConfig.getMaxAllocation());
    this.popConfig = popConfig;
    this.stats     = stats;
  }

  public static int getChildCount(PhysicalOperator popConfig){
    Iterator<PhysicalOperator> iter = popConfig.iterator();
    int i = 0;
    while(iter.hasNext()){
      iter.next();
      i++;
    }

    if(i == 0) i = 1;
    return i;
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
    if (allocator != null) {
      allocator.close();
    }
    closed = true;
  }

  public OperatorStats getStats(){
    return stats;
  }
}
