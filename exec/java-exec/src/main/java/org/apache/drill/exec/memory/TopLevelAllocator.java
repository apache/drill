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
package org.apache.drill.exec.memory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocatorL;
import io.netty.buffer.PooledUnsafeDirectByteBufL;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.util.AssertionUtil;

public class TopLevelAllocator implements BufferAllocator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TopLevelAllocator.class);

  private static final boolean ENABLE_ACCOUNTING = AssertionUtil.isAssertionsEnabled();
  private final Set<ChildAllocator> children;
  private final PooledByteBufAllocatorL innerAllocator = PooledByteBufAllocatorL.DEFAULT;
  private final Accountor acct;
  private final boolean errorOnLeak;

  @Deprecated
  public TopLevelAllocator() {
    this(DrillConfig.getMaxDirectMemory());
  }

  @Deprecated
  public TopLevelAllocator(long maximumAllocation) {
    this(maximumAllocation, true);
  }

  private TopLevelAllocator(long maximumAllocation, boolean errorOnLeak){
    this.errorOnLeak = errorOnLeak;
    this.acct = new Accountor(errorOnLeak, null, null, maximumAllocation, 0);
    this.children = ENABLE_ACCOUNTING ? new HashSet<ChildAllocator>() : null;
  }

  public TopLevelAllocator(DrillConfig config) {
    this(Math.min(DrillConfig.getMaxDirectMemory(), config.getLong(ExecConstants.TOP_LEVEL_MAX_ALLOC)),
        config.getBoolean(ExecConstants.ERROR_ON_MEMORY_LEAK)
        );
  }

  public AccountingByteBuf buffer(int min, int max) {
    if(!acct.reserve(min)) return null;
    ByteBuf buffer = innerAllocator.directBuffer(min, max);
    AccountingByteBuf wrapped = new AccountingByteBuf(acct, (PooledUnsafeDirectByteBufL) buffer);
    acct.reserved(min, wrapped);
    return wrapped;
  }

  @Override
  public AccountingByteBuf buffer(int size) {
    return buffer(size, size);
  }

  @Override
  public long getAllocatedMemory() {
    return acct.getAllocation();
  }

  @Override
  public ByteBufAllocator getUnderlyingAllocator() {
    return innerAllocator;
  }

  @Override
  public BufferAllocator getChildAllocator(FragmentHandle handle, long initialReservation, long maximumReservation) throws OutOfMemoryException {
    if(!acct.reserve(initialReservation)){
      throw new OutOfMemoryException(String.format("You attempted to create a new child allocator with initial reservation %d but only %d bytes of memory were available.", initialReservation, acct.getCapacity() - acct.getAllocation()));
    };
    logger.debug("New child allocator with initial reservation {}", initialReservation);
    ChildAllocator allocator = new ChildAllocator(handle, acct, maximumReservation, initialReservation);
    if(ENABLE_ACCOUNTING) children.add(allocator);
    return allocator;
  }

  @Override
  public void close() {
    if (ENABLE_ACCOUNTING) {
      for (ChildAllocator child : children) {
        if (!child.isClosed()) {
          throw new IllegalStateException("Failure while trying to close allocator: Child level allocators not closed.");
        }
      }
    }
    acct.close();
  }


  private class ChildAllocator implements BufferAllocator{

    private Accountor childAcct;
    private Map<ChildAllocator, StackTraceElement[]> children = new HashMap<>();
    private boolean closed = false;
    private FragmentHandle handle;

    public ChildAllocator(FragmentHandle handle, Accountor parentAccountor, long max, long pre) throws OutOfMemoryException{
      assert max >= pre;
      childAcct = new Accountor(errorOnLeak, handle, parentAccountor, max, pre);
      this.handle = handle;
    }

    @Override
    public AccountingByteBuf buffer(int size, int max) {
      if(!childAcct.reserve(size)){
        logger.warn("Unable to allocate buffer of size {} due to memory limit. Current allocation: {}", size, getAllocatedMemory());
        return null;
      };

      ByteBuf buffer = innerAllocator.directBuffer(size, max);
      AccountingByteBuf wrapped = new AccountingByteBuf(childAcct, (PooledUnsafeDirectByteBufL) buffer);
      childAcct.reserved(buffer.capacity(), wrapped);
      return wrapped;
    }

    public AccountingByteBuf buffer(int size) {
      return buffer(size, size);
    }

    @Override
    public ByteBufAllocator getUnderlyingAllocator() {
      return innerAllocator;
    }

    @Override
    public BufferAllocator getChildAllocator(FragmentHandle handle, long initialReservation, long maximumReservation)
        throws OutOfMemoryException {
      if(!childAcct.reserve(initialReservation)){
        throw new OutOfMemoryException(String.format("You attempted to create a new child allocator with initial reservation %d but only %d bytes of memory were available.", initialReservation, childAcct.getAvailable()));
      };
      logger.debug("New child allocator with initial reservation {}", initialReservation);
      ChildAllocator newChildAllocator = new ChildAllocator(handle, childAcct, maximumReservation, initialReservation);
      this.children.put(newChildAllocator, Thread.currentThread().getStackTrace());
      return newChildAllocator;
    }

    public PreAllocator getNewPreAllocator(){
      return new PreAlloc(this.childAcct);
    }

    @Override
    public void close() {
      if (ENABLE_ACCOUNTING) {
        for (ChildAllocator child : children.keySet()) {
          if (!child.isClosed()) {
            StringBuilder sb = new StringBuilder();
            StackTraceElement[] elements = children.get(child);
            for (int i = 3; i < elements.length; i++) {
              sb.append("\t\t");
              sb.append(elements[i]);
              sb.append("\n");
            }


            IllegalStateException e = new IllegalStateException(String.format(
                    "Failure while trying to close child allocator: Child level allocators not closed. Fragment %d:%d. Stack trace: \n %s",
                    handle.getMajorFragmentId(), handle.getMinorFragmentId(), sb.toString()));
            if(errorOnLeak){
              throw e;
            }else{
              logger.warn("Memory leak.", e);
            }
          }
        }
      }
      childAcct.close();
      closed = true;
    }

    public boolean isClosed() {
      return closed;
    }

    @Override
    public long getAllocatedMemory() {
      return childAcct.getAllocation();
    }

  }

  public PreAllocator getNewPreAllocator(){
    return new PreAlloc(this.acct);
  }

  public class PreAlloc implements PreAllocator{
    int bytes = 0;
    final Accountor acct;
    private PreAlloc(Accountor acct){
      this.acct = acct;
    }

    /**
     *
     */
    public boolean preAllocate(int bytes){

      if(!acct.reserve(bytes)){
        return false;
      }
      this.bytes += bytes;
      return true;

    }


    public AccountingByteBuf getAllocation(){
      AccountingByteBuf b = new AccountingByteBuf(acct, (PooledUnsafeDirectByteBufL) innerAllocator.buffer(bytes));
      acct.reserved(bytes, b);
      return b;
    }
  }
}
