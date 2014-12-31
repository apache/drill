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

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.DrillBuf;
import io.netty.buffer.PooledByteBufAllocatorL;
import io.netty.buffer.UnsafeDirectLittleEndian;

import java.util.IdentityHashMap;
import java.util.HashMap;
import java.util.Map;

import java.util.Map.Entry;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;

import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.util.AssertionUtil;

public class TopLevelAllocator implements BufferAllocator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TopLevelAllocator.class);

  private static final boolean ENABLE_ACCOUNTING = AssertionUtil.isAssertionsEnabled();
  private final Map<ChildAllocator, StackTraceElement[]> childrenMap;
  private final PooledByteBufAllocatorL innerAllocator = PooledByteBufAllocatorL.DEFAULT;
  private final Accountor acct;
  private final boolean errorOnLeak;
  private final DrillBuf empty;
  private final DrillConfig config;

  @Deprecated
  public TopLevelAllocator() {
    this(DrillConfig.getMaxDirectMemory());
  }

  @Deprecated
  public TopLevelAllocator(long maximumAllocation) {
    this(null, maximumAllocation, true);
  }

  private TopLevelAllocator(DrillConfig config, long maximumAllocation, boolean errorOnLeak){
    this.config=(config!=null) ? config : DrillConfig.create();
    this.errorOnLeak = errorOnLeak;
    this.acct = new Accountor(config, errorOnLeak, null, null, maximumAllocation, 0, true);
    this.empty = DrillBuf.getEmpty(this, acct);
    this.childrenMap = ENABLE_ACCOUNTING ? new IdentityHashMap<ChildAllocator, StackTraceElement[]>() : null;
  }

  public TopLevelAllocator(DrillConfig config) {
    this(config, Math.min(DrillConfig.getMaxDirectMemory(), config.getLong(ExecConstants.TOP_LEVEL_MAX_ALLOC)),
        config.getBoolean(ExecConstants.ERROR_ON_MEMORY_LEAK)
        );
  }

  @Override
  public boolean takeOwnership(DrillBuf buf) {
    return buf.transferAccounting(acct);
  }

  public DrillBuf buffer(int min, int max) {
    if (min == 0) {
      return empty;
    }
    if(!acct.reserve(min)) {
      return null;
    }
    UnsafeDirectLittleEndian buffer = innerAllocator.directBuffer(min, max);
    DrillBuf wrapped = new DrillBuf(this, acct, buffer);
    acct.reserved(min, wrapped);
    return wrapped;
  }

  @Override
  public DrillBuf buffer(int size) {
    return buffer(size, size);
  }

  @Override
  public long getAllocatedMemory() {
    return acct.getAllocation();
  }

  @Override
  public long getPeakMemoryAllocation() {
    return acct.getPeakMemoryAllocation();
  }

  @Override
  public ByteBufAllocator getUnderlyingAllocator() {
    return innerAllocator;
  }

  @Override
  public BufferAllocator getChildAllocator(FragmentContext context, long initialReservation, long maximumReservation, boolean applyFragmentLimit) throws OutOfMemoryException {
    if(!acct.reserve(initialReservation)){
      logger.debug(String.format("You attempted to create a new child allocator with initial reservation %d but only %d bytes of memory were available.", initialReservation, acct.getCapacity() - acct.getAllocation()));
      throw new OutOfMemoryException(String.format("You attempted to create a new child allocator with initial reservation %d but only %d bytes of memory were available.", initialReservation, acct.getCapacity() - acct.getAllocation()));
    };
    logger.debug("New child allocator with initial reservation {}", initialReservation);
    ChildAllocator allocator = new ChildAllocator(context, acct, maximumReservation, initialReservation, childrenMap, applyFragmentLimit);
    if(ENABLE_ACCOUNTING){
      childrenMap.put(allocator, Thread.currentThread().getStackTrace());
    }

    return allocator;
  }

  @Override
  public void resetFragmentLimits() {
    acct.resetFragmentLimits();
  }

  @Override
  public void setFragmentLimit(long limit){
    acct.setFragmentLimit(limit);
  }

  @Override
  public long getFragmentLimit(){
    return acct.getFragmentLimit();
  }

  @Override
  public void close() {
    if (ENABLE_ACCOUNTING) {
      for (Entry<ChildAllocator, StackTraceElement[]> child : childrenMap.entrySet()) {
        if (!child.getKey().isClosed()) {
          StringBuilder sb = new StringBuilder();
          StackTraceElement[] elements = child.getValue();
          for (int i = 0; i < elements.length; i++) {
            sb.append("\t\t");
            sb.append(elements[i]);
            sb.append("\n");
          }
          throw new IllegalStateException("Failure while trying to close allocator: Child level allocators not closed. Stack trace: \n" + sb);
        }
      }
    }
    acct.close();
  }



  @Override
  public DrillBuf getEmpty() {
    return empty;
  }



  private class ChildAllocator implements BufferAllocator{
    private final DrillBuf empty;
    private Accountor childAcct;
    private Map<ChildAllocator, StackTraceElement[]> children = new HashMap<>();
    private boolean closed = false;
    private FragmentHandle handle;
    private FragmentContext fragmentContext;
    private Map<ChildAllocator, StackTraceElement[]> thisMap;
    private boolean applyFragmentLimit;

    public ChildAllocator(FragmentContext context,
                          Accountor parentAccountor,
                          long max,
                          long pre,
                          Map<ChildAllocator,
                          StackTraceElement[]> map,
                          boolean applyFragmentLimit) throws OutOfMemoryException{
      assert max >= pre;
      this.applyFragmentLimit=applyFragmentLimit;
      childAcct = new Accountor(context.getConfig(), errorOnLeak, context, parentAccountor, max, pre, applyFragmentLimit);
      this.fragmentContext=context;
      this.handle = context.getHandle();
      thisMap = map;
      this.empty = DrillBuf.getEmpty(this, childAcct);
    }

    @Override
    public boolean takeOwnership(DrillBuf buf) {
      return buf.transferAccounting(childAcct);
    }

    @Override
    public DrillBuf buffer(int size, int max) {
      if (size == 0) {
        return empty;
      }
      if(!childAcct.reserve(size)) {
        logger.warn("Unable to allocate buffer of size {} due to memory limit. Current allocation: {}", size, getAllocatedMemory(), new Exception());
        return null;
      };

      UnsafeDirectLittleEndian buffer = innerAllocator.directBuffer(size, max);
      DrillBuf wrapped = new DrillBuf(this, childAcct, buffer);
      childAcct.reserved(buffer.capacity(), wrapped);
      return wrapped;
    }

    public DrillBuf buffer(int size) {
      return buffer(size, size);
    }

    @Override
    public ByteBufAllocator getUnderlyingAllocator() {
      return innerAllocator;
    }

    @Override
    public BufferAllocator getChildAllocator(FragmentContext context, long initialReservation, long maximumReservation, boolean applyFragmentLimit)
        throws OutOfMemoryException {
      if (!childAcct.reserve(initialReservation)) {
        throw new OutOfMemoryException(String.format("You attempted to create a new child allocator with initial reservation %d but only %d bytes of memory were available.", initialReservation, childAcct.getAvailable()));
      };
      logger.debug("New child allocator with initial reservation {}", initialReservation);
      ChildAllocator newChildAllocator = new ChildAllocator(context, childAcct, maximumReservation, initialReservation, null, applyFragmentLimit);
      this.children.put(newChildAllocator, Thread.currentThread().getStackTrace());
      return newChildAllocator;
    }

    public PreAllocator getNewPreAllocator() {
      return new PreAlloc(this, this.childAcct);
    }

    @Override
    public void resetFragmentLimits(){
      childAcct.resetFragmentLimits();
    }

    @Override
    public void setFragmentLimit(long limit){
      childAcct.setFragmentLimit(limit);
    }

    @Override
    public long getFragmentLimit(){
      return childAcct.getFragmentLimit();
    }

    @Override
    public void close() {
      if (ENABLE_ACCOUNTING) {
        if (thisMap != null) {
          thisMap.remove(this);
        }
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
            if (errorOnLeak) {
              throw e;
            } else {
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

    @Override
    public long getPeakMemoryAllocation() {
      return childAcct.getPeakMemoryAllocation();
    }

    @Override
    public DrillBuf getEmpty() {
      return empty;
    }


  }

  public PreAllocator getNewPreAllocator() {
    return new PreAlloc(this, this.acct);
  }

  public class PreAlloc implements PreAllocator{
    int bytes = 0;
    final Accountor acct;
    final BufferAllocator allocator;
    private PreAlloc(BufferAllocator allocator, Accountor acct) {
      this.acct = acct;
      this.allocator = allocator;
    }

    /**
     *
     */
    public boolean preAllocate(int bytes) {

      if (!acct.reserve(bytes)) {
        return false;
      }
      this.bytes += bytes;
      return true;

    }


    public DrillBuf getAllocation() {
      DrillBuf b = new DrillBuf(allocator, acct, innerAllocator.directBuffer(bytes, bytes));
      acct.reserved(bytes, b);
      return b;
    }
  }

}
