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
import io.netty.buffer.DrillBuf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.util.AssertionUtil;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

public class Accountor {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Accountor.class);

  private static final boolean ENABLE_ACCOUNTING = AssertionUtil.isAssertionsEnabled();
  private final AtomicRemainder remainder;
  private final long total;
  private ConcurrentMap<ByteBuf, DebugStackTrace> buffers = Maps.newConcurrentMap();
  private final FragmentHandle handle;
  private String fragmentStr;
  private Accountor parent;

  private final boolean errorOnLeak;
  // some operators are no subject to the fragment limit. They set the applyFragmentLimit to false

  private final boolean enableFragmentLimit;
  private final double  fragmentMemOvercommitFactor;

  private final boolean  DEFAULT_ENABLE_FRAGMENT_LIMIT=false;
  private final double   DEFAULT_FRAGMENT_MEM_OVERCOMMIT_FACTOR=1.5;

  private final boolean applyFragmentLimit;

  private final FragmentContext fragmentContext;
  long fragmentLimit;

  private long peakMemoryAllocation = 0;

  // The top level Allocator has an accountor that keeps track of all the FragmentContexts currently executing.
  // This enables the top level accountor to calculate a new fragment limit whenever necessary.
  private final List<FragmentContext> fragmentContexts;

  public Accountor(DrillConfig config, boolean errorOnLeak, FragmentContext context, Accountor parent, long max, long preAllocated, boolean applyFragLimit) {
    // TODO: fix preallocation stuff
    this.errorOnLeak = errorOnLeak;
    AtomicRemainder parentRemainder = parent != null ? parent.remainder : null;
    this.parent = parent;

    boolean enableFragmentLimit;
    double  fragmentMemOvercommitFactor;

    try {
      enableFragmentLimit = config.getBoolean(ExecConstants.ENABLE_FRAGMENT_MEMORY_LIMIT);
      fragmentMemOvercommitFactor = config.getDouble(ExecConstants.FRAGMENT_MEM_OVERCOMMIT_FACTOR);
    }catch(Exception e){
      enableFragmentLimit = DEFAULT_ENABLE_FRAGMENT_LIMIT;
      fragmentMemOvercommitFactor = DEFAULT_FRAGMENT_MEM_OVERCOMMIT_FACTOR;
    }
    this.enableFragmentLimit = enableFragmentLimit;
    this.fragmentMemOvercommitFactor = fragmentMemOvercommitFactor;


    this.applyFragmentLimit=applyFragLimit;

    this.remainder = new AtomicRemainder(errorOnLeak, parentRemainder, max, preAllocated, applyFragmentLimit);
    this.total = max;
    this.fragmentContext=context;
    this.handle = (context!=null) ? context.getHandle() : null;
    this.fragmentStr= (handle!=null) ? ( handle.getMajorFragmentId()+":"+handle.getMinorFragmentId() ) : "0:0";
    this.fragmentLimit=this.total; // Allow as much as possible to start with;
    if (ENABLE_ACCOUNTING) {
      buffers = Maps.newConcurrentMap();
    } else {
      buffers = null;
    }
    this.fragmentContexts = new ArrayList<FragmentContext>();
    if(parent!=null && parent.parent==null){ // Only add the fragment context to the fragment level accountor
      synchronized(this) {
        addFragmentContext(this.fragmentContext);
      }
    }
  }

  public boolean transferTo(Accountor target, DrillBuf buf, long size) {
    boolean withinLimit = target.forceAdditionalReservation(size);
    release(buf, size);

    if (ENABLE_ACCOUNTING) {
      target.buffers.put(buf, new DebugStackTrace(buf.capacity(), Thread.currentThread().getStackTrace()));
    }
    return withinLimit;
  }

  public long getAvailable() {
    if (parent != null) {
      return Math.min(parent.getAvailable(), getCapacity() - getAllocation());
    }
    return getCapacity() - getAllocation();
  }

  public long getCapacity() {
    return fragmentLimit;
  }

  public long getAllocation() {
    return remainder.getUsed();
  }

  public long getPeakMemoryAllocation() {
    return peakMemoryAllocation;
  }

  public boolean reserve(long size) {
    logger.trace("Fragment:"+fragmentStr+" Reserved "+size+" bytes. Total Allocated: "+getAllocation());
    boolean status = remainder.get(size, this.applyFragmentLimit);
    peakMemoryAllocation = Math.max(peakMemoryAllocation, getAllocation());
    return status;
  }

  public boolean forceAdditionalReservation(long size) {
    if (size > 0) {
      boolean status = remainder.forceGet(size);
      peakMemoryAllocation = Math.max(peakMemoryAllocation, getAllocation());
      return status;
    } else {
      return true;
    }
  }

  public void reserved(long expected, DrillBuf buf) {
    // make sure to take away the additional memory that happened due to rounding.

    long additional = buf.capacity() - expected;
    if (additional > 0) {
      remainder.forceGet(additional);
    }

    if (ENABLE_ACCOUNTING) {
      buffers.put(buf, new DebugStackTrace(buf.capacity(), Thread.currentThread().getStackTrace()));
    }

    peakMemoryAllocation = Math.max(peakMemoryAllocation, getAllocation());
  }


  public void releasePartial(DrillBuf buf, long size) {
    remainder.returnAllocation(size);
    if (ENABLE_ACCOUNTING) {
      if (buf != null) {
        DebugStackTrace dst = buffers.get(buf);
        if (dst == null) {
          throw new IllegalStateException("Partially releasing a buffer that has already been released. Buffer: " + buf);
        }
        dst.size -= size;
        if (dst.size < 0) {
          throw new IllegalStateException("Partially releasing a buffer that has already been released. Buffer: " + buf);
        }
      }
    }
  }

  public void release(DrillBuf buf, long size) {
    remainder.returnAllocation(size);
    if (ENABLE_ACCOUNTING) {
      if (buf != null && buffers.remove(buf) == null) {
        throw new IllegalStateException("Releasing a buffer that has already been released. Buffer: " + buf);
      }
    }
  }

  private void addFragmentContext(FragmentContext c) {
    if (parent != null){
      parent.addFragmentContext(c);
    }else {
      if(logger.isTraceEnabled()) {
        FragmentHandle hndle;
        String fragStr;
        if(c!=null) {
          hndle = c.getHandle();
          fragStr = (hndle != null) ? (hndle.getMajorFragmentId() + ":" + hndle.getMinorFragmentId()) : "[Null Fragment Handle]";
        }else{
          fragStr = "[Null Context]";
        }
        fragStr+=" (Object Id: "+System.identityHashCode(c)+")";
        StackTraceElement[] ste = (new Throwable()).getStackTrace();
        StringBuffer sb = new StringBuffer();
        for (StackTraceElement s : ste) {
          sb.append(s.toString());
          sb.append("\n");
        }

        logger.trace("Fragment " + fragStr + " added to root accountor.\n"+sb.toString());
      }
      synchronized(this) {
        fragmentContexts.add(c);
      }
    }
  }

  private void removeFragmentContext(FragmentContext c) {
    if (parent != null){
      if (parent.parent==null){
        // only fragment level allocators will have the fragment context saved
        parent.removeFragmentContext(c);
      }
    }else{
      if(logger.isDebugEnabled()) {
        FragmentHandle hndle;
        String fragStr;
        if (c != null) {
          hndle = c.getHandle();
          fragStr = (hndle != null) ? (hndle.getMajorFragmentId() + ":" + hndle.getMinorFragmentId()) : "[Null Fragment Handle]";
        } else {
          fragStr = "[Null Context]";
        }
        fragStr += " (Object Id: " + System.identityHashCode(c) + ")";
        logger.trace("Fragment " + fragStr + " removed from root accountor");
      }
      synchronized(this) {
        fragmentContexts.remove(c);
      }
    }
  }

  public long resetFragmentLimits(){
    // returns the new capacity
    if(!this.enableFragmentLimit){
      return getCapacity();
    }

    if(parent!=null){
      parent.resetFragmentLimits();
    }else {
      //Get remaining memory available per fragment and distribute it EQUALLY among all the fragments.
      //Fragments get the memory limit added to the amount already allocated.
      //This favours fragments that are already running which will get a limit greater than newly started fragments.
      //If the already running fragments end quickly, their limits will be assigned back to the remaining fragments
      //quickly. If they are long running, then we want to favour them with larger limits anyway.
      synchronized (this) {
        int nFragments=fragmentContexts.size();
        long allocatedMemory=0;
        for(FragmentContext fragment: fragmentContexts){
          BufferAllocator a = fragment.getAllocator();
          if(a!=null) {
            allocatedMemory += fragment.getAllocator().getAllocatedMemory();
          }
        }
        if(logger.isTraceEnabled()) {
          logger.trace("Resetting Fragment Memory Limit: total Available memory== "+total
            +" Total Allocated Memory :"+allocatedMemory
            +" Number of fragments: "+nFragments
            + " fragmentMemOvercommitFactor: "+fragmentMemOvercommitFactor
            + " Root fragment limit: "+this.fragmentLimit + "(Root obj: "+System.identityHashCode(this)+")"
          );
        }
        if(nFragments>0) {
          long rem = (total - allocatedMemory) / nFragments;
          for (FragmentContext fragment : fragmentContexts) {
            fragment.setFragmentLimit((long) (rem * fragmentMemOvercommitFactor));
          }
        }
        if(logger.isTraceEnabled() && false){
          StringBuffer sb= new StringBuffer();
          sb.append("[root](0:0)");
          sb.append("Allocated memory: ");
          sb.append(this.getAllocation());
          sb.append(" Fragment Limit: ");
          sb.append(this.getFragmentLimit());
          logger.trace(sb.toString());
          for(FragmentContext fragment: fragmentContexts){
            sb= new StringBuffer();
            if (handle != null) {
              sb.append("[");
              sb.append(QueryIdHelper.getQueryId(handle.getQueryId()));
              sb.append("](");
              sb.append(handle.getMajorFragmentId());
              sb.append(":");
              sb.append(handle.getMinorFragmentId());
              sb.append(")");
            }else{
              sb.append("[fragment](0:0)");
            }
            sb.append("Allocated memory: ");
            sb.append(fragment.getAllocator().getAllocatedMemory());
            sb.append(" Fragment Limit: ");
            sb.append(fragment.getAllocator().getFragmentLimit());
            logger.trace(sb.toString());
          }
          logger.trace("Resetting Complete");
        }
      }
    }
    return getCapacity();
  }

  public void close() {
    // remove the fragment context and reset fragment limits whenever an allocator closes
    if(parent!=null && parent.parent==null) {
      logger.debug("Fragment " + fragmentStr + "  accountor being closed");
      removeFragmentContext(fragmentContext);
    }
    resetFragmentLimits();

    if (ENABLE_ACCOUNTING && !buffers.isEmpty()) {
      StringBuffer sb = new StringBuffer();
      sb.append("Attempted to close accountor with ");
      sb.append(buffers.size());
      sb.append(" buffer(s) still allocated");
      if (handle != null) {
        sb.append("for QueryId: ");
        sb.append(QueryIdHelper.getQueryId(handle.getQueryId()));
        sb.append(", MajorFragmentId: ");
        sb.append(handle.getMajorFragmentId());
        sb.append(", MinorFragmentId: ");
        sb.append(handle.getMinorFragmentId());
      }
      sb.append(".\n");

      Multimap<DebugStackTrace, DebugStackTrace> multi = LinkedListMultimap.create();
      for (DebugStackTrace t : buffers.values()) {
        multi.put(t, t);
      }

      for (DebugStackTrace entry : multi.keySet()) {
        Collection<DebugStackTrace> allocs = multi.get(entry);

        sb.append("\n\n\tTotal ");
        sb.append(allocs.size());
        sb.append(" allocation(s) of byte size(s): ");
        for (DebugStackTrace alloc : allocs) {
          sb.append(alloc.size);
          sb.append(", ");
        }

        sb.append("at stack location:\n");
        entry.addToString(sb);
      }
      if (!buffers.isEmpty()) {
        IllegalStateException e = new IllegalStateException(sb.toString());
        if (errorOnLeak) {
          throw e;
        } else {
          logger.warn("Memory leaked.", e);
        }
      }
    }

    remainder.close();

  }

  public void setFragmentLimit(long add) {
    // We ADD the limit to the current allocation. If none has been allocated, this
    // sets a new limit. If memory has already been allocated, the fragment gets its
    // limit based on the allocation, though this might still result in reducing the
    // limit.

    if (parent != null && parent.parent==null) { // This is a fragment level accountor
      this.fragmentLimit=getAllocation()+add;
      this.remainder.setLimit(this.fragmentLimit);
      logger.trace("Fragment "+fragmentStr+" memory limit set to "+this.fragmentLimit);
    }
  }

  public long getFragmentLimit(){
    return this.fragmentLimit;
  }

  public class DebugStackTrace {

    private StackTraceElement[] elements;
    private long size;

    public DebugStackTrace(long size, StackTraceElement[] elements) {
      super();
      this.elements = elements;
      this.size = size;
    }

    public void addToString(StringBuffer sb) {
      for (int i = 3; i < elements.length; i++) {
        sb.append("\t\t");
        sb.append(elements[i]);
        sb.append("\n");
      }
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Arrays.hashCode(elements);
//      result = prime * result + (int) (size ^ (size >>> 32));
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      DebugStackTrace other = (DebugStackTrace) obj;
      if (!Arrays.equals(elements, other.elements)) {
        return false;
      }
      // weird equal where size doesn't matter for multimap purposes.
//      if (size != other.size)
//        return false;
      return true;
    }

  }

}
