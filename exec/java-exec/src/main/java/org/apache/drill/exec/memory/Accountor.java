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

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

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
  private Accountor parent;

  public Accountor(FragmentHandle handle, Accountor parent, long max, long preAllocated) {
    // TODO: fix preallocation stuff
    AtomicRemainder parentRemainder = parent != null ? parent.remainder : null;
    this.parent = parent;
    this.remainder = new AtomicRemainder(parentRemainder, max, preAllocated);
    this.total = max;
    this.handle = handle;
    if (ENABLE_ACCOUNTING) {
      buffers = Maps.newConcurrentMap();
    } else {
      buffers = null;
    }
  }

  public long getAvailable() {
    if (parent != null) {
      return Math.min(parent.getAvailable(), getCapacity() - getAllocation());
    }
    return getCapacity() - getAllocation();
  }

  public long getCapacity() {
    return total;
  }

  public long getAllocation() {
    return remainder.getUsed();
  }

  public boolean reserve(long size) {
    return remainder.get(size);
  }

  public void forceAdditionalReservation(long size) {
    if(size > 0) remainder.forceGet(size);
  }

  public void reserved(long expected, AccountingByteBuf buf){
    // make sure to take away the additional memory that happened due to rounding.

    long additional = buf.capacity() - expected;
    if(additional > 0) remainder.forceGet(additional);

    if (ENABLE_ACCOUNTING) {
      buffers.put(buf, new DebugStackTrace(buf.capacity(), Thread.currentThread().getStackTrace()));
    }
  }


  public void releasePartial(AccountingByteBuf buf, long size){
    remainder.returnAllocation(size);
    if (ENABLE_ACCOUNTING) {
      if(buf != null){
        DebugStackTrace dst = buffers.get(buf);
        if(dst == null) throw new IllegalStateException("Partially releasing a buffer that has already been released. Buffer: " + buf);
        dst.size -= size;
        if(dst.size < 0){
          throw new IllegalStateException("Partially releasing a buffer that has already been released. Buffer: " + buf);
        }
      }
    }
  }
  
  public void release(AccountingByteBuf buf, long size) {
    remainder.returnAllocation(size);
    if (ENABLE_ACCOUNTING) {
      if(buf != null && buffers.remove(buf) == null) throw new IllegalStateException("Releasing a buffer that has already been released. Buffer: " + buf);
    }
  }

  public void close() {
     
    if (ENABLE_ACCOUNTING && !buffers.isEmpty()) {
      StringBuffer sb = new StringBuffer();
      sb.append("Attempted to close accountor with ");
      sb.append(buffers.size());
      sb.append(" buffer(s) still allocated");
      if(handle != null){
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
        for(DebugStackTrace alloc : allocs){
          sb.append(alloc.size);
          sb.append(", ");
        }

        sb.append("at stack location:\n");
        entry.addToString(sb);
      }

      throw new IllegalStateException(sb.toString());

    }

    remainder.close();
    
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
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      DebugStackTrace other = (DebugStackTrace) obj;
      if (!Arrays.equals(elements, other.elements))
        return false;
      // weird equal where size doesn't matter for multimap purposes.
//      if (size != other.size)
//        return false;
      return true;
    }

  }
}
