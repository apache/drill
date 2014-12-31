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

import java.io.Closeable;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;

/**
 * Wrapper class to deal with byte buffer allocation. Ensures users only use designated methods. Also allows inser
 */
public interface BufferAllocator extends Closeable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BufferAllocator.class);

  /**
   * Allocate a new or reused buffer of the provided size. Note that the buffer may technically be larger than the
   * requested size for rounding purposes. However, the buffers capacity will be set to the configured size.
   *
   * @param size
   *          The size in bytes.
   * @return A new ByteBuf.
   */
  public abstract DrillBuf buffer(int size);

  /**
   * Allocate a new or reused buffer within provided range. Note that the buffer may technically be larger than the
   * requested size for rounding purposes. However, the buffers capacity will be set to the configured size.
   *
   * @param minSize The minimum size in bytes.
   * @param maxSize The maximum size in bytes.
   * @return A new ByteBuf.
   */
  public abstract DrillBuf buffer(int minSize, int maxSize);

  public abstract ByteBufAllocator getUnderlyingAllocator();

  public abstract BufferAllocator getChildAllocator(FragmentContext context, long initialReservation,
      long maximumReservation, boolean applyFragmentLimit) throws OutOfMemoryException;

  /**
   * Take over ownership of fragment accounting.  Always takes over ownership.
   * @param buf
   * @return false if over allocation.
   */
  public boolean takeOwnership(DrillBuf buf) ;


  public PreAllocator getNewPreAllocator();

  //public void addFragmentContext(FragmentContext c);

  /**
   * For Top Level Allocators. Reset the fragment limits for all allocators
   */
  public void resetFragmentLimits();

  /**
   * For Child allocators to set the Fragment limit for the corresponding fragment allocator.
   * @param l the new fragment limit
   */
  public void setFragmentLimit(long l);

  public long getFragmentLimit();


  /**
   * Not thread safe.
   */
  public interface PreAllocator {
    public boolean preAllocate(int bytes);

    public DrillBuf getAllocation();
  }

  /**
   * @param bytes
   * @return
   */

  /**
   *
   */

  /**
   * Close and release all buffers generated from this buffer pool.
   */
  @Override
  public abstract void close();

  public abstract long getAllocatedMemory();

  public abstract long getPeakMemoryAllocation();

  public DrillBuf getEmpty();
}
