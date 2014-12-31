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
package io.netty.buffer;

import org.apache.drill.exec.memory.Accountor;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;

class FakeAllocator implements BufferAllocator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FakeAllocator.class);


  public static final Accountor FAKE_ACCOUNTOR = new FakeAccountor();
  public static final BufferAllocator FAKE_ALLOCATOR = new FakeAllocator();

  @Override
  public DrillBuf buffer(int size) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DrillBuf buffer(int minSize, int maxSize) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBufAllocator getUnderlyingAllocator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BufferAllocator getChildAllocator(FragmentContext context, long initialReservation, long maximumReservation,
                                           boolean applyFragmentLimit)
      throws OutOfMemoryException {
    throw new UnsupportedOperationException();
  }

  @Override
  public DrillBuf getEmpty() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean takeOwnership(DrillBuf buf) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PreAllocator getNewPreAllocator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void resetFragmentLimits() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setFragmentLimit(long l) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getFragmentLimit(){
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
  }

  @Override
  public long getAllocatedMemory() {
    return 0;
  }

  @Override
  public long getPeakMemoryAllocation() {
    return 0;
  }

  static class FakeAccountor extends Accountor {

    public FakeAccountor() {
      super(null, false, null, null, 0, 0, true);
    }

    @Override
    public boolean transferTo(Accountor target, DrillBuf buf, long size) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getAvailable() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getCapacity() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getAllocation() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean reserve(long size) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean forceAdditionalReservation(long size) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void reserved(long expected, DrillBuf buf) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void releasePartial(DrillBuf buf, long size) {

    }

    @Override
    public void release(DrillBuf buf, long size) {

    }

    @Override
    public void close() {

    }


  }

}
