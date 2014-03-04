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

import java.util.HashSet;
import java.util.Set;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.util.AssertionUtil;

public class TopLevelAllocator implements BufferAllocator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TopLevelAllocator.class);

  private static final boolean ENABLE_ACCOUNTING = AssertionUtil.isAssertionsEnabled();
  private final Set<ChildAllocator> children;
  private final PooledByteBufAllocatorL innerAllocator = new PooledByteBufAllocatorL(true);
  private final Accountor acct;

  public TopLevelAllocator() {
    this(DrillConfig.getMaxDirectMemory());
  }
  
  public TopLevelAllocator(long maximumAllocation) {
    this.acct = new Accountor(null, null, maximumAllocation, 0);
    this.children = ENABLE_ACCOUNTING ? new HashSet<ChildAllocator>() : null; 
  }

  public AccountingByteBuf buffer(int min, int max) {
    if(!acct.reserve(min)) return null;
    ByteBuf buffer = innerAllocator.directBuffer(min, max);
    AccountingByteBuf wrapped = new AccountingByteBuf(acct, (PooledUnsafeDirectByteBufL) buffer);
    acct.reserved(buffer.capacity() - min, wrapped);
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
    ChildAllocator allocator = new ChildAllocator(handle, acct, initialReservation, maximumReservation);
    if(ENABLE_ACCOUNTING) children.add(allocator);
    return allocator;
  }

  @Override
  public void close() {
    if(ENABLE_ACCOUNTING && !children.isEmpty()){
      throw new IllegalStateException("Failure while trying to close allocator: Child level allocators not closed.");
    }
    acct.close();
  }

  
  private class ChildAllocator implements BufferAllocator{

    private Accountor childAcct;
    
    public ChildAllocator(FragmentHandle handle, Accountor parentAccountor, long max, long pre) throws OutOfMemoryException{
      childAcct = new Accountor(handle, parentAccountor, max, pre);
    }
    
    @Override
    public AccountingByteBuf buffer(int size, int max) {
      if(!childAcct.reserve(size)){
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
        throw new OutOfMemoryException(String.format("You attempted to create a new child allocator with initial reservation %d but only %d bytes of memory were available.", initialReservation, childAcct.getCapacity() - childAcct.getAllocation()));
      };
      return new ChildAllocator(handle, childAcct, maximumReservation, initialReservation);
    }

    public PreAllocator getNewPreAllocator(){
      return new PreAlloc(this.childAcct); 
    }

    @Override
    public void close() {
      childAcct.close();
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
