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

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;

public class TopLevelAllocator implements BufferAllocator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TopLevelAllocator.class);

  private final PooledByteBufAllocatorL innerAllocator = new PooledByteBufAllocatorL(true);
  private final Accountor acct;

  public TopLevelAllocator() {
    this(DrillConfig.getMaxDirectMemory());
  }
  
  public TopLevelAllocator(long maximumAllocation) {
    this.acct = new Accountor(null, null, maximumAllocation, 0);
  }

  @Override
  public AccountingByteBuf buffer(int size) {
    return buffer(size, null);
  }

  public AccountingByteBuf buffer(int size, String desc){
    if(!acct.reserve(size)) return null;
    ByteBuf buffer = innerAllocator.directBuffer(size);
    AccountingByteBuf wrapped = new AccountingByteBuf(acct, (PooledUnsafeDirectByteBufL) buffer);
    acct.reserved(size, wrapped, desc);
    return wrapped;
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
    return new ChildAllocator(handle, acct, initialReservation, maximumReservation);
  }

  @Override
  public void close() {
    acct.close();
  }

  
  private class ChildAllocator implements BufferAllocator{

    private Accountor innerAcct;
    
    public ChildAllocator(FragmentHandle handle, Accountor parentAccountor, long max, long pre) throws OutOfMemoryException{
      innerAcct = new Accountor(handle, parentAccountor, max, pre);
    }
    
    
    @Override
    public AccountingByteBuf buffer(int size, String desc) {
      if(!innerAcct.reserve(size)){
        return null;
      };
      
      ByteBuf buffer = innerAllocator.directBuffer(size);
      AccountingByteBuf wrapped = new AccountingByteBuf(innerAcct, (PooledUnsafeDirectByteBufL) buffer);
      innerAcct.reserved(size, wrapped);
      return wrapped;
    }
    
    public AccountingByteBuf buffer(int size) {
      return buffer(size, null);
    }

    @Override
    public ByteBufAllocator getUnderlyingAllocator() {
      return innerAllocator;
    }

    @Override
    public BufferAllocator getChildAllocator(FragmentHandle handle, long initialReservation, long maximumReservation)
        throws OutOfMemoryException {
      return new ChildAllocator(handle, innerAcct, maximumReservation, initialReservation);
    }

    public PreAllocator getNewPreAllocator(){
      return new PreAlloc(this.innerAcct); 
    }

    @Override
    public void close() {
      innerAcct.close();
    }

    @Override
    public long getAllocatedMemory() {
      return innerAcct.getAllocation();
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
