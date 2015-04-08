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
package org.apache.drill.exec.vector;

import io.netty.buffer.DrillBuf;

import java.util.Iterator;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.MaterializedField;

import com.google.common.collect.Iterators;

public abstract class BaseDataValueVector extends BaseValueVector{

  protected DrillBuf data;
  protected int valueCount;
  protected int currentValueCount;

  public BaseDataValueVector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
    this.data = allocator.getEmpty();
  }

  /**
   * Release the underlying DrillBuf and reset the ValueVector
   */
  @Override
  public void clear() {
    data.release();
    data = allocator.getEmpty();
  }

  public void setCurrentValueCount(int count) {
    currentValueCount = count;
  }

  public int getCurrentValueCount() {
    return currentValueCount;
  }


  @Override
  public DrillBuf[] getBuffers(boolean clear) {
    DrillBuf[] out;
    if (getBufferSize() == 0) {
      out = new DrillBuf[0];
    } else {
      out = new DrillBuf[]{data};
      if (clear) {
        data.readerIndex(0);
        data.retain();
      }
    }
    if (clear) {
      clear();
    }
    return out;
  }

  public int getBufferSize() {
    if (getAccessor().getValueCount() == 0) {
      return 0;
    }
    return data.writerIndex();
  }

  @Override
  public abstract SerializedField getMetadata();

  public DrillBuf getData() {
    return data;
  }

  public long getDataAddr() {
    return data.memoryAddress();
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return Iterators.emptyIterator();
  }

}
