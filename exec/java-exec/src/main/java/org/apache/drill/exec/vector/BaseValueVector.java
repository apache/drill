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

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.MaterializedField;

import com.google.common.collect.Iterators;

public abstract class BaseValueVector implements ValueVector{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseValueVector.class);

  protected final BufferAllocator allocator;
  protected final MaterializedField field;
  public static final int INITIAL_VALUE_ALLOCATION = 4096;

  BaseValueVector(MaterializedField field, BufferAllocator allocator) {
    this.allocator = allocator;
    this.field = field;
  }

  @Override
  public void close() {
    clear();
  }

  @Override
  public MaterializedField getField() {
    return field;
  }

  public MaterializedField getField(FieldReference ref){
    return getField().clone(ref);
  }

  protected SerializedField.Builder getMetadataBuilder(){
    return getField().getAsBuilder();
  }

  public abstract int getCurrentValueCount();
  public abstract void setCurrentValueCount(int count);

  abstract public DrillBuf getData();

  abstract static class BaseAccessor implements ValueVector.Accessor{
    public abstract int getValueCount();
    public void reset(){}
  }

  abstract class BaseMutator implements Mutator{
    public void reset(){}
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return Iterators.emptyIterator();
  }

}

