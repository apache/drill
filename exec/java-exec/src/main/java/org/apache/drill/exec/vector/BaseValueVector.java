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

import java.util.Iterator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;

public abstract class BaseValueVector implements ValueVector {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseValueVector.class);

  protected final BufferAllocator allocator;
  protected final MaterializedField field;
  public static final int INITIAL_VALUE_ALLOCATION = 4096;

  protected BaseValueVector(MaterializedField field, BufferAllocator allocator) {
    this.field = Preconditions.checkNotNull(field, "field cannot be null");
    this.allocator = Preconditions.checkNotNull(allocator, "allocator cannot be null");
  }

  @Override
  public void clear() {
    getMutator().reset();
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
    return getField().withPath(ref);
  }

  @Override
  public TransferPair getTransferPair() {
    return getTransferPair(new FieldReference(getField().getPath()));
  }

  @Override
  public SerializedField getMetadata() {
    return getMetadataBuilder().build();
  }

  protected SerializedField.Builder getMetadataBuilder() {
    return getField().getAsBuilder()
        .setValueCount(getAccessor().getValueCount())
        .setBufferLength(getBufferSize());
  }

  public abstract static class BaseAccessor implements ValueVector.Accessor {
    protected BaseAccessor() { }

    @Override
    public boolean isNull(int index) {
      return false;
    }
  }

  public abstract static class BaseMutator implements ValueVector.Mutator {
    protected BaseMutator() { }

    @Override
    public void generateTestData(int values) { }

    //TODO: consider making mutator stateless(if possible) on another issue.
    public void reset() { }
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return Iterators.emptyIterator();
  }

}

