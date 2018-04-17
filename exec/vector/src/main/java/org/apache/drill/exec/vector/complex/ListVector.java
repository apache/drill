/*
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
package org.apache.drill.exec.vector.complex;

import java.util.List;
import java.util.Set;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.memory.AllocationManager.BufferLedger;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.util.CallBack;
import org.apache.drill.exec.util.JsonStringArrayList;
import org.apache.drill.exec.vector.AddOrGetResult;
import org.apache.drill.exec.vector.NullableVector;
import org.apache.drill.exec.vector.UInt1Vector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VectorDescriptor;
import org.apache.drill.exec.vector.ZeroVector;
import org.apache.drill.exec.vector.complex.impl.ComplexCopier;
import org.apache.drill.exec.vector.complex.impl.UnionListReader;
import org.apache.drill.exec.vector.complex.impl.UnionListWriter;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.FieldWriter;

import com.google.common.collect.ObjectArrays;

import io.netty.buffer.DrillBuf;

public class ListVector extends BaseRepeatedValueVector {

  public static final String UNION_VECTOR_NAME = "$union$";

  private final UInt1Vector bits;
  private final Mutator mutator = new Mutator();
  private final Accessor accessor = new Accessor();
  private UnionListWriter writer;
  private UnionListReader reader;

  public ListVector(MaterializedField field, BufferAllocator allocator, CallBack callBack) {
    super(field, allocator);
    // Can't do this. See below.
//    super(field.cloneEmpty(), allocator);
    this.bits = new UInt1Vector(MaterializedField.create(BITS_VECTOR_NAME, Types.required(MinorType.UINT1)), allocator);
    this.field.addChild(getDataVector().getField());
    this.writer = new UnionListWriter(this);
    this.reader = new UnionListReader(this);
//
//    // To be consistent with the map vector, create the child if a child is
//    // given in the field. This is a mess. See DRILL-6046.
//    But, can't do this because the deserialization mechanism passes in a
//    field with children filled in, but with no expectation that the vector will
//    match its schema until this vector itself is loaded. Indeed a mess.
//
//    assert field.getChildren().size() <= 1;
//    for (MaterializedField child : field.getChildren()) {
//      if (child.getName().equals(DATA_VECTOR_NAME)) {
//        continue;
//      }
//      setChildVector(BasicTypeHelper.getNewVector(child, allocator, callBack));
//    }
  }

  public UnionListWriter getWriter() {
    return writer;
  }

  @Override
  public void allocateNew() throws OutOfMemoryException {
    super.allocateNewSafe();
    bits.allocateNewSafe();
  }

  public void transferTo(ListVector target) {
    offsets.makeTransferPair(target.offsets).transfer();
    bits.makeTransferPair(target.bits).transfer();
    if (target.getDataVector() instanceof ZeroVector) {
      target.addOrGetVector(new VectorDescriptor(vector.getField().getType()));
    }
    getDataVector().makeTransferPair(target.getDataVector()).transfer();
  }

  public void copyFromSafe(int inIndex, int outIndex, ListVector from) {
    copyFrom(inIndex, outIndex, from);
  }

  public void copyFrom(int inIndex, int outIndex, ListVector from) {
    FieldReader in = from.getReader();
    in.setPosition(inIndex);
    FieldWriter out = getWriter();
    out.setPosition(outIndex);
    ComplexCopier.copy(in, out);
  }

  @Override
  public void copyEntry(int toIndex, ValueVector from, int fromIndex) {
    copyFromSafe(fromIndex, toIndex, (ListVector) from);
  }

  @Override
  public ValueVector getDataVector() { return vector; }

  public ValueVector getBitsVector() { return bits; }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new TransferImpl(field.withPath(ref), allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    return new TransferImpl((ListVector) target);
  }

  private class TransferImpl implements TransferPair {

    private final ListVector to;

    public TransferImpl(MaterializedField field, BufferAllocator allocator) {
      to = new ListVector(field, allocator, null);
      to.addOrGetVector(new VectorDescriptor(vector.getField().getType()));
    }

    public TransferImpl(ListVector to) {
      this.to = to;
      to.addOrGetVector(new VectorDescriptor(vector.getField().getType()));
    }

    @Override
    public void transfer() {
      transferTo(to);
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      to.allocateNew();
      for (int i = 0; i < length; i++) {
        copyValueSafe(startIndex + i, i);
      }
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int from, int to) {
      this.to.copyFrom(from, to, ListVector.this);
    }
  }

  @Override
  public Accessor getAccessor() {
    return accessor;
  }

  @Override
  public Mutator getMutator() {
    return mutator;
  }

  @Override
  public FieldReader getReader() {
    return reader;
  }

  @Override
  public boolean allocateNewSafe() {
    /* boolean to keep track if all the memory allocation were successful
     * Used in the case of composite vectors when we need to allocate multiple
     * buffers for multiple vectors. If one of the allocations failed we need to
     * clear all the memory that we allocated
     */
    boolean success = false;
    try {
      if (! offsets.allocateNewSafe()) {
        return false;
      }
      success = vector.allocateNewSafe();
      success = success && bits.allocateNewSafe();
    } finally {
      if (! success) {
        clear();
      }
    }
    if (success) {
      offsets.zeroVector();
      bits.zeroVector();
    }
    return success;
  }

  @Override
  protected UserBitShared.SerializedField.Builder getMetadataBuilder() {
    return getField().getAsBuilder()
            .setValueCount(getAccessor().getValueCount())
            .setBufferLength(getBufferSize())
            .addChild(offsets.getMetadata())
            .addChild(bits.getMetadata())
            .addChild(vector.getMetadata());
  }

  @Override
  public <T extends ValueVector> AddOrGetResult<T> addOrGetVector(VectorDescriptor descriptor) {
    AddOrGetResult<T> result = super.addOrGetVector(descriptor);
    reader = new UnionListReader(this);
    return result;
  }

  @Override
  public int getBufferSize() {
    if (getAccessor().getValueCount() == 0) {
      return 0;
    }
    return offsets.getBufferSize() + bits.getBufferSize() + vector.getBufferSize();
  }

  @Override
  public void clear() {
    bits.clear();
    lastSet = 0;
    super.clear();
  }

  @Override
  public DrillBuf[] getBuffers(boolean clear) {
    final DrillBuf[] buffers = ObjectArrays.concat(
        offsets.getBuffers(false),
        ObjectArrays.concat(bits.getBuffers(false),
            vector.getBuffers(false), DrillBuf.class),
        DrillBuf.class);
    if (clear) {
      for (DrillBuf buffer : buffers) {
        buffer.retain();
      }
      clear();
    }
    return buffers;
  }

  @Override
  public void load(UserBitShared.SerializedField metadata, DrillBuf buffer) {
    final UserBitShared.SerializedField offsetMetadata = metadata.getChild(0);
    offsets.load(offsetMetadata, buffer);

    final int offsetLength = offsetMetadata.getBufferLength();
    final UserBitShared.SerializedField bitMetadata = metadata.getChild(1);
    final int bitLength = bitMetadata.getBufferLength();
    bits.load(bitMetadata, buffer.slice(offsetLength, bitLength));

    final UserBitShared.SerializedField vectorMetadata = metadata.getChild(2);
    if (isEmptyType()) {
      addOrGetVector(VectorDescriptor.create(vectorMetadata.getMajorType()));
    }

    final int vectorLength = vectorMetadata.getBufferLength();
    vector.load(vectorMetadata, buffer.slice(offsetLength + bitLength, vectorLength));
  }

  public boolean isEmptyType() {
    return getDataVector() == DEFAULT_DATA_VECTOR;
  }

  @Override
  public void setChildVector(ValueVector childVector) {

    // Unlike the repeated list vector, the (plain) list vector
    // adds the dummy vector as a child type.

    assert field.getChildren().size() == 1;
    assert field.getChildren().iterator().next().getType().getMinorType() == MinorType.LATE;
    field.removeChild(vector.getField());

    super.setChildVector(childVector);

    // Initial LATE type vector not added as a subtype initially.
    // So, no need to remove it, just add the new subtype. Since the
    // MajorType is immutable, must build a new one and replace the type
    // in the materialized field. (We replace the type, rather than creating
    // a new materialized field, to preserve the link to this field from
    // a parent map, list or union.)

    assert field.getType().getSubTypeCount() == 0;
    field.replaceType(
        field.getType().toBuilder()
          .addSubType(childVector.getField().getType().getMinorType())
          .build());
  }

  /**
   * Promote the list to a union. Called from old-style writers. This implementation
   * relies on the caller to set the types vector for any existing values.
   * This method simply clears the existing vector.
   *
   * @return the new union vector
   */

  public UnionVector promoteToUnion() {
    UnionVector vector = createUnion();

    // Replace the current vector, clearing its data. (This is the
    // old behavior.

    replaceDataVector(vector);
    reader = new UnionListReader(this);
    return vector;
  }

  /**
   * Revised form of promote to union that correctly fixes up the list
   * field metadata to match the new union type.
   *
   * @return the new union vector
   */

  public UnionVector promoteToUnion2() {
    UnionVector unionVector = createUnion();

    // Replace the current vector, clearing its data. (This is the
    // old behavior.

    setChildVector(unionVector);
    return unionVector;
  }

  /**
   * Promote to a union, preserving the existing data vector as a member of
   * the new union. Back-fill the types vector with the proper type value
   * for existing rows.
   *
   * @return the new union vector
   */

  public UnionVector convertToUnion(int allocValueCount, int valueCount) {
    assert allocValueCount >= valueCount;
    UnionVector unionVector = createUnion();
    unionVector.allocateNew(allocValueCount);

    // Preserve the current vector (and its data) if it is other than
    // the default. (New behavior used by column writers.)

    if (! isEmptyType()) {
      unionVector.addType(vector);
      int prevType = vector.getField().getType().getMinorType().getNumber();
      UInt1Vector.Mutator typeMutator = unionVector.getTypeVector().getMutator();

      // If the previous vector was nullable, then promote the nullable state
      // to the type vector by setting either the null marker or the type
      // marker depending on the original nullable values.

      if (vector instanceof NullableVector) {
        UInt1Vector.Accessor bitsAccessor =
            ((UInt1Vector) ((NullableVector) vector).getBitsVector()).getAccessor();
        for (int i = 0; i < valueCount; i++) {
          typeMutator.setSafe(i, (bitsAccessor.get(i) == 0)
              ? UnionVector.NULL_MARKER
              : prevType);
        }
      } else {

        // The value is not nullable. (Perhaps it is a map.)
        // Note that the original design of lists have a flaw: if the sole member
        // is a map, then map entries can't be nullable when the only type, but
        // become nullable when in a union. What a mess...

        for (int i = 0; i < valueCount; i++) {
          typeMutator.setSafe(i, prevType);
        }
      }
    }
    vector = unionVector;
    return unionVector;
  }

  private UnionVector createUnion() {
    MaterializedField newField = MaterializedField.create(UNION_VECTOR_NAME, Types.optional(MinorType.UNION));
    UnionVector unionVector = new UnionVector(newField, allocator, null);

    // For efficiency, should not create a reader that will never be used.
    // Keeping for backward compatibility.

    reader = new UnionListReader(this);
    return unionVector;
  }

  private int lastSet;

  public class Accessor extends BaseRepeatedAccessor {

    @Override
    public Object getObject(int index) {
      if (isNull(index)) {
        return null;
      }
      final List<Object> vals = new JsonStringArrayList<>();
      final UInt4Vector.Accessor offsetsAccessor = offsets.getAccessor();
      final int start = offsetsAccessor.get(index);
      final int end = offsetsAccessor.get(index + 1);
      final ValueVector.Accessor valuesAccessor = getDataVector().getAccessor();
      for(int i = start; i < end; i++) {
        vals.add(valuesAccessor.getObject(i));
      }
      return vals;
    }

    @Override
    public boolean isNull(int index) {
      return bits.getAccessor().get(index) == 0;
    }
  }

  public class Mutator extends BaseRepeatedMutator {
    public void setNotNull(int index) {
      bits.getMutator().setSafe(index, 1);
      lastSet = index + 1;
    }

    @Override
    public void startNewValue(int index) {
      for (int i = lastSet; i <= index; i++) {
        offsets.getMutator().setSafe(i + 1, offsets.getAccessor().get(i));
      }
      setNotNull(index);
      lastSet = index + 1;
    }

    @Override
    public void setValueCount(int valueCount) {
      // TODO: populate offset end points
      if (valueCount == 0) {
        offsets.getMutator().setValueCount(0);
      } else {
        for (int i = lastSet; i < valueCount; i++) {
          offsets.getMutator().setSafe(i + 1, offsets.getAccessor().get(i));
        }
        offsets.getMutator().setValueCount(valueCount + 1);
      }
      final int childValueCount = valueCount == 0 ? 0 : offsets.getAccessor().get(valueCount);
      vector.getMutator().setValueCount(childValueCount);
      bits.getMutator().setValueCount(valueCount);
    }
  }

  @Override
  public void collectLedgers(Set<BufferLedger> ledgers) {
    offsets.collectLedgers(ledgers);
    bits.collectLedgers(ledgers);
    super.collectLedgers(ledgers);
  }

  @Override
  public int getPayloadByteCount(int valueCount) {
    if (valueCount == 0) {
      return 0;
    }

    return offsets.getPayloadByteCount(valueCount) + bits.getPayloadByteCount(valueCount) +
           super.getPayloadByteCount(valueCount);
  }
}
