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

import io.netty.buffer.DrillBuf;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.expr.BasicTypeHelper;
import org.apache.drill.exec.expr.holders.ComplexHolder;
import org.apache.drill.exec.expr.holders.RepeatedMapHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.AllocationManager.BufferLedger;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.util.CallBack;
import org.apache.drill.exec.util.JsonStringArrayList;
import org.apache.drill.exec.vector.AddOrGetResult;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VectorDescriptor;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.exec.vector.complex.impl.NullReader;
import org.apache.drill.exec.vector.complex.impl.RepeatedMapReaderImpl;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

import org.apache.drill.shaded.guava.com.google.common.collect.Maps;

public class RepeatedMapVector extends AbstractMapVector
    implements RepeatedValueVector {

  public final static MajorType TYPE = MajorType.newBuilder().setMinorType(MinorType.MAP).setMode(DataMode.REPEATED).build();

  private final UInt4Vector offsets;   // offsets to start of each record (considering record indices are 0-indexed)
  private final RepeatedMapReaderImpl reader = new RepeatedMapReaderImpl(RepeatedMapVector.this);
  private final RepeatedMapAccessor accessor = new RepeatedMapAccessor();
  private final Mutator mutator = new Mutator();
  private final EmptyValuePopulator emptyPopulator;

  public RepeatedMapVector(MaterializedField field, BufferAllocator allocator, CallBack callBack) {
    this(field, new UInt4Vector(BaseRepeatedValueVector.OFFSETS_FIELD, allocator), callBack);
  }

  public RepeatedMapVector(MaterializedField field, UInt4Vector offsets, CallBack callBack) {
    super(field, offsets.getAllocator(), callBack);
    this.offsets = offsets;
    this.emptyPopulator = new EmptyValuePopulator(offsets);
  }

  @Override
  public UInt4Vector getOffsetVector() { return offsets; }

  @Override
  public ValueVector getDataVector() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T extends ValueVector> AddOrGetResult<T> addOrGetVector(VectorDescriptor descriptor) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    offsets.setInitialCapacity(numRecords + 1);
    for (final ValueVector v : this) {
      v.setInitialCapacity(numRecords * RepeatedValueVector.DEFAULT_REPEAT_PER_RECORD);
    }
  }

  @Override
  public RepeatedMapReaderImpl getReader() { return reader; }

  public void allocateNew(int groupCount, int innerValueCount) {
    clear();
    try {
      allocateOffsetsNew(groupCount);
      for (ValueVector v : getChildren()) {
        AllocationHelper.allocatePrecomputedChildCount(v, groupCount, 50, innerValueCount);
      }
    } catch (OutOfMemoryException e){
      clear();
      throw e;
    }
    mutator.reset();
  }

  public void allocateOffsetsNew(int groupCount) {
    offsets.allocateNew(groupCount + 1);
    offsets.zeroVector();
  }

  public Iterator<String> fieldNameIterator() {
    return getChildFieldNames().iterator();
  }

  @Override
  public List<ValueVector> getPrimitiveVectors() {
    final List<ValueVector> primitiveVectors = super.getPrimitiveVectors();
    primitiveVectors.add(offsets);
    return primitiveVectors;
  }

  @Override
  public int getBufferSize() {
    if (getAccessor().getValueCount() == 0) {
      return 0;
    }
    return offsets.getBufferSize() + super.getBufferSize();
  }

  @Override
  public int getAllocatedSize() {
    return offsets.getAllocatedSize() + super.getAllocatedSize();
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }

    long bufferSize = offsets.getBufferSizeFor(valueCount);
    for (final ValueVector v : this) {
      bufferSize += v.getBufferSizeFor(valueCount);
    }

    return (int) bufferSize;
  }

  @Override
  public void close() {
    offsets.close();
    super.close();
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return new RepeatedMapTransferPair(this, getField().getName(), allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new RepeatedMapTransferPair(this, (RepeatedMapVector)to);
  }

  MapSingleCopier makeSingularCopier(MapVector to) {
    return new MapSingleCopier(this, to);
  }

  protected static class MapSingleCopier {
    private final TransferPair[] pairs;
    public final RepeatedMapVector from;

    public MapSingleCopier(RepeatedMapVector from, MapVector to) {
      this.from = from;
      this.pairs = new TransferPair[from.size()];

      int i = 0;
      ValueVector vector;
      for (final String child:from.getChildFieldNames()) {
        int preSize = to.size();
        vector = from.getChild(child);
        if (vector == null) {
          continue;
        }
        final ValueVector newVector = to.addOrGet(child, vector.getField().getType(), vector.getClass());
        if (to.size() != preSize) {
          newVector.allocateNew();
        }
        pairs[i++] = vector.makeTransferPair(newVector);
      }
    }

    public void copySafe(int fromSubIndex, int toIndex) {
      for (TransferPair p : pairs) {
        p.copyValueSafe(fromSubIndex, toIndex);
      }
    }
  }

  public TransferPair getTransferPairToSingleMap(String reference, BufferAllocator allocator) {
    return new SingleMapTransferPair(this, reference, allocator);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new RepeatedMapTransferPair(this, ref, allocator);
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
      if (!offsets.allocateNewSafe()) {
        return false;
      }
      success =  super.allocateNewSafe();
    } finally {
      if (!success) {
        clear();
      }
    }
    offsets.zeroVector();
    return success;
  }

  protected static class SingleMapTransferPair implements TransferPair {
    private final TransferPair[] pairs;
    private final RepeatedMapVector from;
    private final MapVector to;
    private static final MajorType MAP_TYPE = Types.required(MinorType.MAP);

    public SingleMapTransferPair(RepeatedMapVector from, String path, BufferAllocator allocator) {
      this(from, new MapVector(MaterializedField.create(path, MAP_TYPE), allocator, new SchemaChangeCallBack()), false);
    }

    public SingleMapTransferPair(RepeatedMapVector from, MapVector to) {
      this(from, to, true);
    }

    public SingleMapTransferPair(RepeatedMapVector from, MapVector to, boolean allocate) {
      this.from = from;
      this.to = to;
      this.pairs = new TransferPair[from.size()];
      int i = 0;
      ValueVector vector;
      for (final String child : from.getChildFieldNames()) {
        int preSize = to.size();
        vector = from.getChild(child);
        if (vector == null) {
          continue;
        }
        final ValueVector newVector = to.addOrGet(child, vector.getField().getType(), vector.getClass());
        if (allocate && to.size() != preSize) {
          newVector.allocateNew();
        }
        pairs[i++] = vector.makeTransferPair(newVector);
      }
    }


    @Override
    public void transfer() {
      for (TransferPair p : pairs) {
        p.transfer();
      }
      to.getMutator().setValueCount(from.getAccessor().getValueCount());
      from.clear();
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int from, int to) {
      for (TransferPair p : pairs) {
        p.copyValueSafe(from, to);
      }
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      for (TransferPair p : pairs) {
        p.splitAndTransfer(startIndex, length);
      }
      to.getMutator().setValueCount(length);
    }
  }

  private static class RepeatedMapTransferPair implements TransferPair{

    private final TransferPair[] pairs;
    private final RepeatedMapVector to;
    private final RepeatedMapVector from;

    public RepeatedMapTransferPair(RepeatedMapVector from, String path, BufferAllocator allocator) {
      this(from, new RepeatedMapVector(MaterializedField.create(path, TYPE), allocator, new SchemaChangeCallBack()), false);
    }

    public RepeatedMapTransferPair(RepeatedMapVector from, RepeatedMapVector to) {
      this(from, to, true);
    }

    public RepeatedMapTransferPair(RepeatedMapVector from, RepeatedMapVector to, boolean allocate) {
      this.from = from;
      this.to = to;
      this.pairs = new TransferPair[from.size()];
      this.to.ephPair = null;

      int i = 0;
      ValueVector vector;
      for (final String child : from.getChildFieldNames()) {
        final int preSize = to.size();
        vector = from.getChild(child);
        if (vector == null) {
          continue;
        }

        final ValueVector newVector = to.addOrGet(child, vector.getField().getType(), vector.getClass());
        if (to.size() != preSize) {
          newVector.allocateNew();
        }

        pairs[i++] = vector.makeTransferPair(newVector);
      }
    }

    @Override
    public void transfer() {
      from.offsets.transferTo(to.offsets);
      for (TransferPair p : pairs) {
        p.transfer();
      }
      from.clear();
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int srcIndex, int destIndex) {
      RepeatedMapHolder holder = new RepeatedMapHolder();
      from.getAccessor().get(srcIndex, holder);
      to.emptyPopulator.populate(destIndex + 1);
      int newIndex = to.offsets.getAccessor().get(destIndex);
      //todo: make these bulk copies
      for (int i = holder.start; i < holder.end; i++, newIndex++) {
        for (TransferPair p : pairs) {
          p.copyValueSafe(i, newIndex);
        }
      }
      to.offsets.getMutator().setSafe(destIndex + 1, newIndex);
    }

    @Override
    public void splitAndTransfer(final int groupStart, final int groups) {
      final UInt4Vector.Accessor a = from.offsets.getAccessor();
      final UInt4Vector.Mutator m = to.offsets.getMutator();

      final int startPos = a.get(groupStart);
      final int endPos = a.get(groupStart + groups);
      final int valuesToCopy = endPos - startPos;

      to.offsets.clear();
      to.offsets.allocateNew(groups + 1);

      int normalizedPos;
      for (int i = 0; i < groups + 1; i++) {
        normalizedPos = a.get(groupStart + i) - startPos;
        m.set(i, normalizedPos);
      }

      m.setValueCount(groups + 1);
      to.emptyPopulator.populate(groups);

      for (final TransferPair p : pairs) {
        p.splitAndTransfer(startPos, valuesToCopy);
      }
    }
  }

  transient private RepeatedMapTransferPair ephPair;

  public void copyFromSafe(int fromIndex, int thisIndex, RepeatedMapVector from) {
    if (ephPair == null || ephPair.from != from) {
      ephPair = (RepeatedMapTransferPair) from.makeTransferPair(this);
    }
    ephPair.copyValueSafe(fromIndex, thisIndex);
  }

  @Override
  public void copyEntry(int toIndex, ValueVector from, int fromIndex) {
    copyFromSafe(fromIndex, toIndex, (RepeatedMapVector) from);
  }

  @Override
  public int getValueCapacity() {
    return Math.max(offsets.getValueCapacity() - 1, 0);
  }

  @Override
  public RepeatedMapAccessor getAccessor() {
    return accessor;
  }

  @Override
  public void exchange(ValueVector other) {
    super.exchange(other);
    offsets.exchange(((RepeatedMapVector) other).offsets);
  }

  @Override
  public DrillBuf[] getBuffers(boolean clear) {
    return ArrayUtils.addAll(offsets.getBuffers(clear), super.getBuffers(clear));
  }

  @Override
  public void load(SerializedField metadata, DrillBuf buffer) {
    final List<SerializedField> children = metadata.getChildList();

    final SerializedField offsetField = children.get(0);
    offsets.load(offsetField, buffer);
    int bufOffset = offsetField.getBufferLength();

    for (int i = 1; i < children.size(); i++) {
      final SerializedField child = children.get(i);
      final MaterializedField fieldDef = MaterializedField.create(child);
      ValueVector vector = getChild(fieldDef.getName());
      if (vector == null) {
        // if we arrive here, we didn't have a matching vector.
        vector = BasicTypeHelper.getNewVector(fieldDef, allocator);
        putChild(fieldDef.getName(), vector);
      }
      final int vectorLength = child.getBufferLength();
      vector.load(child, buffer.slice(bufOffset, vectorLength));
      bufOffset += vectorLength;
    }

    assert bufOffset == buffer.writerIndex();
  }

  @Override
  public SerializedField getMetadata() {
    SerializedField.Builder builder = getField()
        .getAsBuilder()
        .setBufferLength(getBufferSize())
        // while we don't need to actually read this on load, we need it to
        // make sure we don't skip deserialization of this vector
        .setValueCount(accessor.getValueCount());
    builder.addChild(offsets.getMetadata());
    for (final ValueVector child : getChildren()) {
      builder.addChild(child.getMetadata());
    }
    return builder.build();
  }

  @Override
  public Mutator getMutator() {
    return mutator;
  }

  public class RepeatedMapAccessor implements RepeatedAccessor {
    @Override
    public Object getObject(int index) {
      final List<Object> list = new JsonStringArrayList<>();
      final int end = offsets.getAccessor().get(index+1);
      String fieldName;
      for (int i =  offsets.getAccessor().get(index); i < end; i++) {
        final Map<String, Object> vv = Maps.newLinkedHashMap();
        for (final MaterializedField field : getField().getChildren()) {
          if (!field.equals(BaseRepeatedValueVector.OFFSETS_FIELD)) {
            fieldName = field.getName();
            final Object value = getChild(fieldName).getAccessor().getObject(i);
            if (value != null) {
              vv.put(fieldName, value);
            }
          }
        }
        list.add(vv);
      }
      return list;
    }

    @Override
    public int getValueCount() {
      return Math.max(offsets.getAccessor().getValueCount() - 1, 0);
    }

    @Override
    public int getInnerValueCount() {
      final int valueCount = getValueCount();
      if (valueCount == 0) {
        return 0;
      }
      return offsets.getAccessor().get(valueCount);
    }

    @Override
    public int getInnerValueCountAt(int index) {
      return offsets.getAccessor().get(index+1) - offsets.getAccessor().get(index);
    }

    @Override
    public boolean isEmpty(int index) {
      return false;
    }

    @Override
    public boolean isNull(int index) {
      return false;
    }

    public void get(int index, RepeatedMapHolder holder) {
      assert index < getValueCapacity() :
        String.format("Attempted to access index %d when value capacity is %d",
            index, getValueCapacity());
      final UInt4Vector.Accessor offsetsAccessor = offsets.getAccessor();
      holder.start = offsetsAccessor.get(index);
      holder.end = offsetsAccessor.get(index + 1);
    }

    public void get(int index, ComplexHolder holder) {
      final FieldReader reader = getReader();
      reader.setPosition(index);
      holder.reader = reader;
    }

    public void get(int index, int arrayIndex, ComplexHolder holder) {
      final RepeatedMapHolder h = new RepeatedMapHolder();
      get(index, h);
      final int offset = h.start + arrayIndex;

      if (offset >= h.end) {
        holder.reader = NullReader.INSTANCE;
      } else {
        reader.setSinglePosition(index, arrayIndex);
        holder.reader = reader;
      }
    }
  }

  public class Mutator implements RepeatedMutator {
    @Override
    public void startNewValue(int index) {
      emptyPopulator.populate(index + 1);
      offsets.getMutator().setSafe(index + 1, offsets.getAccessor().get(index));
    }

    @Override
    public void setValueCount(int topLevelValueCount) {
      emptyPopulator.populate(topLevelValueCount);
      offsets.getMutator().setValueCount(topLevelValueCount == 0 ? 0 : topLevelValueCount + 1);
      int childValueCount = offsets.getAccessor().get(topLevelValueCount);
      for (final ValueVector v : getChildren()) {
        v.getMutator().setValueCount(childValueCount);
      }
    }

    @Override
    public void reset() {}

    @Override
    public void generateTestData(int values) {}

    public int add(int index) {
      final int prevEnd = offsets.getAccessor().get(index + 1);
      offsets.getMutator().setSafe(index + 1, prevEnd + 1);
      return prevEnd;
    }

    @Override
    public void exchange(ValueVector.Mutator other) { }
  }

  @Override
  public void clear() {
    getMutator().reset();

    offsets.clear();
    for(final ValueVector vector : getChildren()) {
      vector.clear();
    }
  }

  @Override
  public void collectLedgers(Set<BufferLedger> ledgers) {
    super.collectLedgers(ledgers);
    offsets.collectLedgers(ledgers);
  }

  @Override
  public void toNullable(ValueVector nullableVector) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getPayloadByteCount(int valueCount) {
    if (valueCount == 0) {
      return 0;
    }

    int entryCount = offsets.getAccessor().get(valueCount);
    int count = offsets.getPayloadByteCount(valueCount);

    for (final ValueVector v : getChildren()) {
      count += v.getPayloadByteCount(entryCount);
    }
    return count;
  }

}
