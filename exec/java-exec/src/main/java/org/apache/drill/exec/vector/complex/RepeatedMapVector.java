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
package org.apache.drill.exec.vector.complex;

import io.netty.buffer.DrillBuf;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.holders.ComplexHolder;
import org.apache.drill.exec.expr.holders.RepeatedMapHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.util.CallBack;
import org.apache.drill.exec.util.JsonStringArrayList;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.RepeatedFixedWidthVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.impl.NullReader;
import org.apache.drill.exec.vector.complex.impl.RepeatedMapReaderImpl;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class RepeatedMapVector extends AbstractMapVector implements RepeatedFixedWidthVector {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RepeatedMapVector.class);

  public final static MajorType TYPE = MajorType.newBuilder().setMinorType(MinorType.MAP).setMode(DataMode.REPEATED).build();

  final UInt4Vector offsets;   // offsets to start of each record (considering record indices are 0-indexed)
  private final RepeatedMapReaderImpl reader = new RepeatedMapReaderImpl(RepeatedMapVector.this);
  private final RepeatedMapAccessor accessor = new RepeatedMapAccessor();
  private final Mutator mutator = new Mutator();
  private final EmptyValuePopulator emptyPopulator;

  public RepeatedMapVector(MaterializedField field, BufferAllocator allocator, CallBack callBack){
    super(field, allocator, callBack);
    this.offsets = new UInt4Vector(null, allocator);
    this.emptyPopulator = new EmptyValuePopulator(offsets);
  }

  public void setInitialCapacity(int numRecords) {
    offsets.setInitialCapacity(numRecords + 1);
    for(ValueVector v : this) {
      v.setInitialCapacity(numRecords * DEFAULT_REPEAT_PER_RECORD);
    }
  }

  @Override
  public void allocateNew(int groupCount, int valueCount) {
    clear();
    offsets.allocateNew(groupCount+1);
    offsets.zeroVector();
    for (ValueVector v : getChildren()) {
      AllocationHelper.allocatePrecomputedChildCount(v, groupCount, 50, valueCount);
    }
    mutator.reset();
    accessor.reset();
  }

  public void reAlloc() {
    offsets.reAlloc();
  }

  public Iterator<String> fieldNameIterator() {
    return getChildFieldNames().iterator();
  }

  @Override
  public List<ValueVector> getPrimitiveVectors() {
    List<ValueVector> primitiveVectors = super.getPrimitiveVectors();
    primitiveVectors.add(offsets);
    return primitiveVectors;
  }

  @Override
  public int getBufferSize() {
    if (accessor.getGroupCount() == 0) {
      return 0;
    }
    long buffer = offsets.getBufferSize();
    for (ValueVector v : this) {
      buffer += v.getBufferSize();
    }
    return (int) buffer;
  }

  @Override
  public void close() {
    super.close();
    offsets.close();
  }

  @Override
  public TransferPair getTransferPair() {
    return new RepeatedMapTransferPair(this, getField().getPath());
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
      for (String child:from.getChildFieldNames()) {
        int preSize = to.size();
        vector = from.getChild(child);
        if (vector == null) {
          continue;
        }
        ValueVector newVector = to.addOrGet(child, vector.getField().getType(), vector.getClass());
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

  public TransferPair getTransferPairToSingleMap(FieldReference reference) {
    return new SingleMapTransferPair(this, reference);
  }

  @Override
  public TransferPair getTransferPair(FieldReference ref) {
    return new RepeatedMapTransferPair(this, ref);
  }

  @Override
  public boolean allocateNewSafe() {
    if (!offsets.allocateNewSafe()) {
      return false;
    }
    offsets.zeroVector();
    return super.allocateNewSafe();
  }

  protected static class SingleMapTransferPair implements TransferPair {
    private final TransferPair[] pairs;
    private final RepeatedMapVector from;
    private final MapVector to;
    private static final MajorType MAP_TYPE = Types.required(MinorType.MAP);

    public SingleMapTransferPair(RepeatedMapVector from, SchemaPath path) {
      this(from, new MapVector(MaterializedField.create(path, MAP_TYPE), from.allocator, from.callBack), false);
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
      for (String child:from.getChildFieldNames()) {
        int preSize = to.size();
        vector = from.getChild(child);
        if (vector == null) {
          continue;
        }
        ValueVector newVector = to.addOrGet(child, vector.getField().getType(), vector.getClass());
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

    public RepeatedMapTransferPair(RepeatedMapVector from, SchemaPath path) {
      this(from, new RepeatedMapVector(MaterializedField.create(path, TYPE), from.allocator, from.callBack), false);
    }

    public RepeatedMapTransferPair(RepeatedMapVector from, RepeatedMapVector to) {
      this(from, to, true);
    }

    public RepeatedMapTransferPair(RepeatedMapVector from, RepeatedMapVector to, boolean allocate) {
      this.from = from;
      this.to = to;
      this.pairs = new TransferPair[from.size()];

      int i = 0;
      ValueVector vector;
      for (String child:from.getChildFieldNames()) {
        int preSize = to.size();
        vector = from.getChild(child);
        if (vector == null) {
          continue;
        }
        ValueVector newVector = to.addOrGet(child, vector.getField().getType(), vector.getClass());
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
      to.offsets.getMutator().setSafe(destIndex+1, newIndex);
    }

    @Override
    public void splitAndTransfer(final int groupStart, final int groups) {
      final UInt4Vector.Accessor a = from.offsets.getAccessor();
      final UInt4Vector.Mutator m = to.offsets.getMutator();

      final int startPos = a.get(groupStart);
      final int endPos = a.get(groupStart+groups);
      final int valuesToCopy = endPos - startPos;

      to.offsets.clear();
      to.offsets.allocateNew(groups + 1);

      int normalizedPos;
      for (int i=0; i < groups+1; i++) {
        normalizedPos = a.get(groupStart+i) - startPos;
        m.set(i, normalizedPos);
      }

      m.setValueCount(groups + 1);
      to.emptyPopulator.populate(groups);

      for (TransferPair p : pairs) {
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
  public int getValueCapacity() {
    return offsets.getValueCapacity()-1;
  }

  @Override
  public RepeatedMapAccessor getAccessor() {
    return accessor;
  }

  @Override
  public DrillBuf[] getBuffers(boolean clear) {
    int expectedBufferSize = getBufferSize();
    int actualBufferSize = super.getBufferSize();

    Preconditions.checkArgument(expectedBufferSize == actualBufferSize + offsets.getBufferSize());
    return ArrayUtils.addAll(offsets.getBuffers(clear), super.getBuffers(clear));
  }


  @Override
  public void load(SerializedField metadata, DrillBuf buf) {
    List<SerializedField> fields = metadata.getChildList();

    int bufOffset = offsets.load(metadata.getGroupCount()+1, buf);

    for (SerializedField fmd : fields) {
      MaterializedField fieldDef = MaterializedField.create(fmd);
      ValueVector v = getChild(fieldDef.getLastName());
      if (v == null) {
        // if we arrive here, we didn't have a matching vector.
        v = TypeHelper.getNewVector(fieldDef, allocator);
        putChild(fieldDef.getLastName(), v);
      }
      if (fmd.getValueCount() == 0 && (!fmd.hasGroupCount() || fmd.getGroupCount() == 0)) {
        v.clear();
      } else {
        v.load(fmd, buf.slice(bufOffset, fmd.getBufferLength()));
      }
      bufOffset += fmd.getBufferLength();
    }

    Preconditions.checkArgument(bufOffset == buf.capacity());
  }

  @Override
  public SerializedField getMetadata() {
    SerializedField.Builder b = getField() //
        .getAsBuilder() //
        .setBufferLength(getBufferSize()) //
        .setGroupCount(accessor.getGroupCount())
        // while we don't need to actually read this on load, we need it to make sure we don't skip deserialization of this vector
        .setValueCount(accessor.getGroupCount());
    for (ValueVector v : getChildren()) {
      b.addChild(v.getMetadata());
    }
    return b.build();
  }

  @Override
  public Mutator getMutator() {
    return mutator;
  }

  public class RepeatedMapAccessor implements RepeatedAccessor {

    @Override
    public Object getObject(int index) {
      List<Object> l = new JsonStringArrayList();
      int end = offsets.getAccessor().get(index+1);
      String fieldName;
      for (int i =  offsets.getAccessor().get(index); i < end; i++) {
        Map<String, Object> vv = Maps.newLinkedHashMap();
        for (MaterializedField field:getField().getChildren()) {
          fieldName = field.getLastName();
          Object value = getChild(fieldName).getAccessor().getObject(i);
          if (value != null) {
            vv.put(fieldName, value);
          }
        }
        l.add(vv);
      }
      return l;
    }

    @Override
    public int getValueCount() {
      return offsets.getAccessor().get(offsets.getAccessor().getValueCount() - 1);
    }

    public int getGroupSizeAtIndex(int index) {
      return offsets.getAccessor().get(index+1) - offsets.getAccessor().get(index);
    }

    @Override
    public ValueVector getAllChildValues() {
      throw new UnsupportedOperationException("Cannot retrieve inner vector from repeated map.");
    }

    public void get(int index, RepeatedMapHolder holder) {
      assert index < getValueCapacity() : String.format("Attempted to access index %d when value capacity is %d", index, getValueCapacity());
      holder.start = offsets.getAccessor().get(index);
      holder.end = offsets.getAccessor().get(index+1);
    }

    public void get(int index, ComplexHolder holder) {
      FieldReader reader = getReader();
      reader.setPosition(index);
      holder.reader = reader;
    }

    public void get(int index, int arrayIndex, ComplexHolder holder) {
      RepeatedMapHolder h = new RepeatedMapHolder();
      get(index, h);
      int offset = h.start + arrayIndex;

      if (offset >= h.end) {
        holder.reader = NullReader.INSTANCE;
      } else {
        reader.setSinglePosition(index, arrayIndex);
        holder.reader = reader;
      }
    }

    @Override
    public boolean isNull(int index) {
      return false;
    }

    @Override
    public void reset() {
    }

    @Override
    public FieldReader getReader() {
      return reader;
    }

    @Override
    public int getGroupCount() {
      final int valueCount = offsets.getAccessor().getValueCount();
      return valueCount == 0 ? 0 : valueCount - 1;
    }
  }


  public class Mutator implements ValueVector.Mutator, RepeatedMutator {

    public void startNewGroup(int index) {
      emptyPopulator.populate(index+1);
      offsets.getMutator().setSafe(index+1, offsets.getAccessor().get(index));
    }

    public int add(int index) {
      final int prevEnd = offsets.getAccessor().get(index+1);
      offsets.getMutator().setSafe(index + 1, prevEnd + 1);
      return prevEnd;
    }

    public void setValueCount(int topLevelValueCount) {
      emptyPopulator.populate(topLevelValueCount);
      offsets.getMutator().setValueCount(topLevelValueCount == 0 ? 0 : topLevelValueCount+1);
      int childValueCount = offsets.getAccessor().get(topLevelValueCount);
      for (ValueVector v : getChildren()) {
        v.getMutator().setValueCount(childValueCount);
      }
    }

    @Override
    public void reset() { }

    @Override
    public void generateTestData(int values) {
    }

    @Override
    public void setValueCounts(int parentValueCount, int childValueCount) {
      // TODO - determine if this should be implemented for this class
      throw new UnsupportedOperationException();
    }

    @Override
    public void setRepetitionAtIndexSafe(int index, int repetitionCount) {
    }

    @Override
    public BaseDataValueVector getDataVector() {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
  }

  @Override
  public void clear() {
    getMutator().reset();

    offsets.clear();
    for(ValueVector vector:getChildren()) {
      vector.clear();
    }
  }

  @Override
  public int load(int parentValueCount, int childValueCount, DrillBuf buf) {
    throw new UnsupportedOperationException();
  }
}
