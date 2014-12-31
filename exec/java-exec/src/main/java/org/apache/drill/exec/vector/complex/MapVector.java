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

import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;

import io.netty.buffer.DrillBuf;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.holders.ComplexHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.util.CallBack;
import org.apache.drill.exec.util.JsonStringHashMap;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector.MapSingleCopier;
import org.apache.drill.exec.vector.complex.impl.SingleMapReaderImpl;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

import com.google.common.base.Preconditions;

public class MapVector extends AbstractContainerVector {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MapVector.class);

  public final static MajorType TYPE = MajorType.newBuilder().setMinorType(MinorType.MAP).setMode(DataMode.REQUIRED).build();

  private final SingleMapReaderImpl reader = new SingleMapReaderImpl(MapVector.this);
  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();
  private int valueCount;

  public MapVector(String path, BufferAllocator allocator, CallBack callBack){
    this(MaterializedField.create(SchemaPath.getSimplePath(path), TYPE), allocator, callBack);
  }

  public MapVector(MaterializedField field, BufferAllocator allocator, CallBack callBack){
    super(field, allocator, callBack);
  }

  transient private MapTransferPair ephPair;
  transient private MapSingleCopier ephPair2;

  public boolean copyFromSafe(int fromIndex, int thisIndex, MapVector from) {
    if(ephPair == null || ephPair.from != from) {
      ephPair = (MapTransferPair) from.makeTransferPair(this);
    }
    return ephPair.copyValueSafe(fromIndex, thisIndex);
  }

  public boolean copyFromSafe(int fromSubIndex, int thisIndex, RepeatedMapVector from) {
    if(ephPair2 == null || ephPair2.from != from) {
      ephPair2 = from.makeSingularCopier(this);
    }
    return ephPair2.copySafe(fromSubIndex, thisIndex);
  }

  @Override
  protected boolean supportsDirectRead() {
    return true;
  }

  public Iterator<String> fieldNameIterator() {
    return getChildFieldNames().iterator();
  }

  @Override
  public int getBufferSize() {
    if (valueCount == 0 || size() == 0) {
      return 0;
    }
    long buffer = 0;
    for (ValueVector v : this) {
      buffer += v.getBufferSize();
    }

    return (int) buffer;
  }

  @Override
  public TransferPair getTransferPair() {
    return new MapTransferPair(this, getField().getPath());
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new MapTransferPair(this, (MapVector)to);
  }

  @Override
  public TransferPair getTransferPair(FieldReference ref) {
    return new MapTransferPair(this, ref);
  }

  protected static class MapTransferPair implements TransferPair{
    private final TransferPair[] pairs;
    private final MapVector from;
    private final MapVector to;

    public MapTransferPair(MapVector from, SchemaPath path) {
      this(from, new MapVector(MaterializedField.create(path, TYPE), from.allocator, from.callBack), false);
    }

    public MapTransferPair(MapVector from, MapVector to) {
      this(from, to, true);
    }

    protected MapTransferPair(MapVector from, MapVector to, boolean allocate) {
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
      to.valueCount = from.valueCount;
      from.clear();
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public boolean copyValueSafe(int from, int to) {
      for (TransferPair p : pairs) {
        if (!p.copyValueSafe(from, to)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      for (TransferPair p : pairs) {
        p.splitAndTransfer(startIndex, length);
      }
      to.getMutator().setValueCount(length);
    }

  }

  @Override
  public int getValueCapacity() {
    if (size() == 0) {
      return Integer.MAX_VALUE;
    }

    final Ordering<ValueVector> natural = new Ordering<ValueVector>() {
      @Override
      public int compare(@Nullable ValueVector left, @Nullable ValueVector right) {
        return Ints.compare(
            Preconditions.checkNotNull(left).getValueCapacity(),
            Preconditions.checkNotNull(right).getValueCapacity()
        );
      }
    };

    return natural.min(getChildren()).getValueCapacity();
  }

  @Override
  public Accessor getAccessor() {
    return accessor;
  }

  @Override
  public void load(SerializedField metadata, DrillBuf buf) {
    List<SerializedField> fields = metadata.getChildList();
    valueCount = metadata.getValueCount();

    int bufOffset = 0;
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
        .setValueCount(valueCount);


    for(ValueVector v : getChildren()) {
      b.addChild(v.getMetadata());
    }
    return b.build();
  }

  @Override
  public Mutator getMutator() {
    return mutator;
  }

  public class Accessor implements ValueVector.Accessor{

    @Override
    public Object getObject(int index) {
      Map<String, Object> vv = new JsonStringHashMap();
      for (String child:getChildFieldNames()) {
        Object value = getChild(child).getAccessor().getObject(index);
        if (value != null) {
          vv.put(child, value);
        }
      }
      return vv;
    }

    public void get(int index, ComplexHolder holder) {
      reader.setPosition(index);
      holder.reader = reader;
    }

    @Override
    public int getValueCount() {
      return valueCount;
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
      //return new SingleMapReaderImpl(MapVector.this);
      return reader;
    }
  }

  public ValueVector getVectorById(int id) {
    return getChildByOrdinal(id);
  }

  public class Mutator implements ValueVector.Mutator{

    @Override
    public void setValueCount(int valueCount) {
      for (ValueVector v : getChildren()) {
        v.getMutator().setValueCount(valueCount);
      }
      MapVector.this.valueCount = valueCount;
    }

    @Override
    public void reset() { }

    @Override
    public void generateTestData(int values) { }
  }

  @Override
  public void clear() {
    valueCount = 0;
    for (ValueVector v : getChildren()) {
      v.clear();
    }
  }
}
