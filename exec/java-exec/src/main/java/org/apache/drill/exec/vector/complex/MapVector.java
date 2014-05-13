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

import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.holders.ComplexHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.OutOfMemoryRuntimeException;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.util.JsonStringHashMap;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector.MapSingleCopier;
import org.apache.drill.exec.vector.complex.impl.SingleMapReaderImpl;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.hive12.common.collect.Lists;

public class MapVector extends AbstractContainerVector {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MapVector.class);

  public final static MajorType TYPE = MajorType.newBuilder().setMinorType(MinorType.MAP).setMode(DataMode.REQUIRED).build();

  final HashMap<String, ValueVector> vectors = Maps.newHashMap();
  private final Map<String, VectorWithOrdinal> vectorIds = Maps.newHashMap();
  private final IntObjectOpenHashMap<ValueVector> vectorsById = new IntObjectOpenHashMap<>();
  private final SingleMapReaderImpl reader = new SingleMapReaderImpl(MapVector.this);
  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();
  private final BufferAllocator allocator;
  private MaterializedField field;
  private int valueCount;

  public MapVector(String path, BufferAllocator allocator){
    this.field = MaterializedField.create(SchemaPath.getSimplePath(path), TYPE);
    this.allocator = allocator;
  }
  public MapVector(MaterializedField field, BufferAllocator allocator){
    this.field = field;
    this.allocator = allocator;
  }

  public int size(){
    return vectors.size();
  }

  transient private MapTransferPair ephPair;
  transient private MapSingleCopier ephPair2;

  public boolean copyFromSafe(int fromIndex, int thisIndex, MapVector from){
    if(ephPair == null || ephPair.from != from){
      ephPair = (MapTransferPair) from.makeTransferPair(this);
    }
    return ephPair.copyValueSafe(fromIndex, thisIndex);
  }

  public boolean copyFromSafe(int fromSubIndex, int thisIndex, RepeatedMapVector from){
    if(ephPair2 == null || ephPair2.from != from){
      ephPair2 = from.makeSingularCopier(this);
    }
    return ephPair2.copySafe(fromSubIndex, thisIndex);
  }

  @Override
  public <T extends ValueVector> T addOrGet(String name, MajorType type, Class<T> clazz) {
    ValueVector v = vectors.get(name);

    if(v == null){
      v = TypeHelper.getNewVector(field.getPath(), name, allocator, type);
      Preconditions.checkNotNull(v, String.format("Failure to create vector of type %s.", type));
      put(name, v);
    }
    return typeify(v, clazz);

  }

  protected void put(String name, ValueVector vv){
    int ordinal = vectors.size();
    if(vectors.put(name, vv) != null){
      throw new IllegalStateException();
    }
    vectorIds.put(name, new VectorWithOrdinal(vv, ordinal));
    vectorsById.put(ordinal, vv);
    field.addChild(vv.getField());
  }


  @Override
  protected boolean supportsDirectRead() {
    return true;
  }

  public Iterator<String> fieldNameIterator(){
    return vectors.keySet().iterator();
  }

  @Override
  public void allocateNew() throws OutOfMemoryRuntimeException {
    if(!allocateNewSafe()) throw new OutOfMemoryRuntimeException();
  }

  @Override
  public boolean allocateNewSafe() {
    for(ValueVector v : vectors.values()){
      if(!v.allocateNewSafe()) return false;
    }
    return true;
  }

  @Override
  public <T extends ValueVector> T get(String name, Class<T> clazz) {
    ValueVector v = vectors.get(name);
    if(v == null) throw new IllegalStateException(String.format("Attempting to access invalid map field of name %s.", name));
    return typeify(v, clazz);
  }

  @Override
  public int getBufferSize() {
    if(valueCount == 0 || vectors.isEmpty()) return 0;
    long buffer = 0;
    for(ValueVector v : this){
      buffer += v.getBufferSize();
    }

    return (int) buffer;
  }

  @Override
  public void close() {
    for(ValueVector v : this){
      v.close();
    }
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return vectors.values().iterator();
  }

  @Override
  public MaterializedField getField() {
    return field;
  }

  @Override
  public TransferPair getTransferPair() {
    return new MapTransferPair(field.getPath());
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new MapTransferPair( (MapVector) to);
  }

  @Override
  public TransferPair getTransferPair(FieldReference ref) {
    return new MapTransferPair(ref);
  }

  private class MapTransferPair implements TransferPair{
    private MapVector from = MapVector.this;
    private TransferPair[] pairs;
    private MapVector to;

    public MapTransferPair(SchemaPath path){
      MapVector v = new MapVector(MaterializedField.create(path, TYPE), allocator);
      pairs = new TransferPair[vectors.size()];
      int i =0;
      for(Map.Entry<String, ValueVector> e : vectors.entrySet()){
        TransferPair otherSide = e.getValue().getTransferPair();
        v.put(e.getKey(), otherSide.getTo());
        pairs[i++] = otherSide;
      }
      this.to = v;
    }

    public MapTransferPair(MapVector to){
      this.to = to;
      pairs = new TransferPair[vectors.size()];
      int i =0;
      for(Map.Entry<String, ValueVector> e : vectors.entrySet()){
        int preSize = to.vectors.size();
        ValueVector v = to.addOrGet(e.getKey(), e.getValue().getField().getType(), e.getValue().getClass());
        if(to.vectors.size() != preSize) v.allocateNew();
        pairs[i++] = e.getValue().makeTransferPair(v);
      }
    }


    @Override
    public void transfer() {
      for(TransferPair p : pairs){
        p.transfer();
      }
      to.valueCount = valueCount;
      clear();
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public boolean copyValueSafe(int from, int to) {
      for(TransferPair p : pairs){
        if(!p.copyValueSafe(from, to)) return false;
      }
      return true;
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      throw new UnsupportedOperationException();
    }

  }

  @Override
  public int getValueCapacity() {
    if(this.vectors.isEmpty()) return 0;
    return vectors.values().iterator().next().getValueCapacity();
  }

  @Override
  public Accessor getAccessor() {
    return accessor;
  }

  @Override
  public ByteBuf[] getBuffers() {
    List<ByteBuf> bufs = Lists.newArrayList();
    for(ValueVector v : vectors.values()){
      for(ByteBuf b : v.getBuffers()){
        bufs.add(b);
      }
    }
    return bufs.toArray(new ByteBuf[bufs.size()]);
  }

  @Override
  public void load(SerializedField metadata, ByteBuf buf) {
    List<SerializedField> fields = metadata.getChildList();

    int bufOffset = 0;
    for (SerializedField fmd : fields) {
      MaterializedField fieldDef = MaterializedField.create(fmd);

      ValueVector v = vectors.get(fieldDef.getLastName());
      if(v == null) {
        // if we arrive here, we didn't have a matching vector.

        v = TypeHelper.getNewVector(fieldDef, allocator);
      }
      if (fmd.getValueCount() == 0){
        v.clear();
      } else {
        v.load(fmd, buf.slice(bufOffset, fmd.getBufferLength()));
      }
      bufOffset += fmd.getBufferLength();
      put(fieldDef.getLastName(), v);
    }
  }

  @Override
  public SerializedField getMetadata() {
    SerializedField.Builder b = getField() //
        .getAsBuilder() //
        .setBufferLength(getBufferSize()) //
        .setValueCount(valueCount);


    for(ValueVector v : vectors.values()){
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
      for(Map.Entry<String, ValueVector> e : vectors.entrySet()){
        ValueVector v = e.getValue();
        String k = e.getKey();
        Object value = v.getAccessor().getObject(index);
        if(value != null){
          vv.put(k, value);
        }
      }
      return vv;
    }

    public void get(int index, ComplexHolder holder){
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
      return new SingleMapReaderImpl(MapVector.this);
    }

  }

  public ValueVector getVectorById(int id){
    return vectorsById.get(id);
  }

  public class Mutator implements ValueVector.Mutator{

    @Override
    public void setValueCount(int valueCount) {
      for(ValueVector v : vectors.values()){
        v.getMutator().setValueCount(valueCount);
      }
      MapVector.this.valueCount = valueCount;
    }

    @Override
    public void reset() {
    }

    @Override
    public void generateTestData(int values) {
    }

  }

  @Override
  public void clear() {
    for(ValueVector v : vectors.values()){
      v.clear();;
    }
  }

  public VectorWithOrdinal getVectorWithOrdinal(String name){
    return vectorIds.get(name);
  }
}
