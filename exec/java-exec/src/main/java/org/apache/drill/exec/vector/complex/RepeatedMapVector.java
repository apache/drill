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
import org.apache.drill.exec.expr.holders.RepeatedMapHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.OutOfMemoryRuntimeException;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.util.JsonStringArrayList;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.RepeatedFixedWidthVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.impl.NullReader;
import org.apache.drill.exec.vector.complex.impl.RepeatedMapReaderImpl;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class RepeatedMapVector extends AbstractContainerVector implements RepeatedFixedWidthVector {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RepeatedMapVector.class);

  public final static MajorType TYPE = MajorType.newBuilder().setMinorType(MinorType.MAP).setMode(DataMode.REPEATED).build();

  private final UInt4Vector offsets;   // offsets to start of each record
  private final Map<String, ValueVector> vectors = Maps.newLinkedHashMap();
  private final Map<String, VectorWithOrdinal> vectorIds = Maps.newHashMap();
  private final RepeatedMapReaderImpl reader = new RepeatedMapReaderImpl(RepeatedMapVector.this);
  private final IntObjectOpenHashMap<ValueVector> vectorsById = new IntObjectOpenHashMap<>();
  private final RepeatedMapAccessor accessor = new RepeatedMapAccessor();
  private final Mutator mutator = new Mutator();
  private final BufferAllocator allocator;
  private final MaterializedField field;
  private int lastSet = -1;

  public RepeatedMapVector(MaterializedField field, BufferAllocator allocator){
    this.field = field;
    this.allocator = allocator;
    this.offsets = new UInt4Vector(null, allocator);

  }

  @Override
  public void allocateNew(int parentValueCount, int childValueCount) {
    clear();
    offsets.allocateNew(parentValueCount+1);
    offsets.zeroVector();
    for(ValueVector v : vectors.values()){
      AllocationHelper.allocate(v, parentValueCount, 50, childValueCount);
    }
    mutator.reset();
    accessor.reset();
  }

  public Iterator<String> fieldNameIterator(){
    return vectors.keySet().iterator();
  }

  public int size(){
    return vectors.size();
  }

  @Override
  public List<ValueVector> getPrimitiveVectors() {
    List<ValueVector> primitiveVectors = Lists.newArrayList();
    for (ValueVector v : this.vectors.values()) {
      if (v instanceof AbstractContainerVector) {
        AbstractContainerVector av = (AbstractContainerVector) v;
        for (ValueVector vv : av.getPrimitiveVectors()) {
          primitiveVectors.add(vv);
        }
      } else {
        primitiveVectors.add(v);
      }
    }
    primitiveVectors.add(offsets);
    return primitiveVectors;
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

  @Override
  public <T extends ValueVector> T get(String name, Class<T> clazz) {
    ValueVector v = vectors.get(name);
    if(v == null) throw new IllegalStateException(String.format("Attempting to access invalid map field of name %s.", name));
    return typeify(v, clazz);
  }

  @Override
  public int getBufferSize() {
    if(accessor.getValueCount() == 0 || vectors.isEmpty()) return 0;
    long buffer = offsets.getBufferSize();
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
    return new MapTransferPair( (RepeatedMapVector) to);
  }

  MapSingleCopier makeSingularCopier(MapVector to){
    return new MapSingleCopier(to);
  }


  class MapSingleCopier{
    private final TransferPair[] pairs;
    final RepeatedMapVector from = RepeatedMapVector.this;

    public MapSingleCopier(MapVector to){
      pairs = new TransferPair[vectors.size()];
      int i =0;
      for(Map.Entry<String, ValueVector> e : vectors.entrySet()){
        int preSize = to.vectors.size();
        ValueVector v = to.addOrGet(e.getKey(), e.getValue().getField().getType(), e.getValue().getClass());
        if(to.vectors.size() != preSize) v.allocateNew();
        pairs[i++] = e.getValue().makeTransferPair(v);
      }
    }

    public boolean copySafe(int fromSubIndex, int toIndex){
      for(TransferPair p : pairs){
        if(!p.copyValueSafe(fromSubIndex, toIndex)) return false;
      }
      return true;
    }
  }

  @Override
  public TransferPair getTransferPair(FieldReference ref) {
    return new MapTransferPair(ref);
  }

  @Override
  public void allocateNew() throws OutOfMemoryRuntimeException {
    if(!allocateNewSafe()) throw new OutOfMemoryRuntimeException();
  }

  @Override
  public boolean allocateNewSafe() {
    if(!offsets.allocateNewSafe()) return false;
    offsets.zeroVector();
    for(ValueVector v : vectors.values()){
      if(!v.allocateNewSafe()) return false;
    }
    return true;
  }

  private class MapTransferPair implements TransferPair{

    private final TransferPair[] pairs;
    private final RepeatedMapVector to;
    private final RepeatedMapVector from = RepeatedMapVector.this;

    public MapTransferPair(SchemaPath path){
      RepeatedMapVector v = new RepeatedMapVector(MaterializedField.create(path, TYPE), allocator);
      pairs = new TransferPair[vectors.size()];
      int i =0;
      for(Map.Entry<String, ValueVector> e : vectors.entrySet()){
        TransferPair otherSide = e.getValue().getTransferPair();
        v.put(e.getKey(), otherSide.getTo());
        pairs[i++] = otherSide;
      }
      this.to = v;
    }

    public MapTransferPair(RepeatedMapVector to){
      this.to = to;
      pairs = new TransferPair[vectors.size()];
      int i =0;
      for(Map.Entry<String, ValueVector> e : vectors.entrySet()){
        int preSize = to.vectors.size();
        ValueVector v = to.addOrGet(e.getKey(), e.getValue().getField().getType(), e.getValue().getClass());
        if(preSize != to.vectors.size()) v.allocateNew();
        pairs[i++] = e.getValue().makeTransferPair(v);
      }
    }


    @Override
    public void transfer() {
      offsets.transferTo(to.offsets);
      for(TransferPair p : pairs){
        p.transfer();
      }
      clear();
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public boolean copyValueSafe(int from, int to) {
      RepeatedMapHolder holder = new RepeatedMapHolder();
      accessor.get(from, holder);
      int newIndex = this.to.offsets.getAccessor().get(to);
      //todo: make these bulk copies
      for(int i = holder.start; i < holder.end; i++, newIndex++){
        for(TransferPair p : pairs){
          if(!p.copyValueSafe(i, newIndex)) return false;
        }
      }
      if(!this.to.offsets.getMutator().setSafe(to+1, newIndex)) return false;
      this.to.lastSet++;
      return true;
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      throw new UnsupportedOperationException();
    }

  }


  transient private MapTransferPair ephPair;

  public boolean copyFromSafe(int fromIndex, int thisIndex, RepeatedMapVector from){
    if(ephPair == null || ephPair.from != from){
      ephPair = (MapTransferPair) from.makeTransferPair(this);
    }
    return ephPair.copyValueSafe(fromIndex, thisIndex);
  }

  @Override
  public int getValueCapacity() {
    return offsets.getValueCapacity();
  }

  @Override
  public RepeatedMapAccessor getAccessor() {
    return accessor;
  }

  @Override
  public ByteBuf[] getBuffers() {
    List<ByteBuf> bufs = Lists.newArrayList(offsets.getBuffers());

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

    int bufOffset = offsets.load(metadata.getValueCount()+1, buf);

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
        .setValueCount(accessor.getValueCount());

    for(ValueVector v : vectors.values()){
      b.addChild(v.getMetadata());
    }
    return b.build();
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
  public Mutator getMutator() {
    return mutator;
  }

  public class RepeatedMapAccessor implements RepeatedAccessor {

    @Override
    public Object getObject(int index) {
      List<Object> l = new JsonStringArrayList();
      int end = offsets.getAccessor().get(index+1);
      for(int i =  offsets.getAccessor().get(index); i < end; i++){
        Map<String, Object> vv = Maps.newLinkedHashMap();
        for(Map.Entry<String, ValueVector> e : vectors.entrySet()){
          ValueVector v = e.getValue();
          String k = e.getKey();
          Object value =  v.getAccessor().getObject(i);
          if(value != null){
            vv.put(k,value);
          }
        }
        l.add(vv);
      }
      return l;
    }

    @Override
    public int getValueCount() {
      return offsets.getAccessor().getValueCount() - 1;
    }

    public void get(int index, RepeatedMapHolder holder){
      assert index <= getValueCapacity();
      holder.start = offsets.getAccessor().get(index);
      holder.end = offsets.getAccessor().get(index+1);
    }

    public void get(int index, ComplexHolder holder){
      FieldReader reader = getReader();
      reader.setPosition(index);
      holder.reader = reader;
    }

    public void get(int index, int arrayIndex, ComplexHolder holder){
      RepeatedMapHolder h = new RepeatedMapHolder();
      get(index, h);
      int offset = h.start + arrayIndex;

      if(offset >= h.end){
        holder.reader = NullReader.INSTANCE;
      }else{
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
      return size();
    }
  }

  private void populateEmpties(int groupCount){
    int previousEnd = offsets.getAccessor().get(lastSet + 1);
    for(int i = lastSet + 2; i <= groupCount; i++){
      offsets.getMutator().setSafe(i, previousEnd);
    }
    lastSet = groupCount - 1;
  }

  public class Mutator implements ValueVector.Mutator, RepeatedMutator {

    public void startNewGroup(int index) {
      populateEmpties(index);
      lastSet = index;
      offsets.getMutator().set(index+1, offsets.getAccessor().get(index));
    }

    public int add(int index){
      int nextOffset = offsets.getAccessor().get(index+1);
      boolean success = offsets.getMutator().setSafe(index+1, nextOffset+1);
      if(!success) return -1;
      return nextOffset;
    }

    @Override
    public void setValueCount(int groupCount) {
      populateEmpties(groupCount);
      offsets.getMutator().setValueCount(groupCount+1);
      int valueCount = offsets.getAccessor().get(groupCount);
      for(ValueVector v : vectors.values()){
        v.getMutator().setValueCount(valueCount);
      }
    }

    @Override
    public void reset() {
      lastSet = 0;
    }

    @Override
    public void generateTestData(int values) {
    }

    @Override
    public void setValueCounts(int parentValueCount, int childValueCount) {
      // TODO - determine if this should be implemented for this class
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean setRepetitionAtIndexSafe(int index, int repetitionCount) {
      return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public BaseDataValueVector getDataVector() {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
  }

  @Override
  public void clear() {
    lastSet = 0;
    offsets.clear();
    for(ValueVector v : vectors.values()){
      v.clear();;
    }
  }

  @Override
  public int load(int parentValueCount, int childValueCount, ByteBuf buf) {
    throw new UnsupportedOperationException();
  }


  @Override
  public VectorWithOrdinal getVectorWithOrdinal(String name) {
    return vectorIds.get(name);
  }

}
