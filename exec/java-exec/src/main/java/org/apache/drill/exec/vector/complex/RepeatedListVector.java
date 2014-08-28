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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.holders.ComplexHolder;
import org.apache.drill.exec.expr.holders.RepeatedListHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.OutOfMemoryRuntimeException;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.util.JsonStringArrayList;
import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.RepeatedFixedWidthVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.impl.NullReader;
import org.apache.drill.exec.vector.complex.impl.RepeatedListReaderImpl;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

import com.google.common.collect.Lists;
import com.google.common.base.Preconditions;


public class RepeatedListVector extends AbstractContainerVector implements RepeatedFixedWidthVector{

  private final UInt4Vector offsets;   // offsets to start of each record
  private final BufferAllocator allocator;
  private final Mutator mutator = new Mutator();
  private final RepeatedListAccessor accessor = new RepeatedListAccessor();
  private ValueVector vector;
  private final MaterializedField field;
  private final RepeatedListReaderImpl reader = new RepeatedListReaderImpl(null, this);
  private int allocationValueCount = 4000;
  private int allocationMonitor = 0;

  private int lastSet = 0;

  private int valueCount;

  public static MajorType TYPE = Types.repeated(MinorType.LIST);

  public RepeatedListVector(MaterializedField field, BufferAllocator allocator){
    this.allocator = allocator;
    this.offsets = new UInt4Vector(null, allocator);
    this.field = field;
  }

  public int size(){
    return vector != null ? 1 : 0;
  }

  @Override
  public List<ValueVector> getPrimitiveVectors() {
    List<ValueVector> primitiveVectors = Lists.newArrayList();
    if (vector instanceof AbstractContainerVector) {
      for (ValueVector v : ((AbstractContainerVector) vector).getPrimitiveVectors()) {
        primitiveVectors.add(v);
      }
    } else {
      primitiveVectors.add(vector);
    }
    primitiveVectors.add(offsets);
    return primitiveVectors;
  }

  public RepeatedListVector(SchemaPath path, BufferAllocator allocator){
    this(MaterializedField.create(path, TYPE), allocator);
  }

  transient private RepeatedListTransferPair ephPair;

  public boolean copyFromSafe(int fromIndex, int thisIndex, RepeatedListVector from){
    if(ephPair == null || ephPair.from != from){
      ephPair = (RepeatedListTransferPair) from.makeTransferPair(this);
    }
    return ephPair.copyValueSafe(fromIndex, thisIndex);
  }

  public Mutator getMutator(){
    return mutator;
  }

  @Override
  public void allocateNew() throws OutOfMemoryRuntimeException {
    if(!allocateNewSafe()) throw new OutOfMemoryRuntimeException();
  }

  @Override
  public boolean allocateNewSafe() {
    if(!offsets.allocateNewSafe()) return false;
    offsets.zeroVector();

    if(vector != null){
      return vector.allocateNewSafe();
    }else{
      return true;
    }

  }

  public class Mutator implements ValueVector.Mutator, RepeatedMutator{

    public void startNewGroup(int index) {
      offsets.getMutator().set(index+1, offsets.getAccessor().get(index));
    }

    public int add(int index){
      int endOffset = index+1;
      int currentChildOffset = offsets.getAccessor().get(endOffset);
      int newChildOffset = currentChildOffset + 1;
      boolean success = offsets.getMutator().setSafe(endOffset, newChildOffset);
      lastSet = index;
      if(!success) return -1;

      // this is done at beginning so return the currentChildOffset, not the new offset.
      return currentChildOffset;

    }

    @Override
    public void setValueCount(int groupCount) {
      populateEmpties(groupCount);
      offsets.getMutator().setValueCount(groupCount+1);

      if(vector != null){
        int valueCount = offsets.getAccessor().get(groupCount);
        vector.getMutator().setValueCount(valueCount);
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

  public class RepeatedListAccessor implements RepeatedAccessor{

    @Override
    public Object getObject(int index) {
      List<Object> l = new JsonStringArrayList();
      int end = offsets.getAccessor().get(index+1);
      for(int i =  offsets.getAccessor().get(index); i < end; i++){
        l.add(vector.getAccessor().getObject(i));
      }
      return l;
    }

    @Override
    public int getValueCount() {
      return offsets.getAccessor().getValueCount() - 1;
    }

    public void get(int index, RepeatedListHolder holder){
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
      RepeatedListHolder h = new RepeatedListHolder();
      get(index, h);
      int offset = h.start + arrayIndex;

      if(offset >= h.end){
        holder.reader = NullReader.INSTANCE;
      }else{
        FieldReader r = vector.getAccessor().getReader();
        r.setPosition(offset);
        holder.reader = r;
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

  @Override
  public int getBufferSize() {
    return offsets.getBufferSize() + vector.getBufferSize();
  }

  @Override
  public void close() {
    offsets.close();
    if(vector != null) vector.close();
  }

  @Override
  public void clear() {
    lastSet = 0;
    offsets.clear();
    if(vector != null) vector.clear();
  }

  @Override
  public MaterializedField getField() {
    return field;
  }

  @Override
  public TransferPair getTransferPair() {
    return new RepeatedListTransferPair(field.getPath());
  }


  public class RepeatedListTransferPair implements TransferPair{
    private final RepeatedListVector from = RepeatedListVector.this;
    private final RepeatedListVector to;
    private final TransferPair vectorTransfer;

    private RepeatedListTransferPair(RepeatedListVector to){
      this.to = to;
      if(to.vector == null){
        to.vector = to.addOrGet(null, vector.getField().getType(), vector.getClass());
        to.vector.allocateNew();
      }
      this.vectorTransfer = vector.makeTransferPair(to.vector);
    }

    private RepeatedListTransferPair(SchemaPath path){
      this.to = new RepeatedListVector(path, allocator);
      vectorTransfer = vector.getTransferPair();
      this.to.vector = vectorTransfer.getTo();
    }

    @Override
    public void transfer() {
      offsets.transferTo(to.offsets);
      vectorTransfer.transfer();
      to.valueCount = valueCount;
      clear();
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      throw new UnsupportedOperationException();
    }


    @Override
    public boolean copyValueSafe(int from, int to) {
      RepeatedListHolder holder = new RepeatedListHolder();
      accessor.get(from, holder);
      int newIndex = this.to.offsets.getAccessor().get(to);
      //todo: make this a bulk copy.
      for(int i = holder.start; i < holder.end; i++, newIndex++){
        if(!vectorTransfer.copyValueSafe(i, newIndex)) return false;
      }
      if(!this.to.offsets.getMutator().setSafe(to + 1, newIndex)) return false;

      this.to.lastSet++;
      return true;
    }

  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    if(!(to instanceof RepeatedListVector ) ) throw new IllegalArgumentException("You can't make a transfer pair from an incompatible .");
    return new RepeatedListTransferPair( (RepeatedListVector) to);
  }

  @Override
  public TransferPair getTransferPair(FieldReference ref) {
    return new RepeatedListTransferPair(ref);
  }

  @Override
  public int getValueCapacity() {
    if(vector == null) return offsets.getValueCapacity() - 1;
    return  Math.min(offsets.getValueCapacity() - 1, vector.getValueCapacity());
  }

  @Override
  public RepeatedListAccessor getAccessor() {
    return accessor;
  }

  @Override
  public DrillBuf[] getBuffers() {
    return ArrayUtils.addAll(offsets.getBuffers(), vector.getBuffers());
  }

  private void setVector(ValueVector v){
    field.addChild(v.getField());
    this.vector = v;
  }

  @Override
  public void load(SerializedField metadata, DrillBuf buf) {
    SerializedField childField = metadata.getChildList().get(0);

    int bufOffset = offsets.load(metadata.getValueCount()+1, buf);

    MaterializedField fieldDef = MaterializedField.create(childField);
    if(vector == null) {
      setVector(TypeHelper.getNewVector(fieldDef, allocator));
    }

    if (childField.getValueCount() == 0){
      vector.clear();
    } else {
      vector.load(childField, buf.slice(bufOffset, childField.getBufferLength()));
    }
  }

  @Override
  public SerializedField getMetadata() {
    return getField() //
        .getAsBuilder() //
        .setBufferLength(getBufferSize()) //
        .setValueCount(accessor.getValueCount()) //
        .addChild(vector.getMetadata()) //
        .build();
  }

  private void populateEmpties(int groupCount){
    int previousEnd = offsets.getAccessor().get(lastSet + 1);
    for(int i = lastSet + 2; i <= groupCount; i++){
      offsets.getMutator().setSafe(i, previousEnd);
    }
    lastSet = groupCount - 1;
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return Collections.singleton(vector).iterator();
  }

  @Override
  public <T extends ValueVector> T addOrGet(String name, MajorType type, Class<T> clazz) {
    Preconditions.checkArgument(name == null);

    if(vector == null){
      vector = TypeHelper.getNewVector(MaterializedField.create(field.getPath().getUnindexedArrayChild(), type), allocator);
    }
    return typeify(vector, clazz);
  }

  @Override
  public <T extends ValueVector> T get(String name, Class<T> clazz) {
    if(name != null) return null;
    return typeify(vector, clazz);
  }

  @Override
  public void allocateNew(int parentValueCount, int childValueCount) {
    clear();
    offsets.allocateNew(parentValueCount+1);
    mutator.reset();
    accessor.reset();
  }

  @Override
  public int load(int parentValueCount, int childValueCount, DrillBuf buf) {
    throw new UnsupportedOperationException();
  }

  @Override
  public VectorWithOrdinal getVectorWithOrdinal(String name) {
    if(name != null) return null;
    return new VectorWithOrdinal(vector, 0);
  }


}
