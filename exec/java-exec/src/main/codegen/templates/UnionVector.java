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

import org.apache.drill.common.types.TypeProtos.MinorType;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/drill/exec/vector/complex/impl/UnionVector.java" />


<#include "/@includes/license.ftl" />

package org.apache.drill.exec.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />
import java.util.Iterator;
import org.apache.drill.exec.vector.complex.impl.ComplexCopier;
import org.apache.drill.exec.util.CallBack;

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */
@SuppressWarnings("unused")


public class UnionVector implements ValueVector {

  private MaterializedField field;
  private BufferAllocator allocator;
  private Accessor accessor = new Accessor();
  private Mutator mutator = new Mutator();
  private int valueCount;

  private MapVector internalMap;
  private SingleMapWriter internalMapWriter;
  private UInt1Vector typeVector;

  private MapVector mapVector;
  private ListVector listVector;
  private NullableBigIntVector bigInt;
  private NullableVarCharVector varChar;

  private FieldReader reader;
  private NullableBitVector bit;

  private State state = State.INIT;
  private int singleType = 0;
  private ValueVector singleVector;

  private enum State {
    INIT, SINGLE, MULTI
  }

  public UnionVector(MaterializedField field, BufferAllocator allocator, CallBack callBack) {
    this.field = field.clone();
    this.allocator = allocator;
    internalMap = new MapVector("internal", allocator, callBack);
    internalMapWriter = new SingleMapWriter(internalMap, null, true, true);
    this.typeVector = internalMap.addOrGet("types", Types.required(MinorType.UINT1), UInt1Vector.class);
    this.field.addChild(internalMap.getField().clone());
  }

  private void updateState(ValueVector v) {
    if (state == State.INIT) {
      state = State.SINGLE;
      singleVector = v;
      singleType = v.getField().getType().getMinorType().getNumber();
    } else {
      state = State.MULTI;
      singleVector = null;
    }
  }

  public boolean isSingleType() {
    return state == State.SINGLE && singleType != MinorType.LIST_VALUE;
  }

  public ValueVector getSingleVector() {
    assert state != State.MULTI : "Cannot get single vector when there are multiple types";
    assert state != State.INIT : "Cannot get single vector when there are no types";
    return singleVector;
  }

  public MapVector getMap() {
    if (mapVector == null) {
      int vectorCount = internalMap.size();
      mapVector = internalMap.addOrGet("map", Types.optional(MinorType.MAP), MapVector.class);
      updateState(mapVector);
      if (internalMap.size() > vectorCount) {
        mapVector.allocateNew();
      }
    }
    return mapVector;
  }

  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
  <#assign fields = minor.fields!type.fields />
  <#assign uncappedName = name?uncap_first/>
  <#if !minor.class?starts_with("Decimal")>

  private Nullable${name}Vector ${uncappedName}Vector;

  public Nullable${name}Vector get${name}Vector() {
    if (${uncappedName}Vector == null) {
      int vectorCount = internalMap.size();
      ${uncappedName}Vector = internalMap.addOrGet("${uncappedName}", Types.optional(MinorType.${name?upper_case}), Nullable${name}Vector.class);
      updateState(${uncappedName}Vector);
      if (internalMap.size() > vectorCount) {
        ${uncappedName}Vector.allocateNew();
      }
    }
    return ${uncappedName}Vector;
  }

  </#if>

  </#list></#list>

  public ListVector getList() {
    if (listVector == null) {
      int vectorCount = internalMap.size();
      listVector = internalMap.addOrGet("list", Types.optional(MinorType.LIST), ListVector.class);
      updateState(listVector);
      if (internalMap.size() > vectorCount) {
        listVector.allocateNew();
      }
    }
    return listVector;
  }

  public int getTypeValue(int index) {
    return typeVector.getAccessor().get(index);
  }

  public UInt1Vector getTypeVector() {
    return typeVector;
  }

  @Override
  public void allocateNew() throws OutOfMemoryRuntimeException {
    internalMap.allocateNew();
    if (typeVector != null) {
      typeVector.zeroVector();
    }
  }

  @Override
  public boolean allocateNewSafe() {
    boolean safe = internalMap.allocateNewSafe();
    if (safe) {
      if (typeVector != null) {
        typeVector.zeroVector();
      }
    }
    return safe;
  }

  @Override
  public void setInitialCapacity(int numRecords) {
  }

  @Override
  public int getValueCapacity() {
    return Math.min(typeVector.getValueCapacity(), internalMap.getValueCapacity());
  }

  @Override
  public void close() {
  }

  @Override
  public void clear() {
    internalMap.clear();
  }

  @Override
  public MaterializedField getField() {
    return field;
  }

  @Override
  public TransferPair getTransferPair() {
    return new TransferImpl(field);
  }

  @Override
  public TransferPair getTransferPair(FieldReference ref) {
    return new TransferImpl(field.withPath(ref));
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    return new TransferImpl((UnionVector) target);
  }

  public void transferTo(UnionVector target) {
    internalMap.makeTransferPair(target.internalMap).transfer();
    target.valueCount = valueCount;
  }

  public void copyFrom(int inIndex, int outIndex, UnionVector from) {
    from.getReader().setPosition(inIndex);
    getWriter().setPosition(outIndex);
    ComplexCopier copier = new ComplexCopier(from.reader, mutator.writer);
    copier.write();
  }

  public void copyFromSafe(int inIndex, int outIndex, UnionVector from) {
    copyFrom(inIndex, outIndex, from);
  }

  private class TransferImpl implements TransferPair {

    UnionVector to;

    public TransferImpl(MaterializedField field) {
      to = new UnionVector(field, allocator, null);
    }

    public TransferImpl(UnionVector to) {
      this.to = to;
    }

    @Override
    public void transfer() {
      transferTo(to);
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {

    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int from, int to) {
      this.to.copyFrom(from, to, UnionVector.this);
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
    if (reader == null) {
      reader = new UnionReader(this);
    }
    return reader;
  }

  public FieldWriter getWriter() {
    if (mutator.writer == null) {
      mutator.writer = new UnionWriter(this);
    }
    return mutator.writer;
  }

  @Override
  public UserBitShared.SerializedField getMetadata() {
    SerializedField.Builder b = getField() //
            .getAsBuilder() //
            .setBufferLength(getBufferSize()) //
            .setValueCount(valueCount);

    b.addChild(internalMap.getMetadata());
    return b.build();
  }

  @Override
  public int getBufferSize() {
    return internalMap.getBufferSize();
  }

  @Override
  public DrillBuf[] getBuffers(boolean clear) {
    return internalMap.getBuffers(clear);
  }

  @Override
  public void load(UserBitShared.SerializedField metadata, DrillBuf buffer) {
    valueCount = metadata.getValueCount();

    internalMap.load(metadata.getChild(0), buffer);
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return null;
  }

  public class Accessor extends BaseValueVector.BaseAccessor {


    @Override
    public Object getObject(int index) {
      int type = typeVector.getAccessor().get(index);
      switch (type) {
      case 0:
        return null;
      <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
      <#assign fields = minor.fields!type.fields />
      <#assign uncappedName = name?uncap_first/>
      <#if !minor.class?starts_with("Decimal")>
      case MinorType.${name?upper_case}_VALUE:
        return get${name}Vector().getAccessor().getObject(index);
      </#if>

      </#list></#list>
      case MinorType.MAP_VALUE:
        return getMap().getAccessor().getObject(index);
      case MinorType.LIST_VALUE:
        return getList().getAccessor().getObject(index);
      default:
        throw new UnsupportedOperationException("Cannot support type: " + MinorType.valueOf(type));
      }
    }

    public byte[] get(int index) {
      return null;
    }

    public void get(int index, ComplexHolder holder) {
    }

    public void get(int index, UnionHolder holder) {
      if (reader == null) {
        reader = new UnionReader(UnionVector.this);
      }
      reader.setPosition(index);
      holder.reader = reader;
    }

    @Override
    public int getValueCount() {
      return valueCount;
    }

    @Override
    public boolean isNull(int index) {
      return typeVector.getAccessor().get(index) == 0;
    }

    public int isSet(int index) {
      return isNull(index) ? 0 : 1;
    }
  }

  public class Mutator extends BaseValueVector.BaseMutator {

    UnionWriter writer;

    @Override
    public void setValueCount(int valueCount) {
      UnionVector.this.valueCount = valueCount;
      internalMap.getMutator().setValueCount(valueCount);
    }

    public void set(int index, byte[] bytes) {
    }

    public void setSafe(int index, UnionHolder holder) {
      FieldReader reader = holder.reader;
      if (writer == null) {
        writer = new UnionWriter(UnionVector.this);
      }
      writer.setPosition(index);
      MinorType type = reader.getType().getMinorType();
      switch (type) {
      <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
      <#assign fields = minor.fields!type.fields />
      <#assign uncappedName = name?uncap_first/>
      <#if !minor.class?starts_with("Decimal")>
      case ${name?upper_case}:
        Nullable${name}Holder ${uncappedName}Holder = new Nullable${name}Holder();
        reader.read(${uncappedName}Holder);
        if (holder.isSet == 1) {
          writer.write${name}(<#list fields as field>${uncappedName}Holder.${field.name}<#if field_has_next>, </#if></#list>);
        }
        break;
      </#if>
      </#list></#list>
      case MAP: {
        ComplexCopier copier = new ComplexCopier(reader, writer);
        copier.write();
        break;
      }
      case LIST: {
        ComplexCopier copier = new ComplexCopier(reader, writer);
        copier.write();
        break;
      }
      default:
        throw new UnsupportedOperationException();
      }
    }

    public void setType(int index, MinorType type) {
      typeVector.getMutator().setSafe(index, type.getNumber());
    }

    @Override
    public void reset() { }

    @Override
    public void generateTestData(int values) { }
  }
}
