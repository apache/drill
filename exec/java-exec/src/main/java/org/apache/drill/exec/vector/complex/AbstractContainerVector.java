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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.netty.buffer.DrillBuf;
import org.apache.drill.common.collections.MapWithOrdinal;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.OutOfMemoryRuntimeException;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.util.CallBack;
import org.apache.drill.exec.vector.ValueVector;

/**
 * Base class for composite vectors.
 *
 * This class implements common functionality of composite vectors.
 */
public abstract class AbstractContainerVector implements ValueVector {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractContainerVector.class);

  private final MapWithOrdinal<String, ValueVector> vectors =  new MapWithOrdinal<>();
  private final MaterializedField field;
  protected final BufferAllocator allocator;
  protected final CallBack callBack;

  protected AbstractContainerVector(MaterializedField field, BufferAllocator allocator, CallBack callBack) {
    this.field = Preconditions.checkNotNull(field);
    this.allocator = allocator;
    this.callBack = callBack;
  }

  @Override
  public void allocateNew() throws OutOfMemoryRuntimeException {
    if (!allocateNewSafe()) {
      throw new OutOfMemoryRuntimeException();
    }
  }

  @Override
  public boolean allocateNewSafe() {
    for (ValueVector v : vectors.values()) {
      if (!v.allocateNewSafe()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns the field definition of this instance.
   */
  @Override
  public MaterializedField getField() {
    return field;
  }

  /**
   * Adds a new field with the given parameters or replaces the existing one and consequently returns the resultant
   * {@link org.apache.drill.exec.vector.ValueVector}.
   *
   * Execution takes place in the following order:
   * <ul>
   *   <li>
   *     if field is new, create and insert a new vector of desired type.
   *   </li>
   *   <li>
   *     if field exists and existing vector is of desired vector type, return the vector.
   *   </li>
   *   <li>
   *     if field exists and null filled, clear the existing vector; create and insert a new vector of desired type.
   *   </li>
   *   <li>
   *     otherwise, throw an {@link java.lang.IllegalStateException}
   *   </li>
   * </ul>
   *
   * @param name name of the field
   * @param type type of the field
   * @param clazz class of expected vector type
   * @param <T> class type of expected vector type
   * @throws java.lang.IllegalStateException raised if there is a hard schema change
   *
   * @return resultant {@link org.apache.drill.exec.vector.ValueVector}
   */
  public <T extends ValueVector> T addOrGet(String name, MajorType type, Class<T> clazz) {
    final ValueVector existing = getChild(name);
    boolean create = false;
    if (existing == null) {
      create = true;
    } else if (clazz.isAssignableFrom(existing.getClass())) {
      return (T)existing;
    } else if (nullFilled(existing)) {
      existing.clear();
      create = true;
    }
    if (create) {
      final T vector = (T)TypeHelper.getNewVector(field.getPath(), name, allocator, type);
      putChild(name, vector);
      if (callBack!=null) {
        callBack.doWork();
      }
      return vector;
    }
    final String message = "Drill does not support schema change yet. Existing[{}] and desired[{}] vector types mismatch";
    throw new IllegalStateException(String.format(message, existing.getClass().getSimpleName(), clazz.getSimpleName()));
  }

  private boolean nullFilled(ValueVector vector) {
    for (int r=0; r<vector.getAccessor().getValueCount(); r++) {
      if (!vector.getAccessor().isNull(r)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns a {@link org.apache.drill.exec.vector.ValueVector} corresponding to the given ordinal identifier.
   */
  public ValueVector getChildByOrdinal(int id) {
    return vectors.getByOrdinal(id);
  }

  /**
   * Returns a {@link org.apache.drill.exec.vector.ValueVector} corresponding to the given field name if exists or null.
   */
  public ValueVector getChild(String name) {
    return getChild(name, ValueVector.class);
  }

  /**
   * Returns a {@link org.apache.drill.exec.vector.ValueVector} instance of subtype of <T> corresponding to the given
   * field name if exists or null.
   */
  public <T extends ValueVector> T getChild(String name, Class<T> clazz) {
    ValueVector v = vectors.get(name.toLowerCase());
    if (v == null) {
      return null;
    }
    return typeify(v, clazz);
  }

  /**
   * Inserts the vector with the given name if it does not exist else replaces it with the new value.
   *
   * Note that this method does not enforce any vector type check nor throws a schema change exception.
   */
  protected void putChild(String name, ValueVector vector) {
    ValueVector old = vectors.put(
        Preconditions.checkNotNull(name, "field name cannot be null").toLowerCase(),
        Preconditions.checkNotNull(vector, "vector cannot be null")
    );
    if (old != null && old != vector) {
      logger.debug("Field [%s] mutated from [%s] to [%s]", name, old.getClass().getSimpleName(),
          vector.getClass().getSimpleName());
    }

    field.addChild(vector.getField());
  }

  /**
   * Returns a sequence of field names in the order that they show up in the schema.
   */
  protected Collection<String> getChildFieldNames() {
    return Sets.newLinkedHashSet(Iterables.transform(field.getChildren(), new Function<MaterializedField, String>() {
      @Nullable
      @Override
      public String apply(MaterializedField field) {
        return Preconditions.checkNotNull(field).getLastName();
      }
    }));
  }

  /**
   * Returns a sequence of underlying child vectors.
   */
  protected Collection<ValueVector> getChildren() {
    return vectors.values();
  }

  /**
   * Returns the number of underlying child vectors.
   */
  public int size() {
    return vectors.size();
  }

  /**
   * Clears out all underlying child vectors.
   */
 @Override
  public void close() {
    for (ValueVector vector:this) {
      vector.close();
    }
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return vectors.values().iterator();
  }

  protected <T extends ValueVector> T typeify(ValueVector v, Class<T> clazz) {
    if (clazz.isAssignableFrom(v.getClass())) {
      return (T) v;
    }
    throw new IllegalStateException(String.format("Vector requested [%s] was different than type stored [%s].  Drill doesn't yet support hetergenous types.", clazz.getSimpleName(), v.getClass().getSimpleName()));
  }

  /**
   * Returns a list of scalar child vectors recursing the entire vector hierarchy.
   */
  public List<ValueVector> getPrimitiveVectors() {
    List<ValueVector> primitiveVectors = Lists.newArrayList();
    for (ValueVector v : vectors.values()) {
      if (v instanceof AbstractContainerVector) {
        AbstractContainerVector av = (AbstractContainerVector) v;
        primitiveVectors.addAll(av.getPrimitiveVectors());
      } else {
        primitiveVectors.add(v);
      }
    }
    return primitiveVectors;
  }

  /**
   * Returns a vector with its corresponding ordinal mapping if field exists or null.
   */
  public VectorWithOrdinal getChildVectorWithOrdinal(String name) {
    final int ordinal = vectors.getOrdinal(name.toLowerCase());
    if (ordinal < 0) {
      return null;
    }
    final ValueVector vector = vectors.getByOrdinal(ordinal);
    return new VectorWithOrdinal(vector, ordinal);
  }

  public TypedFieldId getFieldIdIfMatches(TypedFieldId.Builder builder, boolean addToBreadCrumb, PathSegment seg) {
    if (seg == null) {
      if (addToBreadCrumb) {
        builder.intermediateType(this.getField().getType());
      }
      return builder.finalType(this.getField().getType()).build();
    }

    if (seg.isArray()) {
      if (seg.isLastPath()) {
        builder //
          .withIndex() //
          .finalType(getLastPathType());

        // remainder starts with the 1st array segment in SchemaPath.
        // only set remainder when it's the only array segment.
        if (addToBreadCrumb) {
          addToBreadCrumb = false;
          builder.remainder(seg);
        }
        return builder.build();
      } else {
        if (addToBreadCrumb) {
          addToBreadCrumb = false;
          builder.remainder(seg);
        }
        // this is a complex array reference, which means it doesn't correspond directly to a vector by itself.
        seg = seg.getChild();
      }
    } else {
      // name segment.
    }

    VectorWithOrdinal vord = getChildVectorWithOrdinal(seg.isArray() ? null : seg.getNameSegment().getPath());
    if (vord == null) {
      return null;
    }

    ValueVector v = vord.vector;
    if (addToBreadCrumb) {
      builder.intermediateType(v.getField().getType());
      builder.addId(vord.ordinal);
    }

    if (v instanceof AbstractContainerVector) {
      // we're looking for a multi path.
      AbstractContainerVector c = (AbstractContainerVector) v;
      return c.getFieldIdIfMatches(builder, addToBreadCrumb, seg.getChild());
    } else {
      if (seg.isNamed()) {
        if(addToBreadCrumb) {
          builder.intermediateType(v.getField().getType());
        }
        builder.finalType(v.getField().getType());
      } else {
        builder.finalType(v.getField().getType().toBuilder().setMode(DataMode.OPTIONAL).build());
      }

      if (seg.isLastPath()) {
        return builder.build();
      } else {
        PathSegment child = seg.getChild();
        if (child.isLastPath() && child.isArray()) {
          if (addToBreadCrumb) {
            builder.remainder(child);
          }
          builder.withIndex();
          builder.finalType(v.getField().getType().toBuilder().setMode(DataMode.OPTIONAL).build());
          return builder.build();
        } else {
          logger.warn("You tried to request a complex type inside a scalar object or path or type is wrong.");
          return null;
        }
      }
    }
  }

  private MajorType getLastPathType() {
    if((this.getField().getType().getMinorType() == MinorType.LIST  &&
        this.getField().getType().getMode() == DataMode.REPEATED)) {  // Use Repeated scalar type instead of Required List.
      VectorWithOrdinal vord = getChildVectorWithOrdinal(null);
      ValueVector v = vord.vector;
      if (! (v instanceof  AbstractContainerVector)) {
        return v.getField().getType();
      }
    } else if (this.getField().getType().getMinorType() == MinorType.MAP  &&
        this.getField().getType().getMode() == DataMode.REPEATED) {  // Use Required Map
      return this.getField().getType().toBuilder().setMode(DataMode.REQUIRED).build();
    }

    return this.getField().getType();
  }

  @Override
  public DrillBuf[] getBuffers(boolean clear) {
    List<DrillBuf> buffers = Lists.newArrayList();
    int expectedBufSize = getBufferSize();
    int actualBufSize = 0 ;

    for (ValueVector v : vectors.values()) {
      for (DrillBuf buf : v.getBuffers(clear)) {
        buffers.add(buf);
        actualBufSize += buf.writerIndex();
      }
    }

    Preconditions.checkArgument(actualBufSize == expectedBufSize);
    return buffers.toArray(new DrillBuf[buffers.size()]);
  }


  protected boolean supportsDirectRead() {
    return false;
  }

}