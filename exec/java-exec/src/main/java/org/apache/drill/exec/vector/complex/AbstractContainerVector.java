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

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
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

  protected final MaterializedField field;
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

  /**
   * Returns the field definition of this instance.
   */
  @Override
  public MaterializedField getField() {
    return field;
  }

  /**
   * Returns a {@link org.apache.drill.exec.vector.ValueVector} corresponding to the given field name if exists or null.
   */
  public ValueVector getChild(String name) {
    return getChild(name, ValueVector.class);
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
   * Clears out all underlying child vectors.
   */
 @Override
  public void close() {
    for (ValueVector vector:this) {
      vector.close();
    }
  }

  protected <T extends ValueVector> T typeify(ValueVector v, Class<T> clazz) {
    if (clazz.isAssignableFrom(v.getClass())) {
      return (T) v;
    }
    throw new IllegalStateException(String.format("Vector requested [%s] was different than type stored [%s].  Drill doesn't yet support hetergenous types.", clazz.getSimpleName(), v.getClass().getSimpleName()));
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

  protected boolean supportsDirectRead() {
    return false;
  }

  // return the number of child vectors
  public abstract int size();

  // add a new vector with the input MajorType or return the existing vector if we already added one with the same type
  public abstract <T extends ValueVector> T addOrGet(String name, MajorType type, Class<T> clazz);

  // return the child vector with the input name
  public abstract <T extends ValueVector> T getChild(String name, Class<T> clazz);

  // return the child vector's ordinal in the composite container
  public abstract VectorWithOrdinal getChildVectorWithOrdinal(String name);
}