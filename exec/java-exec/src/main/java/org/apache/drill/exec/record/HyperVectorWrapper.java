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
package org.apache.drill.exec.record;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractContainerVector;
import org.apache.drill.exec.vector.complex.MapVector;

import com.google.common.base.Preconditions;


public class HyperVectorWrapper<T extends ValueVector> implements VectorWrapper<T>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HyperVectorWrapper.class);

  private T[] vectors;
  private MaterializedField f;
  private final boolean releasable;

  public HyperVectorWrapper(MaterializedField f, T[] v) {
    this(f, v, true);
  }

  public HyperVectorWrapper(MaterializedField f, T[] v, boolean releasable) {
    assert(v.length > 0);
    this.f = f;
    this.vectors = v;
    this.releasable = releasable;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Class<T> getVectorClass() {
    return (Class<T>) vectors.getClass().getComponentType();
  }

  @Override
  public MaterializedField getField() {
    return f;
  }

  @Override
  public T getValueVector() {
    throw new UnsupportedOperationException();
  }

  @Override
  public T[] getValueVectors() {
    return vectors;
  }

  @Override
  public boolean isHyper() {
    return true;
  }

  @Override
  public void clear() {
    if (!releasable) {
      return;
    }
    for (T x : vectors) {
      x.clear();
    }
  }

  @Override
  public VectorWrapper<?> getChildWrapper(int[] ids) {
    if (ids.length == 1) {
      return this;
    }

    ValueVector[] vectors = new ValueVector[this.vectors.length];
    int index = 0;

    for (ValueVector v : this.vectors) {
      ValueVector vector = v;
      for (int i = 1; i < ids.length; i++) {
        MapVector map = (MapVector) vector;
        vector = map.getVectorById(ids[i]);
      }
      vectors[index] = vector;
      index++;
    }
    return new HyperVectorWrapper<ValueVector>(vectors[0].getField(), vectors);
  }

  @Override
  public TypedFieldId getFieldIdIfMatches(int id, SchemaPath expectedPath) {
    ValueVector v = vectors[0];
    if (!expectedPath.getRootSegment().segmentEquals(v.getField().getPath().getRootSegment())) {
      return null;
    }

    if (v instanceof AbstractContainerVector) {
      // we're looking for a multi path.
      AbstractContainerVector c = (AbstractContainerVector) v;
      TypedFieldId.Builder builder = TypedFieldId.newBuilder();
      builder.intermediateType(v.getField().getType());
      builder.hyper();
      builder.addId(id);
      return c.getFieldIdIfMatches(builder, true, expectedPath.getRootSegment().getChild());

    } else {
      return TypedFieldId.newBuilder() //
          .intermediateType(v.getField().getType()) //
          .finalType(v.getField().getType()) //
          .addId(id) //
          .hyper() //
          .build();
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public VectorWrapper<T> cloneAndTransfer() {
    return new HyperVectorWrapper<T>(f, vectors, false);
//    T[] newVectors = (T[]) Array.newInstance(vectors.getClass().getComponentType(), vectors.length);
//    for(int i =0; i < newVectors.length; i++) {
//      TransferPair tp = vectors[i].getTransferPair();
//      tp.transfer();
//      newVectors[i] = (T) tp.getTo();
//    }
//    return new HyperVectorWrapper<T>(f, newVectors);
  }

  public static <T extends ValueVector> HyperVectorWrapper<T> create(MaterializedField f, T[] v, boolean releasable) {
    return new HyperVectorWrapper<T>(f, v, releasable);
  }

  public void addVector(ValueVector v) {
    Preconditions.checkArgument(v.getClass() == this.getVectorClass(), String.format("Cannot add vector type %s to hypervector type %s", v.getClass(), this.getVectorClass()));
    vectors = (T[]) ArrayUtils.add(vectors, v);// TODO optimize this so not copying every time
  }

  public void addVectors(ValueVector[] vv) {
    vectors = (T[]) ArrayUtils.add(vectors, vv);
  }

}
