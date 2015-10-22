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

import com.google.common.collect.Lists;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MajorTypeOrBuilder;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractContainerVector;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.exec.vector.complex.ListVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.UnionVector;

import java.util.ArrayList;
import java.util.List;

public class SimpleVectorWrapper<T extends ValueVector> implements VectorWrapper<T>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SimpleVectorWrapper.class);

  private T v;

  public SimpleVectorWrapper(T v) {
    this.v = v;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Class<T> getVectorClass() {
    return (Class<T>) v.getClass();
  }

  @Override
  public MaterializedField getField() {
    return v.getField();
  }

  @Override
  public T getValueVector() {
    return v;
  }

  @Override
  public T[] getValueVectors() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isHyper() {
    return false;
  }

  @SuppressWarnings("unchecked")
  @Override
  public VectorWrapper<T> cloneAndTransfer() {
    TransferPair tp = v.getTransferPair();
    tp.transfer();
    return new SimpleVectorWrapper<T>((T) tp.getTo());
  }

  @Override
  public void clear() {
    v.clear();
  }

  public static <T extends ValueVector> SimpleVectorWrapper<T> create(T v) {
    return new SimpleVectorWrapper<T>(v);
  }


  @Override
  public VectorWrapper<?> getChildWrapper(int[] ids) {
    if (ids.length == 1) {
      return this;
    }

    ValueVector vector = v;
    for (int i = 1; i < ids.length; i++) {
      final AbstractMapVector mapLike = AbstractMapVector.class.cast(vector);
      if (mapLike == null) {
        return null;
      }
      vector = mapLike.getChildByOrdinal(ids[i]);
    }

    return new SimpleVectorWrapper<>(vector);
  }

  @Override
  public TypedFieldId getFieldIdIfMatches(int id, SchemaPath expectedPath) {
    if (!expectedPath.getRootSegment().segmentEquals(v.getField().getPath().getRootSegment())) {
      return null;
    }
    PathSegment seg = expectedPath.getRootSegment();

    if (v instanceof UnionVector) {
      TypedFieldId.Builder builder = TypedFieldId.newBuilder();
      builder.addId(id).remainder(expectedPath.getRootSegment().getChild());
      List<MinorType> minorTypes = ((UnionVector) v).getSubTypes();
      MajorType.Builder majorTypeBuilder = MajorType.newBuilder().setMinorType(MinorType.UNION);
      for (MinorType type : minorTypes) {
        majorTypeBuilder.addSubType(type);
      }
      MajorType majorType = majorTypeBuilder.build();
      builder.intermediateType(majorType);
      if (seg.isLastPath()) {
        builder.finalType(majorType);
        return builder.build();
      } else {
        return ((UnionVector) v).getFieldIdIfMatches(builder, false, seg.getChild());
      }
    } else if (v instanceof ListVector) {
      ListVector list = (ListVector) v;
      TypedFieldId.Builder builder = TypedFieldId.newBuilder();
      builder.intermediateType(v.getField().getType());
      builder.addId(id);
      return list.getFieldIdIfMatches(builder, true, expectedPath.getRootSegment().getChild());
    } else
    if (v instanceof AbstractContainerVector) {
      // we're looking for a multi path.
      AbstractContainerVector c = (AbstractContainerVector) v;
      TypedFieldId.Builder builder = TypedFieldId.newBuilder();
      builder.intermediateType(v.getField().getType());
      builder.addId(id);
      return c.getFieldIdIfMatches(builder, true, expectedPath.getRootSegment().getChild());

    } else {
      TypedFieldId.Builder builder = TypedFieldId.newBuilder();
      builder.intermediateType(v.getField().getType());
      builder.addId(id);
      builder.finalType(v.getField().getType());
      if (seg.isLastPath()) {
        return builder.build();
      } else {
        PathSegment child = seg.getChild();
        if (child.isArray() && child.isLastPath()) {
          builder.remainder(child);
          builder.withIndex();
          builder.finalType(v.getField().getType().toBuilder().setMode(DataMode.OPTIONAL).build());
          return builder.build();
        } else {
          return null;
        }

      }
    }
  }

}
