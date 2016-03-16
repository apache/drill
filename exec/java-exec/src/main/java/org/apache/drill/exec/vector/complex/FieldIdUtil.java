/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.vector.complex;

import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.vector.ValueVector;

import java.util.List;

public class FieldIdUtil {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FieldIdUtil.class);

  public static TypedFieldId getFieldIdIfMatchesUnion(UnionVector unionVector, TypedFieldId.Builder builder, boolean addToBreadCrumb, PathSegment seg) {
    if (seg.isNamed()) {
      ValueVector v = unionVector.getMap();
      if (v != null) {
        return getFieldIdIfMatches(v, builder, addToBreadCrumb, seg);
      } else {
        return null;
      }
    } else if (seg.isArray()) {
      ValueVector v = unionVector.getList();
      if (v != null) {
        return getFieldIdIfMatches(v, builder, addToBreadCrumb, seg);
      } else {
        return null;
      }
    }
    return null;
  }


  public static TypedFieldId getFieldIdIfMatches(ValueVector vector, TypedFieldId.Builder builder, boolean addToBreadCrumb, PathSegment seg) {
    if (vector instanceof RepeatedMapVector && seg != null && seg.isArray() && !seg.isLastPath()) {
      if (addToBreadCrumb) {
        addToBreadCrumb = false;
        builder.remainder(seg);
      }
      // skip the first array segment as there is no corresponding child vector.
      seg = seg.getChild();

      // multi-level numbered access to a repeated map is not possible so return if the next part is also an array
      // segment.
      if (seg.isArray()) {
        return null;
      }
    }

    if (seg == null) {
      if (addToBreadCrumb) {
        builder.intermediateType(vector.getField().getType());
      }
      return builder.finalType(vector.getField().getType()).build();
    }

    if (seg.isArray()) {
      if (seg.isLastPath()) {
        MajorType type;
        if (vector instanceof AbstractContainerVector) {
          type = ((AbstractContainerVector) vector).getLastPathType();
        } else if (vector instanceof ListVector) {
          type = ((ListVector) vector).getDataVector().getField().getType();
          builder.listVector();
        } else {
          throw new UnsupportedOperationException("FieldIdUtil does not support vector of type " + vector.getField().getType());
        }
        builder //
                .withIndex() //
                .finalType(type);

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
      }
    } else {
      if (vector instanceof ListVector) {
        return null;
      }
    }

    ValueVector v;
    if (vector instanceof AbstractContainerVector) {
      VectorWithOrdinal vord = ((AbstractContainerVector) vector).getChildVectorWithOrdinal(seg.isArray() ? null : seg.getNameSegment().getPath());
      if (vord == null) {
        return null;
      }
      v = vord.vector;
      if (addToBreadCrumb) {
        builder.intermediateType(v.getField().getType());
        builder.addId(vord.ordinal);
      }
    } else if (vector instanceof ListVector) {
      v = ((ListVector) vector).getDataVector();
    } else {
      throw new UnsupportedOperationException("FieldIdUtil does not support vector of type " + vector.getField().getType());
    }

    if (v instanceof AbstractContainerVector) {
      // we're looking for a multi path.
      AbstractContainerVector c = (AbstractContainerVector) v;
      return getFieldIdIfMatches(c, builder, addToBreadCrumb, seg.getChild());
    } else if(v instanceof ListVector) {
      ListVector list = (ListVector) v;
      return getFieldIdIfMatches(list, builder, addToBreadCrumb, seg.getChild());
    } else if (v instanceof  UnionVector) {
      return getFieldIdIfMatchesUnion((UnionVector) v, builder, addToBreadCrumb, seg.getChild());
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

  public static TypedFieldId getFieldId(ValueVector vector, int id, SchemaPath expectedPath, boolean hyper) {
    if (!expectedPath.getRootSegment().getNameSegment().getPath().equalsIgnoreCase(vector.getField().getPath())) {
      return null;
    }
    PathSegment seg = expectedPath.getRootSegment();

    TypedFieldId.Builder builder = TypedFieldId.newBuilder();
    if (hyper) {
      builder.hyper();
    }
    if (vector instanceof UnionVector) {
      builder.addId(id).remainder(expectedPath.getRootSegment().getChild());
      List<MinorType> minorTypes = ((UnionVector) vector).getSubTypes();
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
        return getFieldIdIfMatchesUnion((UnionVector) vector, builder, false, seg.getChild());
      }
    } else if (vector instanceof ListVector) {
      ListVector list = (ListVector) vector;
      builder.intermediateType(vector.getField().getType());
      builder.addId(id);
      return getFieldIdIfMatches(list, builder, true, expectedPath.getRootSegment().getChild());
    } else
    if (vector instanceof AbstractContainerVector) {
      // we're looking for a multi path.
      AbstractContainerVector c = (AbstractContainerVector) vector;
      builder.intermediateType(vector.getField().getType());
      builder.addId(id);
      return getFieldIdIfMatches(c, builder, true, expectedPath.getRootSegment().getChild());

    } else {
      builder.intermediateType(vector.getField().getType());
      builder.addId(id);
      builder.finalType(vector.getField().getType());
      if (seg.isLastPath()) {
        return builder.build();
      } else {
        PathSegment child = seg.getChild();
        if (child.isArray() && child.isLastPath()) {
          builder.remainder(child);
          builder.withIndex();
          builder.finalType(vector.getField().getType().toBuilder().setMode(DataMode.OPTIONAL).build());
          return builder.build();
        } else {
          return null;
        }

      }
    }
  }
}
