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

import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.vector.ValueVector;

public abstract class AbstractContainerVector implements ValueVector{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractContainerVector.class);

  public abstract <T extends ValueVector> T addOrGet(String name, MajorType type, Class<T> clazz);
  public abstract <T extends ValueVector> T get(String name, Class<T> clazz);
  public abstract int size();

  protected <T extends ValueVector> T typeify(ValueVector v, Class<T> clazz){
    if(clazz.isAssignableFrom(v.getClass())){
      return (T) v;
    }else{
      throw new IllegalStateException(String.format("Vector requested [%s] was different than type stored [%s].  Drill doesn't yet support hetergenous types.", clazz.getSimpleName(), v.getClass().getSimpleName()));
    }
  }

  public abstract VectorWithOrdinal getVectorWithOrdinal(String name);


  public TypedFieldId getFieldIdIfMatches(TypedFieldId.Builder builder, boolean addToBreadCrumb, PathSegment seg){
    if(seg == null){
      if(addToBreadCrumb) builder.intermediateType(this.getField().getType());
      return builder.finalType(this.getField().getType()).build();
    }

    if(seg.isArray()){

      if(seg.isLastPath()){
        if(addToBreadCrumb) builder.intermediateType(this.getField().getType());
        return builder //
          .remainder(seg) //
          .finalType(this.getField().getType()) //
          .withIndex() //
          .build();
      }else{
        if(addToBreadCrumb){
          addToBreadCrumb = false;
          builder.remainder(seg);
        }
        // this is a complex array reference, which means it doesn't correspond directly to a vector by itself.
        seg = seg.getChild();

      }

    }else{
      // name segment.
    }

    VectorWithOrdinal vord = getVectorWithOrdinal(seg.isArray() ? null : seg.getNameSegment().getPath());
    if(vord == null) return null;


    if(addToBreadCrumb){
      builder.intermediateType(this.getField().getType());
      builder.addId(vord.ordinal);
    }

    ValueVector v = vord.vector;

    if(v instanceof AbstractContainerVector){
      // we're looking for a multi path.
      AbstractContainerVector c = (AbstractContainerVector) v;
      return c.getFieldIdIfMatches(builder, addToBreadCrumb, seg.getChild());
    }else{
      if(seg.isLastPath()){
        if(addToBreadCrumb) builder.intermediateType(v.getField().getType());
        return builder.finalType(v.getField().getType()).build();
      }else{
        logger.warn("You tried to request a complex type inside a scalar object.");
        return null;
      }
    }

  }

  protected boolean supportsDirectRead(){
    return false;
  }

  protected class VectorWithOrdinal{
    final ValueVector vector;
    final int ordinal;

    public VectorWithOrdinal(ValueVector v, int ordinal){
      this.vector = v;
      this.ordinal = ordinal;
    }
  }
}

