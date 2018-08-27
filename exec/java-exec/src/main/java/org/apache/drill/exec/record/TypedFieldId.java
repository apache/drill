/*
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

import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.expr.BasicTypeHelper;
import org.apache.drill.exec.vector.ValueVector;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

/**
 * Declares a value vector field, providing metadata about the field.
 * Drives code generation by providing type and other structural
 * information that determine code structure.
 */

public class TypedFieldId {
  final MajorType finalType;
  final MajorType secondaryFinal;
  final MajorType intermediateType;
  final int[] fieldIds;
  final boolean isHyperReader;
  final boolean isListVector;
  final PathSegment remainder;

  public TypedFieldId(MajorType type, int... fieldIds) {
    this(type, type, type, false, null, fieldIds);
  }

  public TypedFieldId(MajorType type, IntArrayList breadCrumb, PathSegment remainder) {
    this(type, type, type, false, remainder, breadCrumb.toArray());
  }

  public TypedFieldId(MajorType type, boolean isHyper, int... fieldIds) {
    this(type, type, type, isHyper, null, fieldIds);
  }

  public TypedFieldId(MajorType intermediateType, MajorType secondaryFinal, MajorType finalType, boolean isHyper, PathSegment remainder, int... fieldIds) {
    this(intermediateType, secondaryFinal, finalType, isHyper, false, remainder, fieldIds);
  }

  public TypedFieldId(MajorType intermediateType, MajorType secondaryFinal, MajorType finalType, boolean isHyper, boolean isListVector, PathSegment remainder, int... fieldIds) {
    super();
    this.intermediateType = intermediateType;
    this.finalType = finalType;
    this.secondaryFinal = secondaryFinal;
    this.fieldIds = fieldIds;
    this.isHyperReader = isHyper;
    this.isListVector = isListVector;
    this.remainder = remainder;
  }

  public TypedFieldId cloneWithChild(int id) {
    int[] fieldIds = ArrayUtils.add(this.fieldIds, id);
    return new TypedFieldId(intermediateType, secondaryFinal, finalType, isHyperReader, remainder, fieldIds);
  }

  public PathSegment getLastSegment() {
    if (remainder == null) {
      return null;
    }
    PathSegment seg = remainder;
    while (seg.getChild() != null) {
      seg = seg.getChild();
    }
    return seg;
  }

  public TypedFieldId cloneWithRemainder(PathSegment remainder) {
    return new TypedFieldId(intermediateType, secondaryFinal, finalType, isHyperReader, remainder, fieldIds);
  }

  public boolean hasRemainder() {
    return remainder != null;
  }

  public PathSegment getRemainder() {
    return remainder;
  }

  public boolean isHyperReader() {
    return isHyperReader;
  }

  public boolean isListVector() {
    return isListVector;
  }

  public MajorType getIntermediateType() {
    return intermediateType;
  }

  /**
   * Return the class for the value vector (type, mode).
   *
   * @return the specific, generated ValueVector subclass that
   * stores values of the given (type, mode) combination
   */

  public Class<? extends ValueVector> getIntermediateClass() {
    return (Class<? extends ValueVector>) BasicTypeHelper.getValueVectorClass(intermediateType.getMinorType(),
        intermediateType.getMode());
  }

  public MajorType getFinalType() {
    return finalType;
  }

  public int[] getFieldIds() {
    return fieldIds;
  }

  public MajorType getSecondaryFinal() {
    return secondaryFinal;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder{
    final IntArrayList ids = new IntArrayList();
    MajorType finalType;
    MajorType intermediateType;
    MajorType secondaryFinal;
    PathSegment remainder;
    boolean hyperReader = false;
    boolean withIndex = false;
    boolean isListVector = false;

    public Builder addId(int id) {
      ids.add(id);
      return this;
    }

    public Builder withIndex() {
      withIndex = true;
      return this;
    }

    public Builder remainder(PathSegment remainder) {
      this.remainder = remainder;
      return this;
    }

    public Builder hyper() {
      this.hyperReader = true;
      return this;
    }

    public Builder listVector() {
      this.isListVector = true;
      return this;
    }

    public Builder finalType(MajorType finalType) {
      this.finalType = finalType;
      return this;
    }

    public Builder secondaryFinal(MajorType secondaryFinal) {
      this.secondaryFinal = secondaryFinal;
      return this;
    }

    public Builder intermediateType(MajorType intermediateType) {
      this.intermediateType = intermediateType;
      return this;
    }

    public TypedFieldId build() {
      Preconditions.checkNotNull(intermediateType);
      Preconditions.checkNotNull(finalType);

      if (intermediateType == null) {
        intermediateType = finalType;
      }
      if (secondaryFinal == null) {
        secondaryFinal = finalType;
      }

      MajorType actualFinalType = finalType;
      //MajorType secondaryFinal = finalType;

      // if this has an index, switch to required type for output
      //if(withIndex && intermediateType == finalType) actualFinalType = finalType.toBuilder().setMode(DataMode.REQUIRED).build();

      // if this isn't a direct access, switch the final type to nullable as offsets may be null.
      // TODO: there is a bug here with some things.
      //if(intermediateType != finalType) actualFinalType = finalType.toBuilder().setMode(DataMode.OPTIONAL).build();

      return new TypedFieldId(intermediateType, secondaryFinal, actualFinalType, hyperReader, isListVector, remainder, ids.toArray());
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(fieldIds);
    result = prime * result + ((finalType == null) ? 0 : finalType.hashCode());
    result = prime * result + ((intermediateType == null) ? 0 : intermediateType.hashCode());
    result = prime * result + (isHyperReader ? 1231 : 1237);
    result = prime * result + ((remainder == null) ? 0 : remainder.hashCode());
    result = prime * result + ((secondaryFinal == null) ? 0 : secondaryFinal.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    TypedFieldId other = (TypedFieldId) obj;
    if (!Arrays.equals(fieldIds, other.fieldIds)) {
      return false;
    }
    if (finalType == null) {
      if (other.finalType != null) {
        return false;
      }
    } else if (!finalType.equals(other.finalType)) {
      return false;
    }
    if (intermediateType == null) {
      if (other.intermediateType != null) {
        return false;
      }
    } else if (!intermediateType.equals(other.intermediateType)) {
      return false;
    }
    if (isHyperReader != other.isHyperReader) {
      return false;
    }
    if (remainder == null) {
      if (other.remainder != null) {
        return false;
      }
    } else if (!remainder.equals(other.remainder)) {
      return false;
    }
    if (secondaryFinal == null) {
      if (other.secondaryFinal != null) {
        return false;
      }
    } else if (!secondaryFinal.equals(other.secondaryFinal)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    final int maxLen = 10;
    return "TypedFieldId [fieldIds="
        + (fieldIds != null ? Arrays.toString(Arrays.copyOf(fieldIds, Math.min(fieldIds.length, maxLen))) : null)
        + ", remainder=" + remainder + "]";
  }

  /**
   * Generates the full path to a field from the typefield ids
   *
   * @param typeFieldId
   * @param recordBatch
   * @return
   */
  public static String getPath(TypedFieldId typeFieldId, RecordBatch recordBatch) {
    StringBuilder name = new StringBuilder();
    final String SEPARATOR = ".";
    final int[] fieldIds = typeFieldId.getFieldIds();
    VectorWrapper<?> topLevel = recordBatch.getValueAccessorById(null, fieldIds[0]);
    name.append(topLevel.getField().getName());
    // getChildWrapper(int[] fieldIds) is used to walk down the list of fieldIds.
    // getChildWrapper() has a quirk where if the fieldIds array is of length == 1
    // then it would just return 'this'.
    // For example, if you had a field 'a.b' with field ids {1, 2} and you had the
    // VectorWrapper for 'a', say 'aVW'. Then calling aVW.getChildWrapper({2}) returns
    // aVW and not the vectorWrapper for 'b'.
    // The code works around this quirk by always querying 2 levels deep.
    // i.e. childVectorWrapper = parentVectorWrapper.gerChildWrapper({parentFieldId, childFieldId})
    int[] lookupLevel = new int[2];
    for (int i = 0; i < fieldIds.length - 1; i++) {
      lookupLevel[0] = fieldIds[i];
      // this is the level for which the actual lookup is done
      lookupLevel[1] = fieldIds[i + 1];
      topLevel = topLevel.getChildWrapper(lookupLevel);
      name.append(SEPARATOR + topLevel.getField().getName());
    }
    return name.toString();
  }

}
