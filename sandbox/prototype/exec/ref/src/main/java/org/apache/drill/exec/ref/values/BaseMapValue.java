/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.ref.values;

import java.util.Map;

import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.ValueExpressions.CollisionBehavior;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ref.exceptions.RecordException;
import org.apache.tools.ant.types.DataType;

public abstract class BaseMapValue extends BaseDataValue implements ContainerValue,
    Iterable<Map.Entry<CharSequence, DataValue>> {

  @Override
  public void addValue(PathSegment segment, DataValue v) {
    if(v == null) throw new RecordException("You attempted to add a null value to a map.", null);
    if (segment.isArray())
      throw new RecordException(
          "You're attempted to save something at a particular array location while the location of that setting was a Map.", null);

    CharSequence name = segment.getNameSegment().getPath();
    DataValue current = getByNameNoNulls(name);
    if (!segment.isLastPath() && current != DataValue.NULL_VALUE) {
      current.addValue(segment.getChild(), v);
      return;
    } else {
      DataValue fullPathValue = ValueUtils.getIntermediateValues(segment.getChild(), v);
      DataValue mergedValue = ValueUtils.getMergedDataValue(segment.getCollisionBehavior(), getByNameNoNulls(name),
          fullPathValue);
      setByName(name, mergedValue);

    }

  }

  @Override
  public void removeValue(PathSegment segment) {
    if (segment.isArray())
      throw new RecordException(
          "You're attempted to remove something at a particular array location while the location of that setting was a Map.", null);

    CharSequence name = segment.getNameSegment().getPath();
    DataValue current = getByNameNoNulls(name);
    if (!segment.isLastPath() && current != DataValue.NULL_VALUE) {
      current.removeValue(segment.getChild());
      return;
    } else {
      removeByName(name);
    }

  }

  protected abstract void setByName(CharSequence name, DataValue v);

  protected abstract DataValue getByName(CharSequence name);

  protected abstract void removeByName(CharSequence name);

  private DataValue getByNameNoNulls(CharSequence name) {
    DataValue v = getByName(name);
    if (v == null) return NULL_VALUE;
    return v;
  }

  @Override
  public DataValue getValue(PathSegment segment) {
    if (segment == null) return this;
    if (segment.isArray()) return NULL_VALUE;
    return getByNameNoNulls(segment.getNameSegment().getPath()).getValue(segment.getChild());
  }

  @Override
  public ContainerValue getAsContainer() {
    return this;
  }

  @Override
  public MajorType getDataType() {
    return MajorType.newBuilder().setMinorType(MinorType.REPEATMAP).setMode(DataMode.REPEATED).build();
  }

  public void merge(BaseMapValue otherMap) {
    for (Map.Entry<CharSequence, DataValue> e : otherMap) {
      final DataValue oldValue = getByName(e.getKey());
      if (oldValue == DataValue.NULL_VALUE || oldValue == null) {
        setByName(e.getKey(), e.getValue());
      } else {
        setByName(e.getKey(), ValueUtils.getMergedDataValue(CollisionBehavior.MERGE_OVERRIDE, oldValue, e.getValue()));
      }
    }
  }

}
