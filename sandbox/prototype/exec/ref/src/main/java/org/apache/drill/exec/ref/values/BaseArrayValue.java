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

import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.tools.ant.types.DataType;


public abstract class BaseArrayValue extends BaseDataValue implements ContainerValue{

  private MajorType initiatingType;
  private MajorType runningType = MajorType.newBuilder().setMode(DataMode.REPEATED).setMinorType(MinorType.LATE).build();
  
  @Override
  public void addValue(PathSegment segment, DataValue v) {
    if(initiatingType == null){
      initiatingType = v.getDataType();
      runningType = initiatingType.toBuilder().setMode(DataMode.REPEATED).build();
    }else{
      if(!v.getDataType().equals(initiatingType)){
        throw new RuntimeException("The reference interpreter doesn't support polymorphic types.");
      }
    }
    
    DataValue fullPathValue = ValueUtils.getIntermediateValues(segment.getChild(), v);
    if(segment.isArray()){ // we need to place this object in the given position.
      int index = segment.getArraySegment().getIndex();
      DataValue mergedValue = ValueUtils.getMergedDataValue(segment.getCollisionBehavior(), getByArrayIndex(index), fullPathValue);
      addToArray(index, mergedValue);
    }else{ // add to end of array.
     addToArray(getNextIndex(), fullPathValue);
    }
  }

  protected abstract void addToArray(int index, DataValue v);
  public abstract DataValue getByArrayIndex(int index);
  protected abstract int getNextIndex();
  public abstract void append(BaseArrayValue container);
  public abstract int size();
  
  @Override
  public DataValue getValue(PathSegment segment) {
    if(segment == null){ // return entire array
      return this;
    }else if(!segment.isArray()){  // requesting a named value from within an array.  No value should be returned.
      return DataValue.NULL_VALUE;
    }else{
      DataValue v = getByArrayIndex(segment.getArraySegment().getIndex());
      if(v == null) return DataValue.NULL_VALUE;
      return v.getValue(segment.getChild());
    }
  }

  @Override
  public ContainerValue getAsContainer() {
    return this;
  }

  @Override
  public MajorType getDataType() {
    return runningType;
  }
  
  
  

  
  
}
