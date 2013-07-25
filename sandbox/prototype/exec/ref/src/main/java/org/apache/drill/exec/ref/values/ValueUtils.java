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
import org.apache.drill.common.expression.ValueExpressions.CollisionBehavior;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ref.exceptions.RecordException;
import org.apache.tools.ant.types.DataType;

public class ValueUtils {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ValueUtils.class);
  
  public static DataValue getMergedDataValue(CollisionBehavior behavior, DataValue oldValue, DataValue newValue){
//    logger.debug("Checking merged to see if merged value should be added.  Segment {}, oldValue: {}, new value: " + newValue, segment, oldValue);
    if(oldValue == DataValue.NULL_VALUE) return newValue;
    
    switch(behavior){
    case SKIP:
      return oldValue;
    case REPLACE: 
      return newValue;
    case FAIL:
      throw new RecordException("Failure while doing query.  The destination specified already contains a value and no collision behavior was provdied.", null);
    case OBJECTIFY:
      SimpleMapValue n = new SimpleMapValue();
      n.setByName("old", oldValue);
      n.setByName("new",  newValue);
      return n;
    case ARRAYIFY:
      SimpleArrayValue a = new SimpleArrayValue();
      a.addToArray(0, oldValue);
      a.addToArray(1, newValue);
      return a;
    case MERGE_OVERRIDE:
      MajorType oldT = oldValue.getDataType();
      MajorType newT = newValue.getDataType();
      if(oldT.getMinorType() == MinorType.REPEATMAP && newT.getMinorType() == MinorType.REPEATMAP){
        oldValue.getAsContainer().getAsMap().merge(newValue.getAsContainer().getAsMap());
        return oldValue;
      }else if(oldT.getMode() == DataMode.REPEATED && newT.getMode() == DataMode.REPEATED){
        logger.debug("Merging two arrays. {} and {}", oldValue, newValue);
        oldValue.getAsContainer().getAsArray().append(newValue.getAsContainer().getAsArray());
        return oldValue;
      }else if(oldT.getMode() == DataMode.REPEATED || newT.getMode() == DataMode.REPEATED || oldT.getMinorType() == MinorType.REPEATMAP || newT.getMinorType() == MinorType.REPEATMAP){
        throw new RecordException(String.format("Failure while doing query.  You requested a merge of values that were incompatibile.  Examples include merging an array and a map or merging a map/array with a scalar.  Merge Types were %s and %s.", oldT, newT), null);
      }else{
        // scalar type, just override the value.
        return newValue;
      }
    default:
      throw new UnsupportedOperationException();
      
    }
  }
  
  public static DataValue getIntermediateValues(PathSegment missing, DataValue value){
    if(missing == null) return value;

    DataValue current = missing.isArray() ? new SimpleArrayValue() : new SimpleMapValue();
    current.addValue(missing, value);
    return current;
  }
}
