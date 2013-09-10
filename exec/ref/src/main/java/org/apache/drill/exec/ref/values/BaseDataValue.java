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
package org.apache.drill.exec.ref.values;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.PathSegment;

public abstract class BaseDataValue implements DataValue{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseDataValue.class);
  
  @Override
  public DataValue getValue(PathSegment segment) {
    if(segment == null){
      return this;
    }else{ // looking for a lower level value when there is none.
      return DataValue.NULL_VALUE;
    }
  }
  
  public void addValue(PathSegment segment, DataValue v) {
    throw new IllegalArgumentException("You can't add a value to a non-container type.");
  }

  public void removeValue(PathSegment segment) {
    throw new IllegalArgumentException("You can't remove a value from a non-container type.");
  }

  @Override
  public NumericValue getAsNumeric() {
    throw new DrillRuntimeException(String.format("A %s value is not a NumericValue.", this.getClass().getCanonicalName()));
  }

  @Override
  public ContainerValue getAsContainer() {
    throw new DrillRuntimeException(String.format("A %s value is not a ContainerValue.", this.getClass().getCanonicalName()));
  }

  @Override
  public StringValue getAsStringValue() {
    throw new DrillRuntimeException(String.format("A %s value is not a StringValue.", this.getClass().getCanonicalName()));
  }

  public BytesValue getAsBytesValue(){
    throw new DrillRuntimeException(String.format("A %s value is not a BytesValue.", this.getClass().getCanonicalName()));
  }
  
  public BooleanValue getAsBooleanValue(){
    throw new DrillRuntimeException(String.format("A %s value is not a BooleanValue.", this.getClass().getCanonicalName()));
  }

  @Override
  public abstract int hashCode();

  @Override
  public boolean equals(Object obj) {
    if(obj instanceof DataValue){
      return this.equals((DataValue) obj);
    }else{
      return false;
    }
  }
  

  @Override
  public abstract DataValue copy();

}
