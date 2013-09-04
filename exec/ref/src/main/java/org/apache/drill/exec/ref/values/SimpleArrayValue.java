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

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ref.rops.DataWriter;


public class SimpleArrayValue extends BaseArrayValue{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SimpleArrayValue.class);

  private static int padding = 5;
  private DataValue[] items;
  private int currentLast = -1;
  
  
  public SimpleArrayValue(int size){
    items  = new DataValue[size];
  }
  
  public SimpleArrayValue(){
    this(5);
  }
  
  @Override
  public void addToArray(int index, DataValue v) {
    if( !(index < items.length)){
      DataValue[] newItems = new DataValue[index + padding];
      System.arraycopy(items, 0, newItems, 0, items.length);
      this.items = newItems;
    }
    items[index] = v;
    this.currentLast = Math.max(this.currentLast, index);
  }

  @Override
  public DataValue getByArrayIndex(int index) {
    if(index < items.length){
      DataValue ret = items[index];
      if(ret == null) return NULL_VALUE;
      return ret;
    }else{
      return NULL_VALUE;
    }
  }

  @Override
  protected int getNextIndex() {
    return currentLast+1;
  }

  public int size(){
    return currentLast+1;
  }
  
  @Override
  public void write(DataWriter w) throws IOException {
    w.writeArrayStart(currentLast+1);
    for(int i = 0; i <= currentLast; i++){
      w.writeArrayElementStart();
      DataValue v = items[i];
      if(v == null){
        w.writeNullValue();
      }else{
        v.write(w);
      }
      w.writeArrayElementEnd();
    }
    w.writeArrayEnd();
  }

  
  @Override
  public void append(BaseArrayValue container) {
    int curLen = size();
    int otherLen = container.size();
    
    // go backwards so the array gets resized first.
    for(int i = otherLen -1; i > -1; i--){
      DataValue v = container.getByArrayIndex(otherLen);
      this.addToArray(i + curLen, v);
    }
    
  }

  @Override
  public BaseArrayValue getAsArray() {
    return this;
  }

  @Override
  public BaseMapValue getAsMap() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return "SimpleArrayValue [items=" + Arrays.toString(items) + ", currentLast=" + currentLast + "]";
  }

  @Override
  public boolean equals(DataValue v) {
    if(v.getDataType().getMinorType() == MinorType.REPEATMAP) return false;
    BaseArrayValue other = v.getAsContainer().getAsArray();
    if(this.size() != other.size()) return false;
    for(int i =0; i < this.size(); i++){
      DataValue v1 = this.getByArrayIndex(i);
      DataValue v2 = other.getByArrayIndex(i);
      if(!v1.equals(v2)) return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash((Object[]) items);
  }
  
  @Override
  public DataValue copy() {
    SimpleArrayValue out = new SimpleArrayValue(this.size());
    for(int i =0; i < this.size(); i++)
      out.addToArray(i, this.getByArrayIndex(i));

    return out;
  }
  
}
