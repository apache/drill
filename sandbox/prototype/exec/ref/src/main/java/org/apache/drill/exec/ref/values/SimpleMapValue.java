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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.exec.ref.exceptions.RecordException;
import org.apache.drill.exec.ref.rops.DataWriter;
import org.apache.tools.ant.types.DataType;

public class SimpleMapValue extends BaseMapValue{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SimpleMapValue.class);
  
  private HashMap<CharSequence, DataValue> map = new HashMap<CharSequence, DataValue>();

  
  @Override
  public void setByName(CharSequence name, DataValue v) {
    if( v== null) throw new RecordException(String.format("You attempted to write a null value with the map key of %s.", name), null);
    map.put(name, v);
  }

  @Override
  protected DataValue getByName(CharSequence name) {
    return map.get(name);
  }

  @Override
  protected void removeByName(CharSequence name) {
    map.remove(name);
  }

  @Override
  public void write(DataWriter w) throws IOException {
    w.writeMapStart();
    
    for(Map.Entry<CharSequence, DataValue> e : map.entrySet()){
      DataValue v = e.getValue();
      // skip null values.
      if(v == null) continue;
//      logger.debug("Writing key {}", e.getKey());
      w.writeMapKey(e.getKey());
      w.writeMapValueStart();
      v.write(w);
      w.writeMapValueEnd();
    }
    
    w.writeMapEnd();
  }

  @Override
  public Iterator<Entry<CharSequence, DataValue>> iterator() {
    return map.entrySet().iterator();
  }
  
  @Override
  public BaseArrayValue getAsArray() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BaseMapValue getAsMap() {
    return this;
  }

  @Override
  public boolean equals(DataValue v) {
    if(v == null) return false;
    if(v.getDataType().getMode() != DataMode.REPEATED) return false;
    BaseMapValue other = v.getAsContainer().getAsMap();
    for(Entry<CharSequence, DataValue> e : this){
      DataValue v2 = other.getByName(e.getKey());
      if(!e.getValue().equals(v2)) return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return map.hashCode();
  }

  public DataValue copy() {
      SimpleMapValue out = new SimpleMapValue();
      for(Map.Entry<CharSequence, DataValue> entry : map.entrySet()) {
          out.setByName(entry.getKey().toString(), entry.getValue().copy());
      }
      return out;
  }

  @Override
  public String toString() {
    final int maxLen = 10;
    return "SimpleMapValue [map=" + (map != null ? toString(map.entrySet(), maxLen) : null) + "]";
  }

  private String toString(Collection<?> collection, int maxLen) {
    StringBuilder builder = new StringBuilder();
    builder.append("[");
    int i = 0;
    for (Iterator<?> iterator = collection.iterator(); iterator.hasNext() && i < maxLen; i++) {
      if (i > 0) builder.append(", ");
      builder.append(iterator.next());
    }
    builder.append("]");
    return builder.toString();
  }
  
  
}
