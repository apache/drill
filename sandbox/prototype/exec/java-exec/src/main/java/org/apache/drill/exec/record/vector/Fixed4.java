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
package org.apache.drill.exec.record.vector;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.record.MaterializedField;

public class Fixed4 extends AbstractFixedValueVector<Fixed4>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Fixed4.class);

  public Fixed4(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator, 4*8);
  }

  public final void setInt(int index, int value){
    index*=4;
    data.setInt(index, value);
  }
  
  public final int getInt(int index){
    index*=4;
    return data.getInt(index);
  }
  
  public final void setFloat4(int index, float value){
    index*=4;
    data.setFloat(index, value);
  }
  
  public final float getFloat4(int index){
    index*=4;
    return data.getFloat(index);
  }
  
  @Override
  public Object getObject(int index) {
    if (field != null && field.getType().getMinorType() == SchemaDefProtos.MinorType.FLOAT4) {
      return getFloat4(index);
    } else {
      return getInt(index);
    }
  }
}
