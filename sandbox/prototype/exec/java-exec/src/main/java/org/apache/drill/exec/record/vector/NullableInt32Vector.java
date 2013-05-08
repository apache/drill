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
import org.apache.drill.exec.record.MaterializedField;

public final class NullableInt32Vector extends NullableValueVector<NullableInt32Vector, Int32Vector>{

  public NullableInt32Vector(int fieldId, BufferAllocator allocator) {
    super(fieldId, allocator, NullableInt32Vector.class);
  }


  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NullableInt32Vector.class);
  
  
  public int get(int index){
    return this.value.get(index);
  }
  
  public void set(int index, int value){
    this.value.set(index, value);
  }


  @Override
  protected Int32Vector getNewValueVector(int fieldId, BufferAllocator allocator) {
    return new Int32Vector(fieldId, allocator);
  }

}
