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

/**
 * Convenience/Clarification Fixed2 wrapper.
 */
public class SelectionVector {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SelectionVector.class);

  public SelectionVector(MaterializedField field, BufferAllocator allocator) {

  }

  public int capacity() {
    return -1;
  }

  public void allocateNew(int count) {

  }
<<<<<<< HEAD:sandbox/prototype/exec/java-exec/src/main/java/org/apache/drill/exec/record/vector/SelectionVector.java
=======
  
  public final int getInt(int index){
    index*=4;
    return data.getInt(index);
  }
>>>>>>> Build working:sandbox/prototype/exec/java-exec/src/main/java/org/apache/drill/exec/record/vector/NullableFixed4.java

}
