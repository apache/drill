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

import static com.google.common.base.Preconditions.checkArgument;

public class VarLen4 extends VariableVector<VarLen4, Fixed4>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VarLen4.class);

  public VarLen4(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator, 4);
  }

  @Override
  protected Fixed4 getNewLengthVector(BufferAllocator allocator) {
    return new Fixed4(null, allocator);
  }

    public void setBytes(int index, byte[] bytes) {
        checkArgument(index >= 0);
        if(index == 0) {
            lengthVector.setInt(0, bytes.length);
            data.setBytes(0, bytes);
        } else {
            int previousOffset = lengthVector.getInt(index - 1);
            lengthVector.setInt(index, previousOffset + bytes.length);
            data.setBytes(previousOffset, bytes);
        }
    }
}
