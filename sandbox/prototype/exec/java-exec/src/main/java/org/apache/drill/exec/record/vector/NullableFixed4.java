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

public final class NullableFixed4 extends NullableValueVector<NullableFixed4, Fixed4>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NullableFixed4.class);

  public NullableFixed4(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
  }

  @Override
  protected Fixed4 getNewValueVector(BufferAllocator allocator) {
    return new Fixed4(this.field, allocator);
  }

  public void setInt(int index, int newVal) {
      setNotNull(index);
      value.setInt(index, newVal);
  }

  public void setFloat4(int index, float val) {
      setNotNull(index);
      value.setFloat4(index, val);
  }
}
