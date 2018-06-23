/*
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
package org.apache.drill.exec.vector;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.record.MaterializedField;

public interface VariableWidthVector extends ValueVector {

  int DEFAULT_RECORD_BYTE_COUNT = 8;
  int MIN_BYTE_COUNT = 4096;
  MaterializedField offsetsField = MaterializedField.create(OFFSETS_VECTOR_NAME, Types.required(MinorType.UINT4));

  interface VariableWidthAccessor extends Accessor {
    int getValueLength(int index);
  }

  interface VariableWidthMutator extends Mutator {
    void setValueLengthSafe(int index, int length);
  }

  /**
   * Allocate a new memory space for this vector.  Must be called prior to using the ValueVector.
   *
   * @param totalBytes   Desired size of the underlying data buffer.
   * @param valueCount   Number of values in the vector.
   */
  void allocateNew(int totalBytes, int valueCount);

  /**
   * Provide the maximum amount of variable width bytes that can be stored int his vector.
   * @return
   */
  int getByteCapacity();

  @Override
  VariableWidthMutator getMutator();

  @Override
  VariableWidthAccessor getAccessor();

  int getCurrentSizeInBytes();

  UInt4Vector getOffsetVector();
}
