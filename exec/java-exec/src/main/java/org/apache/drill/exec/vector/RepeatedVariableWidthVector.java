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
package org.apache.drill.exec.vector;

import io.netty.buffer.DrillBuf;

public interface RepeatedVariableWidthVector extends ValueVector, RepeatedVector {
  /**
   * Allocate a new memory space for this vector.  Must be called prior to using the ValueVector.
   *
   * @param totalBytes   Desired size of the underlying data buffer.
   * @param parentValueCount   Number of separate repeating groupings.
   * @param childValueCount   Number of supported values in the vector.
   */
  public void allocateNew(int totalBytes, int parentValueCount, int childValueCount);

  /**
   * Provide the maximum amount of variable width bytes that can be stored int his vector.
   * @return
   */
  public int getByteCapacity();

  /**
   * Load the records in the provided buffer based on the given number of values.
   * @param dataBytes   The number of bytes associated with the data array.
   * @param parentValueCount   Number of separate repeating groupings.
   * @param childValueCount   Number of supported values in the vector.
   * @param buf Incoming buffer.
   * @return The number of bytes of the buffer that were consumed.
   */
  public int load(int dataBytes, int parentValueCount, int childValueCount, DrillBuf buf);
}
