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
package org.apache.drill.exec.vector.accessor.writer;

import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;

/**
 * Index into the vector of elements for a repeated vector.
 * Keeps track of the current offset in terms of value positions.
 * Forwards overflow events to the base index.
 */

public class FixedWidthElementWriterIndex implements ElementWriterIndex {

  private final ColumnWriterIndex baseIndex;
  private int startOffset = 0;
  private int offset = 0;

  public FixedWidthElementWriterIndex(ColumnWriterIndex baseIndex) {
    this.baseIndex = baseIndex;
  }

  public void reset() {
    offset = 0;
    startOffset = 0;
  }

  @Override
  public int vectorIndex() { return offset; }

  @Override
  public void overflowed() {
    baseIndex.overflowed();
  }

  public int arraySize() {
    return offset - startOffset;
  }

  @Override
  public void next() { offset++; }
}
