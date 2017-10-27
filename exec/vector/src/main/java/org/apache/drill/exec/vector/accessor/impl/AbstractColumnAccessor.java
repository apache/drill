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
package org.apache.drill.exec.vector.accessor.impl;

import org.apache.drill.exec.vector.ValueVector;

/**
 * Abstract base class for column readers and writers that
 * implements the mechanism for binding accessors to a row
 * index. The row index is implicit: index a row, then
 * column accessors pull out columns from that row.
 */

public abstract class AbstractColumnAccessor {

  public interface RowIndex {
    int batch();
    int index();
  }

  protected RowIndex vectorIndex;

  protected void bind(RowIndex rowIndex) {
    this.vectorIndex = rowIndex;
  }

  public abstract void bind(RowIndex rowIndex, ValueVector vector);
}
