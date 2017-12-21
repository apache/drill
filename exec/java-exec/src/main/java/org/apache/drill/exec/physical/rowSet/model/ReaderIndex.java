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
package org.apache.drill.exec.physical.rowSet.model;

import org.apache.drill.exec.vector.accessor.ColumnReaderIndex;

/**
 * Row set index base class used when indexing rows within a row
 * set for a row set reader. Keeps track of the current position,
 * which starts before the first row, meaning that the client
 * must call <tt>next()</tt> to advance to the first row.
 */

public abstract class ReaderIndex implements ColumnReaderIndex {

  protected int rowIndex = -1;
  protected final int rowCount;

  public ReaderIndex(int rowCount) {
    this.rowCount = rowCount;
  }

  public int position() { return rowIndex; }
  public void set(int index) { rowIndex = index; }

  public boolean next() {
    if (++rowIndex < rowCount ) {
      return true;
    } else {
      rowIndex--;
      return false;
    }
  }

  public int size() { return rowCount; }

  public boolean valid() { return rowIndex < rowCount; }
}