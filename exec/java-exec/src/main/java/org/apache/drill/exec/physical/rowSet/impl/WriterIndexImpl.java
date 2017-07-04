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
package org.apache.drill.exec.physical.rowSet.impl;

import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;

/**
 * Writer index that points to each row in the row set. The index starts at
 * the 0th row and advances one row on each increment. This allows writers to
 * start positioned at the first row. Writes happen in the current row.
 * Calling <tt>next()</tt> advances to the next position, effectively saving
 * the current row. The most recent row can be abandoned easily simply by not
 * calling <tt>next()</tt>. This means that the number of completed rows is
 * the same as the row index.
 */

class WriterIndexImpl implements ColumnWriterIndex {

  public enum State { OK, VECTOR_OVERFLOW, END_OF_BATCH }

  public interface WriterIndexListener {
    void overflowed();
    boolean writeable();
  }

  private final WriterIndexListener listener;
  private final int rowCountLimit;
  private int rowIndex = 0;
  private WriterIndexImpl.State state = State.OK;

  public WriterIndexImpl(WriterIndexListener listener) {
    this(listener, ValueVector.MAX_ROW_COUNT);
  }

  public WriterIndexImpl(WriterIndexListener listener, int rowCountLimit) {
    this.listener = listener;
    this.rowCountLimit = rowCountLimit;
  }

  @Override
  public int vectorIndex() { return rowIndex; }

  public boolean next() {
    if (++rowIndex < rowCountLimit) {
      return true;
    } else {
      // Should not call next() again once batch is full.
      rowIndex = rowCountLimit;
      state = state == State.OK ? State.END_OF_BATCH : state;
      return false;
    }
  }

  public int size() {
    // The index always points to the next slot past the
    // end of valid rows.
    return rowIndex;
  }

  public boolean valid() { return state == State.OK; }

  /**
   * Indicate if it is legal to write to a column. Used for test-time
   * assertions to validate that column writes occur only when a batch
   * is active.
   * @return true if it is legal to write to a column, false otherwise
   */

  public boolean legal() {
    return valid()  &&  listener.writeable();
  }

  public boolean hasOverflow() { return state == State.VECTOR_OVERFLOW; }

  @Override
  public void overflowed() {
    state = State.VECTOR_OVERFLOW;
    if (listener != null) {
      listener.overflowed();
    }
  }

  public void reset(int index) {
    assert index <= rowIndex;
    state = State.OK;
    rowIndex = index;
  }

  public void reset() {
    state = State.OK;
    rowIndex = 0;
  }
}
