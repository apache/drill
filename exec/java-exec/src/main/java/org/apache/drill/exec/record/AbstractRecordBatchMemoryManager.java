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

package org.apache.drill.exec.record;

import org.apache.drill.exec.vector.ValueVector;

public abstract class AbstractRecordBatchMemoryManager {
  protected static final int OFFSET_VECTOR_WIDTH = 4;
  protected static final int WORST_CASE_FRAGMENTATION_FACTOR = 2;
  protected static final int MAX_NUM_ROWS = ValueVector.MAX_ROW_COUNT;
  protected static final int MIN_NUM_ROWS = 1;
  private int outputRowCount = MAX_NUM_ROWS;
  private int outgoingRowWidth;
  private RecordBatchSizer sizer;

  public void update(int inputIndex) {};

  public void update() {};

  public int getOutputRowCount() {
    return outputRowCount;
  }

  /**
   * Given batchSize and rowWidth, this will set output rowCount taking into account
   * the min and max that is allowed.
   */
  public void setOutputRowCount(int targetBatchSize, int rowWidth) {
    this.outputRowCount = adjustOutputRowCount(RecordBatchSizer.safeDivide(targetBatchSize, rowWidth));
  }

  public void setOutputRowCount(int outputRowCount) {
    this.outputRowCount = outputRowCount;
  }

  /**
   * This will adjust rowCount taking into account the min and max that is allowed.
   * We will round down to nearest power of two - 1 for better memory utilization.
   * -1 is done for adjusting accounting for offset vectors.
   */
  public static int adjustOutputRowCount(int rowCount) {
    return (Math.min(MAX_NUM_ROWS, Math.max(Integer.highestOneBit(rowCount) - 1, MIN_NUM_ROWS)));
  }

  public void setOutgoingRowWidth(int outgoingRowWidth) {
    this.outgoingRowWidth = outgoingRowWidth;
  }

  public int getOutgoingRowWidth() {
    return outgoingRowWidth;
  }

  public void setRecordBatchSizer(RecordBatchSizer sizer) {
    this.sizer = sizer;
  }

  public RecordBatchSizer getRecordBatchSizer() {
    return sizer;
  }

  public RecordBatchSizer.ColumnSize getColumnSize(String name) {
    return sizer.getColumn(name);
  }

}
