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
package org.apache.drill.exec.physical.impl.spill;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BaseAllocator;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.ValueVector;

/**
 * Given a record batch or vector container, determines the actual memory
 * consumed by each column, the average row, and the entire record batch.
 */

public class RecordBatchSizer {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RecordBatchSizer.class);

  /**
   * Column size information.
   */
  public static class ColumnSize {
    public final MaterializedField metadata;

    /**
     * Assumed size from Drill metadata.
     */

    public int stdSize;

    /**
     * Actual memory consumed by all the vectors associated with this column.
     */

    public int totalSize;

    /**
     * Actual average column width as determined from actual memory use. This
     * size is larger than the actual data size since this size includes per-
     * column overhead such as any unused vector space, etc.
     */

    public int estSize;
    public int capacity;
    public int density;
    public int dataSize;

    @SuppressWarnings("resource")
    public ColumnSize(VectorWrapper<?> vw) {
      metadata = vw.getField();
      stdSize = TypeHelper.getSize(metadata.getType());

      // Can't get size estimates if this is an empty batch.

      ValueVector v = vw.getValueVector();
      int rowCount = v.getAccessor().getValueCount();
      if (rowCount == 0) {
        return;
      }

      // Total size taken by all vectors (and underlying buffers)
      // associated with this vector.

      totalSize = v.getAllocatedByteCount();

      // Capacity is the number of values that the vector could
      // contain. This is useful only for fixed-length vectors.

      capacity = v.getValueCapacity();

      // The amount of memory consumed by the payload: the actual
      // data stored in the vectors.

      dataSize = v.getPayloadByteCount();

      // Determine "density" the number of rows compared to potential
      // capacity. Low-density batches occur at block boundaries, ends
      // of files and so on. Low-density batches throw off our estimates
      // for Varchar columns because we don't know the actual number of
      // bytes consumed (that information is hidden behind the Varchar
      // implementation where we can't get at it.)

      density = roundUp(dataSize * 100, totalSize);
      estSize = roundUp(dataSize, rowCount);
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder()
          .append(metadata.getName())
          .append("(type: ")
          .append(metadata.getType().getMinorType().name())
          .append(", std col. size: ")
          .append(stdSize)
          .append(", actual col. size: ")
          .append(estSize)
          .append(", total size: ")
          .append(totalSize)
          .append(", data size: ")
          .append(dataSize)
          .append(", row capacity: ")
          .append(capacity)
          .append(", density: ")
          .append(density)
          .append(")");
      return buf.toString();
    }
  }

  List<ColumnSize> columnSizes = new ArrayList<>();

  /**
   * Number of records (rows) in the batch.
   */
  private int rowCount;
  /**
   * Standard row width using Drill meta-data.
   */
  private int stdRowWidth;
  /**
   * Actual batch size summing all buffers used to store data
   * for the batch.
   */
  private int totalBatchSize;
  /**
   * Actual row width computed by dividing total batch memory by the
   * record count.
   */
  private int grossRowWidth;
  /**
   * Actual row width computed by summing columns. Use this if the
   * vectors are partially full; prevents overestimating row width.
   */
  private int netRowWidth;
  private boolean hasSv2;
  private int sv2Size;
  private int avgDensity;

  private int netBatchSize;

  public RecordBatchSizer(VectorAccessible va) {
    rowCount = va.getRecordCount();
    for (VectorWrapper<?> vw : va) {
      measureColumn(vw);
    }

    if (rowCount > 0) {
      grossRowWidth = roundUp(totalBatchSize, rowCount);
    }

    hasSv2 = va.getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.TWO_BYTE;
    if (hasSv2) {
      @SuppressWarnings("resource")
      SelectionVector2 sv2 = va.getSelectionVector2();
      sv2Size = sv2.getBuffer(false).capacity();
      grossRowWidth += sv2Size / rowCount;
      netRowWidth += 2;
    }

    int totalDensity = 0;
    int usableCount = 0;
    for (ColumnSize colSize : columnSizes) {
      if ( colSize.density > 0 ) {
        usableCount++;
      }
      totalDensity += colSize.density;
    }
    avgDensity = roundUp(totalDensity, usableCount);
  }

  public void applySv2() {
    if (hasSv2) {
      return;
    }

    sv2Size = BaseAllocator.nextPowerOfTwo(2 * rowCount);
    grossRowWidth += roundUp(sv2Size, rowCount);
    totalBatchSize += sv2Size;
  }

  private void measureColumn(VectorWrapper<?> vw) {
    ColumnSize colSize = new ColumnSize(vw);
    columnSizes.add(colSize);

    stdRowWidth += colSize.stdSize;
    totalBatchSize += colSize.totalSize;
    netBatchSize += colSize.dataSize;
    netRowWidth += colSize.estSize;
  }

  public static int roundUp(int num, int denom) {
    if(denom == 0) {
      return 0;
    }
    return (int) Math.ceil((double) num / denom);
  }

  public int rowCount() { return rowCount; }
  public int stdRowWidth() { return stdRowWidth; }
  public int grossRowWidth() { return grossRowWidth; }
  public int netRowWidth() { return netRowWidth; }
  public int actualSize() { return totalBatchSize; }
  public boolean hasSv2() { return hasSv2; }
  public int avgDensity() { return avgDensity; }
  public int netSize() { return netBatchSize; }

  public static final int MAX_VECTOR_SIZE = 16 * 1024 * 1024; // 16 MiB

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("Actual batch schema & sizes {\n");
    for (ColumnSize colSize : columnSizes) {
      buf.append("  ");
      buf.append(colSize.toString());
      buf.append("\n");
    }
    buf.append( "  Records: " );
    buf.append(rowCount);
    buf.append(", Total size: ");
    buf.append(totalBatchSize);
    buf.append(", Row width:");
    buf.append(grossRowWidth);
    buf.append(", Density:");
    buf.append(avgDensity);
    buf.append("}");
    return buf.toString();
  }
}
