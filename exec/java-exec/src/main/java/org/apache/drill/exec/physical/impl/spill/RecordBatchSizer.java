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

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BaseAllocator;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.exec.vector.VariableWidthVector;

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
    public boolean variableWidth;

    public ColumnSize(ValueVector vv) {
      metadata = vv.getField();
      stdSize = TypeHelper.getSize(metadata.getType());

      // Can't get size estimates if this is an empty batch.
      int rowCount = vv.getAccessor().getValueCount();
      if (rowCount == 0) {
        estSize = stdSize;
        return;
      }

      // Total size taken by all vectors (and underlying buffers)
      // associated with this vector.

      totalSize = vv.getAllocatedByteCount();

      // Capacity is the number of values that the vector could
      // contain. This is useful only for fixed-length vectors.

      capacity = vv.getValueCapacity();

      // The amount of memory consumed by the payload: the actual
      // data stored in the vectors.

      dataSize = vv.getPayloadByteCount();

      // Determine "density" the number of rows compared to potential
      // capacity. Low-density batches occur at block boundaries, ends
      // of files and so on. Low-density batches throw off our estimates
      // for Varchar columns because we don't know the actual number of
      // bytes consumed (that information is hidden behind the Varchar
      // implementation where we can't get at it.)

      density = roundUp(dataSize * 100, totalSize);
      estSize = roundUp(dataSize, rowCount);
      variableWidth = vv instanceof VariableWidthVector ;
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

  private List<ColumnSize> columnSizes = new ArrayList<>();

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
  private int netRowWidthCap50;
  private boolean hasSv2;
  private int sv2Size;
  private int avgDensity;

  private int netBatchSize;

  public RecordBatchSizer(RecordBatch batch) {
    this(batch,
         (batch.getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.TWO_BYTE) ?
         batch.getSelectionVector2() : null);
  }

  /**
   *  Maximum width of a column; used for memory estimation in case of Varchars
   */
  public int maxSize;
  /**
   *  Count the nullable columns; used for memory estimation
   */
  public int numNullables;
  /**
   *
   * @param va
   */
  public RecordBatchSizer(VectorAccessible va) {
    this(va, null);
  }

  public RecordBatchSizer(VectorAccessible va, SelectionVector2 sv2) {
    rowCount = va.getRecordCount();
    for (VectorWrapper<?> vw : va) {
      int size = measureColumn(vw.getValueVector());
      if ( size > maxSize ) { maxSize = size; }
      if ( vw.getField().isNullable() ) { numNullables++; }
    }

    if (rowCount > 0) {
      grossRowWidth = roundUp(totalBatchSize, rowCount);
    }

    if (sv2 != null) {
      sv2Size = sv2.getBuffer(false).capacity();
      grossRowWidth += roundUp(sv2Size, rowCount);
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

  /**
   *  Round up (if needed) to the next power of 2 (only up to 64)
   * @param arg Number to round up (must be < 64)
   * @return power of 2 result
   */
  private int roundUpToPowerOf2(int arg) {
    if ( arg <= 2 ) { return 2; }
    if ( arg <= 4 ) { return 4; }
    if ( arg <= 8 ) { return 8; }
    if ( arg <= 16 ) { return 16; }
    if ( arg <= 32 ) { return 32; }
    return 64;
  }
  private int measureColumn(ValueVector vv) {
    // Maps consume no size themselves. However, their contained
    // vectors do consume space, so visit columns recursively.
    if (vv.getField().getType().getMinorType() == MinorType.MAP) {
      return expandMap((AbstractMapVector) vv);
    }

    ColumnSize colSize = new ColumnSize(vv);
    columnSizes.add(colSize);

    stdRowWidth += colSize.stdSize;
    totalBatchSize += colSize.totalSize;
    netBatchSize += colSize.dataSize;
    netRowWidth += colSize.estSize;
    netRowWidthCap50 += ! colSize.variableWidth ? colSize.estSize :
        8 /* offset vector */ + roundUpToPowerOf2( Math.min(colSize.estSize,50) );
        // above change 8 to 4 after DRILL-5446 is fixed
    return colSize.estSize;
  }

  private int expandMap(AbstractMapVector mapVector) {
    int accum = 0;
    for (ValueVector vector : mapVector) {
      accum += measureColumn(vector);
    }
    return accum;
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
  /**
   * Compute the "real" width of the row, taking into account each varchar column size
   * (historically capped at 50, and rounded up to power of 2 to match drill buf allocation)
   * and null marking columns.
   * @return "real" width of the row
   */
  public int netRowWidthCap50() { return netRowWidthCap50 + numNullables; }
  public int actualSize() { return totalBatchSize; }
  public boolean hasSv2() { return hasSv2; }
  public int avgDensity() { return avgDensity; }
  public int netSize() { return netBatchSize; }
  public int maxSize() { return maxSize; }

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
    buf.append(", Gross row width:");
    buf.append(grossRowWidth);
    buf.append(", Net row width:");
    buf.append(netRowWidth);
    buf.append(", Density:");
    buf.append(avgDensity);
    buf.append("}");
    return buf.toString();
  }
}
