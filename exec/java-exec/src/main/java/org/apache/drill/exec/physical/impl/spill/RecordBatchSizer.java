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
import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.NullableVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;

import io.netty.buffer.DrillBuf;

/**
 * Given a record batch or vector container, determines the actual memory
 * consumed by each column, the average row, and the entire record batch.
 */

public class RecordBatchSizer {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RecordBatchSizer.class);

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

    /**
     * The size of the data vector backing the column. Useful for detecting
     * cases of possible direct memory fragmentation.
     */
    public int dataVectorSize;
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
      DrillBuf[] bufs = v.getBuffers(false);
      for (DrillBuf buf : bufs) {
        totalSize += buf.capacity();
      }

      // Capacity is the number of values that the vector could
      // contain. This is useful only for fixed-length vectors.

      capacity = v.getValueCapacity();

      // Crude way to get the size of the buffer underlying simple (scalar) values.
      // Ignores maps, lists and other esoterica. Uses a crude way to subtract out
      // the null "bit" (really byte) buffer size for nullable vectors.

      if (v instanceof BaseDataValueVector) {
        dataVectorSize = totalSize;
        if (v instanceof NullableVector) {
          dataVectorSize -= bufs[0].getActualMemoryConsumed();
        }
      }

      // Determine "density" the number of rows compared to potential
      // capacity. Low-density batches occur at block boundaries, ends
      // of files and so on. Low-density batches throw off our estimates
      // for Varchar columns because we don't know the actual number of
      // bytes consumed (that information is hidden behind the Varchar
      // implementation where we can't get at it.)
      //
      // A better solution is to have each vector do this calc rather
      // than trying to do it generically, but that increases the code
      // change footprint and slows the commit process.

      if (v instanceof FixedWidthVector) {
        dataSize = stdSize * rowCount;
      } else if ( v instanceof VarCharVector ) {
        VarCharVector vv = (VarCharVector) v;
        dataSize = vv.getOffsetVector().getAccessor().get(rowCount);
      } else if ( v instanceof NullableVarCharVector ) {
        NullableVarCharVector vv = (NullableVarCharVector) v;
        dataSize = vv.getValuesVector().getOffsetVector().getAccessor().get(rowCount);
      } else {
        dataSize = 0;
      }
      if (dataSize > 0) {
        density = roundUp(dataSize * 100, dataVectorSize);
        estSize = roundUp(dataSize, rowCount);
      }
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder();
      buf.append(metadata.getName());
      buf.append("(std col. size: ");
      buf.append(stdSize);
      buf.append(", actual col. size: ");
      buf.append(estSize);
      buf.append(", total size: ");
      buf.append(totalSize);
      buf.append(", vector size: ");
      buf.append(dataVectorSize);
      buf.append(", data size: ");
      buf.append(dataSize);
      buf.append(", row capacity: ");
      buf.append(capacity);
      buf.append(", density: ");
      buf.append(density);
      buf.append(")");
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

  public RecordBatchSizer(VectorAccessible va) {
    rowCount = va.getRecordCount();
    for (VectorWrapper<?> vw : va) {
      measureField(vw);
    }

    if (rowCount > 0) {
      grossRowWidth = roundUp(totalBatchSize, rowCount);
    }

    hasSv2 = va.getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.TWO_BYTE;
    if (hasSv2) {
      @SuppressWarnings("resource")
      SelectionVector2 sv2 = va.getSelectionVector2();
      sv2Size = sv2.getBuffer().capacity();
      grossRowWidth += sv2Size;
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

  private void measureField(VectorWrapper<?> vw) {
    ColumnSize colSize = new ColumnSize(vw);
    columnSizes.add(colSize);

    stdRowWidth += colSize.stdSize;
    totalBatchSize += colSize.totalSize;
    netRowWidth += colSize.estSize;
  }

  public static int roundUp(int denom, int num) {
    if(num == 0) {
      return 0;
    }
    return (int) Math.ceil((double) denom / num);
  }

  public int rowCount() { return rowCount; }
  public int stdRowWidth() { return stdRowWidth; }
  public int grossRowWidth() { return grossRowWidth; }
  public int netRowWidth() { return netRowWidth; }
  public int actualSize() { return totalBatchSize; }
  public boolean hasSv2() { return hasSv2; }
  public int getAvgDensity() { return avgDensity; }

  public static final int MAX_VECTOR_SIZE = 16 * 1024 * 1024; // 16 MiB

  /**
   * Look for columns backed by vectors larger than the 16 MiB size
   * employed by the Netty allocator. Such large blocks can lead to
   * memory fragmentation and unexpected OOM errors.
   * @return if any column is oversize
   */
  public boolean checkOversizeCols() {
    boolean hasOversize = false;
    for (ColumnSize colSize : columnSizes) {
      if ( colSize.dataVectorSize > MAX_VECTOR_SIZE) {
        logger.warn( "Column is wider than 256 characters: OOM due to memory fragmentation is possible - " + colSize.metadata.getPath() );
        hasOversize = true;
      }
    }
    return hasOversize;
  }

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
