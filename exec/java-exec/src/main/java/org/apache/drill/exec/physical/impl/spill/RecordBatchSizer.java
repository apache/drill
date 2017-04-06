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
import java.util.Set;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.AllocationManager.BufferLedger;
import org.apache.drill.exec.memory.BaseAllocator;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorInitializer;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.exec.vector.complex.RepeatedListVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;

import com.google.common.collect.Sets;

/**
 * Given a record batch or vector container, determines the actual memory
 * consumed by each column, the average row, and the entire record batch.
 */

public class RecordBatchSizer {

  /**
   * Column size information.
   */
  public static class ColumnSize {
    public final String prefix;
    public final MaterializedField metadata;

    /**
     * Assumed size from Drill metadata. Note that this information is
     * 100% bogus. Do not use it.
     */

    @Deprecated
    public int stdSize;

    /**
     * Actual average column width as determined from actual memory use. This
     * size is larger than the actual data size since this size includes per-
     * column overhead such as any unused vector space, etc.
     */

    public final int estSize;

    /**
     * Number of times the value here (possibly repeated) appears in
     * the record batch.
     */

    public final int valueCount;

    /**
     * The number of elements in the value vector. Consider two cases.
     * A required or nullable vector has one element per row, so the
     * <tt>entryCount</tt> is the same as the <tt>valueCount</tt> (which,
     * in turn, is the same as the row count.) But, if this vector is an
     * array, then the <tt>valueCount</tt> is the number of columns, while
     * <tt>entryCount</tt> is the total number of elements in all the arrays
     * that make up the columns, so <tt>entryCount</tt> will be different than
     * the <tt>valueCount</tt> (normally larger, but possibly smaller if most
     * arrays are empty.
     * <p>
     * Finally, the column may be part of another list. In this case, the above
     * logic still applies, but the <tt>valueCount</tt> is the number of entries
     * in the outer array, not the row count.
     */

    public int entryCount;
    public int dataSize;

    /**
     * The estimated, average number of elements. For a repeated type,
     * this is the average entries per array (per repeated element).
     */

    public int estElementCount;
    public final boolean isVariableWidth;

    public ColumnSize(ValueVector v, String prefix, int valueCount) {
      this.prefix = prefix;
      this.valueCount = valueCount;
      metadata = v.getField();
      isVariableWidth = v instanceof VariableWidthVector;

      // The amount of memory consumed by the payload: the actual
      // data stored in the vectors.

      if (v.getField().getDataMode() == DataMode.REPEATED) {
        buildRepeated(v);
      }
      estElementCount = 1;
      entryCount = 1;
      switch (metadata.getType().getMinorType()) {
      case LIST:
        buildList(v);
        break;
      case MAP:
      case UNION:
        // No standard size for Union type
        dataSize = v.getPayloadByteCount(valueCount);
        break;
      default:
        dataSize = v.getPayloadByteCount(valueCount);
        stdSize = TypeHelper.getSize(metadata.getType());
      }
      estSize = roundUp(dataSize, valueCount);
    }

    private void buildRepeated(ValueVector v) {

      // Repeated vectors are special: they have an associated offset vector
      // that changes the value count of the contained vectors.

      @SuppressWarnings("resource")
      UInt4Vector offsetVector = ((RepeatedValueVector) v).getOffsetVector();
      buildArray(offsetVector);
      if (metadata.getType().getMinorType() == MinorType.MAP) {

        // For map, the only data associated with the map vector
        // itself is the offset vector, if any.

        dataSize = offsetVector.getPayloadByteCount(valueCount);
      }
    }

    private void buildList(ValueVector v) {
      @SuppressWarnings("resource")
      UInt4Vector offsetVector = ((RepeatedListVector) v).getOffsetVector();
      buildArray(offsetVector);
      dataSize = offsetVector.getPayloadByteCount(valueCount);
    }

    private void buildArray(UInt4Vector offsetVector) {
      entryCount = offsetVector.getAccessor().get(valueCount);
      estElementCount = roundUp(entryCount, valueCount);
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder()
          .append(prefix)
          .append(metadata.getName())
          .append("(type: ")
          .append(metadata.getType().getMode().name())
          .append(" ")
          .append(metadata.getType().getMinorType().name())
          .append(", count: ")
          .append(valueCount);
      if (metadata.getDataMode() == DataMode.REPEATED) {
        buf.append(", total entries: ")
           .append(entryCount)
           .append(", per-array: ")
           .append(estElementCount);
      }
      buf .append(", std size: ")
          .append(stdSize)
          .append(", actual size: ")
          .append(estSize)
          .append(", data size: ")
          .append(dataSize)
          .append(")");
      return buf.toString();
    }

    /**
     * Add a single vector initializer to a collection for the entire batch.
     * Uses the observed column size information to predict the size needed
     * when allocating a new vector for the same data.
     *
     * @param initializer the vector initializer to hold the hints
     * for this column
     */

    private void buildVectorInitializer(VectorInitializer initializer) {
      int width = 0;
      switch(metadata.getType().getMinorType()) {
      case VAR16CHAR:
      case VARBINARY:
      case VARCHAR:

        // Subtract out the offset vector width
        width = estSize - 4;

        // Subtract out the bits (is-set) vector width
        if (metadata.getDataMode() == DataMode.OPTIONAL) {
          width -= 1;
        }
        break;
      default:
        break;
      }
      String name = prefix + metadata.getName();
      if (metadata.getDataMode() == DataMode.REPEATED) {
        if (width > 0) {
          // Estimated width is width of entire column. Divide
          // by element count to get per-element size.
          initializer.variableWidthArray(name, width / estElementCount, estElementCount);
        } else {
          initializer.fixedWidthArray(name, estElementCount);
        }
      }
      else if (width > 0) {
        initializer.variableWidth(name, width);
      }
    }
  }

  private List<ColumnSize> columnSizes = new ArrayList<>();

  /**
   * Number of records (rows) in the batch.
   */
  private int rowCount;
  /**
   * Standard row width using Drill meta-data. Note: this information is
   * 100% bogus. Do not use it.
   */
  @Deprecated
  private int stdRowWidth;
  /**
   * Actual batch size summing all buffers used to store data
   * for the batch.
   */
  private int accountedMemorySize;
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

  private Set<BufferLedger> ledgers = Sets.newIdentityHashSet();

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
  public int nullableCount;

  /**
   * Create empirical metadata for a record batch given a vector accessible
   * (basically, an iterator over the vectors in the batch.)
   *
   * @param va iterator over the batch's vectors
   */

  public RecordBatchSizer(VectorAccessible va) {
    this(va, null);
  }

  /**
   * Create empirical metadata for a record batch given a vector accessible
   * (basically, an iterator over the vectors in the batch) along with a
   * selection vector for those records. The selection vector is used to
   * pad the estimated row width with the extra two bytes needed per record.
   * The selection vector memory is added ot the total memory consumed by
   * this batch.
   *
   * @param va iterator over the batch's vectors
   * @param sv2 selection vector associated with this batch
   */

  public RecordBatchSizer(VectorAccessible va, SelectionVector2 sv2) {
    rowCount = va.getRecordCount();
    for (VectorWrapper<?> vw : va) {
      measureColumn(vw.getValueVector(), "", rowCount);
    }

    for (BufferLedger ledger : ledgers) {
      accountedMemorySize += ledger.getAccountedSize();
    }

    if (rowCount > 0) {
      grossRowWidth = roundUp(accountedMemorySize, rowCount);
    }

    if (sv2 != null) {
      sv2Size = sv2.getBuffer(false).capacity();
      accountedMemorySize += sv2Size;
      hasSv2 = true;
    }

    computeEstimates();
  }

  private void computeEstimates() {
    grossRowWidth = roundUp(accountedMemorySize, rowCount);
    netRowWidth = roundUp(netBatchSize, rowCount);
    avgDensity = roundUp(netBatchSize * 100, accountedMemorySize);
  }

  public void applySv2() {
    if (hasSv2) {
      return;
    }

    hasSv2 = true;
    sv2Size = BaseAllocator.nextPowerOfTwo(2 * rowCount);
    accountedMemorySize += sv2Size;
    computeEstimates();
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

  private void measureColumn(ValueVector v, String prefix, int valueCount) {

    ColumnSize colSize = new ColumnSize(v, prefix, valueCount);
    columnSizes.add(colSize);
    stdRowWidth += colSize.stdSize;
    netBatchSize += colSize.dataSize;
    maxSize = Math.max(maxSize, colSize.dataSize);
    if (colSize.metadata.isNullable()) {
      nullableCount++;
    }

    // Maps consume no size themselves. However, their contained
    // vectors do consume space, so visit columns recursively.

    switch (v.getField().getType().getMinorType()) {
    case MAP:
      expandMap((AbstractMapVector) v, prefix + v.getField().getName() + ".", colSize.entryCount);
      break;
    case LIST:
      expandList((RepeatedListVector) v, prefix + v.getField().getName() + ".", colSize.entryCount);
      break;
    default:
      v.collectLedgers(ledgers);
    }

    netRowWidth += colSize.estSize;
    netRowWidthCap50 += ! colSize.isVariableWidth ? colSize.estSize :
        8 /* offset vector */ + roundUpToPowerOf2(Math.min(colSize.estSize,50));
        // above change 8 to 4 after DRILL-5446 is fixed
  }

  private void expandMap(AbstractMapVector mapVector, String prefix, int valueCount) {
    for (ValueVector vector : mapVector) {
      measureColumn(vector, prefix, valueCount);
    }

    // For a repeated map, we need the memory for the offset vector (only).
    // Map elements are recursively expanded above.

    if (mapVector.getField().getDataMode() == DataMode.REPEATED) {
      ((RepeatedMapVector) mapVector).getOffsetVector().collectLedgers(ledgers);
    }
  }

  private void expandList(RepeatedListVector vector, String prefix, int valueCount) {
    measureColumn(vector.getDataVector(), prefix, valueCount);

    // Determine memory for the offset vector (only).

    vector.collectLedgers(ledgers);
  }

  public static int roundUp(int num, int denom) {
    if (denom == 0) {
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
  public int netRowWidthCap50() { return netRowWidthCap50 + nullableCount; }
  public int actualSize() { return accountedMemorySize; }
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
    buf.append(accountedMemorySize);
    buf.append(", Data size: ");
    buf.append(netBatchSize);
    buf.append(", Gross row width: ");
    buf.append(grossRowWidth);
    buf.append(", Net row width: ");
    buf.append(netRowWidth);
    buf.append(", Density: ");
    buf.append(avgDensity);
    buf.append("}");
    return buf.toString();
  }

  /**
   * The column size information gathered here represents empirically-derived
   * schema metadata. Use that metadata to create an instance of a class that
   * allocates memory for new vectors based on the observed size information.
   * The caller provides the row count; the size information here provides
   * column widths, number of elements in each array, etc.
   */

  public VectorInitializer buildVectorInitializer() {
    VectorInitializer initializer = new VectorInitializer();
    for (ColumnSize colSize : columnSizes) {
      colSize.buildVectorInitializer(initializer);
    }
    return initializer;
  }
}
