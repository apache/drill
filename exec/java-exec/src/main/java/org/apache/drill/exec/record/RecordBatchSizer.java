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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map;

import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.AllocationManager.BufferLedger;
import org.apache.drill.exec.memory.BaseAllocator;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.FixedWidthVector;
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

  public static final double FRAGMENTATION_FACTOR = 1.33;

  /**
   * Column size information.
   */
  public static class ColumnSize {
    public final String prefix;
    public final MaterializedField metadata;

    /**
     * Assumed size from Drill metadata. Note that this information is
     * 100% bogus for variable-width columns. Do not use it for such
     * columns.
     */

    public int stdSize;

    /**
     * If the we can determine the exact width of the row of a vector upfront,
     * the row widths is saved here. If we cannot determine the exact width
     * (for example for VarChar or Repeated vectors), then
     */

    private int knownSize = -1;

    /**
     * Actual average column width as determined from actual memory use. This
     * size is larger than the actual data size since this size includes per-
     * column overhead such as any unused vector space, etc.
     */

    public final int estSize;

    /**
     * Number of occurrences of the value in the batch. This is trivial
     * for top-level scalars: it is the record count. For a top-level
     * repeated vector, this is the number of arrays, also the record
     * count. For a value nested inside a repeated map, it is the
     * total number of values across all maps, and may be less than,
     * greater than (but unlikely) same as the row count.
     */

    public final int valueCount;

    /**
     * Total number of elements for a repeated type, or 1 if this is
     * a non-repeated type. That is, a batch of 100 rows may have an
     * array with 10 elements per row. In this case, the element count
     * is 1000.
     */

    public final int elementCount;

    /**
     * Size of the top level value vector. For map and repeated list,
     * this is just size of offset vector.
     */
    public int dataSize;

    /**
     * Total size of the column includes the sum total of memory for all
     * value vectors representing the column.
     */
    public int netSize;

    /**
     * The estimated, average number of elements per parent value.
     * Always 1 for a non-repeated type. For a repeated type,
     * this is the average entries per array (per repeated element).
     */

    public final float estElementCountPerArray;
    public final boolean isVariableWidth;

    public ColumnSize(ValueVector v) {
      this(v, "");
    }

    public ColumnSize(ValueVector v, String prefix) {
      if (v instanceof FixedWidthVector) {
        knownSize = ((FixedWidthVector)v).getValueWidth();
      }

      this.prefix = prefix;
      valueCount = v.getAccessor().getValueCount();
      metadata = v.getField();
      isVariableWidth = v instanceof VariableWidthVector;

      // The amount of memory consumed by the payload: the actual
      // data stored in the vectors.

      if (v.getField().getDataMode() == DataMode.REPEATED) {
        elementCount = buildRepeated(v);
        estElementCountPerArray = valueCount == 0 ? 0 : elementCount * 1.0f / valueCount;
      } else {
        elementCount = 1;
        estElementCountPerArray = 1;
      }
      switch (metadata.getType().getMinorType()) {
      case LIST:
        buildList(v);
        break;
      case MAP:
      case UNION:
        // No standard size for Union type
        dataSize = v.getPayloadByteCount(valueCount);
        break;
      case GENERIC_OBJECT:
        // Object vectors do not consume direct memory so their known size and
        // estSize are 0.
        knownSize = 0;
        break;
      default:
        dataSize = v.getPayloadByteCount(valueCount);
        try {
          stdSize = TypeHelper.getSize(metadata.getType()) * elementCount;
        } catch (Exception e) {
          // For unsupported types, just set stdSize to 0.
          stdSize = 0;
        }
      }
      estSize = safeDivide(dataSize, valueCount);
      netSize = v.getPayloadByteCount(valueCount);
    }

    /**
     * If we can determine the knownSize, return that. Otherwise return the estSize.
     * @return The knownSize or estSize.
     */
    public int getKnownOrEstSize()
    {
      if (hasKnownSize()) {
        // We know the exact size of the column, return it.
        return knownSize;
      }

      return estSize;
    }

    public boolean hasKnownSize()
    {
      return knownSize != -1;
    }

    public int getKnownSize()
    {
      if (!hasKnownSize()) {
        throw new IllegalStateException("Unknown size for column: " + metadata);
      }

      return knownSize;
    }

    @SuppressWarnings("resource")
    private int buildRepeated(ValueVector v) {

      // Repeated vectors are special: they have an associated offset vector
      // that changes the value count of the contained vectors.

      UInt4Vector offsetVector = ((RepeatedValueVector) v).getOffsetVector();
      int childCount = offsetVector.getAccessor().get(valueCount);
      if (metadata.getType().getMinorType() == MinorType.MAP) {

        // For map, the only data associated with the map vector
        // itself is the offset vector, if any.

        dataSize = offsetVector.getPayloadByteCount(valueCount);
      }
      return childCount;
    }

    @SuppressWarnings("resource")
    private void buildList(ValueVector v) {
      // complex ListVector cannot be casted to RepeatedListVector.
      // check the mode.
      if (v.getField().getDataMode() != DataMode.REPEATED) {
        dataSize = v.getPayloadByteCount(valueCount);
        return;
      }
      UInt4Vector offsetVector = ((RepeatedListVector) v).getOffsetVector();
      dataSize = offsetVector.getPayloadByteCount(valueCount);
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
        buf.append(", elements: ")
           .append(elementCount)
           .append(", per-array: ")
           .append(estElementCountPerArray);
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
     * when allocating a new vector for the same data. Adds a hint only for
     * variable-width or repeated types; no extra information is needed for
     * fixed width, non-repeated columns.
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
          initializer.variableWidthArray(name, width / estElementCountPerArray, estElementCountPerArray);
        } else {
          initializer.fixedWidthArray(name, estElementCountPerArray);
        }
      }
      else if (width > 0) {
        initializer.variableWidth(name, width);
      }
    }
  }

  public static ColumnSize getColumn(ValueVector v, String prefix) {
    return new ColumnSize(v, prefix);
  }

  private Map<String, ColumnSize> columnSizes = CaseInsensitiveMap.newHashMap();

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
  private long accountedMemorySize;
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

  private Set<BufferLedger> ledgers = Sets.newIdentityHashSet();

  private long netBatchSize;

  /**
   *  Maximum width of a column; used for memory estimation in case of Varchars
   */

  public int maxSize;

  /**
   *  Count the nullable columns; used for memory estimation
   */

  public int nullableCount;


  public RecordBatchSizer(RecordBatch batch) {
    this(batch,
         (batch.getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.TWO_BYTE) ?
         batch.getSelectionVector2() : null);
  }
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
   * The selection vector memory is added to the total memory consumed by
   * this batch.
   *
   * @param va iterator over the batch's vectors
   * @param sv2 selection vector associated with this batch
   */

  public RecordBatchSizer(VectorAccessible va, SelectionVector2 sv2) {
    rowCount = va.getRecordCount();
    for (VectorWrapper<?> vw : va) {
      measureColumn(vw.getValueVector(), "");
    }

    for (BufferLedger ledger : ledgers) {
      accountedMemorySize += ledger.getAccountedSize();
    }

    if (rowCount > 0) {
      grossRowWidth = safeDivide(accountedMemorySize, rowCount);
    }

    if (sv2 != null) {
      sv2Size = sv2.getBuffer(false).capacity();
      accountedMemorySize += sv2Size;
      hasSv2 = true;
    }

    computeEstimates();
  }

  private void computeEstimates() {
    grossRowWidth = safeDivide(accountedMemorySize, rowCount);
    netRowWidth = safeDivide(netBatchSize, rowCount);
    avgDensity = safeDivide(netBatchSize * 100L, accountedMemorySize);
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

  private void measureColumn(ValueVector v, String prefix) {

    ColumnSize colSize = new ColumnSize(v, prefix);
    columnSizes.put(v.getField().getName(), colSize);
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
        expandMap((AbstractMapVector) v, prefix + v.getField().getName() + ".");
        break;
      case LIST:
        // complex ListVector cannot be casted to RepeatedListVector.
        // do not expand the list if it is not repeated mode.
        if (v.getField().getDataMode() == DataMode.REPEATED) {
          expandList((RepeatedListVector) v, prefix + v.getField().getName() + ".");
        }
        break;
      default:
        v.collectLedgers(ledgers);
    }

    columnSizes.put(v.getField().getName(), colSize);
    netRowWidth += colSize.estSize;
  }

  private void expandMap(AbstractMapVector mapVector, String prefix) {
    for (ValueVector vector : mapVector) {
      measureColumn(vector, prefix);
    }

    // For a repeated map, we need the memory for the offset vector (only).
    // Map elements are recursively expanded above.

    if (mapVector.getField().getDataMode() == DataMode.REPEATED) {
      ((RepeatedMapVector) mapVector).getOffsetVector().collectLedgers(ledgers);
    }
  }

  private void expandList(RepeatedListVector vector, String prefix) {
    measureColumn(vector.getDataVector(), prefix);

    // Determine memory for the offset vector (only).

    vector.collectLedgers(ledgers);
  }

  public static int safeDivide(long num, long denom) {
    if (denom == 0) {
      return 0;
    }
    return (int) Math.ceil((double) num / denom);
  }

  public int rowCount() { return rowCount; }
  public int stdRowWidth() { return stdRowWidth; }
  public int grossRowWidth() { return grossRowWidth; }
  public int netRowWidth() { return netRowWidth; }
  public Map<String, ColumnSize> columns() { return columnSizes; }

  public long actualSize() { return accountedMemorySize; }
  public boolean hasSv2() { return hasSv2; }
  public int avgDensity() { return avgDensity; }
  public long netSize() { return netBatchSize; }
  public int maxAvgColumnSize() { return maxSize / rowCount; }

  public static long multiplyByFragFactor(long size)
  {
    return (long) (((double) size) * FRAGMENTATION_FACTOR);
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("Actual batch schema & sizes {\n");
    for (ColumnSize colSize : columnSizes.values()) {
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
    buf.append("%}");
    return buf.toString();
  }

  /**
   * The column size information gathered here represents empirically-derived
   * schema metadata. Use that metadata to create an instance of a class that
   * allocates memory for new vectors based on the observed size information.
   * The caller provides the row count; the size information here provides
   * column widths and the number of elements in each array.
   */

  public VectorInitializer buildVectorInitializer() {
    VectorInitializer initializer = new VectorInitializer();
    for (ColumnSize colSize : columnSizes.values()) {
      colSize.buildVectorInitializer(initializer);
    }
    return initializer;
  }
}
