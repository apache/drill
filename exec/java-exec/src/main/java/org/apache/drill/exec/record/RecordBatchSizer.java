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

import java.util.Set;
import java.util.Map;

import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.AllocationManager.BufferLedger;
import org.apache.drill.exec.memory.BaseAllocator;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.NullableVector;
import org.apache.drill.exec.vector.UInt1Vector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.UntypedNullVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.exec.vector.complex.RepeatedListVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;

import com.google.common.collect.Sets;
import org.apache.drill.exec.vector.complex.RepeatedVariableWidthVectorLike;

/**
 * Given a record batch or vector container, determines the actual memory
 * consumed by each column, the average row, and the entire record batch.
 */

public class RecordBatchSizer {
  private static final int OFFSET_VECTOR_WIDTH = UInt4Vector.VALUE_WIDTH;
  private static final int BIT_VECTOR_WIDTH = UInt1Vector.VALUE_WIDTH;
  private static final int STD_REPETITION_FACTOR = 10;

  /**
   * Column size information.
   */
  public static class ColumnSize {
    public final String prefix;
    public final MaterializedField metadata;

    /**
     * This is the total size of just pure data for the column
     * for all entries.
     */

    private int totalDataSize;

    /**
     * This is the total size of data for the column + additional
     * metadata vector overhead we add for cardinality, variable length etc.
     * for all entries.
     */

    private int totalNetSize;

    /**
     * Number of occurrences of the value in the batch. This is trivial
     * for top-level scalars: it is the record count. For a top-level
     * repeated vector, this is the number of arrays, also the record
     * count. For a value nested inside a repeated map, it is the
     * total number of values across all maps, and may be less than,
     * greater than (but unlikely) same as the row count.
     */

    private final int valueCount;

    /**
     * Total number of elements for a repeated type, or same as
     * valueCount if this is a non-repeated type. That is, a batch
     * of 100 rows may have an array with 10 elements per row.
     * In this case, the element count is 1000.
     */

    private int elementCount;

    /**
     * The estimated, average number of elements per parent value.
     * Always 1 for a non-repeated type. For a repeated type,
     * this is the average entries per array (per repeated element).
     */

    private float cardinality;

    /**
     * Indicates if it is variable width column.
     * For map columns, this is true if any of the children is variable
     * width column.
     */

    private boolean isVariableWidth;

    /**
     * Indicates if cardinality is repeated(top level only).
     */

    private boolean isRepeated;

    /**
     * Indicates if cardinality is optional i.e. nullable(top level only).
     */
    private boolean isOptional;

    /**
     * Child columns if this is a map column.
     */
    private Map<String, ColumnSize> children = CaseInsensitiveMap.newHashMap();

    /**
     * std pure data size per entry from Drill metadata, based on type.
     * Does not include metadata vector overhead we add for cardinality,
     * variable length etc.
     * For variable-width columns, we use 50 as std size for entry width.
     * For repeated column, we assume repetition of 10.
     */
    public int getStdDataSizePerEntry() {
      int stdDataSize;

      try {
        stdDataSize = TypeHelper.getSize(metadata.getType());

        // For variable width, typeHelper includes offset vector width. Adjust for that.
        if (isVariableWidth) {
          stdDataSize -= OFFSET_VECTOR_WIDTH;
        }

        if (isRepeated) {
          stdDataSize = stdDataSize * STD_REPETITION_FACTOR;
        }
      } catch (Exception e) {
        // For unsupported types, just set stdSize to 0.
        // Map, Union, List etc.
        stdDataSize = 0;
      }

      // Add sizes of children.
      for (ColumnSize columnSize : children.values()) {
        stdDataSize += columnSize.getStdDataSizePerEntry();
      }

      if (isRepeatedList()) {
        stdDataSize = stdDataSize * STD_REPETITION_FACTOR;
      }

      return stdDataSize;
    }

    /**
     * std net size per entry taking into account additional metadata vectors
     * we add on top for variable length, cardinality etc.
     * For variable-width columns, we use 50 as std data size for entry width.
     * For repeated column, we assume repetition of 10.
     */
    public int getStdNetSizePerEntry() {
      int stdNetSize;
      try {
        stdNetSize = TypeHelper.getSize(metadata.getType());
      } catch (Exception e) {
        stdNetSize = 0;
      }

      if (isOptional) {
        stdNetSize += BIT_VECTOR_WIDTH;
      }

      if (isRepeated) {
        stdNetSize = (stdNetSize * STD_REPETITION_FACTOR) + OFFSET_VECTOR_WIDTH;
      }

      for (ColumnSize columnSize : children.values()) {
        stdNetSize += columnSize.getStdNetSizePerEntry();
      }

      if (isRepeatedList()) {
        stdNetSize = (stdNetSize * STD_REPETITION_FACTOR) + OFFSET_VECTOR_WIDTH;
      }

      return stdNetSize;
    }

    /**
     * This is the average actual per entry data size in bytes. Does not
     * include any overhead of metadata vectors.
     * For repeated columns, it is average for the repeated array, not
     * individual entry in the array.
     */
    public int getDataSizePerEntry() {
      return safeDivide(getTotalDataSize(), getValueCount());
    }

    /**
     * This is the average per entry size of just pure data plus
     * overhead of additional vectors we add on top like bits vector,
     * offset vector etc. This
     * size is larger than the actual data size since this size includes per-
     * column overhead for additional vectors we add for
     * cardinality, variable length etc.
     */
    public int getNetSizePerEntry() {
      return safeDivide(getTotalNetSize(), getValueCount());
    }

    /**
     * This is the total data size for the column, including children for map
     * columns. Does not include any overhead of metadata vectors.
     */
    public int getTotalDataSize() {
      int dataSize = this.totalDataSize;
      for (ColumnSize columnSize : children.values()) {
        dataSize += columnSize.getTotalDataSize();
      }
      return dataSize;
    }

    /**
     * This is the total net size for the column, including children for map
     * columns. Includes overhead of metadata vectors.
     */
    public int getTotalNetSize() {
      return this.totalNetSize;
    }

    public int getValueCount() {
      return valueCount;
    }

    public int getElementCount() {
      return elementCount;
    }

    public float getCardinality() {
      return cardinality;
    }

    public boolean isVariableWidth() {
      return isVariableWidth;
    }

    public Map<String, ColumnSize> getChildren() {
      return children;
    }

    public boolean isComplex() {
      return metadata.getType().getMinorType() == MinorType.MAP ||
        metadata.getType().getMinorType() == MinorType.UNION ||
        metadata.getType().getMinorType() == MinorType.LIST;
    }

    public boolean isRepeatedList() {
      if (metadata.getType().getMinorType() == MinorType.LIST &&
        metadata.getDataMode() == DataMode.REPEATED) {
        return true;
      }
      return false;
    }

    /**
     * This is the average per entry width, used for vector allocation.
     */
    public int getEntryWidth() {
      int width = 0;
      if (isVariableWidth) {
        width = getNetSizePerEntry() - OFFSET_VECTOR_WIDTH;

        // Subtract out the bits (is-set) vector width
        if (metadata.getDataMode() == DataMode.OPTIONAL) {
          width -= BIT_VECTOR_WIDTH;
        }
      }

      return (safeDivide(width, cardinality));
    }

    public ColumnSize(ValueVector v, String prefix) {
      this.prefix = prefix;
      valueCount = v.getAccessor().getValueCount();
      metadata = v.getField();
      isVariableWidth = (v instanceof VariableWidthVector || v instanceof RepeatedVariableWidthVectorLike);
      elementCount = valueCount;
      cardinality = 1;
      totalNetSize = v.getPayloadByteCount(valueCount);

      // Special case. For union and list vectors, it is very complex
      // to figure out raw data size. Make it same as net size.
      if (metadata.getType().getMinorType() == MinorType.UNION ||
        (metadata.getType().getMinorType() == MinorType.LIST && v.getField().getDataMode() != DataMode.REPEATED)) {
        totalDataSize = totalNetSize;
      }

      switch(v.getField().getDataMode()) {
        case REPEATED:
          isRepeated = true;
          elementCount  = getElementCount(v);
          cardinality = valueCount == 0 ? 0 : elementCount * 1.0f / valueCount;

          // For complex types, there is nothing more to do for top columns.
          // Data size is calculated recursively for children later.
          if (isComplex()) {
            return;
          }

          // Calculate pure data size.
          if (isVariableWidth) {
            VariableWidthVector dataVector = ((VariableWidthVector) ((RepeatedValueVector) v).getDataVector());
            totalDataSize = dataVector.getCurrentSizeInBytes();
          } else {
            ValueVector dataVector = ((RepeatedValueVector) v).getDataVector();
            totalDataSize = dataVector.getPayloadByteCount(elementCount);
          }

          break;
        case OPTIONAL:
          isOptional = true;

          // For complex types, there is nothing more to do for top columns.
          // Data size is calculated recursively for children later.
          if (isComplex()) {
            return;
          }

          // Calculate pure data size.
          if (isVariableWidth) {
            VariableWidthVector variableWidthVector = ((VariableWidthVector) ((NullableVector) v).getValuesVector());
            totalDataSize = variableWidthVector.getCurrentSizeInBytes();
          } else {
            // Another special case.
            if (v instanceof UntypedNullVector) {
              return;
            }
            totalDataSize = ((NullableVector) v).getValuesVector().getPayloadByteCount(valueCount);
          }
          break;

        case REQUIRED:
          // For complex types, there is nothing more to do for top columns.
          // Data size is calculated recursively for children later.
          if (isComplex()) {
            return;
          }

          // Calculate pure data size.
          if (isVariableWidth) {
            totalDataSize = ((VariableWidthVector) v).getCurrentSizeInBytes();
          } else {
            totalDataSize = v.getPayloadByteCount(valueCount);
          }
          break;

        default:
            break;
      }
    }

    @SuppressWarnings("resource")
    private int getElementCount(ValueVector v) {
      // Repeated vectors are special: they have an associated offset vector
      // that changes the value count of the contained vectors.
      UInt4Vector offsetVector = ((RepeatedValueVector) v).getOffsetVector();
      int childCount = valueCount == 0 ? 0 : offsetVector.getAccessor().get(valueCount);

      return childCount;
    }

    private void allocateMap(AbstractMapVector map, int recordCount) {
      if (map instanceof RepeatedMapVector) {
        ((RepeatedMapVector) map).allocateOffsetsNew(recordCount);
          recordCount *= getCardinality();
        }

      for (ValueVector vector : map) {
        children.get(vector.getField().getName()).allocateVector(vector, recordCount);
      }
    }

    private void allocateRepeatedList(RepeatedListVector vector, int recordCount) {
      vector.allocateOffsetsNew(recordCount);
      recordCount *= getCardinality();
      ColumnSize child = children.get(vector.getField().getName());
      if (vector.getDataVector() != null) {
        child.allocateVector(vector.getDataVector(), recordCount);
      }
    }

    public void allocateVector(ValueVector vector, int recordCount) {
      if (vector instanceof AbstractMapVector) {
        allocateMap((AbstractMapVector) vector, recordCount);
        return;
      }

      if (vector instanceof RepeatedListVector) {
        allocateRepeatedList((RepeatedListVector) vector, recordCount);
        return;
      }

      AllocationHelper.allocate(vector, recordCount, getEntryWidth(), getCardinality());
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
           .append(cardinality);
      }
      buf.append("Per entry: std data size: ")
         .append(getStdDataSizePerEntry())
         .append(", std net size: ")
         .append(getStdNetSizePerEntry())
         .append(", actual data size: ")
         .append(getDataSizePerEntry())
         .append(", actual net size: ")
         .append(getNetSizePerEntry())
         .append("  Totals: data size: ")
         .append(getTotalDataSize())
         .append(", net size: ")
         .append(getTotalNetSize())
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

    public void buildVectorInitializer(VectorInitializer initializer) {
      int width = 0;
      switch(metadata.getType().getMinorType()) {
      case VAR16CHAR:
      case VARBINARY:
      case VARCHAR:

        // Subtract out the offset vector width
        width = getNetSizePerEntry() - OFFSET_VECTOR_WIDTH;

        // Subtract out the bits (is-set) vector width
        if (metadata.getDataMode() == DataMode.OPTIONAL) {
          width -= BIT_VECTOR_WIDTH;
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
          initializer.variableWidthArray(name, width / cardinality, cardinality);
        } else {
          initializer.fixedWidthArray(name, cardinality);
        }
      }
      else if (width > 0) {
        initializer.variableWidth(name, width);
      }

      for (ColumnSize columnSize : children.values()) {
        columnSize.buildVectorInitializer(initializer);
      }
    }

  }

  public static ColumnSize getColumn(ValueVector v, String prefix) {
    return new ColumnSize(v, prefix);
  }

  public ColumnSize getColumn(String name) {
    return columnSizes.get(name);
  }

  // This keeps information for only top level columns. Information for nested
  // columns can be obtained from children of topColumns.
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
  private int netRowWidthCap50;
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
      (batch.getSchema() == null ? null : (batch.getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.TWO_BYTE ?
        batch.getSelectionVector2() : null)));
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
      ColumnSize colSize = measureColumn(vw.getValueVector(), "");
      columnSizes.put(vw.getField().getName(), colSize);
      stdRowWidth += colSize.getStdDataSizePerEntry();
      netBatchSize += colSize.getTotalNetSize();
      maxSize = Math.max(maxSize, colSize.getTotalDataSize());
      if (colSize.metadata.isNullable()) {
        nullableCount++;
      }
      netRowWidth += colSize.getNetSizePerEntry();
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

  private ColumnSize measureColumn(ValueVector v, String prefix) {
    ColumnSize colSize = new ColumnSize(v, prefix);
    switch (v.getField().getType().getMinorType()) {
      case MAP:
        // Maps consume no size themselves. However, their contained
        // vectors do consume space, so visit columns recursively.
        expandMap(colSize, (AbstractMapVector) v, prefix + v.getField().getName() + ".");
        break;
      case LIST:
        // complex ListVector cannot be casted to RepeatedListVector.
        // do not expand the list if it is not repeated mode.
        if (v.getField().getDataMode() == DataMode.REPEATED) {
          expandList(colSize, (RepeatedListVector) v, prefix + v.getField().getName() + ".");
        }
        break;
      default:
        v.collectLedgers(ledgers);
    }

    netRowWidthCap50 += ! colSize.isVariableWidth ? colSize.getNetSizePerEntry() :
        8 /* offset vector */ + roundUpToPowerOf2(Math.min(colSize.getNetSizePerEntry(),50));
        // above change 8 to 4 after DRILL-5446 is fixed

    return colSize;
  }

  private void expandMap(ColumnSize colSize, AbstractMapVector mapVector, String prefix) {
    for (ValueVector vector : mapVector) {
      colSize.children.put(vector.getField().getName(), measureColumn(vector, prefix));
    }

    // For a repeated map, we need the memory for the offset vector (only).
    // Map elements are recursively expanded above.

    if (mapVector.getField().getDataMode() == DataMode.REPEATED) {
      ((RepeatedMapVector) mapVector).getOffsetVector().collectLedgers(ledgers);
    }
  }

  private void expandList(ColumnSize colSize, RepeatedListVector vector, String prefix) {
    colSize.children.put(vector.getField().getName(), measureColumn(vector.getDataVector(), prefix));

    // Determine memory for the offset vector (only).
    vector.collectLedgers(ledgers);
  }

  public static int safeDivide(long num, long denom) {
    if (denom == 0) {
      return 0;
    }
    return (int) Math.ceil((double) num / denom);
  }

  public static int safeDivide(int num, int denom) {
    if (denom == 0) {
      return 0;
    }
    return (int) Math.ceil((double) num / denom);
  }

  public static int safeDivide(int num, float denom) {
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

  /**
   * Compute the "real" width of the row, taking into account each varchar column size
   * (historically capped at 50, and rounded up to power of 2 to match drill buf allocation)
   * and null marking columns.
   * @return "real" width of the row
   */
  public int netRowWidthCap50() { return netRowWidthCap50 + nullableCount; }
  public long actualSize() { return accountedMemorySize; }
  public boolean hasSv2() { return hasSv2; }
  public int avgDensity() { return avgDensity; }
  public long netSize() { return netBatchSize; }
  public int maxAvgColumnSize() { return maxSize / rowCount; }

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

  public void allocateVectors(VectorContainer container, int recordCount) {
    for (VectorWrapper w : container) {
      ColumnSize colSize = columnSizes.get(w.getField().getName());
      colSize.allocateVector(w.getValueVector(), recordCount);
    }
  }
}
