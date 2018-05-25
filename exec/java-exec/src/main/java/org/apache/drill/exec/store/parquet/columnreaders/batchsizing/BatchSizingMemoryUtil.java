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
package org.apache.drill.exec.store.parquet.columnreaders.batchsizing;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BaseAllocator;
import org.apache.drill.exec.store.parquet.columnreaders.ParquetColumnMetadata;
import org.apache.drill.exec.store.parquet.columnreaders.batchsizing.RecordBatchSizerManager.ColumnMemoryQuota;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.NullableVarDecimalVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.VarDecimalVector;
import org.apache.drill.exec.vector.VariableWidthVector;

/** Helper class to assist the Flat Parquet reader build batches which adhere to memory sizing constraints */
public final class BatchSizingMemoryUtil {

  /** BYTE in-memory width */
  public static final int BYTE_VALUE_WIDTH = 1;
  /** INT in-memory width */
  public static final int INT_VALUE_WIDTH  = 4;
  /** Default variable length column average precision;
   * computed in such away that 64k values will fit within one MB to minimize internal fragmentation
   */
  public static final int DEFAULT_VL_COLUMN_AVG_PRECISION = 16;

  /**
   * This method will also load detailed information about this column's current memory usage (with regard
   * to the value vectors).
   *
   * @param columnMemoryUsage container which contains column's memory usage information (usage information will
   *        be automatically updated by this method)
   * @param newBitsMemory New nullable data which might be inserted when processing a new input chunk
   * @param newOffsetsMemory New offsets data which might be inserted when processing a new input chunk
   * @param newDataMemory New data which might be inserted when processing a new input chunk
   *
   * @return true if adding the new data will not lead this column's Value Vector go beyond the allowed
   *         limit; false otherwise
   */
  public static boolean canAddNewData(ColumnMemoryUsageInfo columnMemoryUsage,
    int newBitsMemory,
    int newOffsetsMemory,
    int newDataMemory) {

    // First we need to update the vector memory usage
    final VectorMemoryUsageInfo vectorMemoryUsage = columnMemoryUsage.vectorMemoryUsage;
    getMemoryUsage(columnMemoryUsage.vector, columnMemoryUsage.currValueCount, vectorMemoryUsage);

    // We need to compute the new ValueVector memory usage if we attempt to add the new payload
    // usedCapacity, int newPayload, int currentCapacity
    int totalBitsMemory = computeNewVectorCapacity(vectorMemoryUsage.bitsBytesUsed,
      newBitsMemory,
      vectorMemoryUsage.bitsBytesCapacity);

    int totalOffsetsMemory = computeNewVectorCapacity(vectorMemoryUsage.offsetsBytesUsed,
      newOffsetsMemory,
      vectorMemoryUsage.offsetsByteCapacity);

    int totalDataMemory = computeNewVectorCapacity(vectorMemoryUsage.dataBytesUsed,
      newDataMemory,
      vectorMemoryUsage.dataByteCapacity);

    // Alright now we can figure out whether the new payload will take us over the maximum memory threshold
    int totalMemory = totalBitsMemory + totalOffsetsMemory + totalDataMemory;
    assert totalMemory >= 0;

    return totalMemory <= columnMemoryUsage.memoryQuota.getMaxMemoryUsage();
  }

  /**
   * Load memory usage information for a variable length value vector
   *
   * @param vector source value vector
   * @param currValueCount current value count
   * @param vectorMemory result object which contains source vector memory usage information
   */
  public static void getMemoryUsage(ValueVector sourceVector,
    int currValueCount,
    VectorMemoryUsageInfo vectorMemoryUsage) {

    assert sourceVector instanceof VariableWidthVector;

    vectorMemoryUsage.reset(); // reset result container

    final MajorType type = sourceVector.getField().getType();

    switch (type.getMinorType()) {
    case VARCHAR: {
      switch (type.getMode()) {
        case REQUIRED: {
          VarCharVector vector                  = (VarCharVector) sourceVector;
          vectorMemoryUsage.offsetsByteCapacity = vector.getOffsetVector().getValueCapacity() * INT_VALUE_WIDTH;
          vectorMemoryUsage.dataByteCapacity    = vector.getByteCapacity();
          vectorMemoryUsage.offsetsBytesUsed    = vector.getOffsetVector().getPayloadByteCount(currValueCount);
          vectorMemoryUsage.dataBytesUsed       = vector.getPayloadByteCount(currValueCount) - vectorMemoryUsage.offsetsBytesUsed;
          break;
        }
        case OPTIONAL: {
          NullableVarCharVector vector          = (NullableVarCharVector) sourceVector;
          VarCharVector values                  = vector.getValuesVector();
          vectorMemoryUsage.bitsBytesCapacity   = vector.getBitsValueCapacity();
          vectorMemoryUsage.offsetsByteCapacity = values.getOffsetVector().getValueCapacity() * INT_VALUE_WIDTH;
          vectorMemoryUsage.dataByteCapacity    = values.getByteCapacity();
          vectorMemoryUsage.bitsBytesUsed       = currValueCount * BYTE_VALUE_WIDTH;
          vectorMemoryUsage.offsetsBytesUsed    = values.getOffsetVector().getPayloadByteCount(currValueCount);
          vectorMemoryUsage.dataBytesUsed       = values.getPayloadByteCount(currValueCount) - vectorMemoryUsage.offsetsBytesUsed;
          break;
        }

        default : throw new IllegalArgumentException("Mode [" + type.getMode().name() + "] not supported..");
      }
      break;
    }

    case VARBINARY: {
      switch (type.getMode()) {
        case REQUIRED: {
          VarBinaryVector vector                = (VarBinaryVector) sourceVector;
          vectorMemoryUsage.offsetsByteCapacity = vector.getOffsetVector().getValueCapacity() * INT_VALUE_WIDTH;
          vectorMemoryUsage.dataByteCapacity    = vector.getByteCapacity();
          vectorMemoryUsage.offsetsBytesUsed    = vector.getOffsetVector().getPayloadByteCount(currValueCount);
          vectorMemoryUsage.dataBytesUsed       = vector.getPayloadByteCount(currValueCount) - vectorMemoryUsage.offsetsBytesUsed;
          break;
        }
        case OPTIONAL: {
          NullableVarBinaryVector vector        = (NullableVarBinaryVector) sourceVector;
          VarBinaryVector values                = vector.getValuesVector();
          vectorMemoryUsage.bitsBytesCapacity   = vector.getBitsValueCapacity();
          vectorMemoryUsage.offsetsByteCapacity = values.getOffsetVector().getValueCapacity() * INT_VALUE_WIDTH;
          vectorMemoryUsage.dataByteCapacity    = values.getByteCapacity();
          vectorMemoryUsage.bitsBytesUsed       = currValueCount * BYTE_VALUE_WIDTH;
          vectorMemoryUsage.offsetsBytesUsed    = values.getOffsetVector().getPayloadByteCount(currValueCount);
          vectorMemoryUsage.dataBytesUsed       = values.getPayloadByteCount(currValueCount) - vectorMemoryUsage.offsetsBytesUsed;
          break;
        }

        default : throw new IllegalArgumentException("Mode [" + type.getMode().name() + "] not supported..");
      }
      break;
    }

    case VARDECIMAL: {
      switch (type.getMode()) {
        case REQUIRED: {
          VarDecimalVector vector               = (VarDecimalVector) sourceVector;
          vectorMemoryUsage.offsetsByteCapacity = vector.getOffsetVector().getValueCapacity() * INT_VALUE_WIDTH;
          vectorMemoryUsage.dataByteCapacity    = vector.getByteCapacity();
          vectorMemoryUsage.offsetsBytesUsed    = vector.getOffsetVector().getPayloadByteCount(currValueCount);
          vectorMemoryUsage.dataBytesUsed       = vector.getPayloadByteCount(currValueCount) - vectorMemoryUsage.offsetsBytesUsed;
          break;
        }
        case OPTIONAL: {
          NullableVarDecimalVector vector       = (NullableVarDecimalVector) sourceVector;
          VarDecimalVector values               = vector.getValuesVector();
          vectorMemoryUsage.bitsBytesCapacity   = vector.getBitsValueCapacity();
          vectorMemoryUsage.offsetsByteCapacity = values.getOffsetVector().getValueCapacity() * INT_VALUE_WIDTH;
          vectorMemoryUsage.dataByteCapacity    = values.getByteCapacity();
          vectorMemoryUsage.bitsBytesUsed       = currValueCount * BYTE_VALUE_WIDTH;
          vectorMemoryUsage.offsetsBytesUsed    = values.getOffsetVector().getPayloadByteCount(currValueCount);
          vectorMemoryUsage.dataBytesUsed       = values.getPayloadByteCount(currValueCount) - vectorMemoryUsage.offsetsBytesUsed;
          break;
        }

        default : throw new IllegalArgumentException("Mode [" + type.getMode().name() + "] not supported..");
      }
      break;
    }

    default : throw new IllegalArgumentException("Type [" + type.getMinorType().name() + "] not supported..");
    } // End of minor-type-switch-statement

    assert vectorMemoryUsage.bitsBytesCapacity   >= 0;
    assert vectorMemoryUsage.bitsBytesUsed       >= 0;
    assert vectorMemoryUsage.offsetsByteCapacity >= 0;
    assert vectorMemoryUsage.offsetsBytesUsed    >= 0;
    assert vectorMemoryUsage.dataByteCapacity    >= 0;
    assert vectorMemoryUsage.dataBytesUsed       >= 0;

  }

  /**
   * @param column fixed column's metadata
   * @return column byte precision
   */
  public static int getFixedColumnTypePrecision(ParquetColumnMetadata column) {
    assert column.isFixedLength();

    return TypeHelper.getSize(column.getField().getType());
  }

  /**
   * This method will return a default value for variable columns; it aims at minimizing internal fragmentation.
   * <p><b>Note</b> that the {@link TypeHelper} uses a large default value which might not be always appropriate.
   *
   * @param column fixed column's metadata
   * @return column byte precision
   */
  public static int getAvgVariableLengthColumnTypePrecision(ParquetColumnMetadata column) {
    assert !column.isFixedLength();

    return DEFAULT_VL_COLUMN_AVG_PRECISION;
  }

  /**
   * @param fixed column's metadata
   * @param valueCount number of column values
   * @return memory size required to store "valueCount" within a value vector
   */
  public static int computeFixedLengthVectorMemory(ParquetColumnMetadata column, int valueCount) {
    assert column.isFixedLength();

    // Formula:  memory-usage = next-power-of-two(byte-size * valueCount)  // nullable storage (if any)
    //         + next-power-of-two(DT_LEN * valueCount)                    // data storage

    int memoryUsage = BaseAllocator.nextPowerOfTwo(getFixedColumnTypePrecision(column) * valueCount);

    if (column.getField().isNullable()) {
      memoryUsage += BaseAllocator.nextPowerOfTwo(BYTE_VALUE_WIDTH * valueCount);
    }

    return memoryUsage;
  }

  /**
   * @param variable length column's metadata
   * @param averagePrecision VL column average precision
   * @param valueCount number of column values
   * @return memory size required to store "valueCount" within a value vector
   */
  public static int computeVariableLengthVectorMemory(ParquetColumnMetadata column,
    int averagePrecision, int valueCount) {

    assert !column.isFixedLength();

    // Formula:  memory-usage = next-power-of-two(byte-size * valueCount)  // nullable storage (if any)
    //         + next-power-of-two(int-size * valueCount)                  // offsets storage
    //         + next-power-of-two(DT_LEN * valueCount)                    // data storage
    int memoryUsage = BaseAllocator.nextPowerOfTwo(averagePrecision * valueCount);
    memoryUsage += BaseAllocator.nextPowerOfTwo(INT_VALUE_WIDTH * (valueCount + 1));

    if (column.getField().isNullable()) {
      memoryUsage += BaseAllocator.nextPowerOfTwo(valueCount);
    }
    return memoryUsage;
  }

// ----------------------------------------------------------------------------
// Internal implementation
// ----------------------------------------------------------------------------

  private static int computeNewVectorCapacity(int usedCapacity, int newPayload, int currentCapacity) {
    int newUsedCapacity = BaseAllocator.nextPowerOfTwo(usedCapacity + newPayload);
    assert newUsedCapacity >= 0;

    return newUsedCapacity <= currentCapacity ? currentCapacity : newUsedCapacity;
  }

// ----------------------------------------------------------------------------
// Inner data structure
// ----------------------------------------------------------------------------

  /**
   * A container class to hold a column batch memory usage information.
   */
  public static final class ColumnMemoryUsageInfo {
    /** Value vector which contains the column batch data */
    public ValueVector vector;
    /** Column memory quota */
    public ColumnMemoryQuota memoryQuota;
    /** Current record count stored within the value vector */
    public int currValueCount;
    /** Current vector memory usage */
    public final VectorMemoryUsageInfo vectorMemoryUsage = new VectorMemoryUsageInfo();
  }

  /** Container class which holds memory usage information about a variable length {@link ValueVector};
   * all values are in bytes.
   */
  public static final class VectorMemoryUsageInfo {
    /** Bits vector capacity */
    public int bitsBytesCapacity;
    /** Offsets vector capacity */
    public int offsetsByteCapacity;
    /** Data vector capacity */
    public int dataByteCapacity;
    /** Bits vector used up capacity */
    public int bitsBytesUsed;
    /** Offsets vector used up capacity */
    public int offsetsBytesUsed;
    /** Data vector used up capacity */
    public int dataBytesUsed;

    public void reset() {
      bitsBytesCapacity    = 0;
      offsetsByteCapacity  = 0;
      dataByteCapacity     = 0;
      bitsBytesUsed        = 0;
      offsetsBytesUsed     = 0;
      dataBytesUsed        = 0;
    }
  }

  /** Disabling object instantiation */
  private BatchSizingMemoryUtil() {
    // NOOP
  }

}
