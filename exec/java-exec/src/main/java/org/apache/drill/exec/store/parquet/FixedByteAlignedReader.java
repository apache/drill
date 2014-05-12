/**
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
package org.apache.drill.exec.store.parquet;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.util.DecimalUtility;
import org.apache.drill.exec.expr.holders.Decimal28SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal38SparseHolder;
import org.apache.drill.exec.vector.Decimal28SparseVector;
import org.apache.drill.exec.vector.Decimal38SparseVector;
import org.apache.drill.exec.vector.ValueVector;
import parquet.column.ColumnDescriptor;
import parquet.format.ConvertedType;
import parquet.format.SchemaElement;
import parquet.hadoop.metadata.ColumnChunkMetaData;

import java.math.BigDecimal;

class FixedByteAlignedReader extends ColumnReader {

  protected byte[] bytes;

  
  FixedByteAlignedReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                         boolean fixedLength, ValueVector v, SchemaElement schemaElement) throws ExecutionSetupException {
    super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
  }

  // this method is called by its superclass during a read loop
  @Override
  protected void readField(long recordsToReadInThisPass, ColumnReader firstColumnStatus) {

    recordsReadInThisIteration = Math.min(pageReadStatus.currentPage.getValueCount()
        - pageReadStatus.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

    readStartInBytes = pageReadStatus.readPosInBytes;
    readLengthInBits = recordsReadInThisIteration * dataTypeLengthInBits;
    readLength = (int) Math.ceil(readLengthInBits / 8.0);

    bytes = pageReadStatus.pageDataByteArray;
    // vectorData is assigned by the superclass read loop method
    vectorData.writeBytes(bytes,
        (int) readStartInBytes, (int) readLength);
  }

  public static abstract class ConvertedReader extends FixedByteAlignedReader {

    protected int dataTypeLengthInBytes;

    ConvertedReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                           boolean fixedLength, ValueVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    @Override
    protected void readField(long recordsToReadInThisPass, ColumnReader firstColumnStatus) {
      recordsReadInThisIteration = Math.min(pageReadStatus.currentPage.getValueCount()
              - pageReadStatus.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      readStartInBytes = pageReadStatus.readPosInBytes;
      readLengthInBits = recordsReadInThisIteration * dataTypeLengthInBits;
      readLength = (int) Math.ceil(readLengthInBits / 8.0);

      bytes = pageReadStatus.pageDataByteArray;

      dataTypeLengthInBytes = (int) Math.ceil(dataTypeLengthInBits / 8.0);
      for (int i = 0; i < recordsReadInThisIteration; i++) {
        addNext((int)readStartInBytes + i * dataTypeLengthInBytes, i + valuesReadInCurrentPass);
      }
    }

    /**
     * Reads from bytes, converts, and writes to buffer
     * @param start the index in bytes to start reading from
     * @param index the index of the ValueVector
     */
    abstract void addNext(int start, int index);
  }

  public static class Decimal28Reader extends ConvertedReader {

    Decimal28SparseVector decimal28Vector;

    Decimal28Reader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                    boolean fixedLength, ValueVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      decimal28Vector = (Decimal28SparseVector) v;
    }

    @Override
    void addNext(int start, int index) {
      int width = Decimal28SparseHolder.WIDTH;
      BigDecimal intermediate = DecimalUtility.getBigDecimalFromByteArray(bytes, start, dataTypeLengthInBytes, schemaElement.getScale());
      DecimalUtility.getSparseFromBigDecimal(intermediate, decimal28Vector.getData(), index * width, schemaElement.getScale(),
              schemaElement.getPrecision(), Decimal28SparseHolder.nDecimalDigits);
    }
  }

  public static class Decimal38Reader extends ConvertedReader {

    Decimal38SparseVector decimal38Vector;

    Decimal38Reader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                    boolean fixedLength, ValueVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      decimal38Vector = (Decimal38SparseVector) v;
    }

    @Override
    void addNext(int start, int index) {
      int width = Decimal38SparseHolder.WIDTH;
      BigDecimal intermediate = DecimalUtility.getBigDecimalFromByteArray(bytes, start, dataTypeLengthInBytes, schemaElement.getScale());
      DecimalUtility.getSparseFromBigDecimal(intermediate, decimal38Vector.getData(), index * width, schemaElement.getScale(),
              schemaElement.getPrecision(), Decimal38SparseHolder.nDecimalDigits);
    }
  }
}