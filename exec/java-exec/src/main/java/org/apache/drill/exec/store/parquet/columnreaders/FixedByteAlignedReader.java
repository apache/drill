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
package org.apache.drill.exec.store.parquet.columnreaders;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;

import java.math.BigDecimal;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.expr.holders.Decimal28SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal38SparseHolder;
import org.apache.drill.exec.store.ParquetOutputRecordWriter;
import org.apache.drill.exec.util.DecimalUtility;
import org.apache.drill.exec.vector.DateVector;
import org.apache.drill.exec.vector.Decimal28SparseVector;
import org.apache.drill.exec.vector.Decimal38SparseVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;
import org.joda.time.DateTimeUtils;

import parquet.column.ColumnDescriptor;
import parquet.format.SchemaElement;
import parquet.hadoop.metadata.ColumnChunkMetaData;

class FixedByteAlignedReader extends ColumnReader {

  protected DrillBuf bytebuf;


  FixedByteAlignedReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                         boolean fixedLength, ValueVector v, SchemaElement schemaElement) throws ExecutionSetupException {
    super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
  }

  // this method is called by its superclass during a read loop
  @Override
  protected void readField(long recordsToReadInThisPass) {

    recordsReadInThisIteration = Math.min(pageReader.currentPage.getValueCount()
        - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

    readStartInBytes = pageReader.readPosInBytes;
    readLengthInBits = recordsReadInThisIteration * dataTypeLengthInBits;
    readLength = (int) Math.ceil(readLengthInBits / 8.0);

    bytebuf = pageReader.pageDataByteArray;
    // vectorData is assigned by the superclass read loop method
    writeData();
  }

  protected void writeData() {
    vectorData.writeBytes(bytebuf,
        (int) readStartInBytes, (int) readLength);
  }

  public static class FixedBinaryReader extends FixedByteAlignedReader {
    // TODO - replace this with fixed binary type in drill
    VariableWidthVector castedVector;

    FixedBinaryReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                    VariableWidthVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, true, v, schemaElement);
      castedVector = v;
    }

    @Override
    protected void readField(long recordsToReadInThisPass) {
      // we can use the standard read method to transfer the data
      super.readField(recordsToReadInThisPass);
      // TODO - replace this with fixed binary type in drill
      // now we need to write the lengths of each value
      int byteLength = dataTypeLengthInBits / 8;
      for (int i = 0; i < recordsToReadInThisPass; i++) {
        castedVector.getMutator().setValueLengthSafe(i, byteLength);
      }
    }

  }

  public static abstract class ConvertedReader extends FixedByteAlignedReader {

    protected int dataTypeLengthInBytes;

    ConvertedReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                           boolean fixedLength, ValueVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    @Override
    public void writeData() {
      dataTypeLengthInBytes = (int) Math.ceil(dataTypeLengthInBits / 8.0);
      for (int i = 0; i < recordsReadInThisIteration; i++) {
        addNext((int)readStartInBytes + i * dataTypeLengthInBytes, i + valuesReadInCurrentPass);
      }
    }

    /**
     * Reads from bytebuf, converts, and writes to buffer
     * @param start the index in bytes to start reading from
     * @param index the index of the ValueVector
     */
    abstract void addNext(int start, int index);
  }

  public static class DateReader extends ConvertedReader {

    DateVector dateVector;

    DateReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                    boolean fixedLength, ValueVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      dateVector = (DateVector) v;
    }

    @Override
    void addNext(int start, int index) {
//      dateVector.getMutator().set(index, DateTimeUtils.fromJulianDay(
//          NullableFixedByteAlignedReaders.NullableDateReader.readIntLittleEndian(bytebuf, start)
      dateVector.getMutator().set(index, DateTimeUtils.fromJulianDay(readIntLittleEndian(bytebuf, start)
              - ParquetOutputRecordWriter.JULIAN_DAY_EPOC - 0.5));
    }

    // copied out of parquet library, didn't want to deal with the uneeded throws statement they had declared
    public static int readIntLittleEndian(ByteBuf in, int offset) {
      int ch4 = in.getByte(offset) & 0xff;
      int ch3 = in.getByte(offset + 1) & 0xff;
      int ch2 = in.getByte(offset + 2) & 0xff;
      int ch1 = in.getByte(offset + 3) & 0xff;
      return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }
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
      BigDecimal intermediate = DecimalUtility.getBigDecimalFromDrillBuf(bytebuf, start, dataTypeLengthInBytes, schemaElement.getScale());
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
      BigDecimal intermediate = DecimalUtility.getBigDecimalFromDrillBuf(bytebuf, start, dataTypeLengthInBytes, schemaElement.getScale());
      DecimalUtility.getSparseFromBigDecimal(intermediate, decimal38Vector.getData(), index * width, schemaElement.getScale(),
              schemaElement.getPrecision(), Decimal38SparseHolder.nDecimalDigits);
    }
  }
}