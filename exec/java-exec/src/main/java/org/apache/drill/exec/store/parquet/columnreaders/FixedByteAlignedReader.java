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

import java.math.BigDecimal;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.expr.holders.Decimal28SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal38SparseHolder;
import org.apache.drill.exec.store.ParquetOutputRecordWriter;
import org.apache.drill.exec.store.parquet.ParquetReaderUtility;
import org.apache.drill.exec.util.DecimalUtility;
import org.apache.drill.exec.vector.DateVector;
import org.apache.drill.exec.vector.Decimal28SparseVector;
import org.apache.drill.exec.vector.Decimal38SparseVector;
import org.apache.drill.exec.vector.IntervalVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.joda.time.DateTimeUtils;

import io.netty.buffer.DrillBuf;

class FixedByteAlignedReader<V extends ValueVector> extends ColumnReader<V> {

  protected DrillBuf bytebuf;


  FixedByteAlignedReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                         boolean fixedLength, V v, SchemaElement schemaElement) throws ExecutionSetupException {
    super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
  }

  // this method is called by its superclass during a read loop
  @Override
  protected void readField(long recordsToReadInThisPass) {

    recordsReadInThisIteration = Math.min(pageReader.currentPageCount
        - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

    readStartInBytes = pageReader.readPosInBytes;
    readLengthInBits = recordsReadInThisIteration * dataTypeLengthInBits;
    readLength = (int) Math.ceil(readLengthInBits / 8.0);

    bytebuf = pageReader.pageData;
    // vectorData is assigned by the superclass read loop method
    writeData();
  }

  protected void writeData() {
    vectorData.writeBytes(bytebuf, (int) readStartInBytes, (int) readLength);
  }

  public static class FixedBinaryReader extends FixedByteAlignedReader<VariableWidthVector> {
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
        castedVector.getMutator().setValueLengthSafe(valuesReadInCurrentPass + i, byteLength);
      }
    }

  }

  public static abstract class ConvertedReader<V extends ValueVector> extends FixedByteAlignedReader<V> {

    protected int dataTypeLengthInBytes;

    ConvertedReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                           boolean fixedLength, V v, SchemaElement schemaElement) throws ExecutionSetupException {
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

  public static class DateReader extends ConvertedReader<DateVector> {

    DateReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                    boolean fixedLength, DateVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    @Override
    void addNext(int start, int index) {
      int intValue;
      if (usingDictionary) {
        intValue =  pageReader.dictionaryValueReader.readInteger();
      } else {
        intValue = readIntLittleEndian(bytebuf, start);
      }

      valueVec.getMutator().set(index, DateTimeUtils.fromJulianDay(intValue - ParquetOutputRecordWriter.JULIAN_DAY_EPOC - 0.5));
    }

  }

  public static class Decimal28Reader extends ConvertedReader<Decimal28SparseVector> {

    Decimal28Reader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                    boolean fixedLength, Decimal28SparseVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    @Override
    void addNext(int start, int index) {
      int width = Decimal28SparseHolder.WIDTH;
      BigDecimal intermediate = DecimalUtility.getBigDecimalFromDrillBuf(bytebuf, start, dataTypeLengthInBytes,
          schemaElement.getScale());
      DecimalUtility.getSparseFromBigDecimal(intermediate, valueVec.getBuffer(), index * width, schemaElement.getScale(),
              schemaElement.getPrecision(), Decimal28SparseHolder.nDecimalDigits);
    }
  }

  public static class Decimal38Reader extends ConvertedReader<Decimal38SparseVector> {

    Decimal38Reader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                    boolean fixedLength, Decimal38SparseVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    @Override
    void addNext(int start, int index) {
      int width = Decimal38SparseHolder.WIDTH;
      BigDecimal intermediate = DecimalUtility.getBigDecimalFromDrillBuf(bytebuf, start, dataTypeLengthInBytes,
          schemaElement.getScale());
      DecimalUtility.getSparseFromBigDecimal(intermediate, valueVec.getBuffer(), index * width, schemaElement.getScale(),
              schemaElement.getPrecision(), Decimal38SparseHolder.nDecimalDigits);
    }
  }

  public static class IntervalReader extends ConvertedReader<IntervalVector> {
    IntervalReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                   boolean fixedLength, IntervalVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    @Override
    void addNext(int start, int index) {
      if (usingDictionary) {
        byte[] input = pageReader.dictionaryValueReader.readBytes().getBytes();
        valueVec.getMutator().setSafe(index * 12,
            ParquetReaderUtility.getIntFromLEBytes(input, 0),
            ParquetReaderUtility.getIntFromLEBytes(input, 4),
            ParquetReaderUtility.getIntFromLEBytes(input, 8));
      }
      valueVec.getMutator().setSafe(index, bytebuf.getInt(start), bytebuf.getInt(start + 4), bytebuf.getInt(start + 8));
    }
  }
}