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

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.util.DecimalUtility;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableFloat4Vector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.expr.holders.NullableDecimal28SparseHolder;
import org.apache.drill.exec.expr.holders.NullableDecimal38SparseHolder;
import org.apache.drill.exec.store.ParquetOutputRecordWriter;
import org.apache.drill.exec.vector.NullableDateVector;
import org.apache.drill.exec.vector.NullableDecimal28SparseVector;
import org.apache.drill.exec.vector.NullableDecimal38SparseVector;

import org.joda.time.DateTimeUtils;
import parquet.column.ColumnDescriptor;
import parquet.format.SchemaElement;
import parquet.hadoop.metadata.ColumnChunkMetaData;

import java.math.BigDecimal;

public class NullableFixedByteAlignedReaders {

  static class NullableFixedByteAlignedReader extends NullableColumnReader {
    protected byte[] bytes;

    NullableFixedByteAlignedReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                      ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, ValueVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      this.recordsReadInThisIteration = recordsToReadInThisPass;

      // set up metadata
      this.readStartInBytes = pageReader.readPosInBytes;
      this.readLengthInBits = recordsReadInThisIteration * dataTypeLengthInBits;
      this.readLength = (int) Math.ceil(readLengthInBits / 8.0);
      this.bytes = pageReader.pageDataByteArray;

      // fill in data.
      vectorData.writeBytes(bytes, (int) readStartInBytes, (int) readLength);
    }
  }

  static class NullableDictionaryIntReader extends NullableColumnReader<NullableIntVector> {

    private byte[] bytes;

    NullableDictionaryIntReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableIntVector v,
                                SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      if (usingDictionary) {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readInteger());
        }
      }
    }
  }

  static class NullableDictionaryBigIntReader extends NullableColumnReader<NullableBigIntVector> {

    private byte[] bytes;

    NullableDictionaryBigIntReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                   ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableBigIntVector v,
                                   SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      for (int i = 0; i < recordsToReadInThisPass; i++){
        valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readLong());
      }
    }
  }

  static class NullableDictionaryFloat4Reader extends NullableColumnReader<NullableFloat4Vector> {

    private byte[] bytes;

    NullableDictionaryFloat4Reader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                   ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableFloat4Vector v,
                                   SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      for (int i = 0; i < recordsToReadInThisPass; i++){
        valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readFloat());
      }
    }
  }

  static class NullableDictionaryFloat8Reader extends NullableColumnReader<NullableFloat8Vector> {

    private byte[] bytes;

    NullableDictionaryFloat8Reader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                  ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableFloat8Vector v,
                                  SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      for (int i = 0; i < recordsToReadInThisPass; i++){
        valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readDouble());
      }
    }
  }

  static abstract class NullableConvertedReader extends NullableFixedByteAlignedReader {

    protected int dataTypeLengthInBytes;

    NullableConvertedReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                            ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, ValueVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    @Override
    protected void readField(long recordsToReadInThisPass) {

      this.recordsReadInThisIteration = recordsToReadInThisPass;

      // set up metadata
      this.readStartInBytes = pageReader.readPosInBytes;
      this.readLengthInBits = recordsReadInThisIteration * dataTypeLengthInBits;
      this.readLength = (int) Math.ceil(readLengthInBits / 8.0);
      this.bytes = pageReader.pageDataByteArray;

      dataTypeLengthInBytes = (int) Math.ceil(dataTypeLengthInBits / 8.0);
      for (int i = 0; i < recordsReadInThisIteration; i++) {
        addNext((int) readStartInBytes + i * dataTypeLengthInBytes, i + valuesReadInCurrentPass);
      }
    }

    abstract void addNext(int start, int index);
  }

  public static class NullableDateReader extends NullableConvertedReader {

    NullableDateVector dateVector;

    NullableDateReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                       boolean fixedLength, ValueVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      dateVector = (NullableDateVector) v;
    }

    @Override
    void addNext(int start, int index) {
      dateVector.getMutator().set(index, DateTimeUtils.fromJulianDay(readIntLittleEndian(bytes, start) - ParquetOutputRecordWriter.JULIAN_DAY_EPOC - 0.5));
    }

    // copied out of parquet library, didn't want to deal with the uneeded throws statement they had declared
    public static int readIntLittleEndian(byte[] in, int offset) {
      int ch4 = in[offset] & 0xff;
      int ch3 = in[offset + 1] & 0xff;
      int ch2 = in[offset + 2] & 0xff;
      int ch1 = in[offset + 3] & 0xff;
      return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

  }

  public static class NullableDecimal28Reader extends NullableConvertedReader {

    NullableDecimal28SparseVector decimal28Vector;

    NullableDecimal28Reader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                            boolean fixedLength, ValueVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      decimal28Vector = (NullableDecimal28SparseVector) v;
    }

    @Override
    void addNext(int start, int index) {
      int width = NullableDecimal28SparseHolder.WIDTH;
      BigDecimal intermediate = DecimalUtility.getBigDecimalFromByteArray(bytes, start, dataTypeLengthInBytes, schemaElement.getScale());
      DecimalUtility.getSparseFromBigDecimal(intermediate, decimal28Vector.getData(), index * width, schemaElement.getScale(),
          schemaElement.getPrecision(), NullableDecimal28SparseHolder.nDecimalDigits);
    }
  }

  public static class NullableDecimal38Reader extends NullableConvertedReader {

    NullableDecimal38SparseVector decimal38Vector;

    NullableDecimal38Reader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                            boolean fixedLength, ValueVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      decimal38Vector = (NullableDecimal38SparseVector) v;
    }

    @Override
    void addNext(int start, int index) {
      int width = NullableDecimal38SparseHolder.WIDTH;
      BigDecimal intermediate = DecimalUtility.getBigDecimalFromByteArray(bytes, start, dataTypeLengthInBytes, schemaElement.getScale());
      DecimalUtility.getSparseFromBigDecimal(intermediate, decimal38Vector.getData(), index * width, schemaElement.getScale(),
          schemaElement.getPrecision(), NullableDecimal38SparseHolder.nDecimalDigits);
    }
  }

}