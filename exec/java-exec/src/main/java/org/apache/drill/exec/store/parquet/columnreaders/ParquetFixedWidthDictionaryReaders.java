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
package org.apache.drill.exec.store.parquet.columnreaders;

import org.apache.drill.shaded.guava.com.google.common.primitives.Ints;
import org.apache.drill.shaded.guava.com.google.common.primitives.Longs;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.Float4Vector;
import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.TimeStampVector;
import org.apache.drill.exec.vector.TimeVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.UInt8Vector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.drill.exec.vector.VarDecimalVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.api.Binary;

import static org.apache.drill.exec.store.parquet.ParquetReaderUtility.NanoTimeUtils.getDateTimeValueFromBinary;

public class ParquetFixedWidthDictionaryReaders {

  private static final double BITS_COUNT_IN_BYTE_DOUBLE_VALUE = 8.0;

  static class DictionaryIntReader extends FixedByteAlignedReader<IntVector> {
    DictionaryIntReader(ParquetRecordReader parentReader, ColumnDescriptor descriptor,
                                ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, IntVector v,
                                SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {

      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
          - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      if (usingDictionary) {
        for (int i = 0; i < recordsReadInThisIteration; i++){
          valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readInteger());
        }
      }
    }
  }

  /**
   * This class uses for reading unsigned integer fields.
   */
  static class DictionaryUInt4Reader extends FixedByteAlignedReader<UInt4Vector> {
    DictionaryUInt4Reader(ParquetRecordReader parentReader, ColumnDescriptor descriptor,
                        ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, UInt4Vector v,
                        SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {

      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
        - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      if (usingDictionary) {
        UInt4Vector.Mutator mutator = valueVec.getMutator();
        for (int i = 0; i < recordsReadInThisIteration; i++) {
          mutator.setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readInteger());
        }
        // Set the write Index. The next page that gets read might be a page that does not use dictionary encoding
        // and we will go into the else condition below. The readField method of the parent class requires the
        // writer index to be set correctly.
        readLengthInBits = recordsReadInThisIteration * dataTypeLengthInBits;
        readLength = (int) Math.ceil(readLengthInBits / BITS_COUNT_IN_BYTE_DOUBLE_VALUE);
        int writerIndex = valueVec.getBuffer().writerIndex();
        valueVec.getBuffer().setIndex(0, writerIndex + (int) readLength);
      } else {
        super.readField(recordsToReadInThisPass);
      }
    }
  }

  static class DictionaryFixedBinaryReader extends FixedByteAlignedReader<VarBinaryVector> {
    DictionaryFixedBinaryReader(ParquetRecordReader parentReader, ColumnDescriptor descriptor,
                        ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, VarBinaryVector v,
                        SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {

      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
          - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);
      readLengthInBits = recordsReadInThisIteration * dataTypeLengthInBits;
      readLength = (int) Math.ceil(readLengthInBits / BITS_COUNT_IN_BYTE_DOUBLE_VALUE);

      if (usingDictionary) {
        VarBinaryVector.Mutator mutator =  valueVec.getMutator();
        Binary currDictValToWrite = null;
        for (int i = 0; i < recordsReadInThisIteration; i++){
          currDictValToWrite = pageReader.dictionaryValueReader.readBytes();
          mutator.setSafe(valuesReadInCurrentPass + i, currDictValToWrite.toByteBuffer().slice(), 0,
              currDictValToWrite.length());
        }
        // Set the write Index. The next page that gets read might be a page that does not use dictionary encoding
        // and we will go into the else condition below. The readField method of the parent class requires the
        // writer index to be set correctly.
        int writerIndex = valueVec.getBuffer().writerIndex();
        valueVec.getBuffer().setIndex(0, writerIndex + (int)readLength);
      } else {
        super.readField(recordsToReadInThisPass);
      }

      // TODO - replace this with fixed binary type in drill
      // now we need to write the lengths of each value
      int byteLength = dataTypeLengthInBits / 8;
      for (int i = 0; i < recordsToReadInThisPass; i++) {
        valueVec.getMutator().setValueLengthSafe(valuesReadInCurrentPass + i, byteLength);
      }
    }
  }

  static class DictionaryTimeReader extends FixedByteAlignedReader<TimeVector> {
    DictionaryTimeReader(ParquetRecordReader parentReader, ColumnDescriptor descriptor,
                        ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, TimeVector v,
                        SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {

      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
        - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      if (usingDictionary) {
        for (int i = 0; i < recordsReadInThisIteration; i++){
          valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readInteger());
        }
      }
    }
  }

  static class DictionaryBigIntReader extends FixedByteAlignedReader<BigIntVector> {
    DictionaryBigIntReader(ParquetRecordReader parentReader, ColumnDescriptor descriptor,
                                   ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, BigIntVector v,
                                   SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {

      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
          - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      if (usingDictionary) {
        BigIntVector.Mutator mutator =  valueVec.getMutator();
        for (int i = 0; i < recordsReadInThisIteration; i++){
          mutator.setSafe(valuesReadInCurrentPass + i,  pageReader.dictionaryValueReader.readLong());
        }
        // Set the write Index. The next page that gets read might be a page that does not use dictionary encoding
        // and we will go into the else condition below. The readField method of the parent class requires the
        // writer index to be set correctly.
        readLengthInBits = recordsReadInThisIteration * dataTypeLengthInBits;
        readLength = (int) Math.ceil(readLengthInBits / BITS_COUNT_IN_BYTE_DOUBLE_VALUE);
        int writerIndex = valueVec.getBuffer().writerIndex();
        valueVec.getBuffer().setIndex(0, writerIndex + (int)readLength);
      } else {
        super.readField(recordsToReadInThisPass);
      }
    }
  }

  /**
   * This class uses for reading unsigned BigInt fields.
   */
  static class DictionaryUInt8Reader extends FixedByteAlignedReader<UInt8Vector> {
    DictionaryUInt8Reader(ParquetRecordReader parentReader, ColumnDescriptor descriptor,
                           ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, UInt8Vector v,
                           SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {

      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
        - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      if (usingDictionary) {
        UInt8Vector.Mutator mutator = valueVec.getMutator();
        for (int i = 0; i < recordsReadInThisIteration; i++) {
          mutator.setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readLong());
        }
        // Set the write Index. The next page that gets read might be a page that does not use dictionary encoding
        // and we will go into the else condition below. The readField method of the parent class requires the
        // writer index to be set correctly.
        readLengthInBits = recordsReadInThisIteration * dataTypeLengthInBits;
        readLength = (int) Math.ceil(readLengthInBits / BITS_COUNT_IN_BYTE_DOUBLE_VALUE);
        int writerIndex = valueVec.getBuffer().writerIndex();
        valueVec.getBuffer().setIndex(0, writerIndex + (int) readLength);
      } else {
        super.readField(recordsToReadInThisPass);
      }
    }
  }

  static class DictionaryVarDecimalReader extends FixedByteAlignedReader<VarDecimalVector> {

    DictionaryVarDecimalReader(ParquetRecordReader parentReader, ColumnDescriptor descriptor,
        ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, VarDecimalVector v,
        SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      recordsReadInThisIteration =
          Math.min(pageReader.currentPageCount - pageReader.valuesRead,
              recordsToReadInThisPass - valuesReadInCurrentPass);

      switch (columnDescriptor.getType()) {
        case INT32:
          if (usingDictionary) {
            for (int i = 0; i < recordsReadInThisIteration; i++) {
              byte[] bytes = Ints.toByteArray(pageReader.dictionaryValueReader.readInteger());
              setValueBytes(i, bytes);
            }
            setWriteIndex();
          } else {
            super.readField(recordsToReadInThisPass);
          }
          break;
        case INT64:
          if (usingDictionary) {
            for (int i = 0; i < recordsReadInThisIteration; i++) {
              byte[] bytes = Longs.toByteArray(pageReader.dictionaryValueReader.readLong());
              setValueBytes(i, bytes);
            }
            setWriteIndex();
          } else {
            super.readField(recordsToReadInThisPass);
          }
          break;
      }
    }

    /**
     * Set the write Index. The next page that gets read might be a page that does not use dictionary encoding
     * and we will go into the else condition below. The readField method of the parent class requires the
     * writer index to be set correctly.
     */
    private void setWriteIndex() {
      readLengthInBits = recordsReadInThisIteration * dataTypeLengthInBits;
      readLength = (int) Math.ceil(readLengthInBits / BITS_COUNT_IN_BYTE_DOUBLE_VALUE);
      int writerIndex = valueVec.getBuffer().writerIndex();
      valueVec.getBuffer().setIndex(0, writerIndex + (int) readLength);
    }

    private void setValueBytes(int i, byte[] bytes) {
      valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, bytes, 0, bytes.length);
    }
  }

  static class DictionaryTimeStampReader extends FixedByteAlignedReader<TimeStampVector> {
    DictionaryTimeStampReader(ParquetRecordReader parentReader, ColumnDescriptor descriptor,
                           ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, TimeStampVector v,
                           SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {

      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
        - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      for (int i = 0; i < recordsReadInThisIteration; i++){
        try {
          valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readLong());
        } catch ( Exception ex) {
          throw ex;
        }
      }
    }
  }

  static class DictionaryBinaryAsTimeStampReader extends FixedByteAlignedReader<TimeStampVector> {
    DictionaryBinaryAsTimeStampReader(ParquetRecordReader parentReader, ColumnDescriptor descriptor,
                              ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, TimeStampVector v,
                              SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {

      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
              - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      for (int i = 0; i < recordsReadInThisIteration; i++){
        try {
          Binary binaryTimeStampValue = pageReader.dictionaryValueReader.readBytes();
          valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, getDateTimeValueFromBinary(binaryTimeStampValue, true));
        } catch ( Exception ex) {
          throw ex;
        }
      }
    }
  }

  static class DictionaryFloat4Reader extends FixedByteAlignedReader<Float4Vector> {
    DictionaryFloat4Reader(ParquetRecordReader parentReader, ColumnDescriptor descriptor,
                                   ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, Float4Vector v,
                                   SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
          - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      for (int i = 0; i < recordsReadInThisIteration; i++){
        valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readFloat());
      }
    }
  }

  static class DictionaryFloat8Reader extends FixedByteAlignedReader<Float8Vector> {
    DictionaryFloat8Reader(ParquetRecordReader parentReader, ColumnDescriptor descriptor,
                                   ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, Float8Vector v,
                                   SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
          - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      for (int i = 0; i < recordsReadInThisIteration; i++){
        valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readDouble());
      }
    }
  }
}
