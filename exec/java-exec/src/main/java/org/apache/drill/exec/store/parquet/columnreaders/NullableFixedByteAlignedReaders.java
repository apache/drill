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
import java.nio.ByteBuffer;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.expr.holders.NullableDecimal28SparseHolder;
import org.apache.drill.exec.expr.holders.NullableDecimal38SparseHolder;
import org.apache.drill.exec.store.ParquetOutputRecordWriter;
import org.apache.drill.exec.store.parquet.ParquetReaderUtility;
import org.apache.drill.exec.util.DecimalUtility;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableDateVector;
import org.apache.drill.exec.vector.NullableDecimal18Vector;
import org.apache.drill.exec.vector.NullableDecimal28SparseVector;
import org.apache.drill.exec.vector.NullableDecimal38SparseVector;
import org.apache.drill.exec.vector.NullableDecimal9Vector;
import org.apache.drill.exec.vector.NullableFloat4Vector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableIntervalVector;
import org.apache.drill.exec.vector.NullableTimeStampVector;
import org.apache.drill.exec.vector.NullableTimeVector;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.api.Binary;
import org.joda.time.DateTimeUtils;

import io.netty.buffer.DrillBuf;

public class NullableFixedByteAlignedReaders {

  static class NullableFixedByteAlignedReader<V extends ValueVector> extends NullableColumnReader<V> {
    protected DrillBuf bytebuf;

    NullableFixedByteAlignedReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                      ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, V v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      this.bytebuf = pageReader.pageData;

      // fill in data.
      vectorData.writeBytes(bytebuf, (int) readStartInBytes, (int) readLength);
    }
  }

  /**
   * Class for reading the fixed length byte array type in parquet. Currently Drill does not have
   * a fixed length binary type, so this is read into a varbinary with the same size recorded for
   * each value.
   */
  static class NullableFixedBinaryReader extends NullableFixedByteAlignedReader<NullableVarBinaryVector> {
    NullableFixedBinaryReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                   ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableVarBinaryVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    @Override
    protected void readField(long recordsToReadInThisPass) {
      this.bytebuf = pageReader.pageData;
      if (usingDictionary) {
        NullableVarBinaryVector.Mutator mutator =  valueVec.getMutator();
        Binary currDictValToWrite;
        for (int i = 0; i < recordsReadInThisIteration; i++){
          currDictValToWrite = pageReader.dictionaryValueReader.readBytes();
          ByteBuffer buf = currDictValToWrite.toByteBuffer();
          mutator.setSafe(valuesReadInCurrentPass + i, buf, buf.position(),
              currDictValToWrite.length());
        }
        // Set the write Index. The next page that gets read might be a page that does not use dictionary encoding
        // and we will go into the else condition below. The readField method of the parent class requires the
        // writer index to be set correctly.
        int writerIndex = castedBaseVector.getBuffer().writerIndex();
        castedBaseVector.getBuffer().setIndex(0, writerIndex + (int)readLength);
      } else {
        super.readField(recordsToReadInThisPass);
        // TODO - replace this with fixed binary type in drill
        // for now we need to write the lengths of each value
        int byteLength = dataTypeLengthInBits / 8;
        for (int i = 0; i < recordsToReadInThisPass; i++) {
          valueVec.getMutator().setValueLengthSafe(valuesReadInCurrentPass + i, byteLength);
        }
      }
    }
  }

  static class NullableDictionaryIntReader extends NullableColumnReader<NullableIntVector> {

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
      } else {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.valueReader.readInteger());
        }
      }
    }
  }

  static class NullableDictionaryDecimal9Reader extends NullableColumnReader<NullableDecimal9Vector> {

    NullableDictionaryDecimal9Reader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableDecimal9Vector v,
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
      } else {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.valueReader.readInteger());
        }
      }
    }
  }

  static class NullableDictionaryTimeReader extends NullableColumnReader<NullableTimeVector> {

    NullableDictionaryTimeReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                     ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableTimeVector v,
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
      } else {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.valueReader.readInteger());
        }
      }
    }
  }

  static class NullableDictionaryBigIntReader extends NullableColumnReader<NullableBigIntVector> {

    NullableDictionaryBigIntReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                   ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableBigIntVector v,
                                   SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      if (usingDictionary) {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readLong());
        }
      } else {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.valueReader.readLong());
        }
      }
    }
  }

  static class NullableDictionaryTimeStampReader extends NullableColumnReader<NullableTimeStampVector> {

    NullableDictionaryTimeStampReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                   ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableTimeStampVector v,
                                   SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      if (usingDictionary) {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readLong());
        }
      } else {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.valueReader.readLong());
        }
      }
    }
  }
  static class NullableDictionaryDecimal18Reader extends NullableColumnReader<NullableDecimal18Vector> {

    NullableDictionaryDecimal18Reader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                   ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableDecimal18Vector v,
                                   SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      if (usingDictionary) {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readLong());
        }
      } else {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.valueReader.readLong());
        }
      }
    }
  }
  static class NullableDictionaryFloat4Reader extends NullableColumnReader<NullableFloat4Vector> {

    NullableDictionaryFloat4Reader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                   ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableFloat4Vector v,
                                   SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      if (usingDictionary) {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readFloat());
        }
      } else {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.valueReader.readFloat());
        }
      }
    }
  }

  static class NullableDictionaryFloat8Reader extends NullableColumnReader<NullableFloat8Vector> {

    NullableDictionaryFloat8Reader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                  ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableFloat8Vector v,
                                  SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      if (usingDictionary) {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readDouble());
        }
      } else {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.valueReader.readDouble());
        }
      }
    }
  }

  static abstract class NullableConvertedReader<V extends ValueVector> extends NullableFixedByteAlignedReader<V> {

    protected int dataTypeLengthInBytes;

    NullableConvertedReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                            ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, V v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    @Override
    protected void readField(long recordsToReadInThisPass) {

      this.bytebuf = pageReader.pageData;

      dataTypeLengthInBytes = (int) Math.ceil(dataTypeLengthInBits / 8.0);
      for (int i = 0; i < recordsToReadInThisPass; i++) {
        addNext((int) readStartInBytes + i * dataTypeLengthInBytes, i + valuesReadInCurrentPass);
      }
    }

    abstract void addNext(int start, int index);
  }

  public static class NullableDateReader extends NullableConvertedReader<NullableDateVector> {
    NullableDateReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                       boolean fixedLength, NullableDateVector v, SchemaElement schemaElement) throws ExecutionSetupException {
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

  public static class NullableDecimal28Reader extends NullableConvertedReader<NullableDecimal28SparseVector> {
    NullableDecimal28Reader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                            boolean fixedLength, NullableDecimal28SparseVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    @Override
    void addNext(int start, int index) {
      int width = NullableDecimal28SparseHolder.WIDTH;
      BigDecimal intermediate = DecimalUtility.getBigDecimalFromDrillBuf(bytebuf, start, dataTypeLengthInBytes,
          schemaElement.getScale());
      DecimalUtility.getSparseFromBigDecimal(intermediate, valueVec.getBuffer(), index * width, schemaElement.getScale(),
          schemaElement.getPrecision(), NullableDecimal28SparseHolder.nDecimalDigits);
    }
  }

  public static class NullableDecimal38Reader extends NullableConvertedReader<NullableDecimal38SparseVector> {
    NullableDecimal38Reader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                            boolean fixedLength, NullableDecimal38SparseVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    @Override
    void addNext(int start, int index) {
      int width = NullableDecimal38SparseHolder.WIDTH;
      BigDecimal intermediate = DecimalUtility.getBigDecimalFromDrillBuf(bytebuf, start, dataTypeLengthInBytes,
          schemaElement.getScale());
      DecimalUtility.getSparseFromBigDecimal(intermediate, valueVec.getBuffer(), index * width, schemaElement.getScale(),
          schemaElement.getPrecision(), NullableDecimal38SparseHolder.nDecimalDigits);
    }
  }

  public static class NullableIntervalReader extends NullableConvertedReader<NullableIntervalVector> {
    NullableIntervalReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                   boolean fixedLength, NullableIntervalVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    @Override
    void addNext(int start, int index) {
      if (usingDictionary) {
        byte[] input = pageReader.dictionaryValueReader.readBytes().getBytes();
        valueVec.getMutator().setSafe(index * 12, 1,
            ParquetReaderUtility.getIntFromLEBytes(input, 0),
            ParquetReaderUtility.getIntFromLEBytes(input, 4),
            ParquetReaderUtility.getIntFromLEBytes(input, 8));
      }
      valueVec.getMutator().set(index, 1, bytebuf.getInt(start), bytebuf.getInt(start + 4), bytebuf.getInt(start + 8));
    }
  }
}

