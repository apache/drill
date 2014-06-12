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
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableFloat4Vector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.ValueVector;

import parquet.column.ColumnDescriptor;
import parquet.column.Encoding;
import parquet.format.SchemaElement;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.schema.PrimitiveType;

class NullableFixedByteAlignedReaders {

  public static NullableColumnReader getNullableColumnReader(ParquetRecordReader parentReader, int allocateSize,
                                                      ColumnDescriptor columnDescriptor,
                                                      ColumnChunkMetaData columnChunkMetaData,
                                                      boolean fixedLength,
                                                      ValueVector valueVec,
                                                      SchemaElement schemaElement) throws ExecutionSetupException {
    if (! columnChunkMetaData.getEncodings().contains(Encoding.PLAIN_DICTIONARY)) {
      return new NullableFixedByteAlignedReader(parentReader, allocateSize, columnDescriptor, columnChunkMetaData,
              fixedLength, valueVec, schemaElement);
    } else {
      if (columnDescriptor.getType() == PrimitiveType.PrimitiveTypeName.INT64) {
        return new NullableDictionaryBigIntReader(parentReader, allocateSize, columnDescriptor, columnChunkMetaData,
              fixedLength, (NullableBigIntVector)valueVec, schemaElement);
      }
      else if (columnDescriptor.getType() == PrimitiveType.PrimitiveTypeName.INT32) {
        return new NullableDicationaryIntReader(parentReader, allocateSize, columnDescriptor, columnChunkMetaData,
            fixedLength, (NullableIntVector)valueVec, schemaElement);
      }
      else if (columnDescriptor.getType() == PrimitiveType.PrimitiveTypeName.FLOAT) {
        return new NullableDictionaryFloat4Reader(parentReader, allocateSize, columnDescriptor, columnChunkMetaData,
            fixedLength, (NullableFloat4Vector)valueVec, schemaElement);
      }
      else if (columnDescriptor.getType() == PrimitiveType.PrimitiveTypeName.DOUBLE) {
        return new NullableDictionaryFloat8Reader(parentReader, allocateSize, columnDescriptor, columnChunkMetaData,
            fixedLength, (NullableFloat8Vector)valueVec, schemaElement);
      }
      else{
        throw new ExecutionSetupException("Unsupported nullable column type " + columnDescriptor.getType().name() );
      }
    }
  }

  private static class NullableFixedByteAlignedReader extends NullableColumnReader {
    private byte[] bytes;

    NullableFixedByteAlignedReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                      ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, ValueVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass, ColumnReader firstColumnStatus) {
      this.recordsReadInThisIteration = recordsToReadInThisPass;

      // set up metadata
      this.readStartInBytes = pageReadStatus.readPosInBytes;
      this.readLengthInBits = recordsReadInThisIteration * dataTypeLengthInBits;
      this.readLength = (int) Math.ceil(readLengthInBits / 8.0);
      this.bytes = pageReadStatus.pageDataByteArray;

      // fill in data.
      vectorData.writeBytes(bytes, (int) readStartInBytes, (int) readLength);
    }
  }

  private static class NullableDicationaryIntReader extends NullableColumnReader<NullableIntVector> {

    private byte[] bytes;

    NullableDicationaryIntReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                 ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableIntVector v,
                                 SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass, ColumnReader firstColumnStatus) {
      if (usingDictionary) {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReadStatus.valueReader.readInteger());
        }
      }
    }
  }

  private static class NullableDictionaryBigIntReader extends NullableColumnReader<NullableBigIntVector> {

    private byte[] bytes;

    NullableDictionaryBigIntReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                   ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableBigIntVector v,
                                   SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass, ColumnReader firstColumnStatus) {
      for (int i = 0; i < recordsToReadInThisPass; i++){
        valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReadStatus.valueReader.readLong());
      }
    }
  }

  private static class NullableDictionaryFloat4Reader extends NullableColumnReader<NullableFloat4Vector> {

    private byte[] bytes;

    NullableDictionaryFloat4Reader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                   ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableFloat4Vector v,
                                   SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass, ColumnReader firstColumnStatus) {
      for (int i = 0; i < recordsToReadInThisPass; i++){
        valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReadStatus.valueReader.readFloat());
      }
    }
  }

  private static class NullableDictionaryFloat8Reader extends NullableColumnReader<NullableFloat8Vector> {

    private byte[] bytes;

    NullableDictionaryFloat8Reader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                  ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableFloat8Vector v,
                                  SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass, ColumnReader firstColumnStatus) {
      for (int i = 0; i < recordsToReadInThisPass; i++){
        valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReadStatus.valueReader.readDouble());
      }
    }
  }

}