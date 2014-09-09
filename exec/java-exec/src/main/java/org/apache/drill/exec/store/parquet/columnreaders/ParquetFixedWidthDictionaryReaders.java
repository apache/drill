/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.store.parquet.columnreaders;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.Float4Vector;
import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.IntVector;

import parquet.column.ColumnDescriptor;
import parquet.format.SchemaElement;
import parquet.hadoop.metadata.ColumnChunkMetaData;

public class ParquetFixedWidthDictionaryReaders {

  static class DictionaryIntReader extends FixedByteAlignedReader {

    IntVector castedVector;

    DictionaryIntReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, IntVector v,
                                SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      castedVector = v;
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {

      recordsReadInThisIteration = Math.min(pageReader.currentPage.getValueCount()
          - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      if (usingDictionary) {
        for (int i = 0; i < recordsReadInThisIteration; i++){
          castedVector.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readInteger());
        }
      }
    }
  }

  static class DictionaryBigIntReader extends FixedByteAlignedReader {

    BigIntVector castedVector;

    DictionaryBigIntReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                   ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, BigIntVector v,
                                   SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      castedVector = v;
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {

      recordsReadInThisIteration = Math.min(pageReader.currentPage.getValueCount()
          - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      for (int i = 0; i < recordsReadInThisIteration; i++){
        try {
        castedVector.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readLong());
        } catch ( Exception ex) {
          throw ex;
        }
      }
    }
  }

  static class DictionaryFloat4Reader extends FixedByteAlignedReader {

    Float4Vector castedVector;

    DictionaryFloat4Reader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                   ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, Float4Vector v,
                                   SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      castedVector = v;
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      recordsReadInThisIteration = Math.min(pageReader.currentPage.getValueCount()
          - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      for (int i = 0; i < recordsReadInThisIteration; i++){
        castedVector.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readFloat());
      }
    }
  }

  static class DictionaryFloat8Reader extends FixedByteAlignedReader {

    Float8Vector castedVector;

    DictionaryFloat8Reader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                   ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, Float8Vector v,
                                   SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      castedVector = v;
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      recordsReadInThisIteration = Math.min(pageReader.currentPage.getValueCount()
          - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      for (int i = 0; i < recordsReadInThisIteration; i++){
        castedVector.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readDouble());
      }
    }
  }
}
