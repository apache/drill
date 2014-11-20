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
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.Decimal28SparseVector;
import org.apache.drill.exec.vector.Decimal38SparseVector;
import org.apache.drill.exec.vector.Float4Vector;
import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableDecimal28SparseVector;
import org.apache.drill.exec.vector.NullableDecimal38SparseVector;
import org.apache.drill.exec.vector.NullableFloat4Vector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.VariableWidthVector;

import parquet.column.ColumnDescriptor;
import parquet.column.Encoding;
import parquet.format.ConvertedType;
import parquet.format.SchemaElement;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.schema.PrimitiveType;

public class ColumnReaderFactory {

  /**
   * @param fixedLength
   * @param descriptor
   * @param columnChunkMetaData
   * @param allocateSize - the size of the vector to create
   * @return
   * @throws SchemaChangeException
   */
  static ColumnReader createFixedColumnReader(ParquetRecordReader recordReader, boolean fixedLength, ColumnDescriptor descriptor,
                                               ColumnChunkMetaData columnChunkMetaData, int allocateSize, ValueVector v,
                                               SchemaElement schemaElement)
      throws Exception {
    ConvertedType convertedType = schemaElement.getConverted_type();
    // if the column is required, or repeated (in which case we just want to use this to generate our appropriate
    // ColumnReader for actually transferring data into the data vector inside of our repeated vector
    if (descriptor.getMaxDefinitionLevel() == 0 || descriptor.getMaxRepetitionLevel() > 0){
      if (columnChunkMetaData.getType() == PrimitiveType.PrimitiveTypeName.BOOLEAN){
        return new BitReader(recordReader, allocateSize, descriptor, columnChunkMetaData,
            fixedLength, v, schemaElement);
      } else if (columnChunkMetaData.getType() == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY ) {
        if (convertedType == ConvertedType.DECIMAL){
          int length = schemaElement.type_length;
          if (length <= 12) {
            return new FixedByteAlignedReader.Decimal28Reader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
          } else if (length <= 16) {
            return new FixedByteAlignedReader.Decimal38Reader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
          }
        } else {
          return new FixedByteAlignedReader.FixedBinaryReader(recordReader, allocateSize, descriptor, columnChunkMetaData, (VariableWidthVector) v, schemaElement);
        }
      } else if (columnChunkMetaData.getType() == PrimitiveType.PrimitiveTypeName.INT32 && convertedType == ConvertedType.DATE){
        return new FixedByteAlignedReader.DateReader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      } else{
        if (columnChunkMetaData.getEncodings().contains(Encoding.PLAIN_DICTIONARY)) {
          switch (columnChunkMetaData.getType()) {
            case INT32:
              return new ParquetFixedWidthDictionaryReaders.DictionaryIntReader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (IntVector) v, schemaElement);
            case INT64:
              return new ParquetFixedWidthDictionaryReaders.DictionaryBigIntReader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (BigIntVector) v, schemaElement);
            case FLOAT:
              return new ParquetFixedWidthDictionaryReaders.DictionaryFloat4Reader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (Float4Vector) v, schemaElement);
            case DOUBLE:
              return new ParquetFixedWidthDictionaryReaders.DictionaryFloat8Reader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (Float8Vector) v, schemaElement);
            default:
              throw new ExecutionSetupException("Unsupported dictionary column type " + descriptor.getType().name() );
          }

        } else {
          return new FixedByteAlignedReader(recordReader, allocateSize, descriptor, columnChunkMetaData,
              fixedLength, v, schemaElement);
        }
      }
    }
    else { // if the column is nullable
      if (columnChunkMetaData.getType() == PrimitiveType.PrimitiveTypeName.BOOLEAN){
        return new NullableBitReader(recordReader, allocateSize, descriptor, columnChunkMetaData,
            fixedLength, v, schemaElement);
      } else if (columnChunkMetaData.getType() == PrimitiveType.PrimitiveTypeName.INT32 && convertedType == ConvertedType.DATE){
        return new NullableFixedByteAlignedReaders.NullableDateReader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      } else if (columnChunkMetaData.getType() == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY && convertedType == ConvertedType.DECIMAL){
        int length = schemaElement.type_length;
        if (length <= 12) {
          return new NullableFixedByteAlignedReaders.NullableDecimal28Reader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
        } else if (length <= 16) {
          return new NullableFixedByteAlignedReaders.NullableDecimal38Reader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
        }
      } else {
        return getNullableColumnReader(recordReader, allocateSize, descriptor,
            columnChunkMetaData, fixedLength, v, schemaElement);
      }
    }
    throw new Exception("Unexpected parquet metadata configuration.");
  }

  static VarLengthValuesColumn getReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                          ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, ValueVector v,
                                          SchemaElement schemaElement
  ) throws ExecutionSetupException {
    ConvertedType convertedType = schemaElement.getConverted_type();
    switch (descriptor.getMaxDefinitionLevel()) {
      case 0:
        if (convertedType == null) {
          return new VarLengthColumnReaders.VarBinaryColumn(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (VarBinaryVector) v, schemaElement);
        }
        switch (convertedType) {
          case UTF8:
            return new VarLengthColumnReaders.VarCharColumn(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (VarCharVector) v, schemaElement);
          case DECIMAL:
            if (v instanceof Decimal28SparseVector) {
              return new VarLengthColumnReaders.Decimal28Column(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (Decimal28SparseVector) v, schemaElement);
            } else if (v instanceof Decimal38SparseVector) {
              return new VarLengthColumnReaders.Decimal38Column(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (Decimal38SparseVector) v, schemaElement);
            }
          default:
            return new VarLengthColumnReaders.VarBinaryColumn(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (VarBinaryVector) v, schemaElement);
        }
      default:
        if (convertedType == null) {
          return new VarLengthColumnReaders.NullableVarBinaryColumn(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableVarBinaryVector) v, schemaElement);
        }

        switch (convertedType) {
          case UTF8:
            return new VarLengthColumnReaders.NullableVarCharColumn(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableVarCharVector) v, schemaElement);
          case DECIMAL:
            if (v instanceof NullableDecimal28SparseVector) {
              return new VarLengthColumnReaders.NullableDecimal28Column(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableDecimal28SparseVector) v, schemaElement);
            } else if (v instanceof NullableDecimal38SparseVector) {
              return new VarLengthColumnReaders.NullableDecimal38Column(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableDecimal38SparseVector) v, schemaElement);
            }
          default:
            return new VarLengthColumnReaders.NullableVarBinaryColumn(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableVarBinaryVector) v, schemaElement);
        }
    }
  }

  public static NullableColumnReader getNullableColumnReader(ParquetRecordReader parentReader, int allocateSize,
                                                             ColumnDescriptor columnDescriptor,
                                                             ColumnChunkMetaData columnChunkMetaData,
                                                             boolean fixedLength,
                                                             ValueVector valueVec,
                                                             SchemaElement schemaElement) throws ExecutionSetupException {
    if (! columnChunkMetaData.getEncodings().contains(Encoding.PLAIN_DICTIONARY)) {
      return new NullableFixedByteAlignedReaders.NullableFixedByteAlignedReader(parentReader, allocateSize, columnDescriptor, columnChunkMetaData,
          fixedLength, valueVec, schemaElement);
    } else {
      if (columnDescriptor.getType() == PrimitiveType.PrimitiveTypeName.INT64) {
        return new NullableFixedByteAlignedReaders.NullableDictionaryBigIntReader(parentReader, allocateSize, columnDescriptor, columnChunkMetaData,
            fixedLength, (NullableBigIntVector)valueVec, schemaElement);
      }
      else if (columnDescriptor.getType() == PrimitiveType.PrimitiveTypeName.INT32) {
        return new NullableFixedByteAlignedReaders.NullableDictionaryIntReader(parentReader, allocateSize, columnDescriptor, columnChunkMetaData,
            fixedLength, (NullableIntVector)valueVec, schemaElement);
      }
      else if (columnDescriptor.getType() == PrimitiveType.PrimitiveTypeName.FLOAT) {
        return new NullableFixedByteAlignedReaders.NullableDictionaryFloat4Reader(parentReader, allocateSize, columnDescriptor, columnChunkMetaData,
            fixedLength, (NullableFloat4Vector)valueVec, schemaElement);
      }
      else if (columnDescriptor.getType() == PrimitiveType.PrimitiveTypeName.DOUBLE) {
        return new NullableFixedByteAlignedReaders.NullableDictionaryFloat8Reader(parentReader, allocateSize, columnDescriptor, columnChunkMetaData,
            fixedLength, (NullableFloat8Vector)valueVec, schemaElement);
      }
      else{
        throw new ExecutionSetupException("Unsupported nullable column type " + columnDescriptor.getType().name() );
      }
    }
  }
}
