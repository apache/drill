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
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import parquet.bytes.BytesUtils;
import parquet.column.ColumnDescriptor;
import parquet.hadoop.metadata.ColumnChunkMetaData;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public class VarLenBinaryReader {

  ParquetRecordReader parentReader;
  final List<VarLengthColumn> columns;
  final List<NullableVarLengthColumn> nullableColumns;

  public VarLenBinaryReader(ParquetRecordReader parentReader, List<VarLengthColumn> columns,
                            List<NullableVarLengthColumn> nullableColumns){
    this.parentReader = parentReader;
    this.nullableColumns = nullableColumns;
    this.columns = columns;
  }

  public static class VarLengthColumn extends ColumnReader {

    VarLengthColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, ValueVector v) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v);
    }

    @Override
    protected void readField(long recordsToRead, ColumnReader firstColumnStatus) {
      throw new UnsupportedOperationException();
    }
  }

  public static class NullableVarLengthColumn extends ColumnReader {

    int nullsRead;
    boolean currentValNull = false;

    NullableVarLengthColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, ValueVector v) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v);
    }

    @Override
    protected void readField(long recordsToRead, ColumnReader firstColumnStatus) {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Reads as many variable length values as possible.
   *
   * @param recordsToReadInThisPass - the number of records recommended for reading form the reader
   * @param firstColumnStatus - a reference to the first column status in the parquet file to grab metatdata from
   * @return - the number of fixed length fields that will fit in the batch
   * @throws IOException
   */
  public long readFields(long recordsToReadInThisPass, ColumnReader firstColumnStatus) throws IOException {

    long recordsReadInCurrentPass = 0;
    int lengthVarFieldsInCurrentRecord;
    boolean rowGroupFinished = false;
    byte[] bytes;
    VarBinaryVector currVec;
    NullableVarBinaryVector currNullVec;
    // write the first 0 offset
    for (ColumnReader columnReader : columns) {
      currVec = (VarBinaryVector) columnReader.valueVecHolder.getValueVector();
      currVec.getAccessor().getOffsetVector().getData().writeInt(0);
      columnReader.bytesReadInCurrentPass = 0;
      columnReader.valuesReadInCurrentPass = 0;
    }
    // same for the nullable columns
    for (NullableVarLengthColumn columnReader : nullableColumns) {
      currNullVec = (NullableVarBinaryVector) columnReader.valueVecHolder.getValueVector();
      currNullVec.getMutator().getVectorWithValues().getAccessor().getOffsetVector().getData().writeInt(0);
      columnReader.bytesReadInCurrentPass = 0;
      columnReader.valuesReadInCurrentPass = 0;
      columnReader.nullsRead = 0;
    }
    do {
      lengthVarFieldsInCurrentRecord = 0;
      for (ColumnReader columnReader : columns) {
        if (recordsReadInCurrentPass == columnReader.valueVecHolder.getValueVector().getValueCapacity()){
          rowGroupFinished = true;
          break;
        }
        if (columnReader.pageReadStatus.currentPage == null
            || columnReader.pageReadStatus.valuesRead == columnReader.pageReadStatus.currentPage.getValueCount()) {
          columnReader.totalValuesRead += columnReader.pageReadStatus.valuesRead;
          if (!columnReader.pageReadStatus.next()) {
            rowGroupFinished = true;
            break;
          }
        }
        bytes = columnReader.pageReadStatus.pageDataByteArray;

        // re-purposing this field here for length in BYTES to prevent repetitive multiplication/division
        columnReader.dataTypeLengthInBits = BytesUtils.readIntLittleEndian(bytes,
            (int) columnReader.pageReadStatus.readPosInBytes);
        lengthVarFieldsInCurrentRecord += columnReader.dataTypeLengthInBits;
      }
      for (NullableVarLengthColumn columnReader : nullableColumns) {
        // check to make sure there is capacity for the next value (for nullables this is a check to see if there is
        // still space in the nullability recording vector)
        if (recordsReadInCurrentPass == columnReader.valueVecHolder.getValueVector().getValueCapacity()){
          rowGroupFinished = true;
          break;
        }
        if (columnReader.pageReadStatus.currentPage == null
            || columnReader.pageReadStatus.valuesRead == columnReader.pageReadStatus.currentPage.getValueCount()) {
          columnReader.totalValuesRead += columnReader.pageReadStatus.valuesRead;
          if (!columnReader.pageReadStatus.next()) {
            rowGroupFinished = true;
            break;
          }
        }
        bytes = columnReader.pageReadStatus.pageDataByteArray;
        if ( columnReader.columnDescriptor.getMaxDefinitionLevel() > columnReader.pageReadStatus.definitionLevels.readInteger()){
          columnReader.currentValNull = true;
          columnReader.dataTypeLengthInBits = 0;
          columnReader.nullsRead++;
          continue;// field is null, no length to add to data vector
        }

        // re-purposing  this field here for length in BYTES to prevent repetitive multiplication/division
        columnReader.dataTypeLengthInBits = BytesUtils.readIntLittleEndian(bytes,
            (int) columnReader.pageReadStatus.readPosInBytes);
        lengthVarFieldsInCurrentRecord += columnReader.dataTypeLengthInBits;

      }
      // check that the next record will fit in the batch
      if (rowGroupFinished || (recordsReadInCurrentPass + 1) * parentReader.getBitWidthAllFixedFields() + lengthVarFieldsInCurrentRecord
          > parentReader.getBatchSize()){
        break;
      }
      else{
        recordsReadInCurrentPass++;
      }
      for (ColumnReader columnReader : columns) {
        bytes = columnReader.pageReadStatus.pageDataByteArray;
        currVec = (VarBinaryVector) columnReader.valueVecHolder.getValueVector();
        // again, I am re-purposing the unused field here, it is a length n BYTES, not bits
        currVec.getAccessor().getOffsetVector().getData().writeInt((int) columnReader.bytesReadInCurrentPass  +
            columnReader.dataTypeLengthInBits - 4 * (int) columnReader.valuesReadInCurrentPass);
        currVec.getData().writeBytes(bytes, (int) columnReader.pageReadStatus.readPosInBytes + 4,
            columnReader.dataTypeLengthInBits);
        columnReader.pageReadStatus.readPosInBytes += columnReader.dataTypeLengthInBits + 4;
        columnReader.bytesReadInCurrentPass += columnReader.dataTypeLengthInBits + 4;
        columnReader.pageReadStatus.valuesRead++;
        columnReader.valuesReadInCurrentPass++;
        currVec.getMutator().setValueCount((int)recordsReadInCurrentPass);
      }
      for (NullableVarLengthColumn columnReader : nullableColumns) {
        bytes = columnReader.pageReadStatus.pageDataByteArray;
        currNullVec = (NullableVarBinaryVector) columnReader.valueVecHolder.getValueVector();
        // again, I am re-purposing the unused field here, it is a length n BYTES, not bits
        currNullVec.getMutator().getVectorWithValues().getAccessor().getOffsetVector().getData()
            .writeInt(
                (int) columnReader.bytesReadInCurrentPass  +
                columnReader.dataTypeLengthInBits - 4 * (columnReader.valuesReadInCurrentPass -
                    (columnReader.currentValNull ? Math.max (0, columnReader.nullsRead - 1) : columnReader.nullsRead)));
        columnReader.currentValNull = false;
        if (columnReader.dataTypeLengthInBits > 0){
          currNullVec.getData().writeBytes(bytes, (int) columnReader.pageReadStatus.readPosInBytes + 4,
              columnReader.dataTypeLengthInBits);
          ((NullableVarBinaryVector)columnReader.valueVecHolder.getValueVector()).getMutator().setIndexDefined(columnReader.valuesReadInCurrentPass);
        }
        if (columnReader.dataTypeLengthInBits > 0){
          columnReader.pageReadStatus.readPosInBytes += columnReader.dataTypeLengthInBits + 4;
          columnReader.bytesReadInCurrentPass += columnReader.dataTypeLengthInBits + 4;
        }
        columnReader.pageReadStatus.valuesRead++;
        columnReader.valuesReadInCurrentPass++;
        currNullVec.getMutator().setValueCount((int)recordsReadInCurrentPass);
        // reached the end of a page
        if ( columnReader.pageReadStatus.valuesRead == columnReader.pageReadStatus.currentPage.getValueCount()) {
          columnReader.pageReadStatus.next();
        }
      }
    } while (recordsReadInCurrentPass < recordsToReadInThisPass);
    return recordsReadInCurrentPass;
  }
}