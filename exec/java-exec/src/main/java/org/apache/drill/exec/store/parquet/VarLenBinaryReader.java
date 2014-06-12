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

import java.io.IOException;
import java.util.List;

import org.apache.drill.exec.store.parquet.VarLengthColumnReaders.NullableVarLengthColumn;
import org.apache.drill.exec.store.parquet.VarLengthColumnReaders.VarLengthColumn;

import parquet.bytes.BytesUtils;

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
    // write the first 0 offset
    for (ColumnReader columnReader : columns) {
      columnReader.bytesReadInCurrentPass = 0;
      columnReader.valuesReadInCurrentPass = 0;
    }
    // same for the nullable columns
    for (NullableVarLengthColumn columnReader : nullableColumns) {
      columnReader.bytesReadInCurrentPass = 0;
      columnReader.valuesReadInCurrentPass = 0;
      columnReader.nullsRead = 0;
    }
    outer: do {
      lengthVarFieldsInCurrentRecord = 0;
      for (VarLengthColumn columnReader : columns) {
        if (recordsReadInCurrentPass == columnReader.valueVec.getValueCapacity()){
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

        if (columnReader.bytesReadInCurrentPass + columnReader.dataTypeLengthInBits > columnReader.capacity()) {
          break outer;
        }

      }
      for (NullableVarLengthColumn columnReader : nullableColumns) {
        // check to make sure there is capacity for the next value (for nullables this is a check to see if there is
        // still space in the nullability recording vector)
        if (recordsReadInCurrentPass == columnReader.valueVec.getValueCapacity()){
          rowGroupFinished = true;
          break;
        }
        if (columnReader.pageReadStatus.currentPage == null
            || columnReader.pageReadStatus.valuesRead == columnReader.pageReadStatus.currentPage.getValueCount()) {
          if (!columnReader.pageReadStatus.next()) {
            rowGroupFinished = true;
            break;
          } else {
            columnReader.currDictVal = null;
          }
        }
        bytes = columnReader.pageReadStatus.pageDataByteArray;
        // we need to read all of the lengths to determine if this value will fit in the current vector,
        // as we can only read each definition level once, we have to store the last one as we will need it
        // at the start of the next read if we decide after reading all of the varlength values in this record
        // that it will not fit in this batch
        if ( columnReader.currDefLevel == -1 ) {
          columnReader.currDefLevel = columnReader.pageReadStatus.definitionLevels.readInteger();
        }
        if ( columnReader.columnDescriptor.getMaxDefinitionLevel() > columnReader.currDefLevel){
          columnReader.currentValNull = true;
          columnReader.dataTypeLengthInBits = 0;
          columnReader.nullsRead++;
          continue;// field is null, no length to add to data vector
        }

        if (columnReader.usingDictionary) {
          if (columnReader.currDictVal == null) {
            columnReader.currDictVal = columnReader.pageReadStatus.valueReader.readBytes();
          }
          // re-purposing  this field here for length in BYTES to prevent repetitive multiplication/division
          columnReader.dataTypeLengthInBits = columnReader.currDictVal.length();
        }
        else {
          // re-purposing  this field here for length in BYTES to prevent repetitive multiplication/division
          columnReader.dataTypeLengthInBits = BytesUtils.readIntLittleEndian(bytes,
              (int) columnReader.pageReadStatus.readPosInBytes);
        }
        lengthVarFieldsInCurrentRecord += columnReader.dataTypeLengthInBits;

        if (columnReader.bytesReadInCurrentPass + columnReader.dataTypeLengthInBits > columnReader.capacity()) {
          break outer;
        }
      }
      // check that the next record will fit in the batch
      if (rowGroupFinished || (recordsReadInCurrentPass + 1) * parentReader.getBitWidthAllFixedFields() + lengthVarFieldsInCurrentRecord
          > parentReader.getBatchSize()){
        break outer;
      }
      for (VarLengthColumn columnReader : columns) {
        bytes = columnReader.pageReadStatus.pageDataByteArray;
        // again, I am re-purposing the unused field here, it is a length n BYTES, not bits
        boolean success = columnReader.setSafe(columnReader.valuesReadInCurrentPass, bytes,
            (int) columnReader.pageReadStatus.readPosInBytes + 4, columnReader.dataTypeLengthInBits);
        assert success;
        columnReader.pageReadStatus.readPosInBytes += columnReader.dataTypeLengthInBits + 4;
        columnReader.bytesReadInCurrentPass += columnReader.dataTypeLengthInBits + 4;
        columnReader.pageReadStatus.valuesRead++;
        columnReader.valuesReadInCurrentPass++;
      }
      for (NullableVarLengthColumn columnReader : nullableColumns) {
        bytes = columnReader.pageReadStatus.pageDataByteArray;
        // again, I am re-purposing the unused field here, it is a length n BYTES, not bits
        if (!columnReader.currentValNull && columnReader.dataTypeLengthInBits > 0){
          boolean success = columnReader.setSafe(columnReader.valuesReadInCurrentPass, bytes,
                (int) columnReader.pageReadStatus.readPosInBytes + 4, columnReader.dataTypeLengthInBits);
          assert success;
        }
        columnReader.currentValNull = false;
        columnReader.currDefLevel = -1;
        if (columnReader.dataTypeLengthInBits > 0){
          columnReader.pageReadStatus.readPosInBytes += columnReader.dataTypeLengthInBits + 4;
          columnReader.bytesReadInCurrentPass += columnReader.dataTypeLengthInBits + 4;
        }
        columnReader.pageReadStatus.valuesRead++;
        columnReader.valuesReadInCurrentPass++;
        if ( columnReader.pageReadStatus.valuesRead == columnReader.pageReadStatus.currentPage.getValueCount()) {
          columnReader.totalValuesRead += columnReader.pageReadStatus.valuesRead;
          columnReader.pageReadStatus.next();
        }
        columnReader.currDictVal = null;
      }
      recordsReadInCurrentPass++;
    } while (recordsReadInCurrentPass < recordsToReadInThisPass);
    for (VarLengthColumn columnReader : columns) {
      columnReader.valueVec.getMutator().setValueCount((int) recordsReadInCurrentPass);
    }
    for (NullableVarLengthColumn columnReader : nullableColumns) {
      columnReader.valueVec.getMutator().setValueCount((int) recordsReadInCurrentPass);
    }
    return recordsReadInCurrentPass;
  }
}