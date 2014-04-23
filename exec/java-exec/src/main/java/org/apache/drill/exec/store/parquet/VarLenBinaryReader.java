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
import org.apache.drill.exec.vector.*;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.drill.exec.vector.VarCharVector;
import parquet.bytes.BytesUtils;
import parquet.column.ColumnDescriptor;
import parquet.format.ConvertedType;
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

  public static abstract class VarLengthColumn<V extends ValueVector> extends ColumnReader {

    VarLengthColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                    ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, V v,
                    ConvertedType convertedType) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, convertedType);
    }

    @Override
    protected void readField(long recordsToRead, ColumnReader firstColumnStatus) {
      throw new UnsupportedOperationException();
    }

    public abstract boolean setSafe(int index, byte[] bytes, int start, int length);

    public abstract int capacity();

  }

  public static abstract class NullableVarLengthColumn<V extends ValueVector> extends ColumnReader {

    int nullsRead;
    boolean currentValNull = false;

    NullableVarLengthColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                            ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, V v,
                            ConvertedType convertedType ) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, convertedType);
    }

    public abstract boolean setSafe(int index, byte[] value, int start, int length);

    public abstract int capacity();

    @Override
    protected void readField(long recordsToRead, ColumnReader firstColumnStatus) {
      throw new UnsupportedOperationException();
    }
  }

  public static class VarCharColumn extends VarLengthColumn <VarCharVector> {

    // store a hard reference to the vector (which is also stored in the superclass) to prevent repetitive casting
    protected VarCharVector varCharVector;

    VarCharColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                    ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, VarCharVector v,
                    ConvertedType convertedType) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, convertedType);
      varCharVector = v;
    }

    @Override
    protected void readField(long recordsToRead, ColumnReader firstColumnStatus) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean setSafe(int index, byte[] bytes, int start, int length) {
      return varCharVector.getMutator().setSafe(valuesReadInCurrentPass, bytes,
          (int) (pageReadStatus.readPosInBytes + 4), dataTypeLengthInBits);
    }

    @Override
    public int capacity() {
      return varCharVector.getData().capacity();
    }
  }

  public static class NullableVarCharColumn extends NullableVarLengthColumn <NullableVarCharVector> {

    int nullsRead;
    boolean currentValNull = false;
    // store a hard reference to the vector (which is also stored in the superclass) to prevent repetitive casting
    protected NullableVarCharVector nullableVarCharVector;

    NullableVarCharColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                            ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableVarCharVector v,
                            ConvertedType convertedType ) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, convertedType);
      nullableVarCharVector = v;
    }

    public boolean setSafe(int index, byte[] value, int start, int length) {
      return nullableVarCharVector.getMutator().setSafe(index, value,
          start, length);
    }

    @Override
    public int capacity() {
      return nullableVarCharVector.getData().capacity();
    }

    @Override
    protected void readField(long recordsToRead, ColumnReader firstColumnStatus) {
      throw new UnsupportedOperationException();
    }
  }

  public static class VarBinaryColumn extends VarLengthColumn <VarBinaryVector> {

    // store a hard reference to the vector (which is also stored in the superclass) to prevent repetitive casting
    protected VarBinaryVector varBinaryVector;

    VarBinaryColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                  ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, VarBinaryVector v,
                  ConvertedType convertedType) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, convertedType);
      varBinaryVector = v;
    }

    @Override
    protected void readField(long recordsToRead, ColumnReader firstColumnStatus) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean setSafe(int index, byte[] bytes, int start, int length) {
      return varBinaryVector.getMutator().setSafe(valuesReadInCurrentPass, bytes,
          (int) (pageReadStatus.readPosInBytes + 4), dataTypeLengthInBits);
    }

    @Override
    public int capacity() {
      return varBinaryVector.getData().capacity();
    }
  }

  public static class NullableVarBinaryColumn extends NullableVarLengthColumn <NullableVarBinaryVector> {

    int nullsRead;
    boolean currentValNull = false;
    // store a hard reference to the vector (which is also stored in the superclass) to prevent repetitive casting
    protected NullableVarBinaryVector nullableVarBinaryVector;

    NullableVarBinaryColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                          ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableVarBinaryVector v,
                          ConvertedType convertedType ) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, convertedType);
      nullableVarBinaryVector = v;
    }

    public boolean setSafe(int index, byte[] value, int start, int length) {
      return nullableVarBinaryVector.getMutator().setSafe(index, value,
          start, length);
    }

    @Override
    public int capacity() {
      return nullableVarBinaryVector.getData().capacity();
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
        if (columnReader.dataTypeLengthInBits > 0){
          columnReader.pageReadStatus.readPosInBytes += columnReader.dataTypeLengthInBits + 4;
          columnReader.bytesReadInCurrentPass += columnReader.dataTypeLengthInBits + 4;
        }
        columnReader.pageReadStatus.valuesRead++;
        columnReader.valuesReadInCurrentPass++;
        if ( columnReader.pageReadStatus.valuesRead == columnReader.pageReadStatus.currentPage.getValueCount()) {
          columnReader.pageReadStatus.next();
        }
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