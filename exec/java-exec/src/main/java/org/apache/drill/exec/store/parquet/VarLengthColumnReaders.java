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
package org.apache.drill.exec.store.parquet;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.store.parquet.ColumnReader;
import org.apache.drill.exec.store.parquet.ParquetRecordReader;
import org.apache.drill.exec.vector.*;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.drill.exec.vector.VarCharVector;
import parquet.column.ColumnDescriptor;
import parquet.format.ConvertedType;
import parquet.column.Encoding;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.io.api.Binary;

public class VarLengthColumnReaders {

  public static abstract class VarLengthColumn<V extends ValueVector> extends ColumnReader {

    boolean usingDictionary;
    Binary currDictVal;

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
    boolean usingDictionary;
    Binary currDictVal;

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
      if (columnChunkMetaData.getEncodings().contains(Encoding.PLAIN_DICTIONARY)) {
        usingDictionary = true;
      }
      else {
        usingDictionary = false;
      }
      varCharVector = v;
    }

    @Override
    protected void readField(long recordsToRead, ColumnReader firstColumnStatus) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean setSafe(int index, byte[] bytes, int start, int length) {
      boolean success;
      if (usingDictionary) {
        success = varCharVector.getMutator().setSafe(valuesReadInCurrentPass, currDictVal.getBytes(),
            0, currDictVal.length());
      }
      else {
        success = varCharVector.getMutator().setSafe(index, bytes, start, length);
      }
      return success;
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
      if (columnChunkMetaData.getEncodings().contains(Encoding.PLAIN_DICTIONARY)) {
          usingDictionary = true;
      }
      else {
        usingDictionary = false;
      }
    }

    public boolean setSafe(int index, byte[] value, int start, int length) {
      boolean success;
      if (usingDictionary) {
        success = nullableVarCharVector.getMutator().setSafe(valuesReadInCurrentPass, currDictVal.getBytes(),
            0, currDictVal.length());
      }
      else {
        success = nullableVarCharVector.getMutator().setSafe(index, value, start, length);
      }
      return success;
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
      if (columnChunkMetaData.getEncodings().contains(Encoding.PLAIN_DICTIONARY)) {
        usingDictionary = true;
      }
      else {
        usingDictionary = false;
      }
      varBinaryVector = v;
    }

    @Override
    protected void readField(long recordsToRead, ColumnReader firstColumnStatus) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean setSafe(int index, byte[] bytes, int start, int length) {
      boolean success;
      if (usingDictionary) {
        success = varBinaryVector.getMutator().setSafe(valuesReadInCurrentPass, currDictVal.getBytes(),
            0, currDictVal.length());
      }
      else {
        success = varBinaryVector.getMutator().setSafe(index, bytes, start, length);
      }
      return success;
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
    protected org.apache.drill.exec.vector.NullableVarBinaryVector nullableVarBinaryVector;

    NullableVarBinaryColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                            ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableVarBinaryVector v,
                            ConvertedType convertedType ) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, convertedType);
      nullableVarBinaryVector = v;
      if (columnChunkMetaData.getEncodings().contains(Encoding.PLAIN_DICTIONARY)) {
        usingDictionary = true;
      }
      else {
        usingDictionary = false;
      }
    }

    public boolean setSafe(int index, byte[] value, int start, int length) {
      boolean success;
      if (usingDictionary) {
        success = nullableVarBinaryVector.getMutator().setSafe(valuesReadInCurrentPass, currDictVal.getBytes(),
            0, currDictVal.length());
      }
      else {
        success = nullableVarBinaryVector.getMutator().setSafe(index, value, start, length);
      }
      return  success;
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
}
