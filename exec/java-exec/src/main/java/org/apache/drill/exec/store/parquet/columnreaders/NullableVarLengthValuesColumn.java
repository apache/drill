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
import org.apache.drill.exec.vector.ValueVector;
import parquet.bytes.BytesUtils;
import parquet.column.ColumnDescriptor;
import parquet.format.SchemaElement;
import parquet.hadoop.metadata.ColumnChunkMetaData;

import java.io.IOException;

public abstract class NullableVarLengthValuesColumn<V extends ValueVector> extends VarLengthValuesColumn<V> {

  int nullsRead;
  boolean currentValNull = false;

  NullableVarLengthValuesColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, V v,
                                SchemaElement schemaElement) throws ExecutionSetupException {
    super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
  }

  public abstract boolean setSafe(int index, byte[] value, int start, int length);

  public abstract int capacity();

  public void reset() {
    bytesReadInCurrentPass = 0;
    valuesReadInCurrentPass = 0;
    nullsRead = 0;
    pageReader.valuesReadyToRead = 0;
  }

  protected void postPageRead() {
    currLengthDeterminingDictVal = null;
    pageReader.valuesReadyToRead = 0;
  }

  protected boolean readAndStoreValueSizeInformation() throws IOException {
    // we need to read all of the lengths to determine if this value will fit in the current vector,
    // as we can only read each definition level once, we have to store the last one as we will need it
    // at the start of the next read if we decide after reading all of the varlength values in this record
    // that it will not fit in this batch
    currentValNull = false;
    if ( currDefLevel == -1 ) {
      currDefLevel = pageReader.definitionLevels.readInteger();
    }
    if ( columnDescriptor.getMaxDefinitionLevel() > currDefLevel){
      nullsRead++;
      // set length of zero, each index in the vector defaults to null so no need to set the nullability
      variableWidthVector.getMutator().setValueLengthSafe(
          valuesReadInCurrentPass + pageReader.valuesReadyToRead, 0);
      currentValNull = true;
      return false;// field is null, no length to add to data vector
    }

    if (usingDictionary) {
      if (currLengthDeterminingDictVal == null) {
        currLengthDeterminingDictVal = pageReader.dictionaryLengthDeterminingReader.readBytes();
      }
      currDictValToWrite = currLengthDeterminingDictVal;
      // re-purposing  this field here for length in BYTES to prevent repetitive multiplication/division
      dataTypeLengthInBits = currLengthDeterminingDictVal.length();
    }
    else {
      // re-purposing  this field here for length in BYTES to prevent repetitive multiplication/division
      dataTypeLengthInBits = BytesUtils.readIntLittleEndian(pageReader.pageDataByteArray,
          (int) pageReader.readyToReadPosInBytes);
    }
    // I think this also needs to happen if it is null for the random access
    if (! variableWidthVector.getMutator().setValueLengthSafe((int) valuesReadInCurrentPass + pageReader.valuesReadyToRead, dataTypeLengthInBits)) {
      return true;
    }
    boolean success = setSafe(valuesReadInCurrentPass + pageReader.valuesReadyToRead, pageReader.pageDataByteArray,
        (int) pageReader.readyToReadPosInBytes + 4, dataTypeLengthInBits);
    assert success;
    return false;
  }

  public void updateReadyToReadPosition() {
    if (! currentValNull){
      pageReader.readyToReadPosInBytes += dataTypeLengthInBits + 4;
    }
    pageReader.valuesReadyToRead++;
    currLengthDeterminingDictVal = null;
  }

  public void updatePosition() {
    if (! currentValNull){
      pageReader.readPosInBytes += dataTypeLengthInBits + 4;
      bytesReadInCurrentPass += dataTypeLengthInBits;
    }
    currentValNull = false;
    valuesReadInCurrentPass++;
  }

  @Override
  protected void readField(long recordsToRead) {
    if (usingDictionary) {
      currDictValToWrite = pageReader.dictionaryValueReader.readBytes();
      // re-purposing  this field here for length in BYTES to prevent repetitive multiplication/division
    }
    dataTypeLengthInBits = variableWidthVector.getAccessor().getValueLength(valuesReadInCurrentPass);
    currentValNull = variableWidthVector.getAccessor().getObject(valuesReadInCurrentPass) == null;
    // again, I am re-purposing the unused field here, it is a length n BYTES, not bits
    if (! currentValNull){
      boolean success = setSafe(valuesReadInCurrentPass, pageReader.pageDataByteArray,
          (int) pageReader.readPosInBytes + 4, dataTypeLengthInBits);
      assert success;
    }
    updatePosition();
  }
}
