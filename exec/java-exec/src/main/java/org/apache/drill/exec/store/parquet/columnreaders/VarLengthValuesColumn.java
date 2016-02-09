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

import io.netty.buffer.DrillBuf;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.vector.ValueVector.Mutator;
import org.apache.drill.exec.vector.Decimal28SparseVector;
import org.apache.drill.exec.vector.Decimal28SparseVector.Accessor;
import org.apache.drill.exec.vector.Decimal38SparseVector;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.api.Binary;

public abstract class VarLengthValuesColumn<V extends ValueVector> extends VarLengthColumn {

  Binary currLengthDeterminingDictVal;
  Binary currDictValToWrite;
  VariableWidthVector variableWidthVector;
  FixedWidthVector fixedWidthVector;

  VarLengthValuesColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                        ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, V v,
                        SchemaElement schemaElement) throws ExecutionSetupException {
    super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    if (valueVec instanceof VariableWidthVector) {
      variableWidthVector = (VariableWidthVector) valueVec;
    }
    else {
      fixedWidthVector = (FixedWidthVector) valueVec;
      dataTypeLengthInBits = this.schemaElement.getPrecision() * 8;
    }
    if (columnChunkMetaData.getEncodings().contains(Encoding.PLAIN_DICTIONARY)) {
      usingDictionary = true;
    }
    else {
      usingDictionary = false;
    }
  }

  @Override
  public void reset() {
    super.reset();
    decimalLengths.clear();
  }

  public abstract boolean setSafe(int index, DrillBuf bytes, int start, int length);

  public abstract boolean setSafe(int index, BigDecimal intermediate);

  protected void setDataTypeLength() {
    if (variableWidthVector == null) {
      dataTypeLengthInBits = decimalLengths.get(valuesReadInCurrentPass);
    } else {
      dataTypeLengthInBits = variableWidthVector.getAccessor().getValueLength(valuesReadInCurrentPass);
    }
  }

  @Override
  protected void readField(long recordToRead) {
    setDataTypeLength();

    // again, I am re-purposing the unused field here, it is a length n BYTES, not bits
    boolean success = setSafe((int) valuesReadInCurrentPass, pageReader.pageData,
        (int) pageReader.readPosInBytes + 4, dataTypeLengthInBits);
    assert success;
    updatePosition();
  }

  @Override
  public void updateReadyToReadPosition() {
    pageReader.readyToReadPosInBytes += dataTypeLengthInBits + 4;
    pageReader.valuesReadyToRead++;
    currLengthDeterminingDictVal = null;
  }

  @Override
  public void updatePosition() {
    pageReader.readPosInBytes += dataTypeLengthInBits + 4;
    bytesReadInCurrentPass += dataTypeLengthInBits;
    valuesReadInCurrentPass++;
  }

  @Override
  public boolean skipReadyToReadPositionUpdate() {
    return false;
  }

  // decimalLengths list is part of a near-term fix for DRILL-4184.
  // Decimal[23]8SparseVector classes are fixed width vectors, without ability to "remember" offsets of
  // (variable width) field sizes.  so, we "remember" the array sizes in decimalLengths (also used to
  // "remember" whether a value was null, for nullable decimal columns).
  // TODO: storage of decimal values should support variable length values in a much cleaner way than this,
  // perhaps with a new variable width Decimal vector class.
  protected ArrayList<Integer> decimalLengths = new ArrayList();

  @Override
  protected boolean readAndStoreValueSizeInformation() throws IOException {
    // re-purposing this field here for length in BYTES to prevent repetitive multiplication/division
    if (usingDictionary) {
      if (currLengthDeterminingDictVal == null) {
        currLengthDeterminingDictVal = pageReader.dictionaryLengthDeterminingReader.readBytes();
      }
      currDictValToWrite = currLengthDeterminingDictVal;
      // re-purposing  this field here for length in BYTES to prevent repetitive multiplication/division
      dataTypeLengthInBits = currLengthDeterminingDictVal.length();
    } else {
      // re-purposing  this field here for length in BYTES to prevent repetitive multiplication/division
      dataTypeLengthInBits = pageReader.pageData.getInt((int) pageReader.readyToReadPosInBytes);
    }
    if (variableWidthVector == null) {
      decimalLengths.add(dataTypeLengthInBits);  // store length in BYTES for variable length decimal field
    }
    else {
      // this should not fail
      variableWidthVector.getMutator().setValueLengthSafe((int) valuesReadInCurrentPass + pageReader.valuesReadyToRead,
        dataTypeLengthInBits);
    }
    return false;
  }

}
