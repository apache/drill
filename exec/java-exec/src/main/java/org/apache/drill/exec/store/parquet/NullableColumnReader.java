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
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.NullableVectorDefinitionSetter;
import org.apache.drill.exec.vector.ValueVector;
import parquet.column.ColumnDescriptor;
import parquet.hadoop.metadata.ColumnChunkMetaData;

import java.io.IOException;

abstract class NullableColumnReader extends ColumnReader{

  int nullsFound;
  // used to skip nulls found
  int rightBitShift;
  // used when copying less than a byte worth of data at a time, to indicate the number of used bits in the current byte
  int bitsUsed;

  NullableColumnReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
               boolean fixedLength, ValueVector v) throws ExecutionSetupException {
    super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v);
  }

  public void readAllFixedFields(long recordsToReadInThisPass, ColumnReader firstColumnStatus) throws IOException {
    readStartInBytes = 0;
    readLength = 0;
    readLengthInBits = 0;
    recordsReadInThisIteration = 0;
    vectorData = ((BaseValueVector)valueVecHolder.getValueVector()).getData();

    do {
      // if no page has been read, or all of the records have been read out of a page, read the next one
      if (pageReadStatus.currentPage == null
          || pageReadStatus.valuesRead == pageReadStatus.currentPage.getValueCount()) {
        if (!pageReadStatus.next()) {
          break;
        }
      }

      // values need to be spaced out where nulls appear in the column
      // leaving blank space for nulls allows for random access to values
      // to optimize copying data out of the buffered disk stream, runs of defined values
      // are located and copied together, rather than copying individual values

      long runStart = pageReadStatus.readPosInBytes;
      int runLength = 0;
      int currentDefinitionLevel = 0;
      int currentValueIndexInVector = (int) recordsReadInThisIteration;
      boolean lastValueWasNull = true;
      int definitionLevelsRead;
      // loop to find the longest run of defined values available, can be preceded by several nulls
      while (true){
        definitionLevelsRead = 0;
        lastValueWasNull = true;
        nullsFound = 0;
        if (currentValueIndexInVector - totalValuesRead == recordsToReadInThisPass
            || currentValueIndexInVector >= valueVecHolder.getValueVector().getValueCapacity()){
          break;
        }
        while(currentValueIndexInVector - totalValuesRead < recordsToReadInThisPass
            && currentValueIndexInVector < valueVecHolder.getValueVector().getValueCapacity()
            && pageReadStatus.valuesRead + definitionLevelsRead < pageReadStatus.currentPage.getValueCount()){
          currentDefinitionLevel = pageReadStatus.definitionLevels.readInteger();
          definitionLevelsRead++;
          if ( currentDefinitionLevel < columnDescriptor.getMaxDefinitionLevel()){
            // a run of non-null values was found, break out of this loop to do a read in the outer loop
            nullsFound++;
            if ( ! lastValueWasNull ){
              currentValueIndexInVector++;
              break;
            }
            lastValueWasNull = true;
          }
          else{
            if (lastValueWasNull){
              runStart = pageReadStatus.readPosInBytes;
              runLength = 0;
              lastValueWasNull = false;
            }
            runLength++;
            ((NullableVectorDefinitionSetter)valueVecHolder.getValueVector().getMutator()).setIndexDefined(currentValueIndexInVector);
          }
          currentValueIndexInVector++;
        }
        pageReadStatus.readPosInBytes = runStart;
        recordsReadInThisIteration = runLength;

        readField( runLength, firstColumnStatus);
        int writerIndex = ((BaseValueVector) valueVecHolder.getValueVector()).getData().writerIndex();
        if ( dataTypeLengthInBits > 8  || (dataTypeLengthInBits < 8 && totalValuesRead + runLength % 8 == 0)){
          ((BaseValueVector) valueVecHolder.getValueVector()).getData().setIndex(0, writerIndex + (int) Math.ceil( nullsFound * dataTypeLengthInBits / 8.0));
        }
        else if (dataTypeLengthInBits < 8){
          rightBitShift += dataTypeLengthInBits * nullsFound;
        }
        recordsReadInThisIteration += nullsFound;
        valuesReadInCurrentPass += recordsReadInThisIteration;
        totalValuesRead += recordsReadInThisIteration;
        pageReadStatus.valuesRead += recordsReadInThisIteration;
        if (readStartInBytes + readLength >= pageReadStatus.byteLength && bitsUsed == 0) {
          pageReadStatus.next();
        } else {
          pageReadStatus.readPosInBytes = readStartInBytes + readLength;
        }
      }
    }
    while (valuesReadInCurrentPass < recordsToReadInThisPass && pageReadStatus.currentPage != null);
    valueVecHolder.getValueVector().getMutator().setValueCount(
        valuesReadInCurrentPass);
  }

  protected abstract void readField(long recordsToRead, ColumnReader firstColumnStatus);
}