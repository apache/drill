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
package org.apache.drill.exec.store.parquet.columnreaders;

import io.netty.buffer.ByteBuf;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.ValueVector;

import parquet.column.ColumnDescriptor;
import parquet.format.SchemaElement;
import parquet.hadoop.metadata.ColumnChunkMetaData;

final class BitReader extends ColumnReader {

  private byte currentByte;
  private byte nextByte;
  private ByteBuf bytebuf;

  BitReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
            boolean fixedLength, ValueVector v, SchemaElement schemaElement) throws ExecutionSetupException {
    super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
  }

  @Override
  protected void readField(long recordsToReadInThisPass) {

    recordsReadInThisIteration = Math.min(pageReader.currentPage.getValueCount()
        - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

    readStartInBytes = pageReader.readPosInBytes;
    readLengthInBits = recordsReadInThisIteration * dataTypeLengthInBits;
    readLength = (int) Math.ceil(readLengthInBits / 8.0);

    bytebuf = pageReader.pageDataByteArray;
    // standard read, using memory mapping
    if (pageReader.bitShift == 0) {
      ((BaseDataValueVector) valueVec).getData().writeBytes(bytebuf,
          (int) readStartInBytes, (int) readLength);
    } else { // read in individual values, because a bitshift is necessary with where the last page or batch ended

      vectorData = ((BaseDataValueVector) valueVec).getData();
      nextByte = bytebuf.getByte((int) Math.max(0, Math.ceil(pageReader.valuesRead / 8.0) - 1));
      readLengthInBits = recordsReadInThisIteration + pageReader.bitShift;

      int i = 0;
      // read individual bytes with appropriate shifting
      for (; i < (int) readLength; i++) {
        currentByte = nextByte;
        currentByte = (byte) (currentByte >>> pageReader.bitShift);
        // mask the bits about to be added from the next byte
        currentByte = (byte) (currentByte & ParquetRecordReader.startBitMasks[pageReader.bitShift - 1]);
        // if we are not on the last byte
        if ((int) Math.ceil(pageReader.valuesRead / 8.0) + i < pageReader.byteLength) {
          // grab the next byte from the buffer, shift and mask it, and OR it with the leftover bits
          nextByte = bytebuf.getByte((int) Math.ceil(pageReader.valuesRead / 8.0) + i);
          currentByte = (byte) (currentByte | nextByte
              << (8 - pageReader.bitShift)
              & ParquetRecordReader.endBitMasks[8 - pageReader.bitShift - 1]);
        }
        vectorData.setByte(valuesReadInCurrentPass / 8 + i, currentByte);
      }
      vectorData.setIndex(0, (valuesReadInCurrentPass / 8)
          + (int) readLength - 1);
      vectorData.capacity(vectorData.writerIndex() + 1);
    }

    // check if the values in this page did not end on a byte boundary, store a number of bits the next page must be
    // shifted by to read all of the values into the vector without leaving space
    if (readLengthInBits % 8 != 0) {
      pageReader.bitShift = (int) readLengthInBits % 8;
    } else {
      pageReader.bitShift = 0;
    }
  }
}
