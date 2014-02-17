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
import org.apache.drill.exec.vector.ValueVector;

import parquet.column.ColumnDescriptor;
import parquet.hadoop.metadata.ColumnChunkMetaData;

class NullableFixedByteAlignedReader extends NullableColumnReader {

  private byte[] bytes;

  NullableFixedByteAlignedReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
      ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, ValueVector v) throws ExecutionSetupException {
    super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v);
  }

  // this method is called by its superclass during a read loop
  @Override
  protected void readField(long recordsToReadInThisPass, ColumnReader firstColumnStatus) {

    this.recordsReadInThisIteration = recordsToReadInThisPass;

    // set up metadata
    this.readStartInBytes = pageReadStatus.readPosInBytes;
    this.readLengthInBits = recordsReadInThisIteration * dataTypeLengthInBits;
    this.readLength = (int) Math.ceil(readLengthInBits / 8.0);
    this.bytes = pageReadStatus.pageDataByteArray;
    
    // fill in data.
    vectorData.writeBytes(bytes, (int) readStartInBytes, (int) readLength);
  }
}