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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import parquet.bytes.BytesInput;
import parquet.column.ValuesType;
import parquet.column.page.Page;
import parquet.column.page.PageReader;
import parquet.column.values.ValuesReader;
import parquet.format.PageHeader;

import java.io.IOException;

import static parquet.format.Util.readPageHeader;

// class to keep track of the read position of variable length columns
public final class PageReadStatus {

  ColumnReader parentColumnReader;

  // store references to the pages that have been uncompressed, but not copied to ValueVectors yet
  Page currentPage;
  // buffer to store bytes of current page, set to max size of parquet page
  byte[] pageDataByteArray = new byte[ParquetRecordReader.PARQUET_PAGE_MAX_SIZE];

  // read position in the current page, stored in the ByteBuf in ParquetRecordReader called bufferWithAllData
  long readPosInBytes;
  // bit shift needed for the next page if the last one did not line up with a byte boundary
  int bitShift;
  // storage space for extra bits at the end of a page if they did not line up with a byte boundary
  // prevents the need to keep the entire last page, as these pageDataByteArray need to be added to the next batch
  //byte extraBits;
  // the number of values read out of the last page
  int valuesRead;
  int byteLength;
  int rowGroupIndex;
  ValuesReader definitionLevels;
  ValuesReader valueReader;

  PageReadStatus(ColumnReader parentStatus, int rowGroupIndex, ByteBuf bufferWithAllData){
    this.parentColumnReader = parentStatus;
    this.rowGroupIndex = rowGroupIndex;
  }

  /**
   * Grab the next page.
   *
   * @return - if another page was present
   * @throws java.io.IOException
   */
  public boolean next() throws IOException {

    int shift = 0;
    if (rowGroupIndex == 0) shift = 0;
    else shift = 4;
    // first ROW GROUP has a different endpoint, because there are for bytes at the beginning of the file "PAR1"
    if (parentColumnReader.readPositionInBuffer + shift == parentColumnReader.columnChunkMetaData.getFirstDataPageOffset() + parentColumnReader.columnChunkMetaData.getTotalSize()){
      return false;
    }
    // TODO - in the JIRA for parquet steven put a stack trace for an error with a row group with 3 values in it
    // the Math.min with the end of the buffer should fix it but now I'm not getting results back, leaving it here for now
    // because it is needed, but there might be a problem with it
    ByteBufInputStream f = new ByteBufInputStream(parentColumnReader.parentReader.getBufferWithAllData().slice(
        (int) parentColumnReader.readPositionInBuffer,
        Math.min(200, parentColumnReader.parentReader.getBufferWithAllData().capacity() - (int) parentColumnReader.readPositionInBuffer)));
    int before = f.available();
    PageHeader pageHeader = readPageHeader(f);
    int length = before - f.available();
    f = new ByteBufInputStream(parentColumnReader.parentReader.getBufferWithAllData().slice(
        (int) parentColumnReader.readPositionInBuffer + length, pageHeader.getCompressed_page_size()));

    BytesInput bytesIn = parentColumnReader.parentReader.getCodecFactoryExposer()
        .decompress(BytesInput.from(f, pageHeader.compressed_page_size), pageHeader.getUncompressed_page_size(),
            parentColumnReader.columnChunkMetaData.getCodec());
    currentPage = new Page(
        bytesIn,
        pageHeader.data_page_header.num_values,
        pageHeader.uncompressed_page_size,
        ParquetStorageEngine.parquetMetadataConverter.getEncoding(pageHeader.data_page_header.repetition_level_encoding),
        ParquetStorageEngine.parquetMetadataConverter.getEncoding(pageHeader.data_page_header.definition_level_encoding),
        ParquetStorageEngine.parquetMetadataConverter.getEncoding(pageHeader.data_page_header.encoding)
    );

    parentColumnReader.readPositionInBuffer += pageHeader.compressed_page_size + length;
    byteLength = pageHeader.uncompressed_page_size;

    if (currentPage == null) {
      return false;
    }

    // if the buffer holding each page's data is not large enough to hold the current page, re-allocate, with a little extra space
    if (pageHeader.getUncompressed_page_size() > pageDataByteArray.length) {
      pageDataByteArray = new byte[pageHeader.getUncompressed_page_size() + 100];
    }
    // TODO - would like to get this into the mainline, hopefully before alpha
    pageDataByteArray = currentPage.getBytes().toByteArray();

    if (parentColumnReader.columnDescriptor.getMaxDefinitionLevel() != 0){
      definitionLevels = currentPage.getDlEncoding().getValuesReader(parentColumnReader.columnDescriptor, ValuesType.DEFINITION_LEVEL);
      valueReader = currentPage.getValueEncoding().getValuesReader(parentColumnReader.columnDescriptor, ValuesType.VALUES);
      int endOfDefinitionLevels = definitionLevels.initFromPage(currentPage.getValueCount(), pageDataByteArray, 0);
      valueReader.initFromPage(currentPage.getValueCount(), pageDataByteArray, endOfDefinitionLevels);
      readPosInBytes = endOfDefinitionLevels;
    }

    readPosInBytes = 0;
    valuesRead = 0;
    return true;
  }
}
