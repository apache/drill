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

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import parquet.bytes.BytesInput;
import parquet.column.ValuesType;
import parquet.column.page.Page;
import parquet.column.values.ValuesReader;
import parquet.format.PageHeader;
import parquet.hadoop.metadata.ColumnChunkMetaData;

// class to keep track of the read position of variable length columns
final class PageReadStatus {

  private final ColumnReader parentColumnReader;
  private final ColumnDataReader dataReader;
  // store references to the pages that have been uncompressed, but not copied to ValueVectors yet
  Page currentPage;
  // buffer to store bytes of current page
  byte[] pageDataByteArray;
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
  //int rowGroupIndex;
  ValuesReader definitionLevels;
  ValuesReader valueReader;

  PageReadStatus(ColumnReader parentStatus, FileSystem fs, Path path, ColumnChunkMetaData columnChunkMetaData) throws ExecutionSetupException{
    this.parentColumnReader = parentStatus;

    long totalByteLength = columnChunkMetaData.getTotalSize();
    long start = columnChunkMetaData.getFirstDataPageOffset();

    try{
      this.dataReader = new ColumnDataReader(fs, path, start, totalByteLength);
    } catch (IOException e) {
      throw new ExecutionSetupException("Error opening or reading metatdata for parquet file at location: " + path.getName(), e);
    }
    
  }

  
  /**
   * Grab the next page.
   *
   * @return - if another page was present
   * @throws java.io.IOException
   */
  public boolean next() throws IOException {

    if(!dataReader.hasRemainder()) return false;

    // next, we need to decompress the bytes
    PageHeader pageHeader = dataReader.readPageHeader();

    BytesInput bytesIn = parentColumnReader.parentReader.getCodecFactoryExposer()
        .decompress( //
            dataReader.getPageAsBytesInput(pageHeader.compressed_page_size), // 
            pageHeader.getUncompressed_page_size(), //
            parentColumnReader.columnChunkMetaData.getCodec());
    currentPage = new Page(
        bytesIn,
        pageHeader.data_page_header.num_values,
        pageHeader.uncompressed_page_size,
        ParquetFormatPlugin.parquetMetadataConverter.getEncoding(pageHeader.data_page_header.repetition_level_encoding),
        ParquetFormatPlugin.parquetMetadataConverter.getEncoding(pageHeader.data_page_header.definition_level_encoding),
        ParquetFormatPlugin.parquetMetadataConverter.getEncoding(pageHeader.data_page_header.encoding)
    );

    byteLength = pageHeader.uncompressed_page_size;

    if (currentPage == null) {
      return false;
    }

    // if the buffer holding each page's data is not large enough to hold the current page, re-allocate, with a little extra space
//    if (pageHeader.getUncompressed_page_size() > pageDataByteArray.length) {
//      pageDataByteArray = new byte[pageHeader.getUncompressed_page_size() + 100];
//    }
    // TODO - would like to get this into the mainline, hopefully before alpha
    pageDataByteArray = currentPage.getBytes().toByteArray();

    readPosInBytes = 0;
    valuesRead = 0;
    if (parentColumnReader.columnDescriptor.getMaxDefinitionLevel() != 0){
      definitionLevels = currentPage.getDlEncoding().getValuesReader(parentColumnReader.columnDescriptor, ValuesType.DEFINITION_LEVEL);
      valueReader = currentPage.getValueEncoding().getValuesReader(parentColumnReader.columnDescriptor, ValuesType.VALUES);
      int endOfDefinitionLevels = definitionLevels.initFromPage(currentPage.getValueCount(), pageDataByteArray, 0);
      valueReader.initFromPage(currentPage.getValueCount(), pageDataByteArray, endOfDefinitionLevels);
      readPosInBytes = endOfDefinitionLevels;
    }

    return true;
  }
  
  public void clear(){
    this.dataReader.clear();
  }
}
