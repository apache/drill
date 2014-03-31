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
import java.util.ArrayList;

import com.google.common.base.Preconditions;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import parquet.bytes.BytesInput;
import parquet.column.Dictionary;
import parquet.column.Encoding;
import parquet.column.ValuesType;
import parquet.column.page.DictionaryPage;
import parquet.column.page.Page;
import parquet.column.values.ValuesReader;
import parquet.column.values.dictionary.DictionaryValuesReader;
import parquet.format.PageHeader;
import parquet.format.PageType;
import parquet.format.Util;
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
  Dictionary dictionary;

  PageReadStatus(ColumnReader parentStatus, FileSystem fs, Path path, ColumnChunkMetaData columnChunkMetaData) throws ExecutionSetupException{
    this.parentColumnReader = parentStatus;

    long totalByteLength = columnChunkMetaData.getTotalSize();
    long start = columnChunkMetaData.getFirstDataPageOffset();
    try {
      FSDataInputStream f = fs.open(path);
      if (columnChunkMetaData.getDictionaryPageOffset() > 0) {
        f.seek(columnChunkMetaData.getDictionaryPageOffset());
        PageHeader pageHeader = Util.readPageHeader(f);
        assert pageHeader.type == PageType.DICTIONARY_PAGE;
        DictionaryPage page = new DictionaryPage(BytesInput.copy(BytesInput.from(f, pageHeader.compressed_page_size)),
            pageHeader.uncompressed_page_size,
            pageHeader.dictionary_page_header.num_values,
            parquet.column.Encoding.valueOf(pageHeader.dictionary_page_header.encoding.name())
        );
        this.dictionary = page.getEncoding().initDictionary(parentStatus.columnDescriptor, page);
      }
      this.dataReader = new ColumnDataReader(f, start, totalByteLength);
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

    currentPage = null;

    if(!dataReader.hasRemainder()) {
      return false;
    }

    // next, we need to decompress the bytes
    PageHeader pageHeader = null;
    // TODO - figure out if we need multiple dictionary pages, I believe it may be limited to one
    // I think we are clobbering parts of the dictionary if there can be multiple pages of dictionary
    do {
      pageHeader = dataReader.readPageHeader();
      if (pageHeader.getType() == PageType.DICTIONARY_PAGE) {
        DictionaryPage page = new DictionaryPage(BytesInput.copy(BytesInput.from(dataReader.input, pageHeader.compressed_page_size)),
                pageHeader.uncompressed_page_size,
                pageHeader.dictionary_page_header.num_values,
                parquet.column.Encoding.valueOf(pageHeader.dictionary_page_header.encoding.name())
        );
        this.dictionary = page.getEncoding().initDictionary(parentColumnReader.columnDescriptor, page);
      }
    } while (pageHeader.getType() == PageType.DICTIONARY_PAGE);

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
      if (!currentPage.getValueEncoding().usesDictionary()) {
        definitionLevels = currentPage.getDlEncoding().getValuesReader(parentColumnReader.columnDescriptor, ValuesType.DEFINITION_LEVEL);
        valueReader = currentPage.getValueEncoding().getValuesReader(parentColumnReader.columnDescriptor, ValuesType.VALUES);
        definitionLevels.initFromPage(currentPage.getValueCount(), pageDataByteArray, 0);
        readPosInBytes = definitionLevels.getNextOffset();
        valueReader.initFromPage(currentPage.getValueCount(), pageDataByteArray, (int) readPosInBytes);
      } else {
        definitionLevels = currentPage.getDlEncoding().getValuesReader(parentColumnReader.columnDescriptor, ValuesType.DEFINITION_LEVEL);
        definitionLevels.initFromPage(currentPage.getValueCount(), pageDataByteArray, 0);
        readPosInBytes = definitionLevels.getNextOffset();
        valueReader = new DictionaryValuesReader(dictionary);
        valueReader.initFromPage(currentPage.getValueCount(), pageDataByteArray, (int) readPosInBytes);
      }
    }
    return true;
  }
  
  public void clear(){
    this.dataReader.clear();
  }
}
