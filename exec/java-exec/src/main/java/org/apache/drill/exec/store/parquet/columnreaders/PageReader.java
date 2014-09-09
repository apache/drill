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
import io.netty.buffer.DrillBuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.store.parquet.ColumnDataReader;
import org.apache.drill.exec.store.parquet.ParquetFormatPlugin;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import parquet.bytes.BytesInput;
import parquet.column.Dictionary;
import parquet.column.ValuesType;
import parquet.column.page.DictionaryPage;
import parquet.column.page.Page;
import parquet.column.values.ValuesReader;
import parquet.column.values.dictionary.DictionaryValuesReader;
import parquet.format.PageHeader;
import parquet.format.PageType;
import parquet.format.Util;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.PrimitiveType;

// class to keep track of the read position of variable length columns
final class PageReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PageReader.class);

  private final ColumnReader parentColumnReader;
  private final ColumnDataReader dataReader;
  // store references to the pages that have been uncompressed, but not copied to ValueVectors yet
  Page currentPage;
  // buffer to store bytes of current page
  DrillBuf pageDataByteArray;

  // for variable length data we need to keep track of our current position in the page data
  // as the values and lengths are intermixed, making random access to the length data impossible
  long readyToReadPosInBytes;
  // read position in the current page, stored in the ByteBuf in ParquetRecordReader called bufferWithAllData
  long readPosInBytes;
  // bit shift needed for the next page if the last one did not line up with a byte boundary
  int bitShift;
  // storage space for extra bits at the end of a page if they did not line up with a byte boundary
  // prevents the need to keep the entire last page, as these pageDataByteArray need to be added to the next batch
  //byte extraBits;

  // used for columns where the number of values that will fit in a vector is unknown
  // currently used for variable length
  // TODO - reuse this when compressed vectors are added, where fixed length values will take up a
  // variable amount of space
  // For example: if nulls are stored without extra space left in the data vector
  // (this is currently simplifying random access to the data during processing, but increases the size of the vectors)
  int valuesReadyToRead;

  // the number of values read out of the last page
  int valuesRead;
  int byteLength;
  //int rowGroupIndex;
  ValuesReader definitionLevels;
  ValuesReader repetitionLevels;
  ValuesReader valueReader;
  ValuesReader dictionaryLengthDeterminingReader;
  ValuesReader dictionaryValueReader;
  Dictionary dictionary;
  PageHeader pageHeader = null;

  List<ByteBuf> allocatedBuffers;

  // These need to be held throughout reading of the entire column chunk
  List<ByteBuf> allocatedDictionaryBuffers;

  PageReader(ColumnReader parentStatus, FileSystem fs, Path path, ColumnChunkMetaData columnChunkMetaData)
    throws ExecutionSetupException{
    this.parentColumnReader = parentStatus;
    allocatedBuffers = new ArrayList<ByteBuf>();
    allocatedDictionaryBuffers = new ArrayList<ByteBuf>();

    long totalByteLength = columnChunkMetaData.getTotalUncompressedSize();
    long start = columnChunkMetaData.getFirstDataPageOffset();
    try {
      FSDataInputStream f = fs.open(path);
      this.dataReader = new ColumnDataReader(f, start, columnChunkMetaData.getTotalSize());
      if (columnChunkMetaData.getDictionaryPageOffset() > 0) {
        f.seek(columnChunkMetaData.getDictionaryPageOffset());
        PageHeader pageHeader = Util.readPageHeader(f);
        assert pageHeader.type == PageType.DICTIONARY_PAGE;

        BytesInput bytesIn;
        ByteBuf uncompressedData=allocateBuffer(pageHeader.getUncompressed_page_size());
        allocatedDictionaryBuffers.add(uncompressedData);
        if(parentColumnReader.columnChunkMetaData.getCodec()==CompressionCodecName.UNCOMPRESSED) {
          dataReader.getPageAsBytesBuf(uncompressedData, pageHeader.compressed_page_size);
          bytesIn=parentColumnReader.parentReader.getCodecFactoryExposer().getBytesInput(uncompressedData,
            pageHeader.getUncompressed_page_size());
        }else{
          ByteBuf compressedData=allocateBuffer(pageHeader.compressed_page_size);
          dataReader.getPageAsBytesBuf(compressedData, pageHeader.compressed_page_size);
          bytesIn = parentColumnReader.parentReader.getCodecFactoryExposer()
            .decompress(parentColumnReader.columnChunkMetaData.getCodec(),
              compressedData,
              uncompressedData,
              pageHeader.compressed_page_size,
              pageHeader.getUncompressed_page_size());
          compressedData.release();
        }
        DictionaryPage page = new DictionaryPage(
            bytesIn,
            pageHeader.uncompressed_page_size,
            pageHeader.dictionary_page_header.num_values,
            parquet.column.Encoding.valueOf(pageHeader.dictionary_page_header.encoding.name())
        );
        this.dictionary = page.getEncoding().initDictionary(parentStatus.columnDescriptor, page);
      }
    } catch (IOException e) {
      throw new ExecutionSetupException("Error opening or reading metadata for parquet file at location: "
        + path.getName(), e);
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
    valuesRead = 0;
    valuesReadyToRead = 0;

    // TODO - the metatdata for total size appears to be incorrect for impala generated files, need to find cause
    // and submit a bug report
    if(!dataReader.hasRemainder() || parentColumnReader.totalValuesRead == parentColumnReader.columnChunkMetaData.getValueCount()) {
      return false;
    }
    clearBuffers();

    // next, we need to decompress the bytes
    // TODO - figure out if we need multiple dictionary pages, I believe it may be limited to one
    // I think we are clobbering parts of the dictionary if there can be multiple pages of dictionary
    do {
      pageHeader = dataReader.readPageHeader();
      if (pageHeader.getType() == PageType.DICTIONARY_PAGE) {

        //TODO: Handle buffer allocation exception
        BytesInput bytesIn;
        ByteBuf uncompressedData=allocateBuffer(pageHeader.getUncompressed_page_size());
        allocatedDictionaryBuffers.add(uncompressedData);
        if( parentColumnReader.columnChunkMetaData.getCodec()== CompressionCodecName.UNCOMPRESSED) {
          dataReader.getPageAsBytesBuf(uncompressedData, pageHeader.compressed_page_size);
          bytesIn=parentColumnReader.parentReader.getCodecFactoryExposer().getBytesInput(uncompressedData,
            pageHeader.getUncompressed_page_size());
        }else{
          ByteBuf compressedData=allocateBuffer(pageHeader.compressed_page_size);
          dataReader.getPageAsBytesBuf(compressedData, pageHeader.compressed_page_size);
          bytesIn = parentColumnReader.parentReader.getCodecFactoryExposer()
            .decompress(parentColumnReader.columnChunkMetaData.getCodec(),
              compressedData,
              uncompressedData,
              pageHeader.compressed_page_size,
              pageHeader.getUncompressed_page_size());
          compressedData.release();
        }
        DictionaryPage page = new DictionaryPage(
            bytesIn,
            pageHeader.uncompressed_page_size,
            pageHeader.dictionary_page_header.num_values,
            parquet.column.Encoding.valueOf(pageHeader.dictionary_page_header.encoding.name())
        );
        this.dictionary = page.getEncoding().initDictionary(parentColumnReader.columnDescriptor, page);
      }
    } while (pageHeader.getType() == PageType.DICTIONARY_PAGE);

    //TODO: Handle buffer allocation exception
    BytesInput bytesIn;
    ByteBuf uncompressedData=allocateBuffer(pageHeader.getUncompressed_page_size());
    allocatedBuffers.add(uncompressedData);
    if(parentColumnReader.columnChunkMetaData.getCodec()==CompressionCodecName.UNCOMPRESSED) {
      dataReader.getPageAsBytesBuf(uncompressedData, pageHeader.compressed_page_size);
      bytesIn=parentColumnReader.parentReader.getCodecFactoryExposer().getBytesInput(uncompressedData,
        pageHeader.getUncompressed_page_size());
    }else{
      ByteBuf compressedData=allocateBuffer(pageHeader.compressed_page_size);
      dataReader.getPageAsBytesBuf(compressedData, pageHeader.compressed_page_size);
      bytesIn = parentColumnReader.parentReader.getCodecFactoryExposer()
        .decompress(parentColumnReader.columnChunkMetaData.getCodec(),
          compressedData,
          uncompressedData,
          pageHeader.compressed_page_size,
          pageHeader.getUncompressed_page_size());
      compressedData.release();
    }
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

    pageDataByteArray = DrillBuf.wrapByteBuffer(currentPage.getBytes().toByteBuffer());
    allocatedBuffers.add(pageDataByteArray);

    readPosInBytes = 0;
    if (parentColumnReader.getColumnDescriptor().getMaxRepetitionLevel() > 0) {
      repetitionLevels = currentPage.getRlEncoding().getValuesReader(parentColumnReader.columnDescriptor, ValuesType.REPETITION_LEVEL);
      repetitionLevels.initFromPage(currentPage.getValueCount(), pageDataByteArray.nioBuffer(), (int) readPosInBytes);
      // we know that the first value will be a 0, at the end of each list of repeated values we will hit another 0 indicating
      // a new record, although we don't know the length until we hit it (and this is a one way stream of integers) so we
      // read the first zero here to simplify the reading processes, and start reading the first value the same as all
      // of the rest. Effectively we are 'reading' the non-existent value in front of the first allowing direct access to
      // the first list of repetition levels
      readPosInBytes = repetitionLevels.getNextOffset();
      repetitionLevels.readInteger();
    }
    if (parentColumnReader.columnDescriptor.getMaxDefinitionLevel() != 0){
      parentColumnReader.currDefLevel = -1;
      definitionLevels = currentPage.getDlEncoding().getValuesReader(parentColumnReader.columnDescriptor, ValuesType.DEFINITION_LEVEL);
      definitionLevels.initFromPage(currentPage.getValueCount(), pageDataByteArray.nioBuffer(), (int) readPosInBytes);
      readPosInBytes = definitionLevels.getNextOffset();
      if ( ! currentPage.getValueEncoding().usesDictionary()) {
        valueReader = currentPage.getValueEncoding().getValuesReader(parentColumnReader.columnDescriptor, ValuesType.VALUES);
        valueReader.initFromPage(currentPage.getValueCount(), pageDataByteArray.nioBuffer(), (int) readPosInBytes);
      }
    }
    if (parentColumnReader.columnDescriptor.getType() == PrimitiveType.PrimitiveTypeName.BOOLEAN) {
      valueReader = currentPage.getValueEncoding().getValuesReader(parentColumnReader.columnDescriptor, ValuesType.VALUES);
      valueReader.initFromPage(currentPage.getValueCount(), pageDataByteArray.nioBuffer(), (int) readPosInBytes);
    }
    if (currentPage.getValueEncoding().usesDictionary()) {
      // initialize two of the dictionary readers, one is for determining the lengths of each value, the second is for
      // actually copying the values out into the vectors
      dictionaryLengthDeterminingReader = new DictionaryValuesReader(dictionary);
      dictionaryLengthDeterminingReader.initFromPage(currentPage.getValueCount(), pageDataByteArray.nioBuffer(), (int) readPosInBytes);
      dictionaryValueReader = new DictionaryValuesReader(dictionary);
      dictionaryValueReader.initFromPage(currentPage.getValueCount(), pageDataByteArray.nioBuffer(), (int) readPosInBytes);
      parentColumnReader.usingDictionary = true;
    } else {
      parentColumnReader.usingDictionary = false;
    }
    // readPosInBytes is used for actually reading the values after we determine how many will fit in the vector
    // readyToReadPosInBytes serves a similar purpose for the vector types where we must count up the values that will
    // fit one record at a time, such as for variable length data. Both operations must start in the same location after the
    // definition and repetition level data which is stored alongside the page data itself
    readyToReadPosInBytes = readPosInBytes;
    return true;
  }

  public void clearBuffers() {
    for (ByteBuf b : allocatedBuffers) {
      b.release();
    }
    allocatedBuffers.clear();
  }

  public void clearDictionaryBuffers() {
    for (ByteBuf b : allocatedDictionaryBuffers) {
      b.release();
    }
    allocatedDictionaryBuffers.clear();
  }

  public void clear(){
    this.dataReader.clear();
    // Free all memory, including fixed length types. (Data is being copied for all types not just var length types)
    //if(!this.parentColumnReader.isFixedLength) {
    clearBuffers();
    clearDictionaryBuffers();
    //}
  }

  /*
    Allocate direct memory to read data into
   */
  private ByteBuf allocateBuffer(int size) {
    ByteBuf b;
    try {
      b = parentColumnReader.parentReader.getOperatorContext().getAllocator().buffer(size);
      //b = UnpooledByteBufAllocator.DEFAULT.heapBuffer(size);
    }catch(Exception e){
      throw new DrillRuntimeException("Unable to allocate "+size+" bytes of memory in the Parquet Reader."+
        "[Exception: "+e.getMessage()+"]"
      );
    }
    return b;
  }

}
