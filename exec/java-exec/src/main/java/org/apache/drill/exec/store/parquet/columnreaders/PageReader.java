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

import static org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetStatistics;
import static org.apache.parquet.column.Encoding.valueOf;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.store.parquet.ColumnDataReader;
import org.apache.drill.exec.store.parquet.ParquetFormatPlugin;
import org.apache.drill.exec.store.parquet.ParquetReaderStats;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.dictionary.DictionaryValuesReader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.PrimitiveType;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

// class to keep track of the read position of variable length columns
final class PageReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PageReader.class);

  public static final ParquetMetadataConverter METADATA_CONVERTER = ParquetFormatPlugin.parquetMetadataConverter;

  private final ColumnReader parentColumnReader;
  private final ColumnDataReader dataReader;

  // buffer to store bytes of current page
  DrillBuf pageData;

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

  int currentPageCount = -1;

  private FSDataInputStream inputStream;

  // These need to be held throughout reading of the entire column chunk
  List<ByteBuf> allocatedDictionaryBuffers;

  private final CodecFactory codecFactory;

  private final ParquetReaderStats stats;

  PageReader(ColumnReader<?> parentStatus, FileSystem fs, Path path, ColumnChunkMetaData columnChunkMetaData)
    throws ExecutionSetupException{
    this.parentColumnReader = parentStatus;
    allocatedDictionaryBuffers = new ArrayList<ByteBuf>();
    codecFactory = parentColumnReader.parentReader.getCodecFactory();
    this.stats = parentColumnReader.parentReader.parquetReaderStats;
    long start = columnChunkMetaData.getFirstDataPageOffset();
    try {
      inputStream  = fs.open(path);
      this.dataReader = new ColumnDataReader(inputStream, start, columnChunkMetaData.getTotalSize());
      loadDictionaryIfExists(parentStatus, columnChunkMetaData, inputStream);

    } catch (IOException e) {
      throw new ExecutionSetupException("Error opening or reading metadata for parquet file at location: "
          + path.getName(), e);
    }

  }

  private void loadDictionaryIfExists(final ColumnReader<?> parentStatus,
      final ColumnChunkMetaData columnChunkMetaData, final FSDataInputStream f) throws IOException {
    Stopwatch timer = new Stopwatch();
    if (columnChunkMetaData.getDictionaryPageOffset() > 0) {
      f.seek(columnChunkMetaData.getDictionaryPageOffset());
      long start=f.getPos();
      timer.start();
      final PageHeader pageHeader = Util.readPageHeader(f);
      long timeToRead = timer.elapsed(TimeUnit.MICROSECONDS);
      timer.reset();
      long pageHeaderBytes=f.getPos()-start;
      this.updateStats(pageHeader, "Page Header", start, timeToRead, pageHeaderBytes, pageHeaderBytes);
      assert pageHeader.type == PageType.DICTIONARY_PAGE;
      readDictionaryPage(pageHeader, parentStatus);
    }
  }

  private void readDictionaryPage(final PageHeader pageHeader,
                                  final ColumnReader<?> parentStatus) throws IOException {
    int compressedSize = pageHeader.getCompressed_page_size();
    int uncompressedSize = pageHeader.getUncompressed_page_size();

    final DrillBuf dictionaryData = allocateDictionaryBuffer(uncompressedSize);
    readPage(pageHeader, compressedSize, uncompressedSize, dictionaryData);

    DictionaryPage page = new DictionaryPage(
        asBytesInput(dictionaryData, 0, uncompressedSize),
        pageHeader.uncompressed_page_size,
        pageHeader.dictionary_page_header.num_values,
        valueOf(pageHeader.dictionary_page_header.encoding.name()));

    this.dictionary = page.getEncoding().initDictionary(parentStatus.columnDescriptor, page);
  }

  public void readPage(PageHeader pageHeader, int compressedSize, int uncompressedSize, DrillBuf dest) throws IOException {
    Stopwatch timer = new Stopwatch();
    long timeToRead;
    long start=inputStream.getPos();
    if (parentColumnReader.columnChunkMetaData.getCodec() == CompressionCodecName.UNCOMPRESSED) {
      timer.start();
      dataReader.loadPage(dest, compressedSize);
      timeToRead = timer.elapsed(TimeUnit.MICROSECONDS);
      this.updateStats(pageHeader, "Page Read", start, timeToRead, compressedSize, uncompressedSize);
    } else {
      final DrillBuf compressedData = allocateTemporaryBuffer(compressedSize);
      try {
      timer.start();
      dataReader.loadPage(compressedData, compressedSize);
      timeToRead = timer.elapsed(TimeUnit.MICROSECONDS);
      timer.reset();
      this.updateStats(pageHeader, "Page Read", start, timeToRead, compressedSize, compressedSize);
      start = inputStream.getPos();
      timer.start();
      codecFactory.getDecompressor(parentColumnReader.columnChunkMetaData
          .getCodec()).decompress(compressedData.nioBuffer(0, compressedSize), compressedSize,
          dest.nioBuffer(0, uncompressedSize), uncompressedSize);
        timeToRead = timer.elapsed(TimeUnit.MICROSECONDS);
        this.updateStats(pageHeader, "Decompress", start, timeToRead, compressedSize, uncompressedSize);
      } finally {
        compressedData.release();
      }
    }
  }

  public static BytesInput asBytesInput(DrillBuf buf, int offset, int length) throws IOException {
    return BytesInput.from(buf.nioBuffer(offset, length), 0, length);
  }

  /**
   * Grab the next page.
   *
   * @return - if another page was present
   * @throws java.io.IOException
   */
  public boolean next() throws IOException {
    Stopwatch timer = new Stopwatch();
    currentPageCount = -1;
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
      long start=inputStream.getPos();
      timer.start();
      pageHeader = dataReader.readPageHeader();
      long timeToRead = timer.elapsed(TimeUnit.MICROSECONDS);
      this.updateStats(pageHeader, "Page Header Read", start, timeToRead, 0,0);
      logger.trace("ParquetTrace,{},{},{},{},{},{},{},{}","Page Header Read","",
          this.parentColumnReader.parentReader.hadoopPath,
          this.parentColumnReader.columnDescriptor.toString(), start, 0, 0, timeToRead);
      timer.reset();
      if (pageHeader.getType() == PageType.DICTIONARY_PAGE) {
        readDictionaryPage(pageHeader, parentColumnReader);
      }
    } while (pageHeader.getType() == PageType.DICTIONARY_PAGE);

    //TODO: Handle buffer allocation exception

    allocatePageData(pageHeader.getUncompressed_page_size());
    int compressedSize = pageHeader.getCompressed_page_size();
    int uncompressedSize = pageHeader.getUncompressed_page_size();
    readPage(pageHeader, compressedSize, uncompressedSize, pageData);

    currentPageCount = pageHeader.data_page_header.num_values;

    final int uncompressedPageSize = pageHeader.uncompressed_page_size;
    final Statistics<?> stats = fromParquetStatistics(pageHeader.data_page_header.getStatistics(), parentColumnReader
        .getColumnDescriptor().getType());
    final Encoding rlEncoding = METADATA_CONVERTER.getEncoding(pageHeader.data_page_header.repetition_level_encoding);

    final Encoding dlEncoding = METADATA_CONVERTER.getEncoding(pageHeader.data_page_header.definition_level_encoding);
    final Encoding valueEncoding = METADATA_CONVERTER.getEncoding(pageHeader.data_page_header.encoding);

    byteLength = pageHeader.uncompressed_page_size;

    final ByteBuffer pageDataBuffer = pageData.nioBuffer(0, pageData.capacity());

    readPosInBytes = 0;
    if (parentColumnReader.getColumnDescriptor().getMaxRepetitionLevel() > 0) {
      repetitionLevels = rlEncoding.getValuesReader(parentColumnReader.columnDescriptor, ValuesType.REPETITION_LEVEL);
      repetitionLevels.initFromPage(currentPageCount, pageDataBuffer, (int) readPosInBytes);
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
      definitionLevels = dlEncoding.getValuesReader(parentColumnReader.columnDescriptor, ValuesType.DEFINITION_LEVEL);
      definitionLevels.initFromPage(currentPageCount, pageDataBuffer, (int) readPosInBytes);
      readPosInBytes = definitionLevels.getNextOffset();
      if (!valueEncoding.usesDictionary()) {
        valueReader = valueEncoding.getValuesReader(parentColumnReader.columnDescriptor, ValuesType.VALUES);
        valueReader.initFromPage(currentPageCount, pageDataBuffer, (int) readPosInBytes);
      }
    }
    if (parentColumnReader.columnDescriptor.getType() == PrimitiveType.PrimitiveTypeName.BOOLEAN) {
      valueReader = valueEncoding.getValuesReader(parentColumnReader.columnDescriptor, ValuesType.VALUES);
      valueReader.initFromPage(currentPageCount, pageDataBuffer, (int) readPosInBytes);
    }
    if (valueEncoding.usesDictionary()) {
      // initialize two of the dictionary readers, one is for determining the lengths of each value, the second is for
      // actually copying the values out into the vectors
      dictionaryLengthDeterminingReader = new DictionaryValuesReader(dictionary);
      dictionaryLengthDeterminingReader.initFromPage(currentPageCount, pageDataBuffer, (int) readPosInBytes);
      dictionaryValueReader = new DictionaryValuesReader(dictionary);
      dictionaryValueReader.initFromPage(currentPageCount, pageDataBuffer, (int) readPosInBytes);
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

  /**
   * Allocate a page data buffer. Note that only one page data buffer should be active at a time. The reader will ensure
   * that the page data is released after the reader is completed.
   */
  private void allocatePageData(int size) {
    Preconditions.checkArgument(pageData == null);
    pageData = parentColumnReader.parentReader.getOperatorContext().getAllocator().buffer(size);
  }

  /**
   * Allocate a buffer which the user should release immediately. The reader does not manage release of these buffers.
   */
  private DrillBuf allocateTemporaryBuffer(int size) {
    return parentColumnReader.parentReader.getOperatorContext().getAllocator().buffer(size);
  }

  /**
   * Allocate and return a dictionary buffer. These are maintained for the life of the reader and then released when the
   * reader is cleared.
   */
  private DrillBuf allocateDictionaryBuffer(int size) {
    DrillBuf buf = parentColumnReader.parentReader.getOperatorContext().getAllocator().buffer(size);
    allocatedDictionaryBuffers.add(buf);
    return buf;
  }

  protected boolean hasPage() {
    return currentPageCount != -1;
  }

  private void updateStats(PageHeader pageHeader, String op, long start, long time, long bytesin, long bytesout) {
    String pageType = "Data Page";
    if (pageHeader.type == PageType.DICTIONARY_PAGE) {
      pageType = "Dictionary Page";
    }
    logger.trace("ParquetTrace,{},{},{},{},{},{},{},{}", op, pageType.toString(),
        this.parentColumnReader.parentReader.hadoopPath,
        this.parentColumnReader.columnDescriptor.toString(), start, bytesin, bytesout, time);
    if (pageHeader.type != PageType.DICTIONARY_PAGE) {
      if (bytesin == bytesout) {
        this.stats.timePageLoads += time;
        this.stats.numPageLoads++;
        this.stats.totalPageReadBytes += bytesin;
      } else {
        this.stats.timePagesDecompressed += time;
        this.stats.numPagesDecompressed++;
        this.stats.totalDecompressedBytes += bytesin;
      }
    } else {
      if (bytesin == bytesout) {
        this.stats.timeDictPageLoads += time;
        this.stats.numDictPageLoads++;
        this.stats.totalDictPageReadBytes += bytesin;
      } else {
        this.stats.timeDictPagesDecompressed += time;
        this.stats.numDictPagesDecompressed++;
        this.stats.totalDictDecompressedBytes += bytesin;
      }
    }
  }

  public void clearBuffers() {
    if (pageData != null) {
      pageData.release();
      pageData = null;
    }
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



}
