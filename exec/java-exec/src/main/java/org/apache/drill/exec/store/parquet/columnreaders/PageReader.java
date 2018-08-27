/*
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

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import io.netty.buffer.ByteBufUtil;
import org.apache.drill.exec.util.filereader.BufferedDirectBufInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.store.parquet.ParquetFormatPlugin;
import org.apache.drill.exec.store.parquet.ParquetReaderStats;
import org.apache.drill.exec.util.filereader.DirectBufInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.page.DictionaryPage;
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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.parquet.column.Encoding.valueOf;

// class to keep track of the read position of variable length columns
class PageReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(
      org.apache.drill.exec.store.parquet.columnreaders.PageReader.class);

  public static final ParquetMetadataConverter METADATA_CONVERTER = ParquetFormatPlugin.parquetMetadataConverter;

  protected final org.apache.drill.exec.store.parquet.columnreaders.ColumnReader<?> parentColumnReader;
  protected final DirectBufInputStream dataReader;
  //buffer to store bytes of current page
  protected DrillBuf pageData;

  // for variable length data we need to keep track of our current position in the page data
  // as the values and lengths are intermixed, making random access to the length data impossible
  long readyToReadPosInBytes;
  // read position in the current page, stored in the ByteBuf in ParquetRecordReader called bufferWithAllData
  long readPosInBytes;
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

  protected FSDataInputStream inputStream;

  // These need to be held throughout reading of the entire column chunk
  List<ByteBuf> allocatedDictionaryBuffers;

  protected final CodecFactory codecFactory;
  protected final String fileName;

  protected final ParquetReaderStats stats;
  private final boolean useBufferedReader;
  private final int scanBufferSize;
  private final boolean useFadvise;
  private final boolean enforceTotalSize;

  protected final String debugName;

  PageReader(org.apache.drill.exec.store.parquet.columnreaders.ColumnReader<?> parentStatus, FileSystem fs, Path path, ColumnChunkMetaData columnChunkMetaData)
    throws ExecutionSetupException {
    this.parentColumnReader = parentStatus;
    allocatedDictionaryBuffers = new ArrayList<ByteBuf>();
    codecFactory = parentColumnReader.parentReader.getCodecFactory();
    this.stats = parentColumnReader.parentReader.parquetReaderStats;
    this.fileName = path.toString();
    debugName = new StringBuilder()
       .append(this.parentColumnReader.parentReader.getFragmentContext().getFragIdString())
       .append(":")
       .append(this.parentColumnReader.parentReader.getOperatorContext().getStats().getId() )
       .append(this.parentColumnReader.columnChunkMetaData.toString() )
       .toString();
    try {
      inputStream  = fs.open(path);
      BufferAllocator allocator =  parentColumnReader.parentReader.getOperatorContext().getAllocator();
      columnChunkMetaData.getTotalUncompressedSize();
      useBufferedReader  = parentColumnReader.parentReader.useBufferedReader;
      scanBufferSize = parentColumnReader.parentReader.bufferedReadSize;
      useFadvise = parentColumnReader.parentReader.useFadvise;
      enforceTotalSize = parentColumnReader.parentReader.enforceTotalSize;
      if (useBufferedReader) {
        this.dataReader = new BufferedDirectBufInputStream(inputStream, allocator, path.getName(),
            columnChunkMetaData.getStartingPos(), columnChunkMetaData.getTotalSize(), scanBufferSize,
            enforceTotalSize, useFadvise);
      } else {
        this.dataReader = new DirectBufInputStream(inputStream, allocator, path.getName(),
            columnChunkMetaData.getStartingPos(), columnChunkMetaData.getTotalSize(), enforceTotalSize,
            useFadvise);
      }
    } catch (IOException e) {
      throw new ExecutionSetupException("Error opening or reading metadata for parquet file at location: "
          + path.getName(), e);
    }

  }

  protected void init() throws IOException{
    dataReader.init();
    loadDictionaryIfExists(parentColumnReader, parentColumnReader.columnChunkMetaData, dataReader);
  }

  protected void loadDictionaryIfExists(final org.apache.drill.exec.store.parquet.columnreaders.ColumnReader<?> parentStatus,
      final ColumnChunkMetaData columnChunkMetaData, final DirectBufInputStream f) throws IOException {
    Stopwatch timer = Stopwatch.createUnstarted();
    if (columnChunkMetaData.getDictionaryPageOffset() > 0) {
      long bytesToSkip = columnChunkMetaData.getDictionaryPageOffset() - dataReader.getPos();
      while (bytesToSkip > 0) {
        long skipped = dataReader.skip(bytesToSkip);
        if (skipped > 0) {
          bytesToSkip -= skipped;
        } else {
          // no good way to handle this. Guava uses InputStream.available to check
          // if EOF is reached and because available is not reliable,
          // tries to read the rest of the data.
          DrillBuf skipBuf = dataReader.getNext((int) bytesToSkip);
          if (skipBuf != null) {
            skipBuf.release();
          } else {
            throw new EOFException("End of File reachecd.");
          }
        }
      }

      long start=dataReader.getPos();
      timer.start();
      final PageHeader pageHeader = Util.readPageHeader(f);
      long timeToRead = timer.elapsed(TimeUnit.NANOSECONDS);
      long pageHeaderBytes=dataReader.getPos()-start;
      this.updateStats(pageHeader, "Page Header", start, timeToRead, pageHeaderBytes, pageHeaderBytes);
      assert pageHeader.type == PageType.DICTIONARY_PAGE;
      readDictionaryPage(pageHeader, parentStatus);
    }
  }

  private void readDictionaryPage(final PageHeader pageHeader,
                                  final ColumnReader<?> parentStatus) throws IOException {
    int compressedSize = pageHeader.getCompressed_page_size();
    int uncompressedSize = pageHeader.getUncompressed_page_size();

    final DrillBuf dictionaryData = readPage(pageHeader, compressedSize, uncompressedSize);
    allocatedDictionaryBuffers.add(dictionaryData);

    DictionaryPage page = new DictionaryPage(
        asBytesInput(dictionaryData, 0, uncompressedSize),
        pageHeader.uncompressed_page_size,
        pageHeader.dictionary_page_header.num_values,
        valueOf(pageHeader.dictionary_page_header.encoding.name()));

    this.dictionary = page.getEncoding().initDictionary(parentStatus.columnDescriptor, page);
  }

  private DrillBuf readPage(PageHeader pageHeader, int compressedSize, int uncompressedSize) throws IOException {
    DrillBuf pageDataBuf = null;
    Stopwatch timer = Stopwatch.createUnstarted();
    long timeToRead;
    long start=dataReader.getPos();
    if (parentColumnReader.columnChunkMetaData.getCodec() == CompressionCodecName.UNCOMPRESSED) {
      timer.start();
      pageDataBuf = dataReader.getNext(compressedSize);
      if (logger.isTraceEnabled()) {
        logger.trace("PageReaderTask==> Col: {}  readPos: {}  Uncompressed_size: {}  pageData: {}",
            parentColumnReader.columnChunkMetaData.toString(), dataReader.getPos(),
            pageHeader.getUncompressed_page_size(), ByteBufUtil.hexDump(pageData));
      }
      timeToRead = timer.elapsed(TimeUnit.NANOSECONDS);
      this.updateStats(pageHeader, "Page Read", start, timeToRead, compressedSize, uncompressedSize);
    } else {
      DrillBuf compressedData = null;
      pageDataBuf=allocateTemporaryBuffer(uncompressedSize);

      try {
        timer.start();
        compressedData = dataReader.getNext(compressedSize);
        timeToRead = timer.elapsed(TimeUnit.NANOSECONDS);

        timer.reset();
        this.updateStats(pageHeader, "Page Read", start, timeToRead, compressedSize, compressedSize);
        start = dataReader.getPos();
        timer.start();
        codecFactory.getDecompressor(parentColumnReader.columnChunkMetaData.getCodec())
            .decompress(compressedData.nioBuffer(0, compressedSize), compressedSize,
                pageDataBuf.nioBuffer(0, uncompressedSize), uncompressedSize);
        pageDataBuf.writerIndex(uncompressedSize);
        timeToRead = timer.elapsed(TimeUnit.NANOSECONDS);
        this.updateStats(pageHeader, "Decompress", start, timeToRead, compressedSize, uncompressedSize);
      } finally {
        if (compressedData != null) {
          compressedData.release();
        }
      }
    }
    return pageDataBuf;
  }

  public static BytesInput asBytesInput(DrillBuf buf, int offset, int length) throws IOException {
    return BytesInput.from(buf.nioBuffer(offset, length));
  }


  /**
   * Get the page header and the pageData (uncompressed) for the next page
   */
  protected void nextInternal() throws IOException{
    Stopwatch timer = Stopwatch.createUnstarted();
    // next, we need to decompress the bytes
    // TODO - figure out if we need multiple dictionary pages, I believe it may be limited to one
    // I think we are clobbering parts of the dictionary if there can be multiple pages of dictionary
    do {
      long start=dataReader.getPos();
      timer.start();
      pageHeader = Util.readPageHeader(dataReader);
      long timeToRead = timer.elapsed(TimeUnit.NANOSECONDS);
      long pageHeaderBytes=dataReader.getPos()-start;
      this.updateStats(pageHeader, "Page Header", start, timeToRead, pageHeaderBytes, pageHeaderBytes);
      logger.trace("ParquetTrace,{},{},{},{},{},{},{},{}","Page Header Read","",
          this.parentColumnReader.parentReader.hadoopPath,
          this.parentColumnReader.columnDescriptor.toString(), start, 0, 0, timeToRead);
      timer.reset();
      if (pageHeader.getType() == PageType.DICTIONARY_PAGE) {
        readDictionaryPage(pageHeader, parentColumnReader);
      }
    } while (pageHeader.getType() == PageType.DICTIONARY_PAGE);

    int compressedSize = pageHeader.getCompressed_page_size();
    int uncompressedSize = pageHeader.getUncompressed_page_size();
    pageData = readPage(pageHeader, compressedSize, uncompressedSize);

  }

  /**
   * Grab the next page.
   *
   * @return - if another page was present
   * @throws IOException
   */
  public boolean next() throws IOException {
    Stopwatch timer = Stopwatch.createUnstarted();
    currentPageCount = -1;
    valuesRead = 0;
    valuesReadyToRead = 0;

    // TODO - the metatdata for total size appears to be incorrect for impala generated files, need to find cause
    // and submit a bug report
    long totalValueCount = parentColumnReader.columnChunkMetaData.getValueCount();
    if(parentColumnReader.totalValuesRead >= totalValueCount) {
      return false;
    }
    clearBuffers();

    nextInternal();
    if(pageData == null || pageHeader == null){
      //TODO: Is this an error condition or a normal condition??
      return false;
    }

    timer.start();
    currentPageCount = pageHeader.data_page_header.num_values;

    final Encoding rlEncoding = METADATA_CONVERTER.getEncoding(pageHeader.data_page_header.repetition_level_encoding);
    final Encoding dlEncoding = METADATA_CONVERTER.getEncoding(pageHeader.data_page_header.definition_level_encoding);
    final Encoding valueEncoding = METADATA_CONVERTER.getEncoding(pageHeader.data_page_header.encoding);

    byteLength = pageHeader.uncompressed_page_size;

    final ByteBufferInputStream in = ByteBufferInputStream.wrap(pageData.nioBuffer(0, pageData.capacity()));

    readPosInBytes = 0;
    if (parentColumnReader.getColumnDescriptor().getMaxRepetitionLevel() > 0) {
      repetitionLevels = rlEncoding.getValuesReader(parentColumnReader.columnDescriptor, ValuesType.REPETITION_LEVEL);
      repetitionLevels.initFromPage(currentPageCount, in);
      // we know that the first value will be a 0, at the end of each list of repeated values we will hit another 0 indicating
      // a new record, although we don't know the length until we hit it (and this is a one way stream of integers) so we
      // read the first zero here to simplify the reading processes, and start reading the first value the same as all
      // of the rest. Effectively we are 'reading' the non-existent value in front of the first allowing direct access to
      // the first list of repetition levels
      readPosInBytes = in.position();
      repetitionLevels.readInteger();
    }
    if (parentColumnReader.columnDescriptor.getMaxDefinitionLevel() != 0) {
      parentColumnReader.currDefLevel = -1;
      definitionLevels = dlEncoding.getValuesReader(parentColumnReader.columnDescriptor, ValuesType.DEFINITION_LEVEL);
      definitionLevels.initFromPage(currentPageCount, in);
      readPosInBytes = in.position();
      if (!valueEncoding.usesDictionary()) {
        valueReader = valueEncoding.getValuesReader(parentColumnReader.columnDescriptor, ValuesType.VALUES);
        valueReader.initFromPage(currentPageCount, in);
      }
    }
    if (valueReader == null && parentColumnReader.columnDescriptor.getType() == PrimitiveType.PrimitiveTypeName.BOOLEAN) {
      valueReader = valueEncoding.getValuesReader(parentColumnReader.columnDescriptor, ValuesType.VALUES);
      valueReader.initFromPage(currentPageCount, in);
    }
    if (valueEncoding.usesDictionary()) {
      // initialize two of the dictionary readers, one is for determining the lengths of each value, the second is for
      // actually copying the values out into the vectors
      Preconditions.checkState(readPosInBytes < pageData.capacity());
      int index = (int)readPosInBytes;
      ByteBuffer byteBuffer = pageData.nioBuffer(index, pageData.capacity() - index);
      dictionaryLengthDeterminingReader = new DictionaryValuesReader(dictionary);
      dictionaryLengthDeterminingReader.initFromPage(currentPageCount, ByteBufferInputStream.wrap(byteBuffer));
      dictionaryValueReader = new DictionaryValuesReader(dictionary);
      dictionaryValueReader.initFromPage(currentPageCount, ByteBufferInputStream.wrap(byteBuffer));
      parentColumnReader.usingDictionary = true;
    } else {
      parentColumnReader.usingDictionary = false;
    }
    // readPosInBytes is used for actually reading the values after we determine how many will fit in the vector
    // readyToReadPosInBytes serves a similar purpose for the vector types where we must count up the values that will
    // fit one record at a time, such as for variable length data. Both operations must start in the same location after the
    // definition and repetition level data which is stored alongside the page data itself
    readyToReadPosInBytes = readPosInBytes;
    long timeDecode = timer.elapsed(TimeUnit.NANOSECONDS);
    stats.numDataPagesDecoded.incrementAndGet();
    stats.timeDataPageDecode.addAndGet(timeDecode);
    return true;
  }

  /**
   * Allocate a buffer which the user should release immediately. The reader does not manage release of these buffers.
   */
  protected DrillBuf allocateTemporaryBuffer(int size) {
    return parentColumnReader.parentReader.getOperatorContext().getAllocator().buffer(size);
  }

  protected boolean hasPage() {
    return currentPageCount != -1;
  }

  protected void updateStats(PageHeader pageHeader, String op, long start, long time, long bytesin, long bytesout) {
    String pageType = "Data Page";
    if (pageHeader.type == PageType.DICTIONARY_PAGE) {
      pageType = "Dictionary Page";
    }
    logger.trace("ParquetTrace,{},{},{},{},{},{},{},{}", op, pageType,
        this.parentColumnReader.parentReader.hadoopPath,
        this.parentColumnReader.columnDescriptor.toString(), start, bytesin, bytesout, time);

    if (pageHeader.type != PageType.DICTIONARY_PAGE) {
      if (bytesin == bytesout) {
        this.stats.timeDataPageLoads.addAndGet(time);
        this.stats.numDataPageLoads.incrementAndGet();
        this.stats.totalDataPageReadBytes.addAndGet(bytesin);
      } else {
        this.stats.timeDataPagesDecompressed.addAndGet(time);
        this.stats.numDataPagesDecompressed.incrementAndGet();
        this.stats.totalDataDecompressedBytes.addAndGet(bytesin);
      }
    } else {
      if (bytesin == bytesout) {
        this.stats.timeDictPageLoads.addAndGet(time);
        this.stats.numDictPageLoads.incrementAndGet();
        this.stats.totalDictPageReadBytes.addAndGet(bytesin);
      } else {
        this.stats.timeDictPagesDecompressed.addAndGet(time);
        this.stats.numDictPagesDecompressed.incrementAndGet();
        this.stats.totalDictDecompressedBytes.addAndGet(bytesin);
      }
    }
  }

  protected void clearBuffers() {
    if (pageData != null) {
      pageData.release();
      pageData = null;
    }
  }

  protected void clearDictionaryBuffers() {
    for (ByteBuf b : allocatedDictionaryBuffers) {
      b.release();
    }
    allocatedDictionaryBuffers.clear();
  }

  public void clear(){
    try {
      // data reader also owns the input stream and will close it.
      this.dataReader.close();
    } catch (IOException e) {
      //Swallow the exception which is OK for input streams
    }
    // Free all memory, including fixed length types. (Data is being copied for all types not just var length types)
    clearBuffers();
    clearDictionaryBuffers();
  }

  /**
   * Enables Parquet column readers to reset the definition level reader to a specific state.
   * @param skipCount the number of rows to skip (optional)
   *
   * @throws IOException An IO related condition
   */
  void resetDefinitionLevelReader(int skipCount) throws IOException {
    Preconditions.checkState(parentColumnReader.columnDescriptor.getMaxDefinitionLevel() == 1);
    Preconditions.checkState(currentPageCount > 0);

    final Encoding rlEncoding = METADATA_CONVERTER.getEncoding(pageHeader.data_page_header.repetition_level_encoding);
    final Encoding dlEncoding = METADATA_CONVERTER.getEncoding(pageHeader.data_page_header.definition_level_encoding);

    final ByteBufferInputStream in = ByteBufferInputStream.wrap(pageData.nioBuffer(0, pageData.capacity()));

    if (parentColumnReader.getColumnDescriptor().getMaxRepetitionLevel() > 0) {
      repetitionLevels = rlEncoding.getValuesReader(parentColumnReader.columnDescriptor, ValuesType.REPETITION_LEVEL);
      repetitionLevels.initFromPage(currentPageCount, in);
      repetitionLevels.readInteger();
    }

    definitionLevels = dlEncoding.getValuesReader(parentColumnReader.columnDescriptor, ValuesType.DEFINITION_LEVEL);
    parentColumnReader.currDefLevel = -1;

    // Now reinitialize the underlying decoder
    definitionLevels.initFromPage(currentPageCount, in);

    // Skip values if requested by caller
    for (int idx = 0; idx < skipCount; ++idx) {
      definitionLevels.skip();
    }
  }
}
