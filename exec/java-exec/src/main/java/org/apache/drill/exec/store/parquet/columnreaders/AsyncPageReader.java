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

import com.google.common.base.Stopwatch;
import io.netty.buffer.DrillBuf;
import io.netty.buffer.ByteBufUtil;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DirectDecompressor;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.codec.SnappyCodec;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.drill.exec.util.filereader.DirectBufInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.parquet.column.Encoding.valueOf;

class AsyncPageReader extends PageReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AsyncPageReader.class);


  private ExecutorService threadPool;
  private Future<ReadStatus> asyncPageRead;

  AsyncPageReader(ColumnReader<?> parentStatus, FileSystem fs, Path path,
      ColumnChunkMetaData columnChunkMetaData) throws ExecutionSetupException {
    super(parentStatus, fs, path, columnChunkMetaData);
    if (threadPool == null) {
      threadPool = parentColumnReader.parentReader.getOperatorContext().getScanExecutor();
    }
    asyncPageRead = threadPool.submit(new AsyncPageReaderTask());
  }

  @Override
  protected void loadDictionaryIfExists(final ColumnReader<?> parentStatus,
      final ColumnChunkMetaData columnChunkMetaData, final DirectBufInputStream f) throws UserException {
    if (columnChunkMetaData.getDictionaryPageOffset() > 0) {
      try {
        assert(columnChunkMetaData.getDictionaryPageOffset() >= dataReader.getPos() );
        dataReader.skip(columnChunkMetaData.getDictionaryPageOffset() - dataReader.getPos());
      } catch (IOException e) {
        handleAndThrowException(e, "Error Reading dictionary page.");
      }
      // parent constructor may call this method before the thread pool is set.
      if (threadPool == null) {
        threadPool = parentColumnReader.parentReader.getOperatorContext().getScanExecutor();
      }
      asyncPageRead = threadPool.submit(new AsyncPageReaderTask());
      readDictionaryPage(asyncPageRead, parentStatus);
      asyncPageRead = null; // reset after consuming
    }
  }

  private DrillBuf getDecompressedPageData(ReadStatus readStatus) {
    DrillBuf data;
    boolean isDictionary = false;
    synchronized (this) {
      data = readStatus.getPageData();
      readStatus.setPageData(null);
      isDictionary = readStatus.isDictionaryPage;
    }
    if (parentColumnReader.columnChunkMetaData.getCodec() != CompressionCodecName.UNCOMPRESSED) {
      DrillBuf compressedData = data;
      data = decompress(readStatus.getPageHeader(), compressedData);
      synchronized (this) {
        readStatus.setPageData(null);
      }
      compressedData.release();
    } else {
      if (isDictionary) {
        stats.totalDictPageReadBytes.addAndGet(readStatus.bytesRead);
      } else {
        stats.totalDataPageReadBytes.addAndGet(readStatus.bytesRead);
      }
    }
    return data;
  }

  // Read and decode the dictionary and the header
  private void readDictionaryPage(final Future<ReadStatus> asyncPageRead,
      final ColumnReader<?> parentStatus) throws UserException {
    try {
      Stopwatch timer = Stopwatch.createStarted();
      ReadStatus readStatus = asyncPageRead.get();
      long timeBlocked = timer.elapsed(TimeUnit.NANOSECONDS);
      stats.timeDiskScanWait.addAndGet(timeBlocked);
      stats.timeDiskScan.addAndGet(readStatus.getDiskScanTime());
      stats.numDictPageLoads.incrementAndGet();
      stats.timeDictPageLoads.addAndGet(timeBlocked+readStatus.getDiskScanTime());
      readDictionaryPageData(readStatus, parentStatus);
    } catch (Exception e) {
      handleAndThrowException(e, "Error reading dictionary page.");
    }
  }

  // Read and decode the dictionary data
  private void readDictionaryPageData(final ReadStatus readStatus, final ColumnReader<?> parentStatus)
      throws UserException {
    try {
      pageHeader = readStatus.getPageHeader();
      int uncompressedSize = pageHeader.getUncompressed_page_size();
      final DrillBuf dictionaryData = getDecompressedPageData(readStatus);
      Stopwatch timer = Stopwatch.createStarted();
      allocatedDictionaryBuffers.add(dictionaryData);
      DictionaryPage page = new DictionaryPage(asBytesInput(dictionaryData, 0, uncompressedSize),
          pageHeader.uncompressed_page_size, pageHeader.dictionary_page_header.num_values,
          valueOf(pageHeader.dictionary_page_header.encoding.name()));
      this.dictionary = page.getEncoding().initDictionary(parentStatus.columnDescriptor, page);
      long timeToDecode = timer.elapsed(TimeUnit.NANOSECONDS);
      stats.timeDictPageDecode.addAndGet(timeToDecode);
    } catch (Exception e) {
      handleAndThrowException(e, "Error decoding dictionary page.");
    }
  }

  private void handleAndThrowException(Exception e, String msg) throws UserException {
    UserException ex = UserException.dataReadError(e).message(msg)
        .pushContext("Row Group Start: ", this.parentColumnReader.columnChunkMetaData.getStartingPos())
        .pushContext("Column: ", this.parentColumnReader.schemaElement.getName())
        .pushContext("File: ", this.fileName).build(logger);
    throw ex;
  }

  private DrillBuf decompress(PageHeader pageHeader, DrillBuf compressedData) {
    DrillBuf pageDataBuf = null;
    Stopwatch timer = Stopwatch.createUnstarted();
    long timeToRead;
    int compressedSize = pageHeader.getCompressed_page_size();
    int uncompressedSize = pageHeader.getUncompressed_page_size();
    pageDataBuf = allocateTemporaryBuffer(uncompressedSize);
    try {
      timer.start();
      CompressionCodecName codecName = parentColumnReader.columnChunkMetaData.getCodec();
      ByteBuffer input = compressedData.nioBuffer(0, compressedSize);
      ByteBuffer output = pageDataBuf.nioBuffer(0, uncompressedSize);
      DecompressionHelper decompressionHelper = new DecompressionHelper(codecName);
      decompressionHelper.decompress(input, compressedSize, output, uncompressedSize);
      pageDataBuf.writerIndex(uncompressedSize);
      timeToRead = timer.elapsed(TimeUnit.NANOSECONDS);
      this.updateStats(pageHeader, "Decompress", 0, timeToRead, compressedSize, uncompressedSize);
    } catch (IOException e) {
      handleAndThrowException(e, "Error decompressing data.");
    }
    return pageDataBuf;
  }

  @Override protected void nextInternal() throws IOException {
    ReadStatus readStatus = null;
    try {
      Stopwatch timer = Stopwatch.createStarted();
      readStatus = asyncPageRead.get();
      long timeBlocked = timer.elapsed(TimeUnit.NANOSECONDS);
      stats.timeDiskScanWait.addAndGet(timeBlocked);
      stats.timeDiskScan.addAndGet(readStatus.getDiskScanTime());
      if (readStatus.isDictionaryPage) {
        stats.numDictPageLoads.incrementAndGet();
        stats.timeDictPageLoads.addAndGet(timeBlocked + readStatus.getDiskScanTime());
      } else {
        stats.numDataPageLoads.incrementAndGet();
        stats.timeDataPageLoads.addAndGet(timeBlocked + readStatus.getDiskScanTime());
      }
      pageHeader = readStatus.getPageHeader();
      // reset this. At the time of calling close, if this is not null then a pending asyncPageRead needs to be consumed
      asyncPageRead = null;
    } catch (Exception e) {
      handleAndThrowException(e, "Error reading page data.");
    }

    // TODO - figure out if we need multiple dictionary pages, I believe it may be limited to one
    // I think we are clobbering parts of the dictionary if there can be multiple pages of dictionary

    do {
      if (pageHeader.getType() == PageType.DICTIONARY_PAGE) {
        readDictionaryPageData(readStatus, parentColumnReader);
        // Ugly. Use the Async task to make a synchronous read call.
        readStatus = new AsyncPageReaderTask().call();
        pageHeader = readStatus.getPageHeader();
      }
    } while (pageHeader.getType() == PageType.DICTIONARY_PAGE);

    if (parentColumnReader.totalValuesRead + readStatus.getValuesRead()
        < parentColumnReader.columnChunkMetaData.getValueCount()) {
      asyncPageRead = threadPool.submit(new AsyncPageReaderTask());
    }

    pageHeader = readStatus.getPageHeader();
    pageData = getDecompressedPageData(readStatus);


  }


  @Override public void clear() {
    if (asyncPageRead != null) {
      try {
        final ReadStatus readStatus = asyncPageRead.get();
        readStatus.getPageData().release();
      } catch (Exception e) {
        // Do nothing.
      }
    }
    super.clear();
  }

  public static class ReadStatus {
    private PageHeader pageHeader;
    private DrillBuf pageData;
    private boolean isDictionaryPage = false;
    private long bytesRead = 0;
    private long valuesRead = 0;
    private long diskScanTime = 0;

    public synchronized PageHeader getPageHeader() {
      return pageHeader;
    }

    public synchronized void setPageHeader(PageHeader pageHeader) {
      this.pageHeader = pageHeader;
    }

    public synchronized DrillBuf getPageData() {
      return pageData;
    }

    public synchronized void setPageData(DrillBuf pageData) {
      this.pageData = pageData;
    }

    public synchronized boolean isDictionaryPage() {
      return isDictionaryPage;
    }

    public synchronized void setIsDictionaryPage(boolean isDictionaryPage) {
      this.isDictionaryPage = isDictionaryPage;
    }

    public synchronized long getBytesRead() {
      return bytesRead;
    }

    public synchronized void setBytesRead(long bytesRead) {
      this.bytesRead = bytesRead;
    }

    public synchronized long getValuesRead() {
      return valuesRead;
    }

    public synchronized void setValuesRead(long valuesRead) {
      this.valuesRead = valuesRead;
    }

    public long getDiskScanTime() {
      return diskScanTime;
    }

    public void setDiskScanTime(long diskScanTime) {
      this.diskScanTime = diskScanTime;
    }
  }


  private class AsyncPageReaderTask implements Callable<ReadStatus> {

    private final AsyncPageReader parent = AsyncPageReader.this;

    public AsyncPageReaderTask() {
    }

    @Override public ReadStatus call() throws IOException {
      ReadStatus readStatus = new ReadStatus();

      String oldname = Thread.currentThread().getName();
      String name = parent.parentColumnReader.columnChunkMetaData.toString();
      Thread.currentThread().setName(name);

      long bytesRead = 0;
      long valuesRead = 0;
      Stopwatch timer = Stopwatch.createStarted();

      DrillBuf pageData = null;
      try {
        long s = parent.dataReader.getPos();
        PageHeader pageHeader = Util.readPageHeader(parent.dataReader);
        long e = parent.dataReader.getPos();
        if (logger.isTraceEnabled()) {
          logger.trace("[{}]: Read Page Header : ReadPos = {} : Bytes Read = {} ", name, s, e - s);
        }
        int compressedSize = pageHeader.getCompressed_page_size();
        s = parent.dataReader.getPos();
        pageData = parent.dataReader.getNext(compressedSize);
        e = parent.dataReader.getPos();
        bytesRead = compressedSize;

        if (logger.isTraceEnabled()) {
          DrillBuf bufStart = pageData.slice(0, compressedSize>100?100:compressedSize);
          int endOffset = compressedSize>100?compressedSize-100:0;
          DrillBuf bufEnd = pageData.slice(endOffset, compressedSize-endOffset);
          logger
              .trace("[{}]: Read Page Data : ReadPos = {} : Bytes Read = {} : Buf Start = {} : Buf End = {} ",
                  name, s, e - s, ByteBufUtil.hexDump(bufStart), ByteBufUtil.hexDump(bufEnd));

        }

        synchronized (parent) {
          if (pageHeader.getType() == PageType.DICTIONARY_PAGE) {
            readStatus.setIsDictionaryPage(true);
            valuesRead += pageHeader.getDictionary_page_header().getNum_values();
          } else {
            valuesRead += pageHeader.getData_page_header().getNum_values();
          }
          long timeToRead = timer.elapsed(TimeUnit.NANOSECONDS);
          readStatus.setPageHeader(pageHeader);
          readStatus.setPageData(pageData);
          readStatus.setBytesRead(bytesRead);
          readStatus.setValuesRead(valuesRead);
          readStatus.setDiskScanTime(timeToRead);
        }

      } catch (Exception e) {
        if (pageData != null) {
          pageData.release();
        }
        throw e;
      }
      Thread.currentThread().setName(oldname);
      return readStatus;
    }

  }

  private class DecompressionHelper {
    final CompressionCodecName codecName;

    public DecompressionHelper(CompressionCodecName codecName) {
      this.codecName = codecName;
    }

    public void decompress (ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize)
        throws IOException {
      // GZip != thread_safe, so we go off and do our own thing.
      // The hadoop interface does not support ByteBuffer so we incur some
      // expensive copying.
      if (codecName == CompressionCodecName.GZIP) {
        GzipCodec codec = new GzipCodec();
        // DirectDecompressor: @see https://hadoop.apache.org/docs/r2.7.2/api/org/apache/hadoop/io/compress/DirectDecompressor.html
        DirectDecompressor directDecompressor = codec.createDirectDecompressor();
        if (directDecompressor != null) {
          logger.debug("Using GZIP direct decompressor.");
          directDecompressor.decompress(input, output);
        } else {
          logger.debug("Using GZIP (in)direct decompressor.");
          Decompressor decompressor = codec.createDecompressor();
          decompressor.reset();
          byte[] inputBytes = new byte[compressedSize];
          input.position(0);
          input.get(inputBytes);
          decompressor.setInput(inputBytes, 0, inputBytes.length);
          byte[] outputBytes = new byte[uncompressedSize];
          decompressor.decompress(outputBytes, 0, uncompressedSize);
          output.clear();
          output.put(outputBytes);
        }
      } else if (codecName == CompressionCodecName.SNAPPY) {
        // For Snappy, just call the Snappy decompressor directly instead
        // of going thru the DirectDecompressor class.
        // The Snappy codec is itself thread safe, while going thru the DirectDecompressor path
        // seems to have concurrency issues.
        output.clear();
        int size = Snappy.uncompress(input, output);
        output.limit(size);
      } else {
        CodecFactory.BytesDecompressor decompressor = codecFactory.getDecompressor(parentColumnReader.columnChunkMetaData.getCodec());
        decompressor.decompress(input, compressedSize, output, uncompressedSize);
      }
    }


  }

}
