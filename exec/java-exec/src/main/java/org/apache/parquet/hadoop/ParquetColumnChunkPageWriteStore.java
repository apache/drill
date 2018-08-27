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
package org.apache.parquet.hadoop;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;
import org.apache.drill.exec.store.parquet.ParquetDirectByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.CodecFactory.BytesCompressor;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.bytes.ByteBufferAllocator;

/**
 * This is a copy of ColumnChunkPageWriteStore from parquet library except of OutputStream that is used here.
 * Using of CapacityByteArrayOutputStream allows to use different ByteBuffer allocators.
 * It will be no need in this class once PARQUET-1006 is resolved.
 */
public class ParquetColumnChunkPageWriteStore implements PageWriteStore, Closeable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetDirectByteBufferAllocator.class);

  private static ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

  private final Map<ColumnDescriptor, ColumnChunkPageWriter> writers = Maps.newHashMap();
  private final MessageType schema;

  public ParquetColumnChunkPageWriteStore(BytesCompressor compressor,
                                          MessageType schema,
                                          int initialSlabSize,
                                          int maxCapacityHint,
                                          ByteBufferAllocator allocator) {
    this.schema = schema;
    for (ColumnDescriptor path : schema.getColumns()) {
      writers.put(path,  new ColumnChunkPageWriter(path, compressor, initialSlabSize, maxCapacityHint, allocator));
    }
  }

  @Override
  public PageWriter getPageWriter(ColumnDescriptor path) {
    return writers.get(path);
  }

  /**
   * Writes the column chunks in the corresponding row group
   * @param writer the parquet file writer
   * @throws IOException if the file can not be created
   */
  public void flushToFileWriter(ParquetFileWriter writer) throws IOException {
    for (ColumnDescriptor path : schema.getColumns()) {
      ColumnChunkPageWriter pageWriter = writers.get(path);
      pageWriter.writeToFileWriter(writer);
    }
  }

  @Override
  public void close() {
    for (ColumnChunkPageWriter pageWriter : writers.values()) {
      pageWriter.close();
    }
  }

  private static final class ColumnChunkPageWriter implements PageWriter, Closeable {

    private final ColumnDescriptor path;
    private final BytesCompressor compressor;

    private final CapacityByteArrayOutputStream buf;
    private DictionaryPage dictionaryPage;

    private long uncompressedLength;
    private long compressedLength;
    private long totalValueCount;
    private int pageCount;

    // repetition and definition level encodings are used only for v1 pages and don't change
    private Set<Encoding> rlEncodings = Sets.newHashSet();
    private Set<Encoding> dlEncodings = Sets.newHashSet();
    private List<Encoding> dataEncodings = Lists.newArrayList();

    private Statistics totalStatistics;

    private ColumnChunkPageWriter(ColumnDescriptor path,
                                  BytesCompressor compressor,
                                  int initialSlabSize,
                                  int maxCapacityHint,
                                  ByteBufferAllocator allocator) {
      this.path = path;
      this.compressor = compressor;
      this.buf = new CapacityByteArrayOutputStream(initialSlabSize, maxCapacityHint, allocator);
      this.totalStatistics = Statistics.createStats(this.path.getPrimitiveType());
    }

    @Override
    public void writePage(BytesInput bytes,
                          int valueCount,
                          Statistics statistics,
                          Encoding rlEncoding,
                          Encoding dlEncoding,
                          Encoding valuesEncoding) throws IOException {
      long uncompressedSize = bytes.size();
      // Parquet library creates bad metadata if the uncompressed or compressed size of a page exceeds Integer.MAX_VALUE
      if (uncompressedSize > Integer.MAX_VALUE) {
        throw new ParquetEncodingException(
            "Cannot write page larger than Integer.MAX_VALUE bytes: " +
                uncompressedSize);
      }
      BytesInput compressedBytes = compressor.compress(bytes);
      long compressedSize = compressedBytes.size();
      if (compressedSize > Integer.MAX_VALUE) {
        throw new ParquetEncodingException(
            "Cannot write compressed page larger than Integer.MAX_VALUE bytes: "
                + compressedSize);
      }
      parquetMetadataConverter.writeDataPageHeader(
          (int)uncompressedSize,
          (int)compressedSize,
          valueCount,
          statistics,
          rlEncoding,
          dlEncoding,
          valuesEncoding,
          buf);
      this.uncompressedLength += uncompressedSize;
      this.compressedLength += compressedSize;
      this.totalValueCount += valueCount;
      this.pageCount += 1;
      this.totalStatistics.mergeStatistics(statistics);
      compressedBytes.writeAllTo(buf);
      rlEncodings.add(rlEncoding);
      dlEncodings.add(dlEncoding);
      dataEncodings.add(valuesEncoding);
    }

    @Override
    public void writePageV2(int rowCount,
                            int nullCount,
                            int valueCount,
                            BytesInput repetitionLevels,
                            BytesInput definitionLevels,
                            Encoding dataEncoding,
                            BytesInput data,
                            Statistics<?> statistics) throws IOException {
      int rlByteLength = toIntWithCheck(repetitionLevels.size());
      int dlByteLength = toIntWithCheck(definitionLevels.size());
      int uncompressedSize = toIntWithCheck(
          data.size() + repetitionLevels.size() + definitionLevels.size()
      );
      BytesInput compressedData = compressor.compress(data);
      int compressedSize = toIntWithCheck(
          compressedData.size() + repetitionLevels.size() + definitionLevels.size()
      );
      parquetMetadataConverter.writeDataPageV2Header(
          uncompressedSize, compressedSize,
          valueCount, nullCount, rowCount,
          statistics,
          dataEncoding,
          rlByteLength,
          dlByteLength,
          buf);
      this.uncompressedLength += uncompressedSize;
      this.compressedLength += compressedSize;
      this.totalValueCount += valueCount;
      this.pageCount += 1;
      this.totalStatistics.mergeStatistics(statistics);

      definitionLevels.writeAllTo(buf);
      compressedData.writeAllTo(buf);

      dataEncodings.add(dataEncoding);
    }

    private int toIntWithCheck(long size) {
      if (size > Integer.MAX_VALUE) {
        throw new ParquetEncodingException(
            "Cannot write page larger than " + Integer.MAX_VALUE + " bytes: " +
                size);
      }
      return (int)size;
    }

    @Override
    public long getMemSize() {
      return buf.size();
    }

    /**
     * Writes a number of pages within corresponding column chunk
     * @param writer the parquet file writer
     * @throws IOException if the file can not be created
     */
    public void writeToFileWriter(ParquetFileWriter writer) throws IOException {
      writer.startColumn(path, totalValueCount, compressor.getCodecName());
      if (dictionaryPage != null) {
        writer.writeDictionaryPage(dictionaryPage);
        // tracking the dictionary encoding is handled in writeDictionaryPage
      }
      writer.writeDataPages(BytesInput.from(buf), uncompressedLength, compressedLength, totalStatistics, rlEncodings, dlEncodings, dataEncodings);
      writer.endColumn();
      logger.debug(
          String.format(
              "written %,dB for %s: %,d values, %,dB raw, %,dB comp, %d pages, encodings: %s",
              buf.size(), path, totalValueCount, uncompressedLength, compressedLength, pageCount, Sets.newHashSet(dataEncodings))
              + (dictionaryPage != null ? String.format(
              ", dic { %,d entries, %,dB raw, %,dB comp}",
              dictionaryPage.getDictionarySize(), dictionaryPage.getUncompressedSize(), dictionaryPage.getDictionarySize())
              : ""));
      rlEncodings.clear();
      dlEncodings.clear();
      dataEncodings.clear();
      pageCount = 0;
    }

    @Override
    public long allocatedSize() {
      return buf.getCapacity();
    }

    @Override
    public void writeDictionaryPage(DictionaryPage dictionaryPage) throws IOException {
      if (this.dictionaryPage != null) {
        throw new ParquetEncodingException("Only one dictionary page is allowed");
      }
      BytesInput dictionaryBytes = dictionaryPage.getBytes();
      int uncompressedSize = (int)dictionaryBytes.size();
      BytesInput compressedBytes = compressor.compress(dictionaryBytes);
      this.dictionaryPage = new DictionaryPage(BytesInput.copy(compressedBytes), uncompressedSize,
          dictionaryPage.getDictionarySize(), dictionaryPage.getEncoding());
    }

    @Override
    public String memUsageString(String prefix) {
      return buf.memUsageString(prefix + " ColumnChunkPageWriter");
    }

    @Override
    public void close() {
      buf.close();
    }
  }

}
