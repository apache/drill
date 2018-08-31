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

import static org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetStatistics;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.CodecFactory.BytesDecompressor;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopStreams;

import io.netty.buffer.ByteBuf;


public class ColumnChunkIncReadStore implements PageReadStore {

  private static ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

  private CodecFactory codecFactory;
  private BufferAllocator allocator;
  private FileSystem fs;
  private Path path;
  private long rowCount;
  private List<FSDataInputStream> streams = new ArrayList<>();

  public ColumnChunkIncReadStore(long rowCount, CodecFactory codecFactory, BufferAllocator allocator,
      FileSystem fs, Path path) {
    this.codecFactory = codecFactory;
    this.allocator = allocator;
    this.fs = fs;
    this.path = path;
    this.rowCount = rowCount;
  }


  public class ColumnChunkIncPageReader implements PageReader {

    ColumnChunkMetaData metaData;
    ColumnDescriptor columnDescriptor;
    long fileOffset;
    long size;
    private long valueReadSoFar = 0;

    private DictionaryPage dictionaryPage;
    private FSDataInputStream in;
    private BytesDecompressor decompressor;

    private ByteBuf lastPage;

    public ColumnChunkIncPageReader(ColumnChunkMetaData metaData, ColumnDescriptor columnDescriptor, FSDataInputStream in) throws IOException {
      this.metaData = metaData;
      this.columnDescriptor = columnDescriptor;
      this.size = metaData.getTotalSize();
      this.fileOffset = metaData.getStartingPos();
      this.in = in;
      this.decompressor = codecFactory.getDecompressor(metaData.getCodec());
    }

    @Override
    public DictionaryPage readDictionaryPage() {
      if (dictionaryPage == null) {
        PageHeader pageHeader = new PageHeader();
        long pos = 0;
        try {
          pos = in.getPos();
          pageHeader = Util.readPageHeader(in);
          if (pageHeader.getDictionary_page_header() == null) {
            in.seek(pos);
            return null;
          }
          dictionaryPage =
                  new DictionaryPage(
                          decompressor.decompress(BytesInput.from(in, pageHeader.compressed_page_size), pageHeader.getUncompressed_page_size()),
                          pageHeader.getDictionary_page_header().getNum_values(),
                          parquetMetadataConverter.getEncoding(pageHeader.dictionary_page_header.encoding)
                  );
        } catch (Exception e) {
          throw new DrillRuntimeException("Error reading dictionary page." +
            "\nFile path: " + path.toUri().getPath() +
            "\nRow count: " + rowCount +
            "\nColumn Chunk Metadata: " + metaData +
            "\nPage Header: " + pageHeader +
            "\nFile offset: " + fileOffset +
            "\nSize: " + size +
            "\nValue read so far: " + valueReadSoFar +
            "\nPosition: " + pos, e);
        }
      }
      return dictionaryPage;
    }

    @Override
    public long getTotalValueCount() {
      return metaData.getValueCount();
    }

    @Override
    public DataPage readPage() {
      PageHeader pageHeader = new PageHeader();
      try {
        if (lastPage != null) {
          lastPage.release();
          lastPage = null;
        }
        while(valueReadSoFar < metaData.getValueCount()) {
          pageHeader = Util.readPageHeader(in);
          int uncompressedPageSize = pageHeader.getUncompressed_page_size();
          int compressedPageSize = pageHeader.getCompressed_page_size();
          switch (pageHeader.type) {
            case DICTIONARY_PAGE:
              if (dictionaryPage == null) {
                dictionaryPage =
                        new DictionaryPage(
                                decompressor.decompress(BytesInput.from(in, pageHeader.compressed_page_size), pageHeader.getUncompressed_page_size()),
                                pageHeader.uncompressed_page_size,
                                parquetMetadataConverter.getEncoding(pageHeader.dictionary_page_header.encoding)
                        );
              } else {
                in.skip(pageHeader.compressed_page_size);
              }
              break;
            case DATA_PAGE:
              valueReadSoFar += pageHeader.data_page_header.getNum_values();
              ByteBuf buf = allocator.buffer(pageHeader.compressed_page_size);
              lastPage = buf;
              ByteBuffer buffer = buf.nioBuffer(0, pageHeader.compressed_page_size);
              HadoopStreams.wrap(in).readFully(buffer);
              buffer.flip();
              return new DataPageV1(
                      decompressor.decompress(BytesInput.from(buffer), pageHeader.getUncompressed_page_size()),
                      pageHeader.data_page_header.num_values,
                      pageHeader.uncompressed_page_size,
                      fromParquetStatistics(pageHeader.data_page_header.statistics, columnDescriptor.getType()),
                      parquetMetadataConverter.getEncoding(pageHeader.data_page_header.repetition_level_encoding),
                      parquetMetadataConverter.getEncoding(pageHeader.data_page_header.definition_level_encoding),
                      parquetMetadataConverter.getEncoding(pageHeader.data_page_header.encoding)
              );
            // TODO - finish testing this with more files
            case DATA_PAGE_V2:
              valueReadSoFar += pageHeader.data_page_header_v2.getNum_values();
              buf = allocator.buffer(pageHeader.compressed_page_size);
              lastPage = buf;
              buffer = buf.nioBuffer(0, pageHeader.compressed_page_size);
              HadoopStreams.wrap(in).readFully(buffer);
              buffer.flip();
              DataPageHeaderV2 dataHeaderV2 = pageHeader.getData_page_header_v2();
              int dataSize = compressedPageSize - dataHeaderV2.getRepetition_levels_byte_length() - dataHeaderV2.getDefinition_levels_byte_length();
              BytesInput decompressedPageData =
                  decompressor.decompress(
                      BytesInput.from(buffer),
                      pageHeader.uncompressed_page_size);
              ByteBuffer byteBuffer = decompressedPageData.toByteBuffer();
              int limit = byteBuffer.limit();
              byteBuffer.limit(dataHeaderV2.getRepetition_levels_byte_length());
              BytesInput repetitionLevels = BytesInput.from(byteBuffer.slice());
              byteBuffer.position(dataHeaderV2.getRepetition_levels_byte_length());
              byteBuffer.limit(dataHeaderV2.getRepetition_levels_byte_length() + dataHeaderV2.getDefinition_levels_byte_length());
              BytesInput definitionLevels = BytesInput.from(byteBuffer.slice());
              byteBuffer.position(dataHeaderV2.getRepetition_levels_byte_length() + dataHeaderV2.getDefinition_levels_byte_length());
              byteBuffer.limit(limit);
              BytesInput data = BytesInput.from(byteBuffer.slice());

              return new DataPageV2(
                      dataHeaderV2.getNum_rows(),
                      dataHeaderV2.getNum_nulls(),
                      dataHeaderV2.getNum_values(),
                      repetitionLevels,
                      definitionLevels,
                      parquetMetadataConverter.getEncoding(dataHeaderV2.getEncoding()),
                      data,
                      uncompressedPageSize,
                      fromParquetStatistics(dataHeaderV2.getStatistics(), columnDescriptor.getType()),
                      dataHeaderV2.isIs_compressed()
                  );
            default:
              in.skip(pageHeader.compressed_page_size);
              break;
          }
        }
        in.close();
        return null;
      } catch (OutOfMemoryException e) {
        throw e; // throw as it is
      } catch (Exception e) {
        throw new DrillRuntimeException("Error reading page." +
          "\nFile path: " + path.toUri().getPath() +
          "\nRow count: " + rowCount +
          "\nColumn Chunk Metadata: " + metaData +
          "\nPage Header: " + pageHeader +
          "\nFile offset: " + fileOffset +
          "\nSize: " + size +
          "\nValue read so far: " + valueReadSoFar, e);
      }
    }

    void close() {
      if (lastPage != null) {
        lastPage.release();
        lastPage = null;
      }
    }
  }

  private Map<ColumnDescriptor, ColumnChunkIncPageReader> columns = new HashMap<>();

  public void addColumn(ColumnDescriptor descriptor, ColumnChunkMetaData metaData) throws IOException {
    FSDataInputStream in = fs.open(path);
    streams.add(in);
    in.seek(metaData.getStartingPos());
    ColumnChunkIncPageReader reader = new ColumnChunkIncPageReader(metaData, descriptor, in);

    columns.put(descriptor, reader);
  }

  public void close() throws IOException {
    for (FSDataInputStream stream : streams) {
      stream.close();
    }
    for (ColumnChunkIncPageReader reader : columns.values()) {
      reader.close();
    }
  }

  @Override
  public PageReader getPageReader(ColumnDescriptor descriptor) {
    return columns.get(descriptor);
  }

  @Override
  public long getRowCount() {
    return rowCount;
  }

  @Override
  public String toString() {
    return "ColumnChunkIncReadStore[File=" + path.toUri() + "]";
  }
}
