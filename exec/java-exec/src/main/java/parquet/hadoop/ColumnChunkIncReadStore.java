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
package parquet.hadoop;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import parquet.bytes.BytesInput;
import parquet.column.ColumnDescriptor;
import parquet.column.page.DictionaryPage;
import parquet.column.page.Page;
import parquet.column.page.PageReadStore;
import parquet.column.page.PageReader;
import parquet.format.PageHeader;
import parquet.format.Util;
import parquet.format.converter.ParquetMetadataConverter;
import parquet.hadoop.CodecFactory.BytesDecompressor;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.util.CompatibilityUtil;


public class ColumnChunkIncReadStore implements PageReadStore {

  private static ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

  private CodecFactory codecFactory = new CodecFactory(new Configuration());
  private BufferAllocator allocator;
  private FileSystem fs;
  private Path path;
  private long rowCount;
  private List<FSDataInputStream> streams = new ArrayList();

  public ColumnChunkIncReadStore(long rowCount, CodecFactory codecFactory, BufferAllocator allocator, FileSystem fs, Path path) {
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

    public ColumnChunkIncPageReader(ColumnChunkMetaData metaData, ColumnDescriptor columnDescriptor, FSDataInputStream in) {
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
        try {
          long pos = in.getPos();
          PageHeader pageHeader = Util.readPageHeader(in);
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
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return dictionaryPage;
    }

    @Override
    public long getTotalValueCount() {
      return metaData.getValueCount();
    }

    @Override
    public Page readPage() {
      try {
        if (lastPage != null) {
          lastPage.release();
          lastPage = null;
        }
        while(valueReadSoFar < metaData.getValueCount()) {
          PageHeader pageHeader = Util.readPageHeader(in);
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
              while (buffer.remaining() > 0) {
                CompatibilityUtil.getBuf(in, buffer, pageHeader.compressed_page_size);
              }
              return new Page(
                      decompressor.decompress(BytesInput.from(buffer, 0, pageHeader.compressed_page_size), pageHeader.getUncompressed_page_size()),
                      pageHeader.data_page_header.num_values,
                      pageHeader.uncompressed_page_size,
                      parquetMetadataConverter.fromParquetStatistics(pageHeader.data_page_header.statistics, columnDescriptor.getType()),
                      parquetMetadataConverter.getEncoding(pageHeader.data_page_header.repetition_level_encoding),
                      parquetMetadataConverter.getEncoding(pageHeader.data_page_header.definition_level_encoding),
                      parquetMetadataConverter.getEncoding(pageHeader.data_page_header.encoding)
              );
            default:
              in.skip(pageHeader.compressed_page_size);
              break;
          }
        }
        in.close();
        return null;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    void close() {
      if (lastPage != null) {
        lastPage.release();
        lastPage = null;
      }
    }
  }

  private Map<ColumnDescriptor, ColumnChunkIncPageReader> columns = new HashMap();

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
}
