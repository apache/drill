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

import com.google.common.collect.Lists;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.ParquetOutputRecordWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import parquet.column.ParquetProperties.WriterVersion;
import parquet.column.impl.ColumnWriteStoreImpl;
import parquet.column.page.PageWriteStore;
import parquet.hadoop.ColumnChunkPageWriteStoreExposer;
import parquet.hadoop.ParquetFileWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.io.api.RecordConsumer;
import parquet.schema.DecimalMetadata;
import parquet.schema.MessageType;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type;
import parquet.schema.Type.Repetition;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;

public class ParquetRecordWriter extends ParquetOutputRecordWriter {

  private static final int MINIMUM_BUFFER_SIZE = 64 * 1024;
  private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 100;
  private static final int MAXIMUM_RECORD_COUNT_FOR_CHECK = 10000;

  private ParquetFileWriter w;
  private MessageType schema;
  private Map<String, String> extraMetaData = new HashMap();
  private int blockSize;
  private int pageSize = 1 * 1024 * 1024;
  private int dictionaryPageSize = pageSize;
  private boolean enableDictionary = false;
  private boolean validating = false;
  private CompressionCodecName codec = CompressionCodecName.SNAPPY;
  private WriterVersion writerVersion = WriterVersion.PARQUET_1_0;

  private long recordCount = 0;
  private long recordCountForNextMemCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;

  private ColumnWriteStoreImpl store;
  private PageWriteStore pageStore;

  private RecordConsumer consumer;
  private BatchSchema batchSchema;

  private Configuration conf;
  private String location;
  private String prefix;
  private int index = 0;

  @Override
  public void init(Map<String, String> writerOptions) throws IOException {
    this.location = writerOptions.get("location");
    this.prefix = writerOptions.get("prefix");

    conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, writerOptions.get(FileSystem.FS_DEFAULT_NAME_KEY));
    blockSize = Integer.parseInt(writerOptions.get(ExecConstants.PARQUET_BLOCK_SIZE));
  }

  @Override
  public void updateSchema(BatchSchema batchSchema) throws IOException {
    if (this.batchSchema == null || !this.batchSchema.equals(batchSchema)) {
      if (this.batchSchema != null) {
        flush();
      }
      this.batchSchema = batchSchema;
      newSchema();
    }
  }

  private void newSchema() throws IOException {
    List<Type> types = Lists.newArrayList();
    for (MaterializedField field : batchSchema) {
      String name = field.getAsSchemaPath().getAsUnescapedPath();
      MinorType minorType = field.getType().getMinorType();
      PrimitiveTypeName primitiveTypeName = ParquetTypeHelper.getPrimitiveTypeNameForMinorType(minorType);
      Repetition repetition = ParquetTypeHelper.getRepetitionForDataMode(field.getDataMode());
      OriginalType originalType = ParquetTypeHelper.getOriginalTypeForMinorType(minorType);
      DecimalMetadata decimalMetadata = ParquetTypeHelper.getDecimalMetadataForField(field);
      int length = ParquetTypeHelper.getLengthForMinorType(minorType);
      parquet.schema.Type type = new parquet.schema.PrimitiveType(repetition, primitiveTypeName, length, name, originalType, decimalMetadata);
      types.add(type);
    }
    schema = new MessageType("root", types);

    Path fileName = new Path(location, prefix + "_" + index + ".parquet");
    w = new ParquetFileWriter(conf, schema, fileName);
    w.start();

    int initialBlockBufferSize = max(MINIMUM_BUFFER_SIZE, blockSize / this.schema.getColumns().size() / 5);
    pageStore = ColumnChunkPageWriteStoreExposer.newColumnChunkPageWriteStore(codec, pageSize, this.schema, initialBlockBufferSize);
    int initialPageBufferSize = max(MINIMUM_BUFFER_SIZE, min(pageSize + pageSize / 10, initialBlockBufferSize));
    store = new ColumnWriteStoreImpl(pageStore, pageSize, initialPageBufferSize, dictionaryPageSize, enableDictionary, writerVersion);
    MessageColumnIO columnIO = new ColumnIOFactory(validating).getColumnIO(this.schema);
    consumer = columnIO.getRecordWriter(store);
    setUp(schema, consumer);
  }

  private void flush() throws IOException {
    w.startBlock(recordCount);
    store.flush();
    ColumnChunkPageWriteStoreExposer.flushPageStore(pageStore, w);
    recordCount = 0;
    w.endBlock();
    w.end(extraMetaData);
    store = null;
    pageStore = null;
    index++;
  }

  private void checkBlockSizeReached() throws IOException {
    if (recordCount >= recordCountForNextMemCheck) { // checking the memory size is relatively expensive, so let's not do it for every record.
      long memSize = store.memSize();
      if (memSize > blockSize) {
        System.out.println("Reached block size " + blockSize);
        flush();
        newSchema();
        recordCountForNextMemCheck = min(max(MINIMUM_RECORD_COUNT_FOR_CHECK, recordCount / 2), MAXIMUM_RECORD_COUNT_FOR_CHECK);
      } else {
        float recordSize = (float) memSize / recordCount;
        recordCountForNextMemCheck = min(
                max(MINIMUM_RECORD_COUNT_FOR_CHECK, (recordCount + (long)(blockSize / recordSize)) / 2), // will check halfway
                recordCount + MAXIMUM_RECORD_COUNT_FOR_CHECK // will not look more than max records ahead
        );
      }
    }
  }

  @Override
  public void startRecord() throws IOException {
    consumer.startMessage();
  }

  @Override
  public void endRecord() throws IOException {
    consumer.endMessage();
    recordCount++;
    checkBlockSizeReached();
  }

  @Override
  public void abort() throws IOException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void cleanup() throws IOException {
    flush();
  }
}
