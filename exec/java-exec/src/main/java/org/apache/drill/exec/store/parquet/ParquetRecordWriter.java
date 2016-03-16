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

import static java.lang.Math.max;
import static java.lang.Math.min;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.util.DrillVersionInfo;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.planner.physical.WriterPrel;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.store.EventBasedRecordWriter;
import org.apache.drill.exec.store.EventBasedRecordWriter.FieldConverter;
import org.apache.drill.exec.store.ParquetOutputRecordWriter;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.impl.ColumnWriteStoreV1;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ColumnChunkPageWriteStoreExposer;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import com.google.common.collect.Lists;

public class ParquetRecordWriter extends ParquetOutputRecordWriter {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetRecordWriter.class);

  private static final int MINIMUM_BUFFER_SIZE = 64 * 1024;
  private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 100;
  private static final int MAXIMUM_RECORD_COUNT_FOR_CHECK = 10000;

  public static final String DRILL_VERSION_PROPERTY = "drill.version";

  private ParquetFileWriter parquetFileWriter;
  private MessageType schema;
  private Map<String, String> extraMetaData = new HashMap<>();
  private int blockSize;
  private int pageSize;
  private int dictionaryPageSize;
  private boolean enableDictionary = false;
  private CompressionCodecName codec = CompressionCodecName.SNAPPY;
  private WriterVersion writerVersion = WriterVersion.PARQUET_1_0;
  private CodecFactory codecFactory;

  private long recordCount = 0;
  private long recordCountForNextMemCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;

  private ColumnWriteStore store;
  private PageWriteStore pageStore;

  private RecordConsumer consumer;
  private BatchSchema batchSchema;

  private Configuration conf;
  private String location;
  private String prefix;
  private int index = 0;
  private OperatorContext oContext;
  private List<String> partitionColumns;
  private boolean hasPartitions;

  public ParquetRecordWriter(FragmentContext context, ParquetWriter writer) throws OutOfMemoryException{
    super();
    this.oContext = context.newOperatorContext(writer);
    this.codecFactory = CodecFactory.createDirectCodecFactory(writer.getFormatPlugin().getFsConf(),
        new ParquetDirectByteBufferAllocator(oContext.getAllocator()), pageSize);
    this.partitionColumns = writer.getPartitionColumns();
    this.hasPartitions = partitionColumns != null && partitionColumns.size() > 0;
    this.extraMetaData.put(DRILL_VERSION_PROPERTY, DrillVersionInfo.getVersion());
  }

  @Override
  public void init(Map<String, String> writerOptions) throws IOException {
    this.location = writerOptions.get("location");
    this.prefix = writerOptions.get("prefix");

    conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, writerOptions.get(FileSystem.FS_DEFAULT_NAME_KEY));
    blockSize = Integer.parseInt(writerOptions.get(ExecConstants.PARQUET_BLOCK_SIZE));
    pageSize = Integer.parseInt(writerOptions.get(ExecConstants.PARQUET_PAGE_SIZE));
    dictionaryPageSize= Integer.parseInt(writerOptions.get(ExecConstants.PARQUET_DICT_PAGE_SIZE));
    String codecName = writerOptions.get(ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE).toLowerCase();
    switch(codecName) {
    case "snappy":
      codec = CompressionCodecName.SNAPPY;
      break;
    case "lzo":
      codec = CompressionCodecName.LZO;
      break;
    case "gzip":
      codec = CompressionCodecName.GZIP;
      break;
    case "none":
    case "uncompressed":
      codec = CompressionCodecName.UNCOMPRESSED;
      break;
    default:
      throw new UnsupportedOperationException(String.format("Unknown compression type: %s", codecName));
    }

    enableDictionary = Boolean.parseBoolean(writerOptions.get(ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING));
  }

  private boolean containsComplexVectors(BatchSchema schema) {
    for (MaterializedField field : schema) {
      MinorType type = field.getType().getMinorType();
      switch (type) {
      case MAP:
      case LIST:
        return true;
      default:
      }
    }
    return false;
  }

  @Override
  public void updateSchema(VectorAccessible batch) throws IOException {
    if (this.batchSchema == null || !this.batchSchema.equals(batch.getSchema()) || containsComplexVectors(this.batchSchema)) {
      if (this.batchSchema != null) {
        flush();
      }
      this.batchSchema = batch.getSchema();
      newSchema();
    }
    TypedFieldId fieldId = batch.getValueVectorId(SchemaPath.getSimplePath(WriterPrel.PARTITION_COMPARATOR_FIELD));
    if (fieldId != null) {
      VectorWrapper w = batch.getValueAccessorById(BitVector.class, fieldId.getFieldIds());
      setPartitionVector((BitVector) w.getValueVector());
    }
  }

  private void newSchema() throws IOException {
    List<Type> types = Lists.newArrayList();
    for (MaterializedField field : batchSchema) {
      if (field.getPath().equalsIgnoreCase(WriterPrel.PARTITION_COMPARATOR_FIELD)) {
        continue;
      }
      types.add(getType(field));
    }
    schema = new MessageType("root", types);

    int initialBlockBufferSize = max(MINIMUM_BUFFER_SIZE, blockSize / this.schema.getColumns().size() / 5);
    pageStore = ColumnChunkPageWriteStoreExposer.newColumnChunkPageWriteStore(this.oContext,
        codecFactory.getCompressor(codec),
        schema);
    int initialPageBufferSize = max(MINIMUM_BUFFER_SIZE, min(pageSize + pageSize / 10, initialBlockBufferSize));
    store = new ColumnWriteStoreV1(pageStore, pageSize, initialPageBufferSize, enableDictionary,
        writerVersion, new ParquetDirectByteBufferAllocator(oContext));
    MessageColumnIO columnIO = new ColumnIOFactory(false).getColumnIO(this.schema);
    consumer = columnIO.getRecordWriter(store);
    setUp(schema, consumer);
  }

  private PrimitiveType getPrimitiveType(MaterializedField field) {
    MinorType minorType = field.getType().getMinorType();
    String name = field.getLastName();
    PrimitiveTypeName primitiveTypeName = ParquetTypeHelper.getPrimitiveTypeNameForMinorType(minorType);
    Repetition repetition = ParquetTypeHelper.getRepetitionForDataMode(field.getDataMode());
    OriginalType originalType = ParquetTypeHelper.getOriginalTypeForMinorType(minorType);
    DecimalMetadata decimalMetadata = ParquetTypeHelper.getDecimalMetadataForField(field);
    int length = ParquetTypeHelper.getLengthForMinorType(minorType);
    return new PrimitiveType(repetition, primitiveTypeName, length, name, originalType, decimalMetadata, null);
  }

  private Type getType(MaterializedField field) {
    MinorType minorType = field.getType().getMinorType();
    DataMode dataMode = field.getType().getMode();
    switch(minorType) {
      case MAP:
        List<Type> types = Lists.newArrayList();
        for (MaterializedField childField : field.getChildren()) {
          types.add(getType(childField));
        }
        return new GroupType(dataMode == DataMode.REPEATED ? Repetition.REPEATED : Repetition.OPTIONAL, field.getLastName(), types);
      case LIST:
        throw new UnsupportedOperationException("Unsupported type " + minorType);
      default:
        return getPrimitiveType(field);
    }
  }

  @Override
  public void checkForNewPartition(int index) {
    if (!hasPartitions) {
      return;
    }
    try {
      boolean newPartition = newPartition(index);
      if (newPartition) {
        flush();
        newSchema();
      }
    } catch (Exception e) {
      throw new DrillRuntimeException(e);
    }
  }

  private void flush() throws IOException {
    if (recordCount > 0) {
      parquetFileWriter.startBlock(recordCount);
      consumer.flush();
      store.flush();
      ColumnChunkPageWriteStoreExposer.flushPageStore(pageStore, parquetFileWriter);
      recordCount = 0;
      parquetFileWriter.endBlock();

      // we are writing one single block per file
      parquetFileWriter.end(extraMetaData);
      parquetFileWriter = null;
    }

    store.close();
    // TODO(jaltekruse) - review this close method should no longer be necessary
//    ColumnChunkPageWriteStoreExposer.close(pageStore);

    store = null;
    pageStore = null;
    index++;
  }

  private void checkBlockSizeReached() throws IOException {
    if (recordCount >= recordCountForNextMemCheck) { // checking the memory size is relatively expensive, so let's not do it for every record.
      long memSize = store.getBufferedSize();
      if (memSize > blockSize) {
        logger.debug("Reached block size " + blockSize);
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
  public FieldConverter getNewMapConverter(int fieldId, String fieldName, FieldReader reader) {
    return new MapParquetConverter(fieldId, fieldName, reader);
  }

  public class MapParquetConverter extends FieldConverter {
    List<FieldConverter> converters = Lists.newArrayList();

    public MapParquetConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
      int i = 0;
      for (String name : reader) {
        FieldConverter converter = EventBasedRecordWriter.getConverter(ParquetRecordWriter.this, i++, name, reader.reader(name));
        converters.add(converter);
      }
    }

    @Override
    public void writeField() throws IOException {
      consumer.startField(fieldName, fieldId);
      consumer.startGroup();
      for (FieldConverter converter : converters) {
        converter.writeField();
      }
      consumer.endGroup();
      consumer.endField(fieldName, fieldId);
    }
  }

  @Override
  public FieldConverter getNewRepeatedMapConverter(int fieldId, String fieldName, FieldReader reader) {
    return new RepeatedMapParquetConverter(fieldId, fieldName, reader);
  }

  public class RepeatedMapParquetConverter extends FieldConverter {
    List<FieldConverter> converters = Lists.newArrayList();

    public RepeatedMapParquetConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
      int i = 0;
      for (String name : reader) {
        FieldConverter converter = EventBasedRecordWriter.getConverter(ParquetRecordWriter.this, i++, name, reader.reader(name));
        converters.add(converter);
      }
    }

    @Override
    public void writeField() throws IOException {
      if (reader.size() == 0) {
        return;
      }
      consumer.startField(fieldName, fieldId);
      while (reader.next()) {
        consumer.startGroup();
        for (FieldConverter converter : converters) {
          converter.writeField();
        }
        consumer.endGroup();
      }
      consumer.endField(fieldName, fieldId);
    }
  }


  @Override
  public void startRecord() throws IOException {
    consumer.startMessage();
  }

  @Override
  public void endRecord() throws IOException {
    consumer.endMessage();

    // we wait until there is at least one record before creating the parquet file
    if (parquetFileWriter == null) {
      Path path = new Path(location, prefix + "_" + index + ".parquet");
      parquetFileWriter = new ParquetFileWriter(conf, schema, path);
      parquetFileWriter.start();
    }

    recordCount++;

    checkBlockSizeReached();
  }

  @Override
  public void abort() throws IOException {
  }

  @Override
  public void cleanup() throws IOException {
    flush();

    codecFactory.release();
  }
}
