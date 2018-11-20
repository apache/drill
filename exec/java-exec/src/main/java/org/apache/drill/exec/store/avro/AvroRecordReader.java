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
package org.apache.drill.exec.store.avro;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.util.Utf8;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.parquet.ParquetReaderUtility;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.exec.vector.complex.fn.FieldSelection;
import org.apache.drill.exec.vector.complex.impl.MapOrListWriterImpl;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;

import io.netty.buffer.DrillBuf;
import org.joda.time.DateTimeConstants;

/**
 * A RecordReader implementation for Avro data files.
 *
 * @see org.apache.drill.exec.store.RecordReader
 */
public class AvroRecordReader extends AbstractRecordReader {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AvroRecordReader.class);

  private final Path hadoop;
  private final long start;
  private final long end;
  private final FieldSelection fieldSelection;
  private final OptionManager optionManager;
  private DrillBuf buffer;
  private VectorContainerWriter writer;

  private DataFileReader<GenericContainer> reader = null;
  private FileSystem fs;

  private final String opUserName;
  private final String queryUserName;

  private static final int DEFAULT_BATCH_SIZE = 4096;


  public AvroRecordReader(final FragmentContext fragmentContext,
                          final String inputPath,
                          final long start,
                          final long length,
                          final FileSystem fileSystem,
                          final List<SchemaPath> projectedColumns,
                          final String userName) {
    hadoop = new Path(inputPath);
    this.start = start;
    this.end = start + length;
    buffer = fragmentContext.getManagedBuffer();
    this.fs = fileSystem;
    this.opUserName = userName;
    this.queryUserName = fragmentContext.getQueryUserName();
    setColumns(projectedColumns);
    this.fieldSelection = FieldSelection.getFieldSelection(projectedColumns);
    optionManager = fragmentContext.getOptions();
  }

  private DataFileReader<GenericContainer> getReader(final Path hadoop, final FileSystem fs) throws ExecutionSetupException {
    try {
      final UserGroupInformation ugi = ImpersonationUtil.createProxyUgi(this.opUserName, this.queryUserName);
      return ugi.doAs(new PrivilegedExceptionAction<DataFileReader<GenericContainer>>() {
        @Override
        public DataFileReader<GenericContainer> run() throws Exception {
          return new DataFileReader<>(new FsInput(hadoop, fs.getConf()), new GenericDatumReader<GenericContainer>());
        }
      });
    } catch (IOException | InterruptedException e) {
      throw new ExecutionSetupException(
        String.format("Error in creating avro reader for file: %s", hadoop), e);
    }
  }

  @Override
  public void setup(final OperatorContext context, final OutputMutator output) throws ExecutionSetupException {
    writer = new VectorContainerWriter(output);

    try {
      reader = getReader(hadoop, fs);
      logger.debug("Processing file : {}, start position : {}, end position : {} ", hadoop, start, end);
      reader.sync(this.start);
    } catch (IOException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public int next() {
    final Stopwatch watch = Stopwatch.createStarted();

    if (reader == null) {
      throw new IllegalStateException("Avro reader is not open.");
    }
    if (!reader.hasNext()) {
      return 0;
    }

    int recordCount = 0;
    writer.allocate();
    writer.reset();

    try {
      for (GenericContainer container = null;
           recordCount < DEFAULT_BATCH_SIZE && reader.hasNext() && !reader.pastSync(end);
           recordCount++) {
        writer.setPosition(recordCount);
        container = reader.next(container);
        processRecord(container, container.getSchema());
      }

      writer.setValueCount(recordCount);

    } catch (IOException e) {
      throw new DrillRuntimeException(e);
    }

    logger.debug("Read {} records in {} ms", recordCount, watch.elapsed(TimeUnit.MILLISECONDS));
    return recordCount;
  }

  private void processRecord(final GenericContainer container, final Schema schema) {

    final Schema.Type type = schema.getType();

    switch (type) {
      case RECORD:
        process(container, schema, null, new MapOrListWriterImpl(writer.rootAsMap()), fieldSelection);
        break;
      default:
        throw new DrillRuntimeException("Root object must be record type. Found: " + type);
    }
  }

  private void process(final Object value, final Schema schema, final String fieldName, MapOrListWriterImpl writer, FieldSelection fieldSelection) {
    if (value == null) {
      return;
    }
    final Schema.Type type = schema.getType();

    switch (type) {
      case RECORD:
        // list field of MapOrListWriter will be non null when we want to store array of maps/records.
        MapOrListWriterImpl _writer = writer;

        for (final Schema.Field field : schema.getFields()) {
          if (field.schema().getType() == Schema.Type.RECORD ||
              (field.schema().getType() == Schema.Type.UNION &&
              field.schema().getTypes().get(0).getType() == Schema.Type.NULL &&
              field.schema().getTypes().get(1).getType() == Schema.Type.RECORD)) {
              _writer = (MapOrListWriterImpl) writer.map(field.name());
          }

          process(((GenericRecord) value).get(field.name()), field.schema(), field.name(), _writer, fieldSelection.getChild(field.name()));

        }
        break;
      case ARRAY:
        assert fieldName != null;
        final GenericArray<?> array = (GenericArray<?>) value;
        Schema elementSchema = array.getSchema().getElementType();
        Type elementType = elementSchema.getType();
        if (elementType == Schema.Type.RECORD || elementType == Schema.Type.MAP){
          writer = (MapOrListWriterImpl) writer.list(fieldName).listoftmap(fieldName);
        } else {
          writer = (MapOrListWriterImpl) writer.list(fieldName);
        }
        for (final Object o : array) {
          writer.start();
          process(o, elementSchema, fieldName, writer, fieldSelection.getChild(fieldName));
          writer.end();
        }
        break;
      case UNION:
        // currently supporting only nullable union (optional fields) like ["null", "some-type"].
        if (schema.getTypes().get(0).getType() != Schema.Type.NULL) {
          throw new UnsupportedOperationException("Avro union type must be of the format : [\"null\", \"some-type\"]");
        }
        process(value, schema.getTypes().get(1), fieldName, writer, fieldSelection);
        break;
      case MAP:
        @SuppressWarnings("unchecked")
        final HashMap<Object, Object> map = (HashMap<Object, Object>) value;
        Schema valueSchema = schema.getValueType();
        writer = (MapOrListWriterImpl) writer.map(fieldName);
        writer.start();
        for (Entry<Object, Object> entry : map.entrySet()) {
          process(entry.getValue(), valueSchema, entry.getKey().toString(), writer, fieldSelection.getChild(entry.getKey().toString()));
        }
        writer.end();
        break;
      case FIXED:
      case ENUM:  // Enum symbols are strings
      case NULL:  // Treat null type as a primitive
      default:
        assert fieldName != null;

        if (writer.isMapWriter()) {
          if (fieldSelection.isNeverValid()) {
            break;
          }
        }

        processPrimitive(value, schema, fieldName, writer);
        break;
    }

  }

  private void processPrimitive(final Object value, final Schema schema, final String fieldName,
                                final MapOrListWriterImpl writer) {
    LogicalType logicalType = schema.getLogicalType();
    String logicalTypeName = logicalType != null ? logicalType.getName() : "";

    if (value == null) {
      return;
    }

    switch (schema.getType()) {
      case STRING:
        byte[] binary;
        final int length;
        if (value instanceof Utf8) {
          binary = ((Utf8) value).getBytes();
          length = ((Utf8) value).getByteLength();
        } else {
          binary = value.toString().getBytes(Charsets.UTF_8);
          length = binary.length;
        }
        ensure(length);
        buffer.setBytes(0, binary);
        writer.varChar(fieldName).writeVarChar(0, length, buffer);
        break;
      case INT:
        switch (logicalTypeName) {
          case "date":
            writer.date(fieldName).writeDate((int) value * (long) DateTimeConstants.MILLIS_PER_DAY);
            break;
          case "time-millis":
            writer.time(fieldName).writeTime((Integer) value);
            break;
          default:
            writer.integer(fieldName).writeInt((Integer) value);
        }
        break;
      case LONG:
        switch (logicalTypeName) {
          case "date":
            writer.date(fieldName).writeDate((Long) value);
            break;
          case "time-micros":
            writer.time(fieldName).writeTime((int) ((long) value / 1000));
            break;
          case "timestamp-millis":
            writer.timeStamp(fieldName).writeTimeStamp((Long) value);
            break;
          case "timestamp-micros":
            writer.timeStamp(fieldName).writeTimeStamp((long) value / 1000);
            break;
          default:
            writer.bigInt(fieldName).writeBigInt((Long) value);
        }
        break;
      case FLOAT:
        writer.float4(fieldName).writeFloat4((Float) value);
        break;
      case DOUBLE:
        writer.float8(fieldName).writeFloat8((Double) value);
        break;
      case BOOLEAN:
        writer.bit(fieldName).writeBit((Boolean) value ? 1 : 0);
        break;
      case BYTES:
        final ByteBuffer buf = (ByteBuffer) value;
        length = buf.remaining();
        ensure(length);
        buffer.setBytes(0, buf);
        switch (logicalTypeName) {
          case "decimal":
            ParquetReaderUtility.checkDecimalTypeEnabled(optionManager);
            LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
            writer.varDecimal(fieldName, decimalType.getScale(), decimalType.getPrecision())
                .writeVarDecimal(0, length, buffer, decimalType.getScale(), decimalType.getPrecision());
            break;
          default:
            writer.binary(fieldName).writeVarBinary(0, length, buffer);
        }
        break;
      case NULL:
        // Nothing to do for null type
        break;
      case ENUM:
        final String symbol = value.toString();
        final byte[] b;
        try {
          b = symbol.getBytes(Charsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
          throw new DrillRuntimeException("Unable to read enum value for field: " + fieldName, e);
        }
        ensure(b.length);
        buffer.setBytes(0, b);
        writer.varChar(fieldName).writeVarChar(0, b.length, buffer);
        break;
      case FIXED:
        GenericFixed genericFixed = (GenericFixed) value;
        switch (logicalTypeName) {
          case "decimal":
            ParquetReaderUtility.checkDecimalTypeEnabled(optionManager);
            LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
            writer.varDecimal(fieldName, decimalType.getScale(), decimalType.getPrecision())
                .writeVarDecimal(new BigDecimal(new BigInteger(genericFixed.bytes()), decimalType.getScale()));
            break;
          default:
            throw new UnsupportedOperationException("Unimplemented type: " + schema.getType().toString());
        }
        break;
      default:
        throw new DrillRuntimeException("Unhandled Avro type: " + schema.getType().toString());
    }
  }

  private boolean selected(SchemaPath field) {
    if (isStarQuery()) {
      return true;
    }
    for (final SchemaPath sp : getColumns()) {
      if (sp.contains(field)) {
        return true;
      }
    }
    return false;
  }

  private void ensure(final int length) {
    buffer = buffer.reallocIfNeeded(length);
  }

  @Override
  public void close() {
    if (reader != null) {
      try {
        reader.close();
      } catch (IOException e) {
        logger.warn("Error closing Avro reader", e);
      } finally {
        reader = null;
      }
    }
  }

  @Override
  public String toString() {
    long currentPosition = -1L;
    try {
      if (reader != null) {
        currentPosition = reader.tell();
      }
    } catch (IOException e) {
      logger.trace("Unable to obtain reader position.", e);
    }
    return "AvroRecordReader[File=" + hadoop
        + ", Position=" + currentPosition
        + "]";
  }
}
