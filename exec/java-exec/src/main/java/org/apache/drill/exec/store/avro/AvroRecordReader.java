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
package org.apache.drill.exec.store.avro;

import io.netty.buffer.DrillBuf;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.security.PrivilegedExceptionAction;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.util.Utf8;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * A RecordReader implementation for Avro data files.
 *
 * @see RecordReader
 */
public class AvroRecordReader extends AbstractRecordReader {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AvroRecordReader.class);

  private final Path hadoop;
  private final long start;
  private final long end;
  private DrillBuf buffer;
  private VectorContainerWriter writer;

  private DataFileReader<GenericContainer> reader = null;
  private OperatorContext operatorContext;
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
  }

  private DataFileReader getReader(final Path hadoop, final FileSystem fs) throws ExecutionSetupException {
    try {
      final UserGroupInformation ugi = ImpersonationUtil.createProxyUgi(this.opUserName, this.queryUserName);
      return ugi.doAs(new PrivilegedExceptionAction<DataFileReader>() {
        @Override
        public DataFileReader run() throws Exception {
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
    operatorContext = context;
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
    final Stopwatch watch = new Stopwatch().start();

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
        process(container, schema, null, new MapOrListWriter(writer.rootAsMap()));
        break;
      default:
        throw new DrillRuntimeException("Root object must be record type. Found: " + type);
    }
  }

  private void process(final Object value, final Schema schema, final String fieldName, MapOrListWriter writer) {
    if (value == null) {
      return;
    }
    final Schema.Type type = schema.getType();

    switch (type) {
      case RECORD:
        // list field of MapOrListWriter will be non null when we want to store array of maps/records.
        MapOrListWriter _writer = writer;

        for (final Schema.Field field : schema.getFields()) {
          if (field.schema().getType() == Schema.Type.RECORD ||
              (field.schema().getType() == Schema.Type.UNION &&
              field.schema().getTypes().get(0).getType() == Schema.Type.NULL &&
              field.schema().getTypes().get(1).getType() == Schema.Type.RECORD)) {
            _writer = writer.map(field.name());
          }

          process(((GenericRecord) value).get(field.name()), field.schema(), field.name(), _writer);
        }
        break;
      case ARRAY:
        assert fieldName != null;
        final GenericArray array = (GenericArray) value;
        Schema elementSchema = array.getSchema().getElementType();
        Type elementType = elementSchema.getType();
        if (elementType == Schema.Type.RECORD || elementType == Schema.Type.MAP){
          writer = writer.list(fieldName).listoftmap(fieldName);
        } else {
          writer = writer.list(fieldName);
        }
        writer.start();
        for (final Object o : array) {
          process(o, elementSchema, fieldName, writer);
        }
        writer.end();
        break;
      case UNION:
        // currently supporting only nullable union (optional fields) like ["null", "some-type"].
        if (schema.getTypes().get(0).getType() != Schema.Type.NULL) {
          throw new UnsupportedOperationException("Avro union type must be of the format : [\"null\", \"some-type\"]");
        }
        process(value, schema.getTypes().get(1), fieldName, writer);
        break;
      case MAP:
        @SuppressWarnings("unchecked")
        final HashMap<Object, Object> map = (HashMap<Object, Object>) value;
        Schema valueSchema = schema.getValueType();
        writer = writer.map(fieldName);
        writer.start();
        for (Entry<Object, Object> entry : map.entrySet()) {
          process(entry.getValue(), valueSchema, entry.getKey().toString(), writer);
        }
        writer.end();
        break;
      case FIXED:
        throw new UnsupportedOperationException("Unimplemented type: " + type.toString());
      case ENUM:  // Enum symbols are strings
      case NULL:  // Treat null type as a primitive
      default:
        assert fieldName != null;

        if (writer.isMapWriter()) {
          SchemaPath path;
          if (writer.map.getField().getPath().getRootSegment().getPath().equals("")) {
            path = new SchemaPath(new PathSegment.NameSegment(fieldName));
          } else {
            path = writer.map.getField().getPath().getChild(fieldName);
          }

          if (!selected(path)) {
            break;
          }
        }

        processPrimitive(value, schema.getType(), fieldName, writer);
        break;
    }

  }

  private void processPrimitive(final Object value, final Schema.Type type, final String fieldName,
                                final MapOrListWriter writer) {
    if (value == null) {
      return;
    }

    switch (type) {
      case STRING:
        byte[] binary = null;
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
        writer.integer(fieldName).writeInt((Integer) value);
        break;
      case LONG:
        writer.bigInt(fieldName).writeBigInt((Long) value);
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
        writer.binary(fieldName).writeVarBinary(0, length, buffer);
        break;
      case NULL:
        // Nothing to do for null type
        break;
      case ENUM:
        final String symbol = value.toString();
        final byte[] b;
        try {
          b = symbol.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
          throw new DrillRuntimeException("Unable to read enum value for field: " + fieldName, e);
        }
        ensure(b.length);
        buffer.setBytes(0, b);
        writer.varChar(fieldName).writeVarChar(0, b.length, buffer);

        break;
      default:
        throw new DrillRuntimeException("Unhandled Avro type: " + type.toString());
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
}
