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
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
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
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Stopwatch;

/**
 * A RecordReader implementation for Avro data files.
 *
 * @see RecordReader
 */
public class AvroRecordReader extends AbstractRecordReader {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AvroRecordReader.class);

  private final Path hadoop;
  private DrillBuf buffer;
  private VectorContainerWriter writer;

  private DataFileReader<GenericContainer> reader = null;
  private OperatorContext operatorContext;
  private FileSystem fs;

  private static final int DEFAULT_BATCH_SIZE = 1000;


  public AvroRecordReader(final FragmentContext fragmentContext,
                          final String inputPath,
                          final FileSystem fileSystem,
                          final List<SchemaPath> projectedColumns) {
    this(fragmentContext, inputPath, fileSystem, projectedColumns, DEFAULT_BATCH_SIZE);
  }

  public AvroRecordReader(final FragmentContext fragmentContext,
                          final String inputPath,
                          final FileSystem fileSystem,
                          List<SchemaPath> projectedColumns, final int defaultBatchSize) {

    hadoop = new Path(inputPath);
    buffer = fragmentContext.getManagedBuffer();
    this.fs = fileSystem;

    setColumns(projectedColumns);
  }

  @Override
  public void setup(final OperatorContext context, final OutputMutator output) throws ExecutionSetupException {
    operatorContext = context;
    writer = new VectorContainerWriter(output);

    try {
      reader = new DataFileReader<>(new FsInput(hadoop, fs.getConf()), new GenericDatumReader<GenericContainer>());
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

      // XXX - Implement batch size

      for (GenericContainer container = null; reader.hasNext(); recordCount++) {
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

  private void process(final Object value, final Schema schema, final String fieldName, final MapOrListWriter writer) {

    writer.start();
    final Schema.Type type = schema.getType();

    switch (type) {
      case RECORD:
        for (final Schema.Field field : schema.getFields()) {

          MapOrListWriter _writer = writer;
          if (field.schema().getType() == Schema.Type.RECORD) {
            _writer = writer.map(field.name());
          }

          process(((GenericRecord) value).get(field.name()), field.schema(), field.name(), _writer);
        }
        break;
      case ARRAY:
        assert fieldName != null;
        final GenericArray array = (GenericArray) value;
        for (final Object o : array) {
          process(o, array.getSchema().getElementType(), fieldName, writer.list(fieldName));
        }
        break;
      case FIXED:
      case UNION:
      case MAP:
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

    writer.end();
  }

  private void processPrimitive(final Object value, final Schema.Type type, final String fieldName,
                                final MapOrListWriter writer) {

    switch (type) {
      case STRING:
        final Utf8 utf8 = (Utf8) value;
        final int length = utf8.length();
        final VarCharHolder vh = new VarCharHolder();
        ensure(length);
        buffer.setBytes(0, utf8.getBytes());
        vh.buffer = buffer;
        vh.start = 0;
        vh.end = length;
        writer.varChar(fieldName).write(vh);
        break;
      case INT:
        final IntHolder ih = new IntHolder();
        ih.value = (Integer) value;
        writer.integer(fieldName).write(ih);
        break;
      case LONG:
        final BigIntHolder bh = new BigIntHolder();
        bh.value = (Long) value;
        writer.bigInt(fieldName).write(bh);
        break;
      case FLOAT:
        final Float4Holder fh = new Float4Holder();
        fh.value = (Float) value;
        writer.float4(fieldName).write(fh);
        break;
      case DOUBLE:
        final Float8Holder f8h = new Float8Holder();
        f8h.value = (Double) value;
        writer.float8(fieldName).write(f8h);
        break;
      case BOOLEAN:
        final BitHolder bit = new BitHolder();
        bit.value = (Boolean) value ? 1 : 0;
        writer.bit(fieldName).write(bit);
        break;
      case BYTES:
        // XXX - Not sure if this is correct. Nothing prints from sqlline for byte fields.
        final VarBinaryHolder vb = new VarBinaryHolder();
        final ByteBuffer buf = (ByteBuffer) value;
        final byte[] bytes = buf.array();
        ensure(bytes.length);
        buffer.setBytes(0, bytes);
        vb.buffer = buffer;
        vb.start = 0;
        vb.end = bytes.length;
        writer.binary(fieldName).write(vb);
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
        final VarCharHolder vch = new VarCharHolder();
        ensure(b.length);
        buffer.setBytes(0, b);
        vch.buffer = buffer;
        vch.start = 0;
        vch.end = b.length;
        writer.varChar(fieldName).write(vch);
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
