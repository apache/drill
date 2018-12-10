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
package org.apache.drill.exec.store.msgpack.valuewriter.impl;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.msgpack.valuewriter.ScalarValueWriter;
import org.apache.drill.exec.vector.complex.fn.FieldSelection;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.msgpack.core.MessageUnpacker;

/**
 * This is the base class for all scalar values FLOAT, BOOLEAN, INTEGER, STRING,
 * BINARY, EXTENDED types. It contains writing method for all of these types.
 */
public abstract class AbstractScalarValueWriter extends AbstractValueWriter implements ScalarValueWriter {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractScalarValueWriter.class);

  public AbstractScalarValueWriter() {
  }

  /**
   * Return the drill type corresponding to this value writer, unless a schema
   * says otherwise. In which case the value writer will try to coerce the msgpack
   * value into the desired drill type.
   *
   * @param defaultType
   *                      default type if no using a schema.
   * @param schema
   *                      the desired schema if any
   * @return the target type desired.
   */
  protected MinorType getTargetType(MinorType defaultType, ColumnMetadata schema) {
    if (context.hasSchema()) {
      if (schema != null) {
        return schema.type();
      }
    }
    return defaultType;
  }

  /**
   * Main method of the scalar value writer. It determines the target type, does
   * any logging and dispatches to the doWrite method.
   */
  @Override
  public void write(MessageUnpacker unpacker, MapWriter mapWriter, String fieldName, ListWriter listWriter,
      FieldSelection selection, ColumnMetadata schema) throws IOException {

    MinorType targetSchemaType = getTargetType(getDefaultType(), schema);
    if (logger.isDebugEnabled()) {
      if (targetSchemaType != getDefaultType()) {
        logger.debug("type: '{}' ---> coerced to: '{}'", getDefaultType(), targetSchemaType);
      }
    }

    doWrite(unpacker, mapWriter, fieldName, listWriter, targetSchemaType);
  }

  protected void writeAsVarBinary(ByteBuffer byteBuffer, MapWriter mapWriter, String fieldName, ListWriter listWriter) {
    int length = byteBuffer.remaining();
    context.getDrillBuf(length).setBytes(0, byteBuffer);
    writeAsVarBinary(mapWriter, fieldName, listWriter, length);
  }

  protected void writeAsVarBinary(MapWriter mapWriter, String fieldName, ListWriter listWriter, int length) {
    if (mapWriter != null) {
      mapWriter.varBinary(fieldName).writeVarBinary(0, length, context.getDrillBuf());
    } else {
      listWriter.varBinary().writeVarBinary(0, length, context.getDrillBuf());
    }
  }

  protected void writeAsVarChar(ByteBuffer byteBuffer, MapWriter mapWriter, String fieldName, ListWriter listWriter) {
    int length = byteBuffer.remaining();
    context.getDrillBuf(length).setBytes(0, byteBuffer);
    writeAsVarChar(mapWriter, fieldName, listWriter, length);
  }

  protected void writeAsVarChar(MapWriter mapWriter, String fieldName, ListWriter listWriter, int length) {
    if (mapWriter != null) {
      mapWriter.varChar(fieldName).writeVarChar(0, length, context.getDrillBuf());
    } else {
      listWriter.varChar().writeVarChar(0, length, context.getDrillBuf());
    }
  }

  protected void writeAsFloat8(double d, MapWriter mapWriter, String fieldName, ListWriter listWriter) {
    if (mapWriter != null) {
      mapWriter.float8(fieldName).writeFloat8(d);
    } else {
      listWriter.float8().writeFloat8(d);
    }
  }

  protected void writeAsBit(boolean readBoolean, MapWriter mapWriter, String fieldName, ListWriter listWriter) {
    int value = readBoolean ? 1 : 0;
    if (mapWriter != null) {
      mapWriter.bit(fieldName).writeBit(value);
    } else {
      listWriter.bit().writeBit(value);
    }
  }

  protected void writeAsBigInt(long value, MapWriter mapWriter, String fieldName, ListWriter listWriter) {
    if (mapWriter != null) {
      mapWriter.bigInt(fieldName).writeBigInt(value);
    } else {
      listWriter.bigInt().writeBigInt(value);
    }
  }
}
