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

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.msgpack.MsgpackReaderContext;
import org.apache.drill.exec.store.msgpack.valuewriter.ScalarValueWriter;
import org.apache.drill.exec.vector.complex.fn.FieldSelection;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.msgpack.value.Value;

import io.netty.buffer.DrillBuf;

/**
 * This is the base class for all scalar values FLOAT, BOOLEAN, INTEGER, STRING,
 * BINARY, EXTENDED types. It contains writing method for all of these types.
 */
public abstract class AbstractScalarValueWriter extends AbstractValueWriter implements ScalarValueWriter {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractScalarValueWriter.class);

  private DrillBuf drillBuf;

  public AbstractScalarValueWriter() {
  }

  @Override
  public void setup(MsgpackReaderContext context, DrillBuf drillBuf) {
    super.setup(context);
    this.drillBuf = drillBuf;
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
  public void write(Value v, MapWriter mapWriter, String fieldName, ListWriter listWriter, FieldSelection selection,
      ColumnMetadata schema) {

    MinorType targetSchemaType = getTargetType(getDefaultType(v), schema);
    if (logger.isDebugEnabled()) {
      if (targetSchemaType != getDefaultType(v)) {
        logger.debug("type: '{}' ---> coerced to: '{}'", v.getValueType(), targetSchemaType);
      }
    }

    doWrite(v, mapWriter, fieldName, listWriter, targetSchemaType);
  }

  protected void writeAsVarBinary(byte[] bytes, MapWriter mapWriter, String fieldName, ListWriter listWriter) {
    int length = bytes.length;
    ensure(length);
    drillBuf.setBytes(0, bytes);
    writeAsVarBinary(mapWriter, fieldName, listWriter, length);
  }

  protected void writeAsVarBinary(MapWriter mapWriter, String fieldName, ListWriter listWriter, int length) {
    if (mapWriter != null) {
      mapWriter.varBinary(fieldName).writeVarBinary(0, length, drillBuf);
    } else {
      listWriter.varBinary().writeVarBinary(0, length, drillBuf);
    }
  }

  protected void writeAsVarChar(byte[] readString, MapWriter mapWriter, String fieldName, ListWriter listWriter) {
    int length = readString.length;
    ensure(length);
    drillBuf.setBytes(0, readString);
    writeAsVarChar(mapWriter, fieldName, listWriter, length);
  }

  protected void writeAsVarChar(MapWriter mapWriter, String fieldName, ListWriter listWriter, int length) {
    if (mapWriter != null) {
      mapWriter.varChar(fieldName).writeVarChar(0, length, drillBuf);
    } else {
      listWriter.varChar().writeVarChar(0, length, drillBuf);
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

  protected void writeAsVarBinary(long longValue, MapWriter mapWriter, String fieldName, ListWriter listWriter) {
    ensure(8);
    drillBuf.setLong(0, longValue);
    writeAsVarBinary(mapWriter, fieldName, listWriter, 8);
  }

  protected void writeAsVarBinary(boolean boolValue, MapWriter mapWriter, String fieldName, ListWriter listWriter) {
    ensure(1);
    drillBuf.setBoolean(0, boolValue);
    writeAsVarBinary(mapWriter, fieldName, listWriter, 1);
  }

  protected void writeAsVarBinary(double douvleValue, MapWriter mapWriter, String fieldName, ListWriter listWriter) {
    ensure(8);
    drillBuf.setDouble(0, douvleValue);
    writeAsVarBinary(mapWriter, fieldName, listWriter, 8);
  }

  protected void ensure(final int length) {
    drillBuf = drillBuf.reallocIfNeeded(length);
  }

}
