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
package org.apache.drill.exec.store.mapr.db.json;

import static org.apache.drill.exec.store.mapr.PluginErrorHandler.schemaChangeException;
import static org.apache.drill.exec.store.mapr.PluginErrorHandler.unsupportedError;

import java.nio.ByteBuffer;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.vector.complex.impl.StructOrListWriterImpl;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.StructOrListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.StructWriter;
import org.ojai.DocumentReader;
import org.ojai.DocumentReader.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.org.apache.hadoop.hbase.util.Bytes;

import io.netty.buffer.DrillBuf;

public class OjaiValueWriter {
  protected static final Logger logger = LoggerFactory.getLogger(OjaiValueWriter.class);

  protected static final long MILLISECONDS_IN_A_DAY  = (long)1000 * 60 * 60 * 24;

  protected DrillBuf buffer;

  public OjaiValueWriter(DrillBuf buffer) {
    this.buffer = buffer;
  }

  /*
   * Precondition to call this function is that the DBDocumentReader has already emitted START_MAP/START_ARRAY event.
   */
  protected void writeToListOrStruct(StructOrListWriterImpl writer, DocumentReader reader) throws SchemaChangeException {
    String fieldName = null;
    writer.start();
    outside: while (true) {
      EventType event = reader.next();
      if (event == null
          || event == EventType.END_MAP
          || event == EventType.END_ARRAY) {
        break outside;
      } else if (reader.inMap()) {
        fieldName = reader.getFieldName();
      }

      try {
        switch (event) {
        case NULL:
          break; // not setting the field will leave it as null
        case BINARY:
          writeBinary(writer, fieldName, reader.getBinary());
          break;
        case BOOLEAN:
          writeBoolean(writer, fieldName, reader);
          break;
        case STRING:
          writeString(writer, fieldName, reader.getString());
          break;
        case BYTE:
          writeByte(writer, fieldName, reader);
          break;
        case SHORT:
          writeShort(writer, fieldName, reader);
          break;
        case INT:
          writeInt(writer, fieldName, reader);
          break;
        case LONG:
          writeLong(writer, fieldName, reader);
          break;
        case FLOAT:
          writeFloat(writer, fieldName, reader);
          break;
        case DOUBLE:
          writeDouble(writer, fieldName, reader);
          break;
        case DECIMAL:
          throw unsupportedError(logger, "Decimal type is currently not supported.");
        case DATE:
          writeDate(writer, fieldName, reader);
          break;
        case TIME:
          writeTime(writer, fieldName, reader);
          break;
        case TIMESTAMP:
          writeTimeStamp(writer, fieldName, reader);
          break;
        case INTERVAL:
          throw unsupportedError(logger, "Interval type is currently not supported.");
        case START_MAP:
          writeToListOrStruct((StructOrListWriterImpl) (reader.inMap() ? writer.struct(fieldName) : writer.listoftstruct(fieldName)), reader);
          break;
        case START_ARRAY:
          writeToListOrStruct((StructOrListWriterImpl) writer.list(fieldName), reader);
          break;
        default:
          throw unsupportedError(logger, "Unsupported type: %s encountered during the query.", event);
        }
      } catch (IllegalStateException | IllegalArgumentException e) {
        throw schemaChangeException(logger, e, "Possible schema change for field: '%s'", fieldName);
      }
    }
    writer.end();
  }

  protected void writeTimeStamp(StructOrListWriterImpl writer, String fieldName, DocumentReader reader) {
    writer.timeStamp(fieldName).writeTimeStamp(reader.getTimestampLong());
  }

  protected void writeTime(StructOrListWriterImpl writer, String fieldName, DocumentReader reader) {
    writer.time(fieldName).writeTime(reader.getTimeInt());
  }

  protected void writeDate(StructOrListWriterImpl writer, String fieldName, DocumentReader reader) {
    long milliSecondsSinceEpoch = reader.getDateInt() * MILLISECONDS_IN_A_DAY;
    writer.date(fieldName).writeDate(milliSecondsSinceEpoch);
  }

  protected void writeDouble(StructOrListWriterImpl writer, String fieldName, DocumentReader reader) {
    writer.float8(fieldName).writeFloat8(reader.getDouble());
  }

  protected void writeFloat(StructOrListWriterImpl writer, String fieldName, DocumentReader reader) {
    writer.float4(fieldName).writeFloat4(reader.getFloat());
  }

  protected void writeLong(StructOrListWriterImpl writer, String fieldName, DocumentReader reader) {
    writer.bigInt(fieldName).writeBigInt(reader.getLong());
  }

  protected void writeInt(StructOrListWriterImpl writer, String fieldName, DocumentReader reader) {
    writer.integer(fieldName).writeInt(reader.getInt());
  }

  protected void writeShort(StructOrListWriterImpl writer, String fieldName, DocumentReader reader) {
    writer.smallInt(fieldName).writeSmallInt(reader.getShort());
  }

  protected void writeByte(StructOrListWriterImpl writer, String fieldName, DocumentReader reader) {
    writer.tinyInt(fieldName).writeTinyInt(reader.getByte());
  }

  protected void writeBoolean(StructOrListWriterImpl writer, String fieldName, DocumentReader reader) {
    writer.bit(fieldName).writeBit(reader.getBoolean() ? 1 : 0);
  }

  protected void writeBinary(StructOrListWriter writer, String fieldName, ByteBuffer buf) {
    int bufLen = buf.remaining();
    buffer = buffer.reallocIfNeeded(bufLen);
    buffer.setBytes(0, buf, buf.position(), bufLen);
    writer.varBinary(fieldName).writeVarBinary(0, bufLen, buffer);
  }

  protected void writeString(StructOrListWriter writer, String fieldName, String value) {
    final byte[] strBytes = Bytes.toBytes(value);
    buffer = buffer.reallocIfNeeded(strBytes.length);
    buffer.setBytes(0, strBytes);
    writer.varChar(fieldName).writeVarChar(0, strBytes.length, buffer);
  }

  protected void writeBinary(StructWriter writer, String fieldName, ByteBuffer buf) {
    int bufLen = buf.remaining();
    buffer = buffer.reallocIfNeeded(bufLen);
    buffer.setBytes(0, buf, buf.position(), bufLen);
    writer.varBinary(fieldName).writeVarBinary(0, bufLen, buffer);
  }

  protected void writeString(StructWriter writer, String fieldName, String value) {
    final byte[] strBytes = Bytes.toBytes(value);
    buffer = buffer.reallocIfNeeded(strBytes.length);
    buffer.setBytes(0, strBytes);
    writer.varChar(fieldName).writeVarChar(0, strBytes.length, buffer);
  }

}
