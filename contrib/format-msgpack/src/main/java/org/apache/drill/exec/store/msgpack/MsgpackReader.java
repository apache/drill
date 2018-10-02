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
package org.apache.drill.exec.store.msgpack;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.vector.complex.fn.FieldSelection;
import org.apache.drill.exec.vector.complex.impl.MapOrListWriterImpl;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.FloatValue;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

import io.netty.buffer.DrillBuf;
import jline.internal.Log;

public class MsgpackReader extends BaseMsgpackReader {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MsgpackReader.class);
  private final List<SchemaPath> columns;
  public boolean atLeastOneWrite = false;
  private DrillBuf workBuf;
  private final FieldSelection rootSelection;
  private final ByteBuffer timestampReadBuffer = ByteBuffer.allocate(12);
  private final boolean skipInvalidKeyTypes = true;
 
  public MsgpackReader(DrillBuf managedBuf) {
    this(managedBuf, GroupScan.ALL_COLUMNS);
  }

  public MsgpackReader(DrillBuf managedBuf, List<SchemaPath> columns) {
    assert Preconditions.checkNotNull(columns).size() > 0 : "msgpack record reader requires at least a column";
    this.workBuf = managedBuf;
    this.columns = columns;
    rootSelection = FieldSelection.getFieldSelection(columns);
  }

  @Override
  protected ReadState writeRecord(Value mapValue, ComplexWriter writer) throws IOException {
    return writeToMap(mapValue, new MapOrListWriterImpl(writer.rootAsMap()), this.rootSelection);
  }

  private ReadState writeToList(Value listOrMapValue, final MapOrListWriterImpl writer, FieldSelection selection)
      throws IOException {
    ReadState readState = ReadState.WRITE_SUCCEED;
    writer.start();
    try {
      ArrayValue arrayValue = listOrMapValue.asArrayValue();
      for (int i = 0; i < arrayValue.size(); i++) {
        Value value = arrayValue.get(i);
        readState = writeElement(value, writer, true, "", selection);
        if(readState != ReadState.WRITE_SUCCEED) {
          break;
        }
      }
    } finally {
      writer.end();
    }
    return readState;
  }

  private ReadState writeToMap(Value listOrMapValue, final MapOrListWriterImpl writer, FieldSelection selection)
      throws IOException {
    ReadState readState = ReadState.WRITE_SUCCEED;
    writer.start();
    try {
      MapValue mapValue = listOrMapValue.asMapValue();
      Set<Map.Entry<Value, Value>> entries = mapValue.entrySet();
      for (Map.Entry<Value, Value> entry : entries) {
        Value key = entry.getKey();
        String fieldName = getFieldName(key);
        if (fieldName != null) {
          FieldSelection childSelection = selection.getChild(fieldName);
          if (!childSelection.isNeverValid()) {
            Value value = entry.getValue();
            readState = writeElement(value, writer, false, fieldName, childSelection);
          }
        }
        else {
          Log.warn("Failed to read field name of type: " + key.getValueType());
          if(!skipInvalidKeyTypes) {
            readState = ReadState.MSG_RECORD_PARSE_ERROR;
          }
        }
        if(readState != ReadState.WRITE_SUCCEED)
        {
          break;
        }
      }
    } finally {
      writer.end();
    }
    return readState;
  }

  private String getFieldName(Value v) {

    String fieldName = null;

    ValueType valueType = v.getValueType();
    switch (valueType) {
    case STRING:
      fieldName = v.asStringValue().asString();
      break;
    case BINARY:
      byte[] bytes = v.asBinaryValue().asByteArray();
      fieldName = new String(bytes);
      break;
    case INTEGER:
      IntegerValue iv = v.asIntegerValue();
      fieldName = iv.toString();
      break;
    case ARRAY:
    case BOOLEAN:
    case MAP:
    case FLOAT:
    case EXTENSION:
    case NIL:
      break;
    default:
      logger.warn("UnSupported messagepack type: " + valueType);
    }
    return fieldName;
  }

  private ReadState writeElement(Value v, final MapOrListWriterImpl writer, boolean isList, String fieldName,
      FieldSelection selection) throws IOException {

    ReadState readState = ReadState.WRITE_SUCCEED;
    ValueType valueType = v.getValueType();
    switch (valueType) {
    case INTEGER:
      IntegerValue iv = v.asIntegerValue();
      if (iv.isInIntRange() || iv.isInLongRange()) {
        long longVal = iv.toLong();
        writeInt64(longVal, writer, fieldName, isList);
      } else {
        BigInteger i = iv.toBigInteger();
        throw new DrillRuntimeException("UnSupported messagepack type: " + valueType + " with BigInteger value: " + i);
      }
      atLeastOneWrite = true;
      break;
    case ARRAY:
      readState = writeToList(v, (MapOrListWriterImpl) writer.list(fieldName), selection);
      atLeastOneWrite = true;
      break;
    case BOOLEAN:
      boolean b = v.asBooleanValue().getBoolean();
      writeBoolean(b, writer, fieldName, isList);
      atLeastOneWrite = true;
      break;
    case MAP:
      // To handle nested Documents.
      MapOrListWriterImpl _writer = writer;
      if (!isList) {
        _writer = (MapOrListWriterImpl) writer.map(fieldName);
      } else {
        _writer = (MapOrListWriterImpl) writer.listoftmap(fieldName);
      }
      readState = writeToMap(v, _writer, selection);
      atLeastOneWrite = true;
      break;
    case FLOAT:
      FloatValue fv = v.asFloatValue();
      double d = fv.toDouble(); // use as double
      writeDouble(d, writer, fieldName, isList);
      atLeastOneWrite = true;
      break;
    case EXTENSION:
      ExtensionValue ev = v.asExtensionValue();
      byte extType = ev.getType();
      if (extType == -1) {
        writeTimestamp(ev, writer, fieldName, isList);
      } else {
        byte[] bytes = ev.getData();
        writeBinary(bytes, writer, fieldName, isList);
      }
      atLeastOneWrite = true;
      break;
    case NIL:
      // just read and ignore.
      v.isNilValue(); // true
      break;
    case STRING:
      byte[] buff = v.asStringValue().asByteArray();
      writeString(buff, writer, fieldName, isList);
      atLeastOneWrite = true;
      break;
    case BINARY:
      byte[] bytes = v.asBinaryValue().asByteArray();
      writeBinary(bytes, writer, fieldName, isList);
      atLeastOneWrite = true;
      break;
    default:
      logger.warn("UnSupported messagepack type: " + valueType);
      readState = ReadState.MSG_RECORD_PARSE_ERROR;
    }

    return readState;
  }

  /**
   * <code>
   * <pre>
   * timestamp 32 stores the number of seconds that have elapsed since 1970-01-01 00:00:00 UTC
   * in an 32-bit unsigned integer:
   * +--------+--------+--------+--------+--------+--------+
   * |  0xd6  |   -1   |   seconds in 32-bit unsigned int  |
   * +--------+--------+--------+--------+--------+--------+
   *
   * timestamp 64 stores the number of seconds and nanoseconds that have elapsed since 1970-01-01 00:00:00 UTC
   * in 32-bit unsigned integers:
   * +--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
   * |  0xd7  |   -1   |nanoseconds in 30-bit unsigned int |  seconds in 34-bit unsigned int   |
   * +--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
   *
   * timestamp 96 stores the number of seconds and nanoseconds that have elapsed since 1970-01-01 00:00:00 UTC
   * in 64-bit signed integer and 32-bit unsigned integer:
   * +--------+--------+--------+--------+--------+--------+--------+
   * |  0xc7  |   12   |   -1   |nanoseconds in 32-bit unsigned int |
   * +--------+--------+--------+--------+--------+--------+--------+
   * +--------+--------+--------+--------+--------+--------+--------+--------+
   *                     seconds in 64-bit signed int                        |
   * +--------+--------+--------+--------+--------+--------+--------+--------+
   *</pre>
   *</code>
   */
  private void writeTimestamp(ExtensionValue ev, MapOrListWriterImpl writer, String fieldName, boolean isList) {
    long epochSeconds = 0;
    byte zero = 0;
    byte[] data = ev.getData();
    switch (data.length) {
    case 4: {
      timestampReadBuffer.position(0);
      timestampReadBuffer.put(zero);
      timestampReadBuffer.put(zero);
      timestampReadBuffer.put(zero);
      timestampReadBuffer.put(zero);
      timestampReadBuffer.put(data);
      timestampReadBuffer.position(0);
      epochSeconds = timestampReadBuffer.getLong();
    }
      break;
    case 8: {
      timestampReadBuffer.position(0);
      timestampReadBuffer.put(data);
      timestampReadBuffer.position(0);
      long data64 = timestampReadBuffer.getLong();
      @SuppressWarnings("unused")
      long nanos = data64 >>> 34;
      epochSeconds = data64 & 0x00000003ffffffffL;
    }
      break;
    case 12: {
      timestampReadBuffer.position(0);
      timestampReadBuffer.put(data);
      timestampReadBuffer.position(0);
      int data32 = timestampReadBuffer.getInt();
      @SuppressWarnings("unused")
      long nanosLong = data32;
      long data64 = timestampReadBuffer.getLong();
      epochSeconds = data64;
    }
      break;
    default:
      throw new DrillRuntimeException(
          "UnSupported built-in messagepack timestamp type (-1) with data length of: " + data.length);
    }

    if (isList == false) {
      writer.timeStamp(fieldName).writeTimeStamp(epochSeconds);
    } else {
      writer.list.timeStamp().writeTimeStamp(epochSeconds);
    }
  }

  private void writeBinary(byte[] bytes, final MapOrListWriterImpl writer, String fieldName, boolean isList) {
    int length = bytes.length;
    ensure(length);
    workBuf.setBytes(0, bytes);
    if (isList == false) {
      writer.varBinary(fieldName).writeVarBinary(0, length, workBuf);
    } else {
      writer.list.varBinary().writeVarBinary(0, length, workBuf);
    }
  }

  private void writeString(byte[] readString, final MapOrListWriterImpl writer, String fieldName, boolean isList) {
    int length = readString.length;
    ensure(length);
    workBuf.setBytes(0, readString);
    if (isList == false) {
      writer.varChar(fieldName).writeVarChar(0, length, workBuf);
    } else {
      writer.list.varChar().writeVarChar(0, length, workBuf);
    }
  }

  private void writeDouble(double readDouble, final MapOrListWriterImpl writer, String fieldName, boolean isList) {
    if (isList == false) {
      writer.float8(fieldName).writeFloat8(readDouble);
    } else {
      writer.list.float8().writeFloat8(readDouble);
    }
  }

  private void writeBoolean(boolean readBoolean, final MapOrListWriterImpl writer, String fieldName, boolean isList) {
    int value = readBoolean ? 1 : 0;
    if (isList == false) {
      writer.bit(fieldName).writeBit(value);
    } else {
      writer.list.bit().writeBit(value);
    }
  }

  private void writeInt64(long readInt64, final MapOrListWriterImpl writer, String fieldName, boolean isList) {
    if (isList == false) {
      writer.bigInt(fieldName).writeBigInt(readInt64);
    } else {
      writer.list.bigInt().writeBigInt(readInt64);
    }
  }

  public void ensureAtLeastOneField(ComplexWriter writer) {
    if (!atLeastOneWrite) {
      // if we had no columns, create one empty one so we can return some data
      // for count purposes.
      SchemaPath sp = columns.get(0);
      PathSegment root = sp.getRootSegment();
      BaseWriter.MapWriter fieldWriter = writer.rootAsMap();
      while (root.getChild() != null && !root.getChild().isArray()) {
        fieldWriter = fieldWriter.map(root.getNameSegment().getPath());
        root = root.getChild();
      }
      fieldWriter.integer(root.getNameSegment().getPath());
    }
  }

  public UserException.Builder getExceptionWithContext(UserException.Builder exceptionBuilder, String field, String msg,
      Object... args) {
    return null;
  }

  public UserException.Builder getExceptionWithContext(Throwable exception, String field, String msg, Object... args) {
    return null;
  }

  private void ensure(final int length) {
    workBuf = workBuf.reallocIfNeeded(length);
  }

}