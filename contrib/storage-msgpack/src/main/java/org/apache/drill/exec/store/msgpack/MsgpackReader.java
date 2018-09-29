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
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
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

public class MsgpackReader extends BaseMsgpackReader {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MsgpackReader.class);
  private final List<SchemaPath> columns;
  private boolean atLeastOneWrite = false;
  private DrillBuf workBuf;
  private FieldSelection rootSelection;

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
  protected void writeRecord(Value mapValue, ComplexWriter writer) throws IOException {
    writeToMap(mapValue, new MapOrListWriterImpl(writer.rootAsMap()), this.rootSelection);
  }

  private void writeToList(Value listOrMapValue, final MapOrListWriterImpl writer, FieldSelection selection)
      throws IOException {
    writer.start();
    try {
      ArrayValue arrayValue = listOrMapValue.asArrayValue();
      for (int i = 0; i < arrayValue.size(); i++) {
        Value value = arrayValue.get(i);
        writeElement(value, writer, true, "", selection);
      }
    } finally {
      writer.end();
    }
  }

  private void writeToMap(Value listOrMapValue, final MapOrListWriterImpl writer, FieldSelection selection)
      throws IOException {
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
            writeElement(value, writer, false, fieldName, childSelection);
          }
        }
      }
    } finally {
      writer.end();
    }
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
      throw new DrillRuntimeException("UnSupported messagepack type: " + valueType);
    }
    return fieldName;
  }

  private void writeElement(Value v, final MapOrListWriterImpl writer, boolean isList, String fieldName,
      FieldSelection selection) throws IOException {

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
      writeToList(v, (MapOrListWriterImpl) writer.list(fieldName), selection);
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
      writeToMap(v, _writer, selection);
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
      throw new DrillRuntimeException("UnSupported messagepack type: " + valueType);
    }
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
    byte zero = 0;
    byte[] v = ev.getData();
    final TimeStampHolder ts = new TimeStampHolder();
    switch (v.length) {
    case 4: {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
      buffer.put(zero);
      buffer.put(zero);
      buffer.put(zero);
      buffer.put(zero);
      buffer.put(v);
      buffer.position(0);
      ts.value = buffer.getLong();
    }
      break;
    // uint32_t data32 = value.payload;
    // result.tv_nsec = 0;
    // result.tv_sec = data32;
    case 8: {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
      buffer.put(zero);
      buffer.put(zero);
      buffer.put(zero);
      buffer.put(zero);
      buffer.put(v);
      buffer.position(0);
      ts.value = buffer.getLong();
    }
      break;
    // uint64_t data64 = value.payload;
    // result.tv_nsec = data64 >> 34;
    // result.tv_sec = data64 & 0x00000003ffffffffL;
    case 12: {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
      buffer.put(zero);
      buffer.put(zero);
      buffer.put(zero);
      buffer.put(zero);
      buffer.put(v);
      buffer.position(0);
      ts.value = buffer.getLong();
    }
      break;
    // uint32_t data32 = value.payload;
    // uint64_t data64 = value.payload + 4;
    // result.tv_nsec = data32;
    // result.tv_sec = data64;
    default:
      // error
    }

    if (isList == false) {
      writer.timeStamp(fieldName).write(ts);
    } else {
      writer.list.timeStamp().write(ts);
    }
  }

  private void writeBinary(byte[] bytes, final MapOrListWriterImpl writer, String fieldName, boolean isList) {
    int length = bytes.length;
    ensure(length);
    workBuf.setBytes(0, bytes);
    final VarBinaryHolder vb = new VarBinaryHolder();
    vb.buffer = workBuf;
    vb.start = 0;
    vb.end = length;
    if (isList == false) {
      writer.varBinary(fieldName).write(vb);
    } else {
      writer.list.varBinary().write(vb);
    }
  }

  private void writeString(byte[] readString, final MapOrListWriterImpl writer, String fieldName, boolean isList) {
    int length = readString.length;
    ensure(length);
    workBuf.setBytes(0, readString);
    final VarCharHolder vh = new VarCharHolder();
    vh.buffer = workBuf;
    vh.start = 0;
    vh.end = length;
    if (isList == false) {
      writer.varChar(fieldName).write(vh);
    } else {
      writer.list.varChar().write(vh);
    }
  }

  private void writeDouble(double readDouble, final MapOrListWriterImpl writer, String fieldName, boolean isList) {
    final Float8Holder f8h = new Float8Holder();
    f8h.value = readDouble;
    if (isList == false) {
      writer.float8(fieldName).write(f8h);
    } else {
      writer.list.float8().write(f8h);
    }
  }

  private void writeBoolean(boolean readBoolean, final MapOrListWriterImpl writer, String fieldName, boolean isList) {
    final BitHolder bit = new BitHolder();
    bit.value = readBoolean ? 1 : 0;
    if (isList == false) {
      writer.bit(fieldName).write(bit);
    } else {
      writer.list.bit().write(bit);
    }
  }

  private void writeInt64(long readInt64, final MapOrListWriterImpl writer, String fieldName, boolean isList) {
    final BigIntHolder bh = new BigIntHolder();
    bh.value = readInt64;
    if (isList == false) {
      writer.bigInt(fieldName).write(bh);
    } else {
      writer.list.bigInt().write(bh);
    }
  }

  private void writeInt32(int readInt32, final MapOrListWriterImpl writer, String fieldName, boolean isList) {
    final IntHolder ih = new IntHolder();
    ih.value = readInt32;
    if (isList == false) {
      writer.integer(fieldName).write(ih);
    } else {
      writer.list.integer().write(ih);
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