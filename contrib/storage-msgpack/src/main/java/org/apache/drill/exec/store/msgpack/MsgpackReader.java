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
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.vector.complex.fn.FieldSelection;
import org.apache.drill.exec.vector.complex.impl.MapOrListWriterImpl;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.msgpack.value.ArrayValue;
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
  private final boolean readNumbersAsDouble;
  private DrillBuf workBuf;
  private FieldSelection rootSelection;

  public MsgpackReader(DrillBuf managedBuf, boolean allTextMode, boolean readNumbersAsDouble) {
    this(managedBuf, GroupScan.ALL_COLUMNS, readNumbersAsDouble);
  }

  public MsgpackReader(DrillBuf managedBuf, List<SchemaPath> columns, boolean readNumbersAsDouble) {
    assert Preconditions.checkNotNull(columns).size() > 0 : "bson record reader requires at least a column";
    this.readNumbersAsDouble = readNumbersAsDouble;
    this.workBuf = managedBuf;
    this.columns = columns;
    rootSelection = FieldSelection.getFieldSelection(columns);
  }

  @Override
  public ReadState write(ComplexWriter writer) throws IOException {
    if (!unpacker.hasNext()) {
      return ReadState.END_OF_STREAM;
    }

    Value v = unpacker.unpackValue();
    ValueType type = v.getValueType();
    switch (type) {
    case MAP:
      writeToMap(v, new MapOrListWriterImpl(writer.rootAsMap()), this.rootSelection);
      break;
    default:
      throw new DrillRuntimeException("Root objects must be of MAP type. Found: " + type);
    }

    return ReadState.WRITE_SUCCEED;
  }

  private void writeToList(Value listOrMapValue, final MapOrListWriterImpl writer, FieldSelection selection) throws IOException {
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

  private void writeToMap(Value listOrMapValue, final MapOrListWriterImpl writer, FieldSelection selection) throws IOException {
    writer.start();
    try {
      MapValue mapValue = listOrMapValue.asMapValue();
      Set<Map.Entry<Value, Value>> entries = mapValue.entrySet();
      for (Map.Entry<Value, Value> entry : entries) {
        Value key = entry.getKey();
        String fieldName = key.asStringValue().asString();
        FieldSelection childSelection = selection.getChild(fieldName);
        if (childSelection.isNeverValid()) {
          // consume entire value by simply not writing it.
          continue;
        }
        Value value = entry.getValue();
        writeElement(value, writer, false, fieldName, childSelection);
      }

    } finally {
      writer.end();
    }
  }

  private void writeElement(Value v, final MapOrListWriterImpl writer, boolean isList, String fieldName, FieldSelection selection)
      throws IOException {

    ValueType valueType = v.getValueType();
    switch (valueType) {
    case INTEGER:
      IntegerValue iv = v.asIntegerValue();
      if (iv.isInIntRange()) {
        int i = iv.toInt();
        if (readNumbersAsDouble) {
          writeDouble(i, writer, fieldName, isList);
        } else {
          writeInt32(i, writer, fieldName, isList);
        }
      } else if (iv.isInLongRange()) {
        long l = iv.toLong();
        if (readNumbersAsDouble) {
          writeDouble(l, writer, fieldName, isList);
        } else {
          writeInt64(l, writer, fieldName, isList);
        }
      } else {
        @SuppressWarnings("unused")
        BigInteger i = iv.toBigInteger();
        throw new DrillRuntimeException("UnSupported messagepack type: " + valueType + " with BigInteger value");
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
    default:
      // BINARY not supported yet
      throw new DrillRuntimeException("UnSupported messagepack type: " + valueType);
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