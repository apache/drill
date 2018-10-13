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

import static org.apache.drill.exec.store.msgpack.BaseMsgpackReader.ReadState.MSG_RECORD_PARSE_ERROR;
import static org.apache.drill.exec.store.msgpack.BaseMsgpackReader.ReadState.WRITE_SUCCEED;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.complex.fn.FieldSelection;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
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
  private boolean skipInvalidSchemaMsgRecords = false;
  /**
   * Collection for tracking empty array writers during reading
   * and storing them for initializing empty arrays
   */
  private final List<ListWriter> emptyArrayWriters = Lists.newArrayList();
  private boolean allTextMode = false;

  public MsgpackReader(DrillBuf managedBuf, List<SchemaPath> columns) {
    assert Preconditions.checkNotNull(columns).size() > 0 : "msgpack record reader requires at least a column";
    this.workBuf = managedBuf;
    this.columns = columns;
    rootSelection = FieldSelection.getFieldSelection(columns);
  }

  @Override
  protected ReadState writeRecord(Value mapValue, ComplexWriter writer, MaterializedField schema) throws IOException {
    this.skipInvalidSchemaMsgRecords = schema != null;
    return writeToMap(mapValue, writer.rootAsMap(), this.rootSelection, schema);
  }

  private ReadState writeToList(Value value, ListWriter listWriter, FieldSelection selection, MaterializedField schema)
      throws IOException {
    listWriter.startList();
    try {
      ArrayValue arrayValue = value.asArrayValue();
      for (int i = 0; i < arrayValue.size(); i++) {
        Value element = arrayValue.get(i);
        if (!element.isNilValue()) {
          ReadState readState = writeElement(element, null, listWriter, null, selection, schema);
          if (readState != WRITE_SUCCEED) {
            return readState;
          }
        }
      }
    } finally {
      addIfNotInitialized(listWriter);
      listWriter.endList();
    }
    return WRITE_SUCCEED;
  }

  private ReadState writeToMap(Value value, MapWriter writer, FieldSelection selection, MaterializedField schema)
      throws IOException {

    writer.start();
    try {
      MapValue mapValue = value.asMapValue();
      Set<Map.Entry<Value, Value>> valueEntries = mapValue.entrySet();
      for (Map.Entry<Value, Value> valueEntry : valueEntries) {
        Value key = valueEntry.getKey();
        Value element = valueEntry.getValue();
        if (element.isNilValue()) {
          continue;
        }
        String fieldName = getFieldName(key);
        if (fieldName == null) {
          Log.warn("Failed to read field name of type: " + key.getValueType());
          if (skipInvalidKeyTypes) {
            continue;
          } else {
            return MSG_RECORD_PARSE_ERROR;
          }
        }
        FieldSelection childSelection = selection.getChild(fieldName);
        if (childSelection.isNeverValid()) {
          continue;
        }
        MaterializedField childSchema = getChildSchema(schema, fieldName);
        if (!checkChildSchema(childSchema)) {
          return MSG_RECORD_PARSE_ERROR;
        }
        ReadState readState = writeElement(element, writer, null, fieldName, childSelection, childSchema);
        if (readState != WRITE_SUCCEED) {
          return readState;
        }
      }
    } finally {
      writer.end();
    }

    return WRITE_SUCCEED;
  }

  private MaterializedField getChildSchema(MaterializedField schema, String fieldName) {
    if (!skipInvalidSchemaMsgRecords) {
      return null;
    }
    for (MaterializedField c : schema.getChildren()) {
      if (fieldName.equalsIgnoreCase(c.getName())) {
        return c;
      }
    }
    return null;
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

  private ReadState writeElement(Value value, MapWriter mapWriter, ListWriter listWriter, String fieldName,
      FieldSelection selection, MaterializedField schema) throws IOException {

    if (!checkElementSchema(value, schema)) {
      return MSG_RECORD_PARSE_ERROR;
    }

    ValueType valueType = value.getValueType();
    switch (valueType) {
    case ARRAY: {
      ListWriter subListWriter;
      MaterializedField childSchema;
      if (mapWriter != null) {
        // Write array in map.
        subListWriter = mapWriter.list(fieldName);
        childSchema = getArrayInMapChildSchema(schema);
      } else {
        // Write array in array.
        subListWriter = listWriter.list();
        childSchema = getArrayInArrayChildSchema(schema);
      }
      if (!checkChildSchema(childSchema)) {
        return MSG_RECORD_PARSE_ERROR;
      }
      ReadState readState = writeToList(value, subListWriter, selection, childSchema);
      if (readState == WRITE_SUCCEED) {
        atLeastOneWrite = true;
      }
      return readState;
    }
    case MAP: {
      MapWriter subMapWriter;
      if (mapWriter != null) {
        // Write map in a map.
        subMapWriter = mapWriter.map(fieldName);
      } else {
        // Write map in a list.
        subMapWriter = listWriter.map();
      }
      ReadState readState = writeToMap(value, subMapWriter, selection, schema);
      if (readState == WRITE_SUCCEED) {
        atLeastOneWrite = true;
      }
      return readState;
    }
    case FLOAT: {
      FloatValue fv = value.asFloatValue();
      double d = fv.toDouble(); // use as double
      writeDouble(d, mapWriter, fieldName, listWriter);
      atLeastOneWrite = true;
      return WRITE_SUCCEED;
    }
    case INTEGER: {
      IntegerValue iv = value.asIntegerValue();
      if (iv.isInIntRange() || iv.isInLongRange()) {
        long longVal = iv.toLong();
        writeInt64(longVal, mapWriter, fieldName, listWriter);
        atLeastOneWrite = true;
        return WRITE_SUCCEED;
      } else {
        BigInteger i = iv.toBigInteger();
        logger.warn("UnSupported messagepack type: " + valueType + " with BigInteger value: " + i);
        return MSG_RECORD_PARSE_ERROR;
      }
    }
    case EXTENSION: {
      ExtensionValue ev = value.asExtensionValue();
      byte extType = ev.getType();
      if (extType == -1) {
        writeTimestamp(ev, mapWriter, fieldName, listWriter);
      } else {
        byte[] bytes = ev.getData();
        writeBinary(bytes, mapWriter, fieldName, listWriter);
      }
      atLeastOneWrite = true;
      return WRITE_SUCCEED;
    }
    case BOOLEAN: {
      boolean b = value.asBooleanValue().getBoolean();
      writeBoolean(b, mapWriter, fieldName, listWriter);
      atLeastOneWrite = true;
      return WRITE_SUCCEED;
    }
    case STRING: {
      byte[] buff = value.asStringValue().asByteArray();
      writeString(buff, mapWriter, fieldName, listWriter);
      atLeastOneWrite = true;
      return WRITE_SUCCEED;
    }
    case BINARY: {
      byte[] bytes = value.asBinaryValue().asByteArray();
      writeBinary(bytes, mapWriter, fieldName, listWriter);
      atLeastOneWrite = true;
      return WRITE_SUCCEED;
    }
    default:
      logger.warn("UnSupported messagepack type: " + valueType);
      return MSG_RECORD_PARSE_ERROR;
    }
  }

  private boolean checkChildSchema(MaterializedField childSchema) {
    if (!skipInvalidSchemaMsgRecords) {
      return true;
    }
    return childSchema != null;
  }

  private MaterializedField getArrayInArrayChildSchema(MaterializedField schema) throws IOException {
    if (!skipInvalidSchemaMsgRecords) {
      return null;
    }
    Collection<MaterializedField> children = schema.getChildren();
    return children.iterator().next();
  }

  private MaterializedField getArrayInMapChildSchema(MaterializedField schema) throws IOException {
    if (!skipInvalidSchemaMsgRecords) {
      return null;
    }
    if (schema.getType().getMinorType() == MinorType.MAP) {
      return schema;
    } else {
      Collection<MaterializedField> children = schema.getChildren();
      return children.iterator().next();
    }
  }

  private boolean checkElementSchema(Value value, MaterializedField schema) {
    if (!skipInvalidSchemaMsgRecords) {
      return true;
    }

    ValueType valueType = value.getValueType();
    MinorType schemaType = schema.getType().getMinorType();
    switch (valueType) {
    case INTEGER:
      return schemaType == MinorType.BIGINT;
    case ARRAY:
      return schema.getDataMode() == DataMode.REPEATED;
    case BOOLEAN:
      return schemaType == MinorType.BIT;
    case MAP:
      return schemaType == MinorType.MAP;
    case FLOAT:
      return schemaType == MinorType.FLOAT8;
    case EXTENSION:
      ExtensionValue ev = value.asExtensionValue();
      byte extType = ev.getType();
      if (extType == -1) {
        return schemaType == MinorType.TIMESTAMP;
      } else {
        return schemaType == MinorType.VARBINARY;
      }
    case STRING:
      return schemaType == MinorType.VARCHAR;
    case BINARY:
      return schemaType == MinorType.VARBINARY;
    default:
      logger.warn("UnSupported messagepack type: " + value);
      return false;
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
  private void writeTimestamp(ExtensionValue ev, MapWriter mapWriter, String fieldName, ListWriter listWriter) {
    long epochMilliSeconds = 0;
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
      epochMilliSeconds = timestampReadBuffer.getLong() * 1000;
      break;
    }
    case 8: {
      timestampReadBuffer.position(0);
      timestampReadBuffer.put(data);
      timestampReadBuffer.position(0);
      long data64 = timestampReadBuffer.getLong();
      @SuppressWarnings("unused")
      long nanos = data64 >>> 34;
      long seconds = data64 & 0x00000003ffffffffL;
      epochMilliSeconds = (seconds * 1000) + (nanos / 1000000);
      break;
    }
    case 12: {
      timestampReadBuffer.position(0);
      timestampReadBuffer.put(data);
      timestampReadBuffer.position(0);
      int data32 = timestampReadBuffer.getInt();
      @SuppressWarnings("unused")
      long nanos = data32;
      long data64 = timestampReadBuffer.getLong();
      long seconds = data64;
      epochMilliSeconds = (seconds * 1000) + (nanos / 1000000);
      break;
    }
    default:
      logger.error("UnSupported built-in messagepack timestamp type (-1) with data length of: " + data.length);
    }

    if (mapWriter != null) {
      mapWriter.timeStamp(fieldName).writeTimeStamp(epochMilliSeconds);
    } else {
      listWriter.timeStamp().writeTimeStamp(epochMilliSeconds);
    }
  }

  private void writeBinary(byte[] bytes, MapWriter mapWriter, String fieldName, ListWriter listWriter) {
    int length = bytes.length;
    ensure(length);
    workBuf.setBytes(0, bytes);
    if (mapWriter != null) {
      mapWriter.varBinary(fieldName).writeVarBinary(0, length, workBuf);
    } else {
      listWriter.varBinary().writeVarBinary(0, length, workBuf);
    }
  }

  private void writeString(byte[] readString, MapWriter mapWriter, String fieldName, ListWriter listWriter) {
    int length = readString.length;
    ensure(length);
    workBuf.setBytes(0, readString);
    if (mapWriter != null) {
      mapWriter.varChar(fieldName).writeVarChar(0, length, workBuf);
    } else {
      listWriter.varChar().writeVarChar(0, length, workBuf);
    }
  }

  private void writeDouble(double value, MapWriter mapWriter, String fieldName, ListWriter listWriter) {
    if (mapWriter != null) {
      mapWriter.float8(fieldName).writeFloat8(value);
    } else {
      listWriter.float8().writeFloat8(value);
    }
  }

  private void writeBoolean(boolean readBoolean, MapWriter mapWriter, String fieldName, ListWriter listWriter) {
    int value = readBoolean ? 1 : 0;
    if (mapWriter != null) {
      mapWriter.bit(fieldName).writeBit(value);
    } else {
      listWriter.bit().writeBit(value);
    }
  }

  private void writeInt64(long value, MapWriter mapWriter, String fieldName, ListWriter listWriter) {
    if (mapWriter != null) {
      mapWriter.bigInt(fieldName).writeBigInt(value);
    } else {
      listWriter.bigInt().writeBigInt(value);
    }
  }

  private void ensure(final int length) {
    workBuf = workBuf.reallocIfNeeded(length);
  }

  /**
   * Checks that list has not been initialized and adds it to the emptyArrayWriters collection.
   * @param list ListWriter that should be checked
   */
  private void addIfNotInitialized(ListWriter list) {
    if (list.getValueCapacity() == 0) {
      emptyArrayWriters.add(list);
    }
  }

  @SuppressWarnings("resource")
  @Override
  public void ensureAtLeastOneField(ComplexWriter writer) {
    List<BaseWriter.MapWriter> writerList = Lists.newArrayList();
    List<PathSegment> fieldPathList = Lists.newArrayList();
    BitSet emptyStatus = new BitSet(columns.size());
    int i = 0;

    // first pass: collect which fields are empty
    for (SchemaPath sp : columns) {
      PathSegment fieldPath = sp.getRootSegment();
      BaseWriter.MapWriter fieldWriter = writer.rootAsMap();
      while (fieldPath.getChild() != null && !fieldPath.getChild().isArray()) {
        fieldWriter = fieldWriter.map(fieldPath.getNameSegment().getPath());
        fieldPath = fieldPath.getChild();
      }
      writerList.add(fieldWriter);
      fieldPathList.add(fieldPath);
      if (fieldWriter.isEmptyMap()) {
        emptyStatus.set(i, true);
      }
      if (i == 0 && !allTextMode ) {
        // when allTextMode is false, there is not much benefit to producing all
        // the empty fields; just produce 1 field. The reason is that the type of the
        // fields is unknown, so if we produce multiple Integer fields by default, a
        // subsequent batch that contains non-integer fields will error out in any case.
        // Whereas, with allTextMode true, we are sure that all fields are going to be
        // treated as varchar, so it makes sense to produce all the fields, and in fact
        // is necessary in order to avoid schema change exceptions by downstream operators.
        break;
      }
      i++;
    }

    // second pass: create default typed vectors corresponding to empty fields
    // Note: this is not easily do-able in 1 pass because the same fieldWriter
    // may be shared by multiple fields whereas we want to keep track of all fields
    // independently, so we rely on the emptyStatus.
    for (int j = 0; j < fieldPathList.size(); j++) {
      BaseWriter.MapWriter fieldWriter = writerList.get(j);
      PathSegment fieldPath = fieldPathList.get(j);
      if (emptyStatus.get(j)) {
        if (allTextMode) {
          fieldWriter.varChar(fieldPath.getNameSegment().getPath());
        } else {
          fieldWriter.integer(fieldPath.getNameSegment().getPath());
        }
      }
    }

    for (BaseWriter.ListWriter field : emptyArrayWriters) {
      // checks that array has not been initialized
      if (field.getValueCapacity() == 0) {
        if (allTextMode) {
          field.varChar();
        } else {
          field.integer();
        }
      }
    }
  }
}