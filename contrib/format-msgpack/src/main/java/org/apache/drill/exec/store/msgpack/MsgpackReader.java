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
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.drill.common.exceptions.DrillRuntimeException;
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
import org.msgpack.value.BinaryValue;
import org.msgpack.value.BooleanValue;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.FloatValue;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.StringValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

import io.netty.buffer.DrillBuf;

public class MsgpackReader extends BaseMsgpackReader {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MsgpackReader.class);
  private final List<SchemaPath> columns;
  public boolean atLeastOneWrite = false;
  private DrillBuf workBuf;
  private final FieldSelection rootSelection;
  private final ByteBuffer timestampReadBuffer = ByteBuffer.allocate(12);
  private boolean useSchema = false;

  /**
   * Collection for tracking empty array writers during reading and storing them
   * for initializing empty arrays
   */
  private final List<ListWriter> emptyArrayWriters = Lists.newArrayList();
  private boolean allTextMode = false;

  public MsgpackReader(InputStream stream, MsgpackReaderContext context, DrillBuf managedBuf,
      List<SchemaPath> columns) {
    super(stream, context);
    assert Preconditions.checkNotNull(columns).size() > 0 : "msgpack record reader requires at least a column";
    this.workBuf = managedBuf;
    this.columns = columns;
    rootSelection = FieldSelection.getFieldSelection(columns);
  }

  @Override
  protected ReadState writeRecord(MapValue value, ComplexWriter writer, MaterializedField schema) throws IOException {
    this.useSchema = schema != null;
    return writeToMap(value, writer.rootAsMap(), this.rootSelection, schema);
  }

  private ReadState writeMapValue(MapValue value, MapWriter mapWriter, String fieldName, ListWriter listWriter,
      FieldSelection selection, MaterializedField schema) {
    MapWriter subMapWriter;
    if (mapWriter != null) {
      // Write map in a map.
      subMapWriter = mapWriter.map(fieldName);
    } else {
      // Write map in a list.
      subMapWriter = listWriter.map();
    }
    return writeToMap(value, subMapWriter, selection, schema);
  }

  private ReadState writeArrayValue(ArrayValue value, MapWriter mapWriter, String fieldName, ListWriter listWriter,
      FieldSelection selection, MaterializedField schema) {
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
    return writeToList(value, subListWriter, selection, childSchema);
  }

  private ReadState writeToList(Value value, ListWriter listWriter, FieldSelection selection,
      MaterializedField schema) {
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

  private ReadState writeToMap(MapValue value, MapWriter writer, FieldSelection selection, MaterializedField schema) {

    writer.start();
    try {
      Set<Map.Entry<Value, Value>> valueEntries = value.entrySet();
      for (Map.Entry<Value, Value> valueEntry : valueEntries) {
        Value key = valueEntry.getKey();
        Value element = valueEntry.getValue();
        if (element.isNilValue()) {
          continue;
        }
        String fieldName = getFieldName(key);
        if (fieldName == null) {
          if (context.lenient) {
            context.parseWarn();
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
    if (!useSchema) {
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
      throw new DrillRuntimeException("UnSupported msgpack type: " + valueType);
    }
    return fieldName;
  }

  private ReadState writeElement(Value value, MapWriter mapWriter, ListWriter listWriter, String fieldName,
      FieldSelection selection, MaterializedField schema) {
    try {
      // if (!checkElementSchema(value, schema)) {
      // return MSG_RECORD_PARSE_ERROR;
      // }

      ValueType valueType = value.getValueType();
      switch (valueType) {
      case ARRAY:
        return writeArrayValue(value.asArrayValue(), mapWriter, fieldName, listWriter, selection, schema);
      case MAP:
        return writeMapValue(value.asMapValue(), mapWriter, fieldName, listWriter, selection, schema);
      case EXTENSION:
        ExtensionValue ev = value.asExtensionValue();
        byte extType = ev.getType();
        if (extType == -1) {
          writeTimestamp(ev, mapWriter, fieldName, listWriter);
        } else {
          byte[] bytes = ev.getData();
          writeAsVarBinary(bytes, mapWriter, fieldName, listWriter);
        }
        break;
      case FLOAT:
        writeFloatValue(value.asFloatValue(), mapWriter, fieldName, listWriter, schema);
        break;
      case INTEGER:
        writeIntegerValue(value.asIntegerValue(), mapWriter, fieldName, listWriter, schema);
        break;
      case BOOLEAN:
        writeBooleanValue(value.asBooleanValue(), mapWriter, fieldName, listWriter, schema);
        break;
      case STRING:
        writeStringValue(value.asStringValue(), mapWriter, fieldName, listWriter, schema);
        break;
      case BINARY:
        writeBinaryValue(value.asBinaryValue(), mapWriter, fieldName, listWriter, schema);
        break;
      default:
        throw new DrillRuntimeException("UnSupported msgpack type: " + valueType);
      }
    } catch (Exception e) {
      if (context.lenient) {
        context.warn("Failed to write element name: " + fieldName + " of type: " + value.getValueType()
            + " into list. File: " + context.hadoopPath + " line no: " + context.currentRecordNumberInFile() + " ", e);
        return WRITE_SUCCEED;
      } else {
        throw new DrillRuntimeException(e);
      }
    }
    return WRITE_SUCCEED;
  }

  private boolean checkChildSchema(MaterializedField childSchema) {
    if (!useSchema) {
      return true;
    }
    return childSchema != null;
  }

  private MaterializedField getArrayInArrayChildSchema(MaterializedField schema) {
    if (!useSchema) {
      return null;
    }
    Collection<MaterializedField> children = schema.getChildren();
    return children.iterator().next();
  }

  private MaterializedField getArrayInMapChildSchema(MaterializedField schema) {
    if (!useSchema) {
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
    if (!useSchema) {
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
      throw new DrillRuntimeException("Unsupported msgpack type: " + value);
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

    atLeastOneWrite = true;
    if (mapWriter != null) {
      mapWriter.timeStamp(fieldName).writeTimeStamp(epochMilliSeconds);
    } else {
      listWriter.timeStamp().writeTimeStamp(epochMilliSeconds);
    }
  }

  private void writeIntegerValue(IntegerValue value, MapWriter mapWriter, String fieldName, ListWriter listWriter,
      MaterializedField schema) {
    if (!value.isInLongRange()) {
      BigInteger i = value.toBigInteger();
      throw new DrillRuntimeException(
          "UnSupported messagepack type: " + value.getValueType() + " with BigInteger value: " + i);
    }

    MinorType targetSchemaType = MinorType.BIGINT;
    if (useSchema) {
      targetSchemaType = schema.getType().getMinorType();
    }
    switch (targetSchemaType) {
    case VARCHAR:
      String s = value.toString();
      writeAsVarChar(s.getBytes(), mapWriter, fieldName, listWriter);
      break;
    case VARBINARY:
      long longValue = value.toLong();
      ensure(8);
      workBuf.setLong(0, longValue);
      writeAsVarBinary(mapWriter, fieldName, listWriter, 8);
      break;
    case BIGINT:
      writeAsBigInt(value.toLong(), mapWriter, fieldName, listWriter);
      break;
    default:
      throw new DrillRuntimeException("Can't cast " + value.getValueType() + " into " + targetSchemaType);
    }
  }

  private void writeBinaryValue(BinaryValue value, MapWriter mapWriter, String fieldName, ListWriter listWriter,
      MaterializedField schema) {
    MinorType targetSchemaType = MinorType.VARBINARY;
    if (useSchema) {
      targetSchemaType = schema.getType().getMinorType();
    }
    if (context.readBinaryAsString) {
      targetSchemaType = MinorType.VARCHAR;
    }
    switch (targetSchemaType) {
    case VARCHAR:
      byte[] buff = value.asByteArray();
      writeAsVarChar(buff, mapWriter, fieldName, listWriter);
      break;
    case VARBINARY:
      byte[] binBuff = value.asByteArray();
      writeAsVarBinary(binBuff, mapWriter, fieldName, listWriter);
      break;
    default:
      throw new DrillRuntimeException("Can't cast " + value.getValueType() + " into " + targetSchemaType);
    }
  }

  private void writeStringValue(StringValue value, MapWriter mapWriter, String fieldName, ListWriter listWriter,
      MaterializedField schema) {
    MinorType targetSchemaType = MinorType.VARCHAR;
    if (useSchema) {
      targetSchemaType = schema.getType().getMinorType();
    }
    switch (targetSchemaType) {
    case VARCHAR:
      byte[] buff = value.asByteArray();
      writeAsVarChar(buff, mapWriter, fieldName, listWriter);
      break;
    case VARBINARY:
      byte[] binBuff = value.asByteArray();
      writeAsVarBinary(binBuff, mapWriter, fieldName, listWriter);
      break;
    default:
      throw new DrillRuntimeException("Can't cast " + value.getValueType() + " into " + targetSchemaType);
    }
  }

  private void writeFloatValue(FloatValue value, MapWriter mapWriter, String fieldName, ListWriter listWriter,
      MaterializedField schema) {
    MinorType targetSchemaType = MinorType.FLOAT8;
    if (useSchema) {
      targetSchemaType = schema.getType().getMinorType();
    }
    switch (targetSchemaType) {
    case VARCHAR:
      String s = value.toString();
      writeAsVarChar(s.getBytes(), mapWriter, fieldName, listWriter);
      break;
    case VARBINARY:
      double d = value.toDouble(); // use as double
      ensure(8);
      workBuf.setDouble(0, d);
      writeAsVarBinary(mapWriter, fieldName, listWriter, 8);
      break;
    case FLOAT8:
      writeAsFloat8(value.toDouble(), mapWriter, fieldName, listWriter);
      break;
    default:
      throw new DrillRuntimeException("Can't cast " + value.getValueType() + " into " + targetSchemaType);
    }
  }

  private void writeBooleanValue(BooleanValue value, MapWriter mapWriter, String fieldName, ListWriter listWriter,
      MaterializedField schema) {

    MinorType targetSchemaType = MinorType.BIT;
    if (useSchema) {
      targetSchemaType = schema.getType().getMinorType();
    }

    switch (targetSchemaType) {
    case VARCHAR:
      String s = value.toString();
      writeAsVarChar(s.getBytes(), mapWriter, fieldName, listWriter);
      break;
    case VARBINARY:
      boolean b = value.getBoolean();
      ensure(1);
      workBuf.setBoolean(0, b);
      writeAsVarBinary(mapWriter, fieldName, listWriter, 1);
      break;
    case BIT:
      writeAsBit(value.getBoolean(), mapWriter, fieldName, listWriter);
      break;
    default:
      throw new DrillRuntimeException("Can't cast " + value.getValueType() + " into " + targetSchemaType);
    }
  }

  private void writeAsVarBinary(byte[] bytes, MapWriter mapWriter, String fieldName, ListWriter listWriter) {
    int length = bytes.length;
    ensure(length);
    workBuf.setBytes(0, bytes);
    writeAsVarBinary(mapWriter, fieldName, listWriter, length);
  }

  private void writeAsVarBinary(MapWriter mapWriter, String fieldName, ListWriter listWriter, int length) {
    atLeastOneWrite = true;
    if (mapWriter != null) {
      mapWriter.varBinary(fieldName).writeVarBinary(0, length, workBuf);
    } else {
      listWriter.varBinary().writeVarBinary(0, length, workBuf);
    }
  }

  private void writeAsVarChar(byte[] readString, MapWriter mapWriter, String fieldName, ListWriter listWriter) {
    int length = readString.length;
    ensure(length);
    workBuf.setBytes(0, readString);
    writeAsVarChar(mapWriter, fieldName, listWriter, length);
  }

  private void writeAsVarChar(MapWriter mapWriter, String fieldName, ListWriter listWriter, int length) {
    atLeastOneWrite = true;
    if (mapWriter != null) {
      mapWriter.varChar(fieldName).writeVarChar(0, length, workBuf);
    } else {
      listWriter.varChar().writeVarChar(0, length, workBuf);
    }
  }

  private void writeAsFloat8(double d, MapWriter mapWriter, String fieldName, ListWriter listWriter) {
    atLeastOneWrite = true;
    if (mapWriter != null) {
      mapWriter.float8(fieldName).writeFloat8(d);
    } else {
      listWriter.float8().writeFloat8(d);
    }
  }

  private void writeAsBit(boolean readBoolean, MapWriter mapWriter, String fieldName, ListWriter listWriter) {
    atLeastOneWrite = true;
    int value = readBoolean ? 1 : 0;
    if (mapWriter != null) {
      mapWriter.bit(fieldName).writeBit(value);
    } else {
      listWriter.bit().writeBit(value);
    }
  }

  private void writeAsBigInt(long value, MapWriter mapWriter, String fieldName, ListWriter listWriter) {
    atLeastOneWrite = true;
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
   * Checks that list has not been initialized and adds it to the
   * emptyArrayWriters collection.
   *
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
      if (i == 0 && !allTextMode) {
        // when allTextMode is false, there is not much benefit to producing all
        // the empty fields; just produce 1 field. The reason is that the type of the
        // fields is unknown, so if we produce multiple Integer fields by default, a
        // subsequent batch that contains non-integer fields will error out in any case.
        // Whereas, with allTextMode true, we are sure that all fields are going to be
        // treated as varchar, so it makes sense to produce all the fields, and in fact
        // is necessary in order to avoid schema change exceptions by downstream
        // operators.
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