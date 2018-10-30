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
import java.io.InputStream;
import java.util.BitSet;
import java.util.EnumMap;
import java.util.List;

import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.msgpack.valuewriter.AbstractValueWriter;
import org.apache.drill.exec.store.msgpack.valuewriter.ArrayValueWriter;
import org.apache.drill.exec.store.msgpack.valuewriter.BinaryValueWriter;
import org.apache.drill.exec.store.msgpack.valuewriter.BooleanValueWriter;
import org.apache.drill.exec.store.msgpack.valuewriter.ExtensionValueWriter;
import org.apache.drill.exec.store.msgpack.valuewriter.FloatValueWriter;
import org.apache.drill.exec.store.msgpack.valuewriter.IntegerValueWriter;
import org.apache.drill.exec.store.msgpack.valuewriter.MapValueWriter;
import org.apache.drill.exec.store.msgpack.valuewriter.StringValueWriter;
import org.apache.drill.exec.vector.complex.fn.FieldSelection;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.msgpack.core.MessageInsufficientBufferException;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

import io.netty.buffer.DrillBuf;

public class MsgpackReader {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MsgpackReader.class);
  private final List<SchemaPath> columns;
  private final FieldSelection rootSelection;
  protected MessageUnpacker unpacker;
  protected MsgpackReaderContext context;
  private final boolean skipQuery;

  /**
   * Collection for tracking empty array writers during reading and storing them
   * for initializing empty arrays
   */
  private final List<ListWriter> emptyArrayWriters = Lists.newArrayList();
  private boolean allTextMode = false;
  private MapValueWriter mapValueWriter;

  public MsgpackReader(InputStream stream, MsgpackReaderContext context, DrillBuf managedBuf, List<SchemaPath> columns,
      boolean skipQuery) {

    this.context = context;
    this.context.workBuf = managedBuf;
    this.unpacker = MessagePack.newDefaultUnpacker(stream);
    this.columns = columns;
    this.skipQuery = skipQuery;
    rootSelection = FieldSelection.getFieldSelection(columns);
    EnumMap<ValueType, AbstractValueWriter> valueWriterMap = new EnumMap<>(ValueType.class);
    valueWriterMap.put(ValueType.ARRAY, new ArrayValueWriter(valueWriterMap, emptyArrayWriters));
    valueWriterMap.put(ValueType.FLOAT, new FloatValueWriter());
    valueWriterMap.put(ValueType.INTEGER, new IntegerValueWriter());
    valueWriterMap.put(ValueType.BOOLEAN, new BooleanValueWriter());
    valueWriterMap.put(ValueType.STRING, new StringValueWriter());
    valueWriterMap.put(ValueType.BINARY, new BinaryValueWriter());
    valueWriterMap.put(ValueType.EXTENSION, new ExtensionValueWriter());
    mapValueWriter = new MapValueWriter(valueWriterMap);
    valueWriterMap.put(ValueType.MAP, mapValueWriter);

    for (AbstractValueWriter w : valueWriterMap.values()) {
      w.setup(this.context);
    }
  }

  public boolean write(ComplexWriter writer, MaterializedField schema)
      throws IOException, MessageInsufficientBufferException {
    if (!unpacker.hasNext()) {
      return false;
    }

    context.useSchema = schema != null;

    try {
      Value v = unpacker.unpackValue();
      ValueType type = v.getValueType();
      if (type == ValueType.MAP) {
        writeRecord(v.asMapValue(), writer, schema);
      } else {
        throw new MsgpackParsingException(
            "Value in root of message pack file is not of type MAP. Skipping type found: " + type);
      }
    } catch (MessageInsufficientBufferException e) {
      throw new MsgpackParsingException("Failed to unpack MAP, possibly because key/value tuples do not match.");
    }
    return true;
  }

  protected void writeRecord(MapValue value, ComplexWriter writer, MaterializedField schema) throws IOException {
    if (skipQuery) {
      writer.rootAsMap().bit("count").writeBit(1);
    } else {
      mapValueWriter.writeToMap(value, writer.rootAsMap(), this.rootSelection, schema);
    }
  }

//
//
//  private void ensure(final int length) {
//    workBuf = workBuf.reallocIfNeeded(length);
//  }

  @SuppressWarnings("resource")
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