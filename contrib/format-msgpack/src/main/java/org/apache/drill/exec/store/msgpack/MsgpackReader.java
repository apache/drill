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
import java.util.ServiceLoader;

import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.msgpack.valuewriter.ExtensionValueWriter;
import org.apache.drill.exec.store.msgpack.valuewriter.impl.AbstractValueWriter;
import org.apache.drill.exec.store.msgpack.valuewriter.impl.ArrayValueWriter;
import org.apache.drill.exec.store.msgpack.valuewriter.impl.BinaryValueWriter;
import org.apache.drill.exec.store.msgpack.valuewriter.impl.BooleanValueWriter;
import org.apache.drill.exec.store.msgpack.valuewriter.impl.FallbackExtensionValueWriter;
import org.apache.drill.exec.store.msgpack.valuewriter.impl.FloatValueWriter;
import org.apache.drill.exec.store.msgpack.valuewriter.impl.IntegerValueWriter;
import org.apache.drill.exec.store.msgpack.valuewriter.impl.MapValueWriter;
import org.apache.drill.exec.store.msgpack.valuewriter.impl.StringValueWriter;
import org.apache.drill.exec.vector.complex.fn.FieldSelection;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessageInsufficientBufferException;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ValueType;

public class MsgpackReader {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MsgpackReader.class);
  private final List<SchemaPath> columns;
  private final FieldSelection rootSelection;
  private final MessageUnpacker unpacker;
  private final boolean isSelectCount;

  /**
   * Collection for tracking empty array writers during reading and storing them
   * for initializing empty arrays
   */
  private final List<ListWriter> emptyArrayWriters = Lists.newArrayList();
  private MapValueWriter mapValueWriter;
  private EnumMap<ValueType, AbstractValueWriter> valueWriterMap = new EnumMap<>(ValueType.class);

  public MsgpackReader(InputStream stream, List<SchemaPath> columns, boolean isSelectCount) {
    // TODO: how to handle splits? can msgpack be split?

    this.unpacker = MessagePack.newDefaultUnpacker(stream);
    this.columns = columns;
    this.isSelectCount = isSelectCount;
    this.rootSelection = FieldSelection.getFieldSelection(columns);
  }

  public void setup(MsgpackReaderContext context) {
    // Pass the drillBuf to all value type writers they need to write to it.
    // Also passing in the context which has the lenient flag, record count, file
    // name which are used to print detailed error messages.

    ExtensionValueWriter[] extensionWriters = new ExtensionValueWriter[128];

    FallbackExtensionValueWriter fallbackExtensionValueWriter = new FallbackExtensionValueWriter();
    fallbackExtensionValueWriter.setup(context);
    for (int i = 0; i < extensionWriters.length; i++) {
      extensionWriters[i] = fallbackExtensionValueWriter;
    }

    ServiceLoader<ExtensionValueWriter> loader = ServiceLoader.load(ExtensionValueWriter.class);
    for (ExtensionValueWriter msgpackExtensionWriter : loader) {
      logger.debug("Loaded msgpack extension reader: " + msgpackExtensionWriter.getClass());
      msgpackExtensionWriter.setup(context);
      byte idx = msgpackExtensionWriter.getExtensionTypeNumber();
      extensionWriters[idx] = msgpackExtensionWriter;
    }

    // Create a value writer for each type supported by the msgpack library.
    FloatValueWriter fvw = new FloatValueWriter();
    fvw.setup(context);
    valueWriterMap.put(fvw.getMsgpackValueType(), fvw);
    IntegerValueWriter ivw = new IntegerValueWriter();
    ivw.setup(context);
    valueWriterMap.put(ivw.getMsgpackValueType(), ivw);
    BooleanValueWriter bvw = new BooleanValueWriter();
    bvw.setup(context);
    valueWriterMap.put(bvw.getMsgpackValueType(), bvw);
    StringValueWriter svw = new StringValueWriter();
    svw.setup(context);
    valueWriterMap.put(svw.getMsgpackValueType(), svw);
    BinaryValueWriter bbvw = new BinaryValueWriter();
    bbvw.setup(context);
    valueWriterMap.put(bbvw.getMsgpackValueType(), bbvw);

    // The array and map value writers use this map to retrieve the child value
    // writer. See ComplexValueWriter.
    // We also want to keep track of empty arrays we encounter. This is used by the
    // ensureAtLeastOneField method below.
    ArrayValueWriter avw = new ArrayValueWriter(valueWriterMap, extensionWriters, emptyArrayWriters);
    avw.setup(context);
    valueWriterMap.put(avw.getMsgpackValueType(), avw);
    // We are keeping a reference on the map value writer since the root is a map.
    // See writeRecord() below.
    mapValueWriter = new MapValueWriter(valueWriterMap, extensionWriters);
    mapValueWriter.setup(context);
    valueWriterMap.put(mapValueWriter.getMsgpackValueType(), mapValueWriter);
  }

  /**
   * Write a single message from the msgpack file into the given writer.
   *
   * @param writer writer to write a single message to.
   * @param schema schema of the messages.
   * @return true if there are more messages in the msgpack file.
   * @throws IOException
   * @throws MessageInsufficientBufferException
   */
  public boolean write(ComplexWriter writer, TupleMetadata schema)
      throws IOException, MessageInsufficientBufferException {
    if (!unpacker.hasNext()) {
      // The unpacker has no more messages, we reached the end of the file.
      return false;
    }

    try {
      // unpack a single message (a map with all it's children)
      MessageFormat nextFormat = unpacker.getNextFormat();
      ValueType type = nextFormat.getValueType();
      if (type == ValueType.MAP) {
        writeOneMessage(unpacker, writer, schema);
      } else {
        unpacker.skipValue();
        throw new MsgpackParsingException(
            "Value in root of message pack file is not of type MAP. Skipping type found: " + type);
      }
    } catch (MessageInsufficientBufferException e) {
      // When the msgpack library encounters a message which contains a map with an
      // odd number of entries
      // it cannot pair the key and the values. It will throw this exception.
      throw new MsgpackParsingException("Failed to unpack MAP, possibly because key/value tuples do not match.");
    }
    return true;
  }

  /**
   * Write a single message (a map). Possibly skipping over the actuall writing if
   * the query is a select count(*).
   *
   * @param value
   * @param writer
   * @param schema
   * @throws IOException
   */
  private void writeOneMessage(MessageUnpacker unpacker, ComplexWriter writer, TupleMetadata schema)
      throws IOException {
    if (isSelectCount) {
      unpacker.skipValue();
      writer.rootAsMap().bit("count").writeBit(1);
    } else {
      logger.debug("start writing message");
      mapValueWriter.writeMapValue(unpacker, writer.rootAsMap(), this.rootSelection, schema);
      logger.debug("end writing message");
    }
  }

  /**
   * This method was taken from the JSON format plugin. Same idea as in JSON if
   * arrays are empty we want to write a single integer into them.
   */
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
      if (i == 0) {
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
        fieldWriter.integer(fieldPath.getNameSegment().getPath());
      }
    }

    for (BaseWriter.ListWriter field : emptyArrayWriters) {
      // checks that array has not been initialized
      if (field.getValueCapacity() == 0) {
        field.integer();
      }
    }
  }
}