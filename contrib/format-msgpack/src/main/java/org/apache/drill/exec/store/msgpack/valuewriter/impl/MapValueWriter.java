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
import java.util.EnumMap;

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.ColumnMetadata.StructureType;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.msgpack.MsgpackParsingException;
import org.apache.drill.exec.store.msgpack.valuewriter.ExtensionValueWriter;
import org.apache.drill.exec.vector.complex.fn.FieldSelection;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.MessageBuffer;
import org.msgpack.value.ValueType;

/**
 * This writer handles the msgpack value type MAP.
 */
public class MapValueWriter extends ComplexValueWriter {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MapValueWriter.class);

  public MapValueWriter(EnumMap<ValueType, AbstractValueWriter> valueWriterMap,
      ExtensionValueWriter[] extensionReaders) {
    super(valueWriterMap, extensionReaders);
  }

  @Override
  public ValueType getMsgpackValueType() {
    return ValueType.MAP;
  }

  /**
   * Write the given map value into drill's map or list writers.
   */
  @Override
  public void write(MessageUnpacker unpacker, MapWriter mapWriter, String fieldName, ListWriter listWriter,
      FieldSelection selection, ColumnMetadata schema) throws IOException {

    if (context.hasSchema()) {
      if (schema == null) {
        logger.debug("------no schema for map value -> skipping.");
        return;
      } else if (schema.structureType() != StructureType.TUPLE) {
        logger.debug("------schema is not a MAP for map value -> skipping.");
        return;
      }
    }

    if (mapWriter != null) {
      // We are inside a mapWriter (inside a map) and we encoutered a field of type
      // MAP. Write map in a map.
      MapWriter subMapWriter = mapWriter.map(fieldName);
      writeMapValue(unpacker, subMapWriter, selection, schema == null ? null : schema.mapSchema());
    } else {
      // We are inside a listWriter (inside an array) and we encoutered an element of
      // type MAP. Write map in a list.
      MapWriter subMapWriter = listWriter.map();
      writeMapValue(unpacker, subMapWriter, selection, schema == null ? null : schema.mapSchema());
    }
  }

  public void writeMapValue(MessageUnpacker unpacker, MapWriter writer, FieldSelection selection,
      TupleMetadata tupleMetadata) throws IOException {

    try {
      context.getFieldPathTracker().enterMap();
      writer.start();

      int n = unpacker.unpackMapHeader();
      for (int i = 0; i < n; i++) {
        String fieldName = getFieldName(unpacker);
        if (fieldName == null) {
          // skip the value associated with this key (the key we can't read)
          unpacker.skipValue();
          if (context.isLenient()) {
            context.parseWarn();
            continue;
          } else {
            throw new MsgpackParsingException("Failed to parse fieldname.");
          }
        }
        FieldSelection childSelection = selection.getChild(fieldName);
        if (childSelection.isNeverValid()) {
          logger.debug("Skipping not selected field: {}", context.getFieldPathTracker(), fieldName);
          unpacker.skipValue();
          continue;
        }

        if (unpacker.getNextFormat().getValueType() == ValueType.NIL) {
          unpacker.skipValue();
          continue;
        }

        ColumnMetadata childSchema = null;
        if (context.hasSchema()) {
          childSchema = tupleMetadata.metadata(fieldName);
          if (!context.isLearningSchema()) {
            if (childSchema == null) {
              unpacker.skipValue();
              if (context.isLenient()) {
                logger.debug("Skipping field with no schema: {}", context.getFieldPathTracker(), fieldName);
                continue;
              } else {
                throw new MsgpackParsingException(context.getFieldPathTracker() + " has no child schema.");
              }
            }
          }
        }
        writeElement(unpacker, writer, null, fieldName, childSelection, childSchema);
      }
    } finally {
      writer.end();
      context.getFieldPathTracker().leaveMap();
    }
  }

  /**
   * Get the schema of the given field
   *
   * @param tupleMetadata
   *                        the schema of the map we are writing.
   * @param fieldName
   *                        the name of the field we want to write.
   * @return the schema of the field
   */
  private ColumnMetadata getChildSchema(TupleMetadata tupleMetadata, String fieldName) {
    if (!context.hasSchema()) {
      // Not using a schema.
      return null;
    }
    // Find the schema of the field.
    ColumnMetadata c = tupleMetadata.metadata(fieldName);
    if (c == null) {
      throw new MsgpackParsingException("Field name: " + fieldName + " has no child schema.");
    }
    return c;
  }

  /**
   * In message pack the field names (key) are msgpack values. That is they can be
   * string or number etc. This method tries coerce the msgpack value into a
   * string so it can write the key into drill's MapWriter.
   *
   * @param unpacker
   *                   the value (key)
   * @return the key represented as a java string.
   */
  private String getFieldName(MessageUnpacker unpacker) throws IOException {
    ValueType type = unpacker.getNextFormat().getValueType();
    if (type == ValueType.STRING) {
      int size = unpacker.unpackRawStringHeader();
      MessageBuffer messageBuffer = unpacker.readPayloadAsReference(size);
      ByteBuffer byteBuffer = messageBuffer.sliceAsByteBuffer();
      return context.getFieldPathTracker().select(byteBuffer);
    } else if (type == ValueType.BINARY) {
      int size = unpacker.unpackBinaryHeader();
      MessageBuffer messageBuffer = unpacker.readPayloadAsReference(size);
      ByteBuffer byteBuffer = messageBuffer.sliceAsByteBuffer();
      return context.getFieldPathTracker().select(byteBuffer);
    } else if (type == ValueType.INTEGER) {
      long longValue = unpacker.unpackLong();
      String fieldName = Long.toString(longValue);
      return context.getFieldPathTracker().select(ByteBuffer.wrap(fieldName.getBytes()));
    } else {
      unpacker.skipValue();
      return null;
    }
  }
}
