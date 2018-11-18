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

import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.ColumnMetadata.StructureType;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.msgpack.MsgpackParsingException;
import org.apache.drill.exec.vector.complex.fn.FieldSelection;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

/**
 * This writer handles the msgpack value type MAP.
 */
public class MapValueWriter extends ComplexValueWriter {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MapValueWriter.class);

  public MapValueWriter(EnumMap<ValueType, AbstractValueWriter> valueWriterMap) {
    super(valueWriterMap);
  }

  @Override
  public ValueType getMsgpackValueType() {
    return ValueType.MAP;
  }

  /**
   * Write the given map value into drill's map or list writers.
   */
  @Override
  public void write(Value v, MapWriter mapWriter, String fieldName, ListWriter listWriter, FieldSelection selection,
      ColumnMetadata schema) {

    if (context.hasSchema()) {
      if (schema == null) {
        logger.debug("------no schema for map value -> skipping.");
        return;
      } else if (schema.structureType() != StructureType.TUPLE) {
        logger.debug("------schema is not a MAP for map value -> skipping.");
        return;
      }
    }

    MapValue value = v.asMapValue();
    if (mapWriter != null) {
      // We are inside a mapWriter (inside a map) and we encoutered a field of type
      // MAP. Write map in a map.
      MapWriter subMapWriter = mapWriter.map(fieldName);
      context.getFieldPathTracker().enter(fieldName);
      writeMapValue(value, subMapWriter, selection, schema == null ? null : schema.mapSchema());
      context.getFieldPathTracker().leave();
    } else {
      // We are inside a listWriter (inside an array) and we encoutered an element of
      // type MAP. Write map in a list.
      MapWriter subMapWriter = listWriter.map();
      context.getFieldPathTracker().enter("[]");
      writeMapValue(value, subMapWriter, selection, schema == null ? null : schema.mapSchema());
      context.getFieldPathTracker().leave();
    }
  }

  public void writeMapValue(MapValue value, MapWriter writer, FieldSelection selection, TupleMetadata tupleMetadata) {

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
          if (context.isLenient()) {
            context.parseWarn();
            continue;
          } else {
            throw new MsgpackParsingException("Failed to parse fieldname.");
          }
        }
        FieldSelection childSelection = selection.getChild(fieldName);
        if (childSelection.isNeverValid()) {
          logger.trace("Skipping not selected field: {}.{}", context.getFieldPathTracker(), fieldName);
          continue;
        }
        ColumnMetadata childSchema = getChildSchema(tupleMetadata, fieldName);
        writeElement(element, writer, null, fieldName, childSelection, childSchema);
      }
    } finally {
      writer.end();
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
   * @param v
   *            the value (key)
   * @return the key represented as a java string.
   */
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
}
