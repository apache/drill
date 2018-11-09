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

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.msgpack.MsgpackParsingException;
import org.apache.drill.exec.vector.complex.fn.FieldSelection;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;
import org.slf4j.helpers.MessageFormatter;

/**
 * This is the base class for complex values either MAP or ARRAY. This class
 * central method is the writeElement method which handles writing any value
 * type MAP, ARRAY, BOOLEAN, STRING, FLOAT, INTEGER etc.
 */
public abstract class ComplexValueWriter extends AbstractValueWriter {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ComplexValueWriter.class);

  protected EnumMap<ValueType, AbstractValueWriter> valueWriterMap;

  public ComplexValueWriter(EnumMap<ValueType, AbstractValueWriter> valueWriterMap) {
    super();
    this.valueWriterMap = valueWriterMap;
  }

  protected MinorType getTypeSafe(MaterializedField schema) {
    if (schema != null) {
      return schema.getType().getMinorType();
    }
    return null;
  }

  protected DataMode getDataModeSafe(MaterializedField schema) {
    if (schema != null) {
      return schema.getDataMode();
    }
    return null;
  }

  /**
   * Writes a value into the given map or list.
   *
   * @param value
   *                     the value to write
   * @param mapWriter
   * @param listWriter
   * @param fieldName
   * @param selection
   * @param schema
   *                     the desired schema for the given value
   */
  protected void writeElement(Value value, MapWriter mapWriter, ListWriter listWriter, String fieldName,
      FieldSelection selection, MaterializedField schema) {

    if (logger.isDebugEnabled()) {
      logDebug(value, mapWriter, fieldName, schema);
    } else if (logger.isTraceEnabled()) {
      logTrace(value, mapWriter, fieldName, schema);
    }
    try {
      // Get the type of the value. It can be any of the MAP, ARRAY, FLOAT, BOOLEAN,
      // STRING, INTEGER.
      ValueType valueType = value.getValueType();
      // We use that type to retrieve the corresponding writer.
      AbstractValueWriter writer = valueWriterMap.get(valueType);
      // Use writer to write the value into the drill map or list writers.
      writer.write(value, mapWriter, fieldName, listWriter, selection, schema);
    } catch (Exception e) {
      String message = null;
      if (mapWriter != null) {
        message = MessageFormatter
            .arrayFormat("failed to write type: '{}' value: '{}' into map at '{}.{}' target schema: '{}'\n",
                new Object[] { value.getValueType(), value, context.getFieldPathTracker(), fieldName, schema })
            .getMessage();
      } else {
        message = MessageFormatter
            .arrayFormat("failed to write type: '{}' value: '{}' into list at '{}.[]' target schema: '{}'\n",
                new Object[] { value.getValueType(), value, context.getFieldPathTracker(), schema })
            .getMessage();
      }
      if (context.isLenient()) {
        context.warn(message, e);
      } else {
        throw new MsgpackParsingException(message, e);
      }
    }
  }

  private void logDebug(Value value, MapWriter mapWriter, String fieldName, MaterializedField schema) {
    if (schema != null) {
      logger.debug("write type: '{}' into {} at '{}.{}' target type: '{}' mode: '{}'", value.getValueType(),
          mapWriter == null ? "list" : "map", context.getFieldPathTracker(), fieldName, schema.getType().getMinorType(),
          schema.getDataMode());
    } else {
      logger.debug("write type: '{}' into {} at '{}.{}'", value.getValueType(), mapWriter == null ? "list" : "map",
          context.getFieldPathTracker(), fieldName);
    }
  }

  private void logTrace(Value value, MapWriter mapWriter, String fieldName, MaterializedField schema) {
    if (schema != null) {
      logger.trace("write type: '{}' value: '{}' into {} at '{}.{}' target type: '{}' mode: '{}'", value.getValueType(),
          value, mapWriter == null ? "list" : "map", context.getFieldPathTracker(), fieldName,
          schema.getType().getMinorType(), schema.getDataMode());
    } else {
      logger.trace("write type: '{}' value: '{}' into {} at '{}.{}'", value.getValueType(), value,
          mapWriter == null ? "list" : "map", context.getFieldPathTracker(), fieldName);
    }
  }

}
