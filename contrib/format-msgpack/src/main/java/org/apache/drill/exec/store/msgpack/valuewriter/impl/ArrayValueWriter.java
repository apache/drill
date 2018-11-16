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

import java.util.Collection;
import java.util.EnumMap;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.msgpack.MsgpackParsingException;
import org.apache.drill.exec.vector.complex.fn.FieldSelection;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

/**
 * This writer handles the msgpack value type ARRAY.
 */
public class ArrayValueWriter extends ComplexValueWriter {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ArrayValueWriter.class);

  private List<ListWriter> emptyArrayWriters;

  public ArrayValueWriter(EnumMap<ValueType, AbstractValueWriter> valueWriterMap, List<ListWriter> emptyArrayWriters) {
    super(valueWriterMap);
    this.emptyArrayWriters = emptyArrayWriters;
  }

  @Override
  public ValueType getMsgpackValueType() {
    return ValueType.ARRAY;
  }

  /**
   * Write the given array value into drill's map or list writers.
   */
  @Override
  public void write(Value v, MapWriter mapWriter, String fieldName, ListWriter listWriter, FieldSelection selection,
      MaterializedField schema) {

    ArrayValue value = v.asArrayValue();
    if (mapWriter != null) {
      // Write the array in map.
      ListWriter subListWriter = mapWriter.list(fieldName);
      MaterializedField subSchema = getArrayInMapSubSchema(schema);
      context.getFieldPathTracker().enter(fieldName);
      writeArrayValue(value, subListWriter, selection, subSchema);
      context.getFieldPathTracker().leave();
    } else {
      // Write the array in array.
      ListWriter subListWriter = listWriter.list();
      MaterializedField subSchema = getArrayInArraySubSchema(schema);
      context.getFieldPathTracker().enter("[]");
      writeArrayValue(value, subListWriter, selection, subSchema);
      context.getFieldPathTracker().leave();
    }
  }

  private void writeArrayValue(Value value, ListWriter listWriter, FieldSelection selection, MaterializedField schema) {
    if (context.hasSchema()) {
      if (schema == null) {
        logger.debug("------no schema to write list value -> skipping");
        return;
      }
    }
    listWriter.startList();
    try {
      ArrayValue arrayValue = value.asArrayValue();
      for (int i = 0; i < arrayValue.size(); i++) {
        Value element = arrayValue.get(i);
        if (!element.isNilValue()) {
          writeElement(element, null, listWriter, null, selection, schema);
        }
      }
    } finally {
      addIfNotInitialized(listWriter);
      listWriter.endList();
    }
  }

  /**
   * This method is used to determine the type contained in an array when that
   * array is a field of a map.
   *
   * @param schema
   *                 the schema of the array
   * @return the type of the elements in the array
   */
  private MaterializedField getArrayInMapSubSchema(MaterializedField schema) {
    if (!context.hasSchema()) {
      // We don't have a shema to work with.
      return null;
    }
    if (schema.getType().getMinorType() == MinorType.MAP) {
      // If we are dealing with a MAP REPEATED, that is an array of map we know
      // the schema of the elements are MAP.
      return schema;
    } else {
      // TODO: give more details what we are doing here.
      Collection<MaterializedField> children = schema.getChildren();
      if (!children.isEmpty()) {
        MaterializedField childSchema = children.iterator().next();
        return childSchema;
      }
      return null;
    }
  }

  /**
   * This method is used to determine the type contained in an array when that
   * array is within an array.
   *
   * @param schema
   *                 the schema of the array
   * @return the type of the elements in the array
   */
  private MaterializedField getArrayInArraySubSchema(MaterializedField schema) {
    if (!context.hasSchema()) {
      // We don't have a shema to work with.
      return null;
    }
    Collection<MaterializedField> children = schema.getChildren();
    MaterializedField childSchema = children.iterator().next();
    // TODO: give more details what we are doing here.
    if (childSchema == null) {
      throw new MsgpackParsingException("Array in array element has no child schema.");
    }
    return childSchema;
  }

  /**
   * Checks that list has not been initialized and adds it to the
   * emptyArrayWriters collection.
   *
   * @param list
   *               ListWriter that should be checked
   */
  private void addIfNotInitialized(ListWriter list) {
    if (list.getValueCapacity() == 0) {
      emptyArrayWriters.add(list);
    }
  }
}
