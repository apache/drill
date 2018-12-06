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
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.VariantMetadata;
import org.apache.drill.exec.store.msgpack.MsgpackParsingException;
import org.apache.drill.exec.store.msgpack.valuewriter.ExtensionValueWriter;
import org.apache.drill.exec.vector.complex.fn.FieldSelection;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ValueType;

/**
 * This writer handles the msgpack value type ARRAY.
 */
public class ArrayValueWriter extends ComplexValueWriter {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ArrayValueWriter.class);

  private List<ListWriter> emptyArrayWriters;

  public ArrayValueWriter(EnumMap<ValueType, AbstractValueWriter> valueWriterMap,
      ExtensionValueWriter[] extensionReaders, List<ListWriter> emptyArrayWriters) {
    super(valueWriterMap, extensionReaders);
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
  public void write(MessageUnpacker unpacker, MapWriter mapWriter, String fieldName, ListWriter listWriter,
      FieldSelection selection, ColumnMetadata schema) throws IOException {

    if (mapWriter != null) {
      // Write the array in map.
      ListWriter subListWriter = mapWriter.list(fieldName);
      ColumnMetadata subSchema = getArrayInMapSubSchema(schema);
      writeArrayValue(unpacker, subListWriter, selection, subSchema);
    } else {
      // Write the array in array.
      ListWriter subListWriter = listWriter.list();
      ColumnMetadata subSchema = getArrayInArraySubSchema(schema);
      writeArrayValue(unpacker, subListWriter, selection, subSchema);
    }
  }

  private void writeArrayValue(MessageUnpacker unpacker, ListWriter listWriter, FieldSelection selection,
      ColumnMetadata subSchema) throws IOException {
    if (context.hasSchema()) {
      if (subSchema == null) {
        logger.debug("------no schema to write list value -> skipping");
        return;
      }
    }
    try {
      listWriter.startList();
      context.getFieldPathTracker().enterArray();
      int size = unpacker.unpackArrayHeader();
      for (int i = 0; i < size; i++) {
        writeElement(unpacker, null, listWriter, null, selection, subSchema);
      }
    } finally {
      addIfNotInitialized(listWriter);
      listWriter.endList();
      context.getFieldPathTracker().leaveArray();
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
  private ColumnMetadata getArrayInMapSubSchema(ColumnMetadata schema) {
    if (!context.hasSchema()) {
      // We don't have a shema to work with.
      return null;
    }
    return schema;
  }

  /**
   * This method is used to determine the type contained in an array when that
   * array is within an array.
   *
   * @param schema
   *                 the schema of the array
   * @return the type of the elements in the array
   */
  private ColumnMetadata getArrayInArraySubSchema(ColumnMetadata schema) {
    if (!context.hasSchema()) {
      // We don't have a shema to work with.
      return null;
    }
    VariantMetadata s = schema.variantSchema();
    Collection<ColumnMetadata> children = s.members();
    ColumnMetadata childSchema = children.iterator().next();
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
