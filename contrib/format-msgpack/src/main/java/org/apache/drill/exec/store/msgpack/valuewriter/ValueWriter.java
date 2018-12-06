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
package org.apache.drill.exec.store.msgpack.valuewriter;

import java.io.IOException;

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.complex.fn.FieldSelection;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ValueType;

/**
 * This interface is implemented for all the types supported by the msgpack java
 * library. That is all of the types in {@link org.msgpack.value.ValueType}.
 * MAP, ARRAY, FLOAT, INTEGER, STRING, BINARY, EXTENSION, BOOLEAN.
 */
public interface ValueWriter {
  /**
   * Return the msgpack value type handled by this writer.
   */
  public ValueType getMsgpackValueType();

  /**
   * This method is used to write a msgpack value (ARRAY, FLOAT, etc) into either
   * a MapWriter or ListWriter.
   *
   * @param unpacker   the msgpack value to write into the drill structures
   * @param mapWriter
   * @param fieldName  field name used when writing to a map writer, it will be
   *                   null when writing to a list writer.
   * @param listWriter
   * @param selection  the selection from the select statement, used to skip over
   *                   the fields that are not of interest.
   * @param schema     the desired type for the value, if it's not an exact match
   *                   we try to coerce the value into that desired type.
   */
  public abstract void write(MessageUnpacker unpacker, MapWriter mapWriter, String fieldName, ListWriter listWriter,
      FieldSelection selection, ColumnMetadata schema) throws IOException;
}
