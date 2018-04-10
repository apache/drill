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
package org.apache.drill.exec.vector.accessor;

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.ScalarWriter.ColumnWriterListener;
import org.apache.drill.exec.vector.accessor.TupleWriter.TupleWriterListener;

/**
 * Represents a column within a tuple. A column can be an array, a scalar or a
 * tuple. Each has an associated column metadata (schema) and a writer. The
 * writer is one of three kinds, depending on the kind of the column. If the
 * column is a map, then the column also has an associated tuple loader to
 * define and write to the tuple.
 * <p>
 * This interface defines a writer to set values for value vectors using a
 * simple, uniform interface modeled after a JSON object. Every column value is
 * an object of one of three types: scalar, array or tuple. Methods exist to
 * "cast" this object to the proper type. This model allows a very simple
 * representation: tuples (rows, maps) consist of objects. Arrays are lists of
 * objects.
 * <p>
 * Every column resides at an index, is defined by a schema, is backed by a
 * value vector, and and is written to by a writer. Each column also tracks the
 * schema version in which it was added to detect schema evolution. Each column
 * has an optional overflow vector that holds overflow record values when a
 * batch becomes full.
 * <p>
 * {@see ObjectReader}
 */

public interface ObjectWriter {

  /**
   * Returns the schema of the column associated with this writer.
   *
   * @return schema for this writer's column
   */

  ColumnMetadata schema();

  /**
   * Bind a listener to the underlying scalar column, or array of scalar
   * columns. Not valid if the underlying writer is a map or array of maps.
   *
   * @param listener
   *          the column listener to bind
   */

  void bindListener(ColumnWriterListener listener);

  /**
   * Bind a listener to the underlying map or map array column. Not valid if the
   * underlying writer is a scalar or scalar array.
   *
   * @param listener
   *          the tuple listener to bind
   */

  void bindListener(TupleWriterListener listener);

  /**
   * Return the object (structure) type of this writer.
   *
   * @return type indicating if this is a scalar, tuple or array
   */

  ObjectType type();

  ScalarWriter scalar();

  TupleWriter tuple();

  ArrayWriter array();

  /**
   * For debugging, set the object to the proper form of Java object as defined
   * by the underlying writer type.
   *
   * @param value
   *          Java object value to write
   * @throws VectorOverflowException
   */

  void set(Object value);
}
