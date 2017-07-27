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

import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.vector.VectorOverflowException;

/**
 * Interface for writing to rows via a column writer. Column writers can be
 * obtained by name or index. Column indexes are defined by the tuple schema.
 * Also provides a convenience method to set the column value from a Java
 * object. The caller is responsible for providing the correct object type for
 * each column. (The object type must match the column accessor type.)
 * <p>
 * A tuple is composed of columns with a fixed order and unique names: either
 * can be used to reference columns. Columns are scalar (simple values), tuples
 * (i.e. maps), or arrays (of scalars, tuples or arrays.)
 * <p>
 * Convenience methods allow getting a column as a scalar, tuple or array. These
 * methods throw an exception if the column is not of the requested type.
 */

public interface TupleWriter {
  TupleMetadata schema();
  int size();

  // Return the column as a generic object

  ObjectWriter column(int colIndex);
  ObjectWriter column(String colName);

  // Convenience methods

  ScalarWriter scalar(int colIndex);
  ScalarWriter scalar(String colName);
  TupleWriter tuple(int colIndex);
  TupleWriter tuple(String colName);
  ArrayWriter array(int colIndex);
  ArrayWriter array(String colName);
  ObjectType type(int colIndex);
  ObjectType type(String colName);

  /**
   * Set one column given a generic object value. Most helpful for testing,
   * not performant for production code due to object creation and dynamic
   * type checking.
   *
   * @param colIndex the index of the column to set
   * @param value the value to set. The type of the object must be compatible
   * with the type of the target column
   * @throws VectorOverflowException if the vector overflows
   */

  void set(int colIndex, Object value) throws VectorOverflowException;

  /**
   * Write a row or map of values, given by Java objects. Object type must
   * match expected column type.
   * <p>
   * Note that a single-column tuple is ambiguous if that column is an
   * array. To avoid ambiguity, use <tt>set(0, value)</tt> in this case.
   *
   * @param values variable-length argument list of column values
   * @return true if the row was written, false if any column
   * caused vector overflow.
   * @throws VectorOverflowException if the vector overflows
   */

  void setTuple(Object ...values) throws VectorOverflowException;

  /**
   * Set the tuple from an array of objects. Primarily for use in
   * test tools.
   *
   * @param value
   * @throws VectorOverflowException
   */

  void setObject(Object value) throws VectorOverflowException;
}
