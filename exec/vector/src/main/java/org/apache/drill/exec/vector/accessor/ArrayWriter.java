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

/**
 * Writer for values into an array. Array writes are write-once, sequential:
 * each call to a <tt>setFoo()</tt> method writes a value and advances the array
 * index.
 * <p>
 * {@see ArrayReader}
 */

public interface ArrayWriter {

  /**
   * Number of elements written thus far to the array.
   * @return the number of elements
   */

  int size();

  /**
   * The object type of the list entry. All entries have the same
   * type.
   * @return the object type of each entry
   */

  ObjectWriter entry();

  /**
   * Return a generic object writer for the array entry.
   *
   * @return generic object reader
   */

  ObjectType entryType();
  ScalarWriter scalar();
  TupleWriter tuple();
  ArrayWriter array();

  /**
   * When the array contains a tuple or an array, call <tt>save()</tt>
   * after each array value. Not necessary when writing scalars; each
   * set operation calls save automatically.
   */

  void save();

  /**
   * Write the values of an array from a list of arguments.
   * @param values values for each array element
   * @throws VectorOverflowException
   */
  void set(Object ...values);

  /**
   * Write the array given an array of values. The type of array must match
   * the type of element in the array. That is, if the value is an <tt>int</tt>,
   * provide an <tt>int[]</tt> array.
   *
   * @param array array of values to write
   * @throws VectorOverflowException
   */

  void setObject(Object array);
//  void setList(List<? extends Object> list);
}
