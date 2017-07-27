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

import org.apache.drill.exec.vector.VectorOverflowException;

/**
 * Defines a writer to set values for value vectors using
 * a simple, uniform interface modeled after a JSON object.
 * Every column value is an object of one of three types:
 * scalar, array or tuple. Methods exist to "cast" this object
 * to the proper type. This model allows a very simple representation:
 * tuples (rows, maps) consist of objects. Arrays are lists of
 * objects.
 * <p>
 * {@see ObjectReader}
 */

public interface ObjectWriter {

  /**
   * Return the object (structure) type of this writer.
   * @return type indicating if this is a scalar, tuple or array
   */

  ObjectType type();
  ScalarWriter scalar();
  TupleWriter tuple();
  ArrayWriter array();

  /**
   * For debugging, set the object to the proper form of Java object
   * as defined by the underlying writer type.
   *
   * @param value Java object value to write
   * @throws VectorOverflowException
   */

  void set(Object value) throws VectorOverflowException;
}
