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

import java.math.BigDecimal;

import org.joda.time.Period;

/**
 * Interface to access the values of an array column. In general, each
 * vector implements just one of the get methods. Check the vector type
 * to know which method to use. Though, generally, when writing test
 * code, the type is known to the test writer.
 * <p>
 * Arrays allow random access to the values within the array. The index
 * passed to each method is the index into the array for the current
 * row and column. (This means that arrays are three dimensional:
 * the usual (row, column) dimensions plus an array index dimension:
 * (row, column, array index).
 * <p>
 * Note that the <tt>isNull()</tt> method is provided for completeness,
 * but no Drill array allows null values at present.
 * <p>
 * {@see ScalarWriter}
 */

public interface ScalarElementReader {
  /**
   * Describe the type of the value. This is a compression of the
   * value vector type: it describes which method will return the
   * vector value.
   * @return the value type which indicates which get method
   * is valid for the column
   */

  ValueType valueType();
  int size();

  boolean isNull(int index);
  int getInt(int index);
  long getLong(int index);
  double getDouble(int index);
  String getString(int index);
  byte[] getBytes(int index);
  BigDecimal getDecimal(int index);
  Period getPeriod(int index);

  Object getObject(int index);
  String getAsString(int index);
}
