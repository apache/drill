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
 * Represents a scalar value: a required column, a nullable column,
 * or one element within an array of scalars.
 * <p>
 * Vector values are mapped to
 * their "natural" representations: the representation closest
 * to the actual vector value. For date and time values, this
 * generally means a numeric value. Applications can then map
 * this value to Java objects as desired. Decimal types all
 * map to BigDecimal as that is the only way in Java to
 * represent large decimal values.
 * <p>
 * In general, a column maps to just one value. However, derived
 * classes may choose to provide type conversions if convenient.
 * An exception is thrown if a call is made to a method that
 * is not supported by the column type.
 * <p>
 * {@see ScalarReader}
 * {@see ScalarElementReader}
 */

public interface ScalarWriter extends ColumnWriter {

  /**
   * Describe the type of the value. This is a compression of the
   * value vector type: it describes which method will return the
   * vector value.
   * @return the value type which indicates which get method
   * is valid for the column
   */

  ValueType valueType();
  void setInt(int value);
  void setLong(long value);
  void setDouble(double value);
  void setString(String value);
  void setBytes(byte[] value, int len);
  void setDecimal(BigDecimal value);
  void setPeriod(Period value);
}
