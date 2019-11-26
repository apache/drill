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

import org.joda.time.Instant;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
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

  /**
   * The extended type of the value, describes the secondary type
   * for DATE, TIME and TIMESTAMP for which the value type is
   * int or long.
   */

  ValueType extendedType();
  void setBoolean(boolean value);
  void setInt(int value);
  void setLong(long value);
  void setDouble(double value);
  void setString(String value);
  void setBytes(byte[] value, int len);
  void appendBytes(byte[] value, int len);
  void setDecimal(BigDecimal value);
  void setPeriod(Period value);
  void setDate(LocalDate value);
  void setTime(LocalTime value);
  void setTimestamp(Instant value);

  /**
   * Write value to a vector as a Java object of the "native" type for
   * the column. This form is available only on scalar writers. The
   * object must be of the form for the primary write method above.
   * <p>
   * Primarily to be used when the code already knows the object type.
   *
   * @param value a value that matches the primary setter above, or null
   * to set the column to null
   *
   * @see ColumnWriter#setObject(Object) for the generic case
   */

  void setValue(Object value);

  /**
   * Set the default value to be used to fill empties for this writer.
   * Only valid for required writers: null writers set this is-set bit
   * to 0 and set the data value to 0.
   *
   * @param value the value to set. Cannot be null. The type of the value
   * must match that legal for {@link #setValue(Object)}
   */

  void setDefaultValue(Object value);
}
