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
package org.apache.drill.exec.store.base.filter;

import java.math.BigDecimal;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.store.base.PlanStringBuilder;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Description of a constant argument of an expression.
 */

@JsonPropertyOrder({"type", "value"})
public class ConstantHolder {
  @JsonProperty("type")
  public final MinorType type;
  @JsonProperty("value")
  public final Object value;

  @JsonCreator
  public ConstantHolder(
      @JsonProperty("type") MinorType type,
      @JsonProperty("value") Object value) {
    this.type = type;
    this.value = value;
  }

  /**
   * Convert a constant to the given type. Conversion is defined only for
   * some types (where conversion makes sense) and only for some values
   * (only those that would result in a valid conversion.)
   *
   * @param toType the target type
   * @return a constant of the requested type
   * @throws RuntimeException if the conversion is not legal
   * @see {@link #normalize(MinorType)} for a "save" version of this
   * method
   */
  public ConstantHolder convertTo(MinorType toType) {
    if (type == toType) {
      return this;
    }
    switch (toType) {
    case INT:
      return toInt();
    case BIGINT:
      return toBigInt();
    case TIMESTAMP:
      return toTimestamp(null);
    case VARCHAR:
      return toVarChar();
    case FLOAT8:
      return toDouble();
    case VARDECIMAL:
      return toDecimal();
    default:
      throw conversionError(toType);
    }
  }

  /**
   * Normalize the constant to the given type. Return null if the constant
   * cannot be converted. Use this test to determine if the constant is of
   * a form that can be pushed down when push-down only supports certain
   * types. In such a case, the query will likely fail at execution time
   * when Drill tries to compare the remaining filter with an incompatible
   * column type.
   *
   * @param toType the target type
   * @return a constant of the requested type or null if the conversion
   * is not defined or is not legal for the given value
   */
  public ConstantHolder normalize(MinorType toType) {
    try {
      return convertTo(toType);
    } catch (Throwable e) {
      return null;
    }
  }

  public ConstantHolder toInt() {
    int intValue;
    switch (type) {
      case INT:
        return this;
      case BIGINT: {
        long value = (long) this.value;
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
          throw conversionError(MinorType.INT);
        }
        intValue = (int) value;
        break;
      }
      case VARCHAR:
        try {
          intValue = Integer.parseInt((String) value);
        } catch (NumberFormatException e) {
          throw conversionError(MinorType.INT);
        }
        break;
      default:
        throw conversionError(MinorType.INT);
    }
    return new ConstantHolder(MinorType.INT, intValue);
  }

  public ConstantHolder toBigInt() {
    long longValue;
    switch (type) {
      case INT:
        longValue = (Integer) value;
        break;
      case BIGINT:
        return this;
      case VARCHAR:
        try {
          longValue = Long.parseLong((String) value);
        } catch (NumberFormatException e) {
          throw conversionError(MinorType.BIGINT);
        }
        break;
      default:
        throw conversionError(MinorType.BIGINT);
    }
    return new ConstantHolder(MinorType.BIGINT, longValue);
  }

  public ConstantHolder toTimestamp(String tz) {
    long longValue;
    switch (type) {
      case TIMESTAMP:
        return this;
      case INT:
        longValue = (Integer) value;
        break;
      case BIGINT:
        longValue = (Long) value;
        break;
      case VARCHAR: {
        DateTimeFormatter format = ISODateTimeFormat.dateTimeNoMillis();
        if (tz != null) {
          format = format.withZone(DateTimeZone.forID(tz));
        }
        try {
          longValue = format.parseDateTime((String) value).getMillis();
        } catch (Exception e) {
          throw conversionError(MinorType.TIMESTAMP);
        }
        break;
      }
      default:
        throw conversionError(MinorType.TIMESTAMP);
    }
    return new ConstantHolder(MinorType.TIMESTAMP, longValue);
  }

  /**
   * Convert the value to a String. Consider this as a debug tool as
   * no attempt is made to format values in any particular way. Date, time,
   * interval and bit values will appear as numbers, which is probably
   * not what most target systems expect.
   * @return the value as a string using the {@code toString()} method
   * on the value
   */
  public ConstantHolder toVarChar() {
    if (type == MinorType.VARCHAR) {
      return this;
    } else {
      return new ConstantHolder(MinorType.VARCHAR, value.toString());
    }
  }

  public ConstantHolder toDouble() {
    if (type == MinorType.FLOAT8) {
      return this;
    }
    double doubleValue;
    switch (type) {
    case BIGINT:
      doubleValue = (Long) value;
      break;
    case INT:
      doubleValue = (Integer) value;
      break;
    case VARCHAR:
      try {
        doubleValue = Double.parseDouble((String) value);
      } catch (Exception e) {
        throw conversionError(MinorType.FLOAT8);
      }
      break;
    case VARDECIMAL:
      doubleValue = ((BigDecimal) value).doubleValue();
      break;
    default:
      throw conversionError(MinorType.FLOAT8);
    }
    return new ConstantHolder(MinorType.FLOAT8, doubleValue);
  }

  public ConstantHolder toDecimal() {
    if (type == MinorType.VARDECIMAL) {
      return this;
    }
    BigDecimal decimalValue;
    switch (type) {
    case BIGINT:
      decimalValue = BigDecimal.valueOf((Long) value);
      break;
    case FLOAT8:
      decimalValue = BigDecimal.valueOf((Double) value);
      break;
    case INT:
      decimalValue = BigDecimal.valueOf((Integer) value);
      break;
    case VARCHAR:
      try {
        decimalValue = new BigDecimal((String) value);
      } catch (Exception e) {
        throw conversionError(MinorType.VARDECIMAL);
      }
      break;
    default:
      throw conversionError(MinorType.VARDECIMAL);
    }
    return new ConstantHolder(MinorType.VARDECIMAL, decimalValue);
  }

  public RuntimeException conversionError(MinorType toType) {
    return new IllegalStateException(String.format(
        "Cannot convert a constant %s of type %s to type %s",
        value.toString(), type.name(), toType.name()));
  }

  @Override
  public String toString() {
    return new PlanStringBuilder("Constant")
      .field("type", type.name())
      .field("value", value)
      .toString();
  }
}
