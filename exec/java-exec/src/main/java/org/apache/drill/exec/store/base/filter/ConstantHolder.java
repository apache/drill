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
    default:
      throw conversionError(toType);
    }
  }

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

  public ConstantHolder toVarChar() {
    if (type == MinorType.VARCHAR) {
      return this;
    } else {
      return new ConstantHolder(MinorType.VARCHAR, value.toString());
    }
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
