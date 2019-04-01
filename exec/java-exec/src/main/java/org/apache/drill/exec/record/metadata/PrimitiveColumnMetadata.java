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
package org.apache.drill.exec.record.metadata;

import org.apache.drill.common.types.BooleanType;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.record.MaterializedField;
import org.joda.time.Instant;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.math.BigDecimal;

/**
 * Primitive (non-map) column. Describes non-nullable, nullable and array types
 * (which differ only in mode, but not in metadata structure.)
 * <p>
 * Metadata is of two types:
 * <ul>
 * <li>Storage metadata that describes how the column is materialized in a
 * vector. Storage metadata is immutable because revising an existing vector is
 * a complex operation.</li>
 * <li>Supplemental metadata used when reading or writing the column.
 * Supplemental metadata can be changed after the column is created, though it
 * should generally be set before invoking code that uses the metadata.</li>
 * </ul>
 */

public class PrimitiveColumnMetadata extends AbstractColumnMetadata {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PrimitiveColumnMetadata.class);

  public PrimitiveColumnMetadata(MaterializedField schema) {
    super(schema);
  }

  public PrimitiveColumnMetadata(String name, MajorType type) {
    super(name, type);
  }

  public PrimitiveColumnMetadata(String name, MinorType type, DataMode mode) {
    super(name, type, mode);
  }

  private int estimateWidth(MajorType majorType) {
    if (type() == MinorType.NULL || type() == MinorType.LATE) {
      return 0;
    } else if (isVariableWidth()) {

      // The above getSize() method uses the deprecated getWidth()
      // method to get the expected VarChar size. If zero (which
      // it will be), try the revised precision field.

      int precision = majorType.getPrecision();
      if (precision > 0) {
        return precision;
      } else {
        // TypeHelper includes the offset vector width

        return TypeHelper.getSize(majorType) - 4;
      }
    } else {
      return TypeHelper.getSize(majorType);
    }
  }

  public PrimitiveColumnMetadata(PrimitiveColumnMetadata from) {
    super(from);
  }

  @Override
  public ColumnMetadata copy() {
    return new PrimitiveColumnMetadata(this);
  }

  @Override
  public ColumnMetadata.StructureType structureType() { return ColumnMetadata.StructureType.PRIMITIVE; }

  @Override
  public int expectedWidth() {

    // If the property is not set, estimate width from the type.

    int width = PropertyAccessor.getInt(this, EXPECTED_WIDTH_PROP);
    if (width == 0) {
      width = estimateWidth(majorType());
    }
    return width;
  }

  @Override
  public int precision() { return precision; }

  @Override
  public int scale() { return scale; }

  @Override
  public void setExpectedWidth(int width) {
    // The allocation utilities don't like a width of zero, so set to
    // 1 as the minimum. Adjusted to avoid trivial errors if the caller
    // makes an error.

    if (isVariableWidth()) {
      PropertyAccessor.set(this, EXPECTED_WIDTH_PROP, Math.max(1, width));
    }
  }

  @Override
  public DateTimeFormatter dateTimeFormatter() {
    String formatValue = format();
    try {
      switch (type) {
        case TIME:
          return formatValue == null
            ? ISODateTimeFormat.localTimeParser() : DateTimeFormat.forPattern(formatValue);
        case DATE:
          formatValue = format();
          return formatValue == null
            ? ISODateTimeFormat.localDateParser() : DateTimeFormat.forPattern(formatValue);
        case TIMESTAMP:
          formatValue = format();
          return formatValue == null
            ? ISODateTimeFormat.dateTimeNoMillis() : DateTimeFormat.forPattern(formatValue);
        default:
          throw new IllegalArgumentException("Column is not a date/time type: " + type.toString());
      }
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format("The format \"%s\" is not valid for type %s",
          formatValue, type), e);
    }
  }

  @Override
  public ColumnMetadata cloneEmpty() {
    return new PrimitiveColumnMetadata(this);
  }

  public ColumnMetadata mergeWith(MaterializedField field) {
    PrimitiveColumnMetadata merged = new PrimitiveColumnMetadata(field);
    merged.setExpectedWidth(Math.max(expectedWidth(), field.getPrecision()));
    merged.setProperties(properties());
    return merged;
  }

  @Override
  public MajorType majorType() {
    return MajorType.newBuilder()
        .setMinorType(type)
        .setMode(mode)
        .setPrecision(precision)
        .setScale(scale)
        .build();
  }

  @Override
  public MaterializedField schema() {
    return MaterializedField.create(name, majorType());
  }

  @Override
  public MaterializedField emptySchema() { return schema(); }

  @Override
  public String typeString() {
    StringBuilder builder = new StringBuilder();
    if (isArray()) {
      builder.append("ARRAY<");
    }

    switch (type) {
      case VARDECIMAL:
        builder.append("DECIMAL");
        break;
      case FLOAT4:
        builder.append("FLOAT");
        break;
      case FLOAT8:
        builder.append("DOUBLE");
        break;
      case BIT:
        builder.append("BOOLEAN");
        break;
      case INTERVALYEAR:
        builder.append("INTERVAL YEAR");
        break;
      case INTERVALDAY:
        builder.append("INTERVAL DAY");
        break;
      default:
        // other minor types names correspond to SQL-like equivalents
        builder.append(type.name());
    }

    if (precision() > 0) {
      builder.append("(").append(precision());
      if (scale() > 0) {
        builder.append(", ").append(scale());
      }
      builder.append(")");
    }

    if (isArray()) {
      builder.append(">");
    }
    return builder.toString();
  }

  /**
   * Converts value in string literal form into Object instance based on {@link MinorType} value.
   * Returns null in case of error during parsing or unsupported type.
   *
   * @param value value in string literal form
   * @return Object instance
   */
  @Override
  public Object valueFromString(String value) {
    if (value == null) {
      return null;
    }
    try {
      switch (type) {
        case INT:
          return Integer.parseInt(value);
        case BIGINT:
          return Long.parseLong(value);
        case FLOAT4:
          return (double) Float.parseFloat(value);
        case FLOAT8:
          return Double.parseDouble(value);
        case VARDECIMAL:
          return new BigDecimal(value);
        case BIT:
          return BooleanType.fromString(value);
        case VARCHAR:
        case VARBINARY:
          return value;
        case TIME:
          return LocalTime.parse(value, dateTimeFormatter());
        case DATE:
          return LocalDate.parse(value, dateTimeFormatter());
        case TIMESTAMP:
          return Instant.parse(value, dateTimeFormatter());
        case INTERVAL:
        case INTERVALDAY:
        case INTERVALYEAR:
          return Period.parse(value);
        default:
          throw new IllegalArgumentException("Unsupported conversion: " + type.toString());
      }
    } catch (IllegalArgumentException e) {
      logger.warn("Error while parsing type {} default value {}", type, value, e);
      throw new IllegalArgumentException(String.format("The string \"%s\" is not valid for type %s",
          value, type), e);
    }
  }

  /**
   * Converts given value instance into String literal representation based on column metadata type.
   *
   * @param value value instance
   * @return value in string literal representation
   */
  @Override
  public String valueToString(Object value) {
    if (value == null) {
      return null;
    }
    switch (type) {
      case TIME:
        return dateTimeFormatter().print((LocalTime) value);
      case DATE:
        return dateTimeFormatter().print((LocalDate) value);
      case TIMESTAMP:
        return dateTimeFormatter().print((Instant) value);
      default:
       return value.toString();
    }
  }
}
