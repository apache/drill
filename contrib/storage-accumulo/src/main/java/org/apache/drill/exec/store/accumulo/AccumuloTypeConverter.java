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
package org.apache.drill.exec.store.accumulo;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;

import org.apache.drill.exec.store.accumulo.schema.AccumuloColumnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for converting Accumulo byte arrays to Java types.
 *
 * <p>Accumulo stores all data as byte arrays. This class provides methods to
 * convert those byte arrays to appropriate Java types based on the configured
 * column type in the schema.</p>
 *
 * <p>Conversion strategies:</p>
 * <ul>
 *   <li>String types: UTF-8 decode the bytes</li>
 *   <li>Numeric types: Try parsing string representation first, fall back to binary</li>
 *   <li>Boolean: Check for "true"/"false" strings or binary 0/1</li>
 *   <li>Temporal types: Parse ISO-8601 string or epoch milliseconds</li>
 * </ul>
 */
public final class AccumuloTypeConverter {
  private static final Logger logger = LoggerFactory.getLogger(AccumuloTypeConverter.class);

  private AccumuloTypeConverter() {
    // Utility class
  }

  /**
   * Converts Accumulo byte array to the appropriate Java type based on column type.
   *
   * @param value the byte array from Accumulo
   * @param columnType the target type for conversion
   * @return the converted Java object, or null if conversion fails
   */
  public static Object convert(byte[] value, AccumuloColumnType columnType) {
    if (value == null || value.length == 0) {
      return null;
    }

    switch (columnType) {
      case VARCHAR:
        return toVarchar(value);
      case INT:
      case INTEGER:
        return toInteger(value);
      case BIGINT:
      case LONG:
        return toLong(value);
      case SMALLINT:
        return toShort(value);
      case TINYINT:
        return toByte(value);
      case FLOAT:
        return toFloat(value);
      case DOUBLE:
        return toDouble(value);
      case DECIMAL:
        return toDecimal(value);
      case BOOLEAN:
        return toBoolean(value);
      case DATE:
        return toDate(value);
      case TIME:
        return toTime(value);
      case TIMESTAMP:
        return toTimestamp(value);
      case VARBINARY:
        return value;
      case ANY:
      default:
        // For ANY type, return as string
        return toVarchar(value);
    }
  }

  /**
   * Converts byte array to String using UTF-8 encoding.
   */
  public static String toVarchar(byte[] value) {
    return new String(value, StandardCharsets.UTF_8);
  }

  /**
   * Converts byte array to Integer.
   * Tries string parsing first, then binary interpretation.
   */
  public static Integer toInteger(byte[] value) {
    // Try parsing as string first (more common)
    try {
      String strValue = new String(value, StandardCharsets.UTF_8).trim();
      return Integer.parseInt(strValue);
    } catch (NumberFormatException e) {
      // Fall back to binary interpretation
      if (value.length == 4) {
        return ByteBuffer.wrap(value).getInt();
      }
      logger.debug("Failed to convert byte array to Integer");
      return null;
    }
  }

  /**
   * Converts byte array to Long.
   * Tries string parsing first, then binary interpretation.
   */
  public static Long toLong(byte[] value) {
    try {
      String strValue = new String(value, StandardCharsets.UTF_8).trim();
      return Long.parseLong(strValue);
    } catch (NumberFormatException e) {
      if (value.length == 8) {
        return ByteBuffer.wrap(value).getLong();
      }
      logger.debug("Failed to convert byte array to Long");
      return null;
    }
  }

  /**
   * Converts byte array to Short.
   */
  public static Short toShort(byte[] value) {
    try {
      String strValue = new String(value, StandardCharsets.UTF_8).trim();
      return Short.parseShort(strValue);
    } catch (NumberFormatException e) {
      if (value.length == 2) {
        return ByteBuffer.wrap(value).getShort();
      }
      logger.debug("Failed to convert byte array to Short");
      return null;
    }
  }

  /**
   * Converts byte array to Byte.
   */
  public static Byte toByte(byte[] value) {
    try {
      String strValue = new String(value, StandardCharsets.UTF_8).trim();
      return Byte.parseByte(strValue);
    } catch (NumberFormatException e) {
      if (value.length == 1) {
        return value[0];
      }
      logger.debug("Failed to convert byte array to Byte");
      return null;
    }
  }

  /**
   * Converts byte array to Float.
   */
  public static Float toFloat(byte[] value) {
    try {
      String strValue = new String(value, StandardCharsets.UTF_8).trim();
      return Float.parseFloat(strValue);
    } catch (NumberFormatException e) {
      if (value.length == 4) {
        return ByteBuffer.wrap(value).getFloat();
      }
      logger.debug("Failed to convert byte array to Float");
      return null;
    }
  }

  /**
   * Converts byte array to Double.
   */
  public static Double toDouble(byte[] value) {
    try {
      String strValue = new String(value, StandardCharsets.UTF_8).trim();
      return Double.parseDouble(strValue);
    } catch (NumberFormatException e) {
      if (value.length == 8) {
        return ByteBuffer.wrap(value).getDouble();
      }
      logger.debug("Failed to convert byte array to Double");
      return null;
    }
  }

  /**
   * Converts byte array to BigDecimal.
   */
  public static BigDecimal toDecimal(byte[] value) {
    try {
      String strValue = new String(value, StandardCharsets.UTF_8).trim();
      return new BigDecimal(strValue);
    } catch (NumberFormatException e) {
      logger.debug("Failed to convert byte array to BigDecimal");
      return null;
    }
  }

  /**
   * Converts byte array to Boolean.
   * Accepts "true"/"false" strings (case insensitive), "1"/"0", or binary 0/1.
   */
  public static Boolean toBoolean(byte[] value) {
    // First try string parsing (handles "true", "false", "1", "0", etc.)
    String strValue = new String(value, StandardCharsets.UTF_8).trim().toLowerCase();
    if ("true".equals(strValue) || "1".equals(strValue) || "yes".equals(strValue)) {
      return true;
    } else if ("false".equals(strValue) || "0".equals(strValue) || "no".equals(strValue)) {
      return false;
    }

    // Fall back to binary interpretation for single byte
    if (value.length == 1) {
      return value[0] != 0;
    }

    logger.debug("Failed to convert byte array to Boolean: {}", strValue);
    return null;
  }

  /**
   * Converts byte array to LocalDate.
   * Tries ISO-8601 date format first, then epoch days as long.
   *
   * @return epoch milliseconds at start of day UTC, or null if conversion fails
   */
  public static Long toDate(byte[] value) {
    String strValue = new String(value, StandardCharsets.UTF_8).trim();
    try {
      LocalDate date = LocalDate.parse(strValue);
      return date.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    } catch (DateTimeParseException e) {
      // Try as epoch days
      try {
        long epochDays = Long.parseLong(strValue);
        return LocalDate.ofEpochDay(epochDays).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
      } catch (NumberFormatException e2) {
        logger.debug("Failed to convert byte array to Date: {}", strValue);
        return null;
      }
    }
  }

  /**
   * Converts byte array to LocalTime.
   * Tries ISO-8601 time format first.
   *
   * @return milliseconds since midnight, or null if conversion fails
   */
  public static Integer toTime(byte[] value) {
    String strValue = new String(value, StandardCharsets.UTF_8).trim();
    try {
      LocalTime time = LocalTime.parse(strValue);
      return (int) (time.toNanoOfDay() / 1_000_000);
    } catch (DateTimeParseException e) {
      // Try as milliseconds since midnight
      try {
        return Integer.parseInt(strValue);
      } catch (NumberFormatException e2) {
        logger.debug("Failed to convert byte array to Time: {}", strValue);
        return null;
      }
    }
  }

  /**
   * Converts byte array to Instant.
   * Tries ISO-8601 timestamp format first, then epoch milliseconds.
   *
   * @return epoch milliseconds, or null if conversion fails
   */
  public static Long toTimestamp(byte[] value) {
    String strValue = new String(value, StandardCharsets.UTF_8).trim();
    try {
      Instant instant = Instant.parse(strValue);
      return instant.toEpochMilli();
    } catch (DateTimeParseException e) {
      // Try as epoch milliseconds
      try {
        return Long.parseLong(strValue);
      } catch (NumberFormatException e2) {
        logger.debug("Failed to convert byte array to Timestamp: {}", strValue);
        return null;
      }
    }
  }

  /**
   * Returns the string representation of the byte array for display/debugging.
   */
  public static String toDisplayString(byte[] value) {
    if (value == null) {
      return "null";
    }
    if (value.length == 0) {
      return "";
    }

    // Try to interpret as UTF-8 string
    String strValue = new String(value, StandardCharsets.UTF_8);

    // Check if it looks like valid text (printable characters)
    boolean isPrintable = strValue.chars().allMatch(c ->
        !Character.isISOControl(c) || Character.isWhitespace(c));

    if (isPrintable) {
      return strValue;
    }

    // Return hex representation for binary data
    StringBuilder hex = new StringBuilder("0x");
    for (byte b : value) {
      hex.append(String.format("%02X", b));
    }
    return hex.toString();
  }
}
