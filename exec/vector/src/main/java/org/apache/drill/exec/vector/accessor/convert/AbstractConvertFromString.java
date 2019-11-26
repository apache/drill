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
package org.apache.drill.exec.vector.accessor.convert;

import java.util.Map;
import java.util.function.Function;

import org.apache.drill.common.types.Types;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;

/**
 * Abstract class for string-to-something conversions. Handles the
 * multiple ways that strings can be set.
 */
public abstract class AbstractConvertFromString extends AbstractWriteConverter {

  /**
   * Property to control how the conversion handles blanks. Blanks are
   * zero-length text fields (after trimming whitespace.)
   */
  public static final String BLANK_ACTION_PROP = "blank-as";

  /**
   * Convert blanks to null values (if the column is nullable), or
   * fill with the default value (non-nullable.)
   */
  public static final String BLANK_AS_NULL = "null";

  /**
   * Convert blanks for numeric fields to 0. For non-numeric
   * fields, convert to null (for nullable) or the default value
   * (for non-nullable). Works best if non-numeric fields are declared
   * as nullable.
   */
  public static final String BLANK_AS_ZERO = "0";

  /**
   * Skip blank values. This will result in the column being set to null for
   * nullable columns, and to the default value (else 0 for numeric columns)
   * for non-nullable columns.
   */
  public static final String BLANK_AS_SKIP = "skip";

  protected static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(AbstractConvertFromString.class);

  protected final Function<String,String> prepare;

  public AbstractConvertFromString(ScalarWriter baseWriter) {
    this(baseWriter, null);
  }

  public AbstractConvertFromString(ScalarWriter baseWriter, Map<String, String> properties) {
    super(baseWriter);
    this.prepare = buildPrepare(baseWriter.schema(), properties);
  }

  /**
   * Create a function to prepare the string. This turns out to be surprisingly
   * complex. Behavior is determined by a property which can be:
   * <p>
   * <ul>
   * <li>Property is set in the column schema.</li>
   * <li>Property is set via the framework to a plugin-specific value.</li>
   * <li>Properties are provided but the blank property is not set.</li>
   * <li>No framework properties provided.</li>
   * </ul>
   *
   * In the last two cases, no special handling is done for blank properties.
   * <p>
   * In all cases, nulls:
   * <p>
   * <ul>
   * <li>Cause the column to be set to null, if the column is nullable. (This step
   * is actually optional, but is done explicitly to allow a column to be rewritten.)</li>
   * <li>Cause the column to be skipped, if the column is not nullable.</li>
   * </ul>
   *
   * In all cases, the (non-null) string value is trimmed. Next, there are
   * multiple ways to handle nulls, depending on context:
   *
   * <ul>
   * <li>Leave nulls unchanged (default if no property is set.)</li>
   * <li>{@link #BLANK_AS_DEFAULT}: skip the value for the row, letting the
   * fill-empties logic fill in the default value.</li>
   * <li>{@link #BLANK_AS_ZERO}: for numeric types, replace the null value with
   * "0", which will then be parsed to a numeric zero.</li>
   * <li>{@link #BLANK_AS_NULL}: for nullable modes, set the column to null
   * and ignore the value.</li>
   * </ul>
   * <p>
   * The preparation function handles setting the column to null if needed.
   * The function returns a non-null string if the converter should handle it,
   * or null if the converter should do nothing.
   * <p>
   * The logic is handled as a lambda function for two reasons:
   * <p>
   * <ul>
   * <li>Perform all column-wide conditional checks once on writer creation
   * rather than on every row.</li>
   * <li>Allow the function to call this writers {@link #setNull()} method.</li>
   * </ul>
   *
   * @param schema the output schema, with user properties optionally set
   * @param properties optional framework-specific properties
   * @return a function to call to prepare each string value for conversion
   */

  private Function<String,String> buildPrepare(ColumnMetadata schema,
      Map<String, String> properties) {

    String blankProp = schema.property(ColumnMetadata.BLANK_AS_PROP);
    if (blankProp == null && properties != null) {
      blankProp = properties.get(BLANK_ACTION_PROP);
    }

    if (blankProp != null) {
      switch (blankProp.toLowerCase()) {
      case BLANK_AS_NULL:
        return skipBlankFn(schema);
      case BLANK_AS_ZERO:
        return blankAsZeroFn(schema);
     case BLANK_AS_SKIP:
        if (schema.isNullable()) {
          return blankToNullFn();
        } else {
          return blankAsZeroFn(schema);
        }
      default:
        // Silently ignore invalid values
        logger.warn("Invalid conversion option '{}', skipping", blankProp);
        break;
      }
    }

    // Else, if the mode, then if the string is null, set the
    // column to null, else trim the string.

    if (schema.isNullable()) {
      return nullableStrFn();
    }

    // Otherwise, trim the string, but have the converter skip this row if the
    // string is blank. The column loader will then fill in the default value.

    return skipBlanksFn();
  }

  private Function<String, String> blankAsZeroFn(ColumnMetadata schema) {
    if (! Types.isNumericType(schema.type())) {
      return skipBlankFn(schema);
    } else if (schema.isNullable()) {
      return nullableBlankToZeroFn();
    } else {
      return blankToZeroFn();
    }
  }

  private Function<String, String> skipBlankFn(ColumnMetadata schema) {
    if (schema.isNullable()) {
      return blankToNullFn();
    } else {
      return skipBlankFn();
    }
  }

  private static Function<String, String> skipBlanksFn() {
     return (String s) -> s == null ? s : s.trim();
  }

  private Function<String, String> nullableStrFn() {
    return (String s) -> {
      if (s == null) {
        setNull();
        return null;
      }
      return s.trim();
     };
  }

  private Function<String, String> blankToNullFn() {
    return (String s) -> {
      if (s == null) {
        setNull();
        return null;
      }
      s = s.trim();
      if (s.isEmpty()) {
        setNull();
        return null;
      }
      return s;
    };
  }

  private Function<String, String> skipBlankFn() {
    return (String s) -> {
      if (s == null) {
        return null;
      }
      s = s.trim();
      return s.isEmpty() ? null : s;
    };
  }

  private Function<String, String> nullableBlankToZeroFn() {
    return (String s) -> {
      if (s == null) {
        setNull();
        return null;
      }
      s = s.trim();
      return s.isEmpty() ? "0" : s;
    };
  }

  private Function<String, String> blankToZeroFn() {
    return (String s) -> {
      if (s == null) {
        return null;
      }
      s = s.trim();
      return s.isEmpty() ? "0" : s;
    };
  }

  @Override
  public void setValue(final Object value) {
    if (value == null) {
      setNull();
    } else {
      setString((String) value);
    }
  }

  @Override
  public void setBytes(byte[] bytes, int length) {
    setString(new String(bytes, 0, length, Charsets.UTF_8));
  }
}
