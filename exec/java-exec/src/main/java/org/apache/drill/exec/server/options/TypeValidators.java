/**
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
package org.apache.drill.exec.server.options;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

import org.apache.drill.common.exceptions.ExpressionParsingException;
import org.apache.drill.exec.server.options.OptionValue.Kind;
import org.apache.drill.exec.server.options.OptionValue.OptionType;
import org.eigenbase.sql.SqlLiteral;
import org.eigenbase.util.NlsString;

public class TypeValidators {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TypeValidators.class);

  public static class PositiveLongValidator extends LongValidator {

    private final long max;

    public PositiveLongValidator(String name, long max, long def) {
      super(name, def);
      this.max = max;
    }

    @Override
    public void validate(OptionValue v) throws ExpressionParsingException {
      super.validate(v);
      if (v.num_val > max || v.num_val < 0) {
        throw new ExpressionParsingException(String.format("Option %s must be between %d and %d.", getOptionName(), 0,
            max));
      }
    }
  }

  public static class PowerOfTwoLongValidator extends PositiveLongValidator {

    public PowerOfTwoLongValidator(String name, long max, long def) {
      super(name, max, def);
    }

    @Override
    public void validate(OptionValue v) throws ExpressionParsingException {
      super.validate(v);
      if (!isPowerOfTwo(v.num_val)) {
        throw new ExpressionParsingException(String.format("Option %s must be a power of two.", getOptionName()));
      }
    }

    private boolean isPowerOfTwo(long num) {
      return (num & (num - 1)) == 0;
    }
  }

  public static class RangeDoubleValidator extends DoubleValidator {
    private final double min;
    private final double max;

    public RangeDoubleValidator(String name, double min, double max, double def) {
      super(name, def);
      this.min = min;
      this.max = max;
    }

    @Override
    public void validate(OptionValue v) throws ExpressionParsingException {
      super.validate(v);
      if (v.float_val > max || v.float_val < min) {
        throw new ExpressionParsingException(String.format("Option %s must be between %d and %d.", getOptionName(), min,
            max));
      }
    }

  }

  public static class BooleanValidator extends TypeValidator{
    public BooleanValidator(String name, boolean def) {
      super(name, Kind.BOOLEAN, OptionValue.createBoolean(OptionType.SYSTEM, name, def));
    }
  }

  public static class StringValidator extends TypeValidator{
    public StringValidator(String name, String def) {
      super(name, Kind.STRING, OptionValue.createString(OptionType.SYSTEM, name, def));
    }

  }

  public static class LongValidator extends TypeValidator{
    public LongValidator(String name, long def) {
      super(name, Kind.LONG, OptionValue.createLong(OptionType.SYSTEM, name, def));
    }
  }

  public static class DoubleValidator extends TypeValidator{
    public DoubleValidator(String name, double def) {
      super(name, Kind.DOUBLE, OptionValue.createDouble(OptionType.SYSTEM, name, def));
    }
  }

  public static class RangeLongValidator extends LongValidator {
    private final long min;
    private final long max;

    public RangeLongValidator(String name, long min, long max, long def) {
      super(name, def);
      this.min = min;
      this.max = max;
    }

    @Override
    public void validate(OptionValue v) throws ExpressionParsingException {
      super.validate(v);
      if (v.num_val > max || v.num_val < min) {
        throw new ExpressionParsingException(String.format("Option %s must be between %d and %d.", getOptionName(), min,
            max));
      }
    }

  }

  /**
   * Validator that checks if the given value is included in a list of acceptable values. Case insensitive.
   */
  public static class EnumeratedStringValidator extends StringValidator {
    Set<String> valuesSet = new HashSet<>();

    public EnumeratedStringValidator(String name, String def, String... values) {
      super(name, def);
      for (String value : values) {
        valuesSet.add(value.toLowerCase());
      }
    }

    @Override
    public void validate(OptionValue v) throws ExpressionParsingException {
      super.validate(v);
      if (!valuesSet.contains(v.string_val.toLowerCase())) {
        throw new ExpressionParsingException(String.format("Option %s must be one of: %s", getOptionName(), valuesSet));
      }
    }

  }

  public static abstract class TypeValidator extends OptionValidator {
    final Kind kind;
    private OptionValue defaultValue;

    public TypeValidator(String name, Kind kind, OptionValue defValue) {
      super(name);
      this.kind = kind;
      this.defaultValue = defValue;
    }

    @Override
    public OptionValue getDefault() {
      return defaultValue;
    }

    @Override
    public OptionValue validate(SqlLiteral value) throws ExpressionParsingException {
      OptionValue op = getPartialValue(getOptionName(), (OptionType) null, value);
      validate(op);
      return op;
    }

    @Override
    public void validate(OptionValue v) throws ExpressionParsingException {
      if (v.kind != kind) {
        throw new ExpressionParsingException(String.format("Option %s must be of type %s but you tried to set to %s.",
            getOptionName(), kind.name(), v.kind.name()));
      }
    }

    public void extraValidate(OptionValue v) throws ExpressionParsingException {
    }

  }

  public static OptionValue getPartialValue(String name, OptionType type, SqlLiteral literal) {
    switch (literal.getTypeName()) {
    case DECIMAL:
      if (((BigDecimal) literal.getValue()).scale() == 0) {
        return OptionValue.createLong(type, name, ((BigDecimal) literal.getValue()).longValue());
      } else {
        return OptionValue.createDouble(type, name, ((BigDecimal) literal.getValue()).doubleValue());
      }
    case DOUBLE:
    case FLOAT:
      return OptionValue.createDouble(type, name, ((BigDecimal) literal.getValue()).doubleValue());

    case SMALLINT:
    case TINYINT:
    case BIGINT:
    case INTEGER:
      return OptionValue.createLong(type, name, ((BigDecimal) literal.getValue()).longValue());

    case VARBINARY:
    case VARCHAR:
    case CHAR:
      return OptionValue.createString(type, name, ((NlsString) literal.getValue()).getValue());

    case BOOLEAN:
      return OptionValue.createBoolean(type, name, (Boolean) literal.getValue());

    }

    throw new ExpressionParsingException(String.format(
        "Drill doesn't support set option expressions with literals of type %s.", literal.getTypeName()));
  }

}
