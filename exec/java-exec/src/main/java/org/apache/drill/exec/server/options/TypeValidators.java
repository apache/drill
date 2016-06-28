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

import java.util.HashSet;
import java.util.Set;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.server.options.OptionValue.Kind;
import org.apache.drill.exec.server.options.OptionValue.OptionType;

import static com.google.common.base.Preconditions.checkArgument;

public class TypeValidators {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TypeValidators.class);

  public static class PositiveLongValidator extends LongValidator {
    private final long max;

    @Deprecated
    public PositiveLongValidator(String name, long max, long def) {
      this(name, max, def, null);
    }

    public PositiveLongValidator(String name, long max, long def, String description) {
      super(name, def, description);
      this.max = max;
    }

    @Override
    public void validate(OptionValue v) {
      super.validate(v);
      if (v.num_val > max || v.num_val < 1) {
        throw UserException.validationError()
            .message(String.format("Option %s must be between %d and %d.", getOptionName(), 1, max))
            .build(logger);
      }
    }
  }

  public static class PowerOfTwoLongValidator extends PositiveLongValidator {

    @Deprecated
    public PowerOfTwoLongValidator(String name, long max, long def) {
      this(name, max, def, null);
    }

    public PowerOfTwoLongValidator(String name, long max, long def, String description) {
      super(name, max, def, description);
    }

    @Override
    public void validate(OptionValue v) {
      super.validate(v);
      if (!isPowerOfTwo(v.num_val)) {
        throw UserException.validationError()
            .message(String.format("Option %s must be a power of two.", getOptionName()))
            .build(logger);
      }
    }

    private static boolean isPowerOfTwo(long num) {
      return (num & (num - 1)) == 0;
    }
  }

  public static class RangeDoubleValidator extends DoubleValidator {
    private final double min;
    private final double max;

    @Deprecated
    public RangeDoubleValidator(String name, double min, double max, double def) {
      this(name, min, max, def, null);
    }

    public RangeDoubleValidator(String name, double min, double max, double def, String description) {
      super(name, def, description);
      this.min = min;
      this.max = max;
    }

    @Override
    public void validate(OptionValue v) {
      super.validate(v);
      if (v.float_val > max || v.float_val < min) {
        throw UserException.validationError()
            .message(String.format("Option %s must be between %f and %f.", getOptionName(), min, max))
            .build(logger);
      }
    }
  }

  public static class BooleanValidator extends TypeValidator {

    @Deprecated
    public BooleanValidator(String name, boolean def) {
      this(name, def, null);
    }

    public BooleanValidator(String name, boolean def, String description) {
      super(name, Kind.BOOLEAN, OptionValue.createBoolean(OptionType.SYSTEM, name, def), description);
    }
  }

  public static class StringValidator extends TypeValidator {

    @Deprecated
    public StringValidator(String name, String def) {
      this(name, def, null);
    }

    public StringValidator(String name, String def, String description) {
      super(name, Kind.STRING, OptionValue.createString(OptionType.SYSTEM, name, def), description);
    }
  }

  public static class LongValidator extends TypeValidator {

    @Deprecated
    public LongValidator(String name, long def) {
      this(name, def, null);
    }

    public LongValidator(String name, long def, String description) {
      super(name, Kind.LONG, OptionValue.createLong(OptionType.SYSTEM, name, def), description);
    }
  }

  public static class DoubleValidator extends TypeValidator {

    @Deprecated
    public DoubleValidator(String name, double def) {
      this(name, def, null);
    }

    public DoubleValidator(String name, double def, String description) {
      super(name, Kind.DOUBLE, OptionValue.createDouble(OptionType.SYSTEM, name, def), description);
    }
  }

  public static class RangeLongValidator extends LongValidator {
    private final long min;
    private final long max;

    @Deprecated
    public RangeLongValidator(String name, long min, long max, long def) {
      this(name, min, max, def, null);
    }

    public RangeLongValidator(String name, long min, long max, long def, String description) {
      super(name, def, description);
      this.min = min;
      this.max = max;
    }

    @Override
    public void validate(OptionValue v) {
      super.validate(v);
      if (v.num_val > max || v.num_val < min) {
        throw UserException.validationError()
            .message(String.format("Option %s must be between %d and %d.", getOptionName(), min, max))
            .build(logger);
      }
    }
  }

  public static class AdminOptionValidator extends StringValidator {

    @Deprecated
    public AdminOptionValidator(String name, String def) {
      this(name, def, null);
    }

    public AdminOptionValidator(String name, String def, String description) {
      super(name, def, description);
    }

    @Override
    public void validate(OptionValue v) {
      if (v.type != OptionType.SYSTEM) {
        throw UserException.validationError()
            .message("Admin related settings can only be set at SYSTEM level scope. Given scope '%s'.", v.type)
            .build(logger);
      }
      super.validate(v);
    }
  }

  /**
   * Validator that checks if the given value is included in a list of acceptable values. Case insensitive.
   */
  public static class EnumeratedStringValidator extends StringValidator {
    private final Set<String> valuesSet = new HashSet<>();

    public EnumeratedStringValidator(String name, String def, String description, String... values) {
      super(name, def, description);
      for (String value : values) {
        valuesSet.add(value.toLowerCase());
      }
    }

    @Override
    public void validate(final OptionValue v) {
      super.validate(v);
      if (!valuesSet.contains(v.string_val.toLowerCase())) {
        throw UserException.validationError()
            .message(String.format("Option %s must be one of: %s.", getOptionName(), valuesSet))
            .build(logger);
      }
    }
  }

  public static abstract class TypeValidator extends OptionValidator {
    private final Kind kind;
    private final OptionValue defaultValue;
    private final String description;

    public TypeValidator(final String name, final Kind kind, final OptionValue defValue, String description) {
      super(name);
      checkArgument(defValue.type == OptionType.SYSTEM, "Default value must be SYSTEM type.");
      this.kind = kind;
      this.defaultValue = defValue;
      this.description = description;
    }

    @Override
    public String getOptionDescription() {
      return description != null ? description :
          "A description of this option is unavailable.";
    }

    @Override
    public OptionValue getDefault() {
      return defaultValue;
    }

    @Override
    public void validate(final OptionValue v) {
      if (v.kind != kind) {
        throw UserException.validationError()
            .message(String.format("Option %s must be of type %s but you tried to set to %s.", getOptionName(),
              kind.name(), v.kind.name()))
            .build(logger);
      }
    }
  }
}
