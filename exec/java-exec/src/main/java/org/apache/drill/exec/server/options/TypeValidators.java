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
package org.apache.drill.exec.server.options;

import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.server.options.OptionValue.Kind;
import org.apache.drill.exec.server.options.OptionValue.OptionType;
import org.apache.drill.exec.server.options.OptionValue.OptionScope;
import static com.google.common.base.Preconditions.checkArgument;

public class TypeValidators {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TypeValidators.class);
  public static class PositiveLongValidator extends LongValidator {
    private final long max;

    public PositiveLongValidator(String name, long max) {
      super(name);
      this.max = max;
    }

    @Override
    public void validate(final OptionValue v, final OptionSet manager) {
      super.validate(v, manager);
      if (v.num_val > max || v.num_val < 1) {
        throw UserException.validationError()
            .message(String.format("Option %s must be between %d and %d.", getOptionName(), 1, max))
            .build(logger);
      }
    }
  }

  public static class PowerOfTwoLongValidator extends PositiveLongValidator {

    public PowerOfTwoLongValidator(String name, long max) {
      super(name, max);
    }

    @Override
    public void validate(final OptionValue v, final OptionSet manager) {
      super.validate(v, manager);
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

    public RangeDoubleValidator(String name, double min, double max) {
      super(name);
      this.min = min;
      this.max = max;
    }

    @Override
    public void validate(final OptionValue v, final OptionSet manager) {
      super.validate(v, manager);
      if (v.float_val > max || v.float_val < min) {
        throw UserException.validationError()
            .message(String.format("Option %s must be between %f and %f.", getOptionName(), min, max))
            .build(logger);
      }
    }
  }

  public static class MinRangeDoubleValidator extends RangeDoubleValidator {
    private final String maxValidatorName;

    public MinRangeDoubleValidator(String name, double min, double max, double def, String maxValidatorName) {
      super(name, min, max);
      this.maxValidatorName = maxValidatorName;
    }

    @Override
    public void validate(final OptionValue v, final OptionSet manager) {
      super.validate(v, manager);
      OptionValue maxValue = manager.getOption(maxValidatorName);
      if (v.float_val > maxValue.float_val) {
        throw UserException.validationError()
                .message(String.format("Option %s must be less than or equal to Option %s",
                        getOptionName(), maxValidatorName))
                .build(logger);
      }
    }
  }

  public static class MaxRangeDoubleValidator extends RangeDoubleValidator {
    private final String minValidatorName;

    public MaxRangeDoubleValidator(String name, double min, double max, double def, String minValidatorName) {
      super(name, min, max);
      this.minValidatorName = minValidatorName;
    }

    @Override
    public void validate(final OptionValue v, final OptionSet manager) {
      super.validate(v, manager);
      OptionValue minValue = manager.getOption(minValidatorName);
      if (v.float_val < minValue.float_val) {
        throw UserException.validationError()
                .message(String.format("Option %s must be greater than or equal to Option %s",
                        getOptionName(), minValidatorName))
                .build(logger);
      }
    }
  }

  public static class BooleanValidator extends TypeValidator {
    public BooleanValidator(String name) {
      this(name, false);
    }

    public BooleanValidator(String name, boolean isAdminOption) {
      super(name, Kind.BOOLEAN, isAdminOption);
    }

    public void loadDefault(DrillConfig bootConfig){
      OptionValue value = OptionValue.createBoolean(OptionType.SYSTEM, getOptionName(), bootConfig.getBoolean(getConfigProperty()), OptionScope.BOOT);
      setDefaultValue(value);
    }
  }

  public static class StringValidator extends TypeValidator {
    public StringValidator(String name) {
      this(name, false);
    }
    public StringValidator(String name, boolean isAdminOption) {
      super(name, Kind.STRING, isAdminOption);
    }

    public void loadDefault(DrillConfig bootConfig){
      OptionValue value = OptionValue.createString(OptionType.SYSTEM, getOptionName(), bootConfig.getString(getConfigProperty()), OptionScope.BOOT);
      setDefaultValue(value);
    }
  }

  public static class LongValidator extends TypeValidator {
    public LongValidator(String name) {
      this(name, false);
    }

    public LongValidator(String name, boolean isAdminOption) {
      super(name, Kind.LONG, isAdminOption);
    }

    public void loadDefault(DrillConfig bootConfig){
      OptionValue value = OptionValue.createLong(OptionType.SYSTEM, getOptionName(), bootConfig.getLong(getConfigProperty()), OptionScope.BOOT);
      setDefaultValue(value);
    }
  }

  public static class DoubleValidator extends TypeValidator {
    public DoubleValidator(String name) {
      this(name,false);
    }

    public DoubleValidator(String name, boolean isAdminOption) {
      super(name, Kind.DOUBLE, isAdminOption);
    }

    public void loadDefault(DrillConfig bootConfig){
      OptionValue value = OptionValue.createDouble(OptionType.SYSTEM, getOptionName(),bootConfig.getDouble(getConfigProperty()), OptionScope.BOOT);
      setDefaultValue(value);
    }
  }

  public static class RangeLongValidator extends LongValidator {
    private final long min;
    private final long max;

    public RangeLongValidator(String name, long min, long max) {
      super(name);
      this.min = min;
      this.max = max;
    }

    @Override
    public void validate(final OptionValue v, final OptionSet manager) {
      super.validate(v, manager);
      if (v.num_val > max || v.num_val < min) {
        throw UserException.validationError()
            .message(String.format("Option %s must be between %d and %d.", getOptionName(), min, max))
            .build(logger);
      }
    }
  }

  /**
   * Validator that checks if the given value is included in a list of acceptable values. Case insensitive.
   */
  public static class EnumeratedStringValidator extends StringValidator {
    private final Set<String> valuesSet = Sets.newLinkedHashSet();

    public EnumeratedStringValidator(String name, String... values) {
      super(name);
      for (String value : values) {
        valuesSet.add(value.toLowerCase());
      }
    }

    @Override
    public void validate(final OptionValue v, final OptionSet manager) {
      super.validate(v, manager);
      if (!valuesSet.contains(v.string_val.toLowerCase())) {
        throw UserException.validationError()
            .message(String.format("Option %s must be one of: %s.", getOptionName(), valuesSet))
            .build(logger);
      }
    }
  }

  /** Max width is a special validator which computes and validates
   *  the maxwidth. If the maxwidth is already set in system/session
   * the value is returned or else it is computed dynamically based on
   * the available number of processors and cpu load average
   */
  public static class MaxWidthValidator extends LongValidator{

    public MaxWidthValidator(String name) {
      this(name, false);
    }

    public MaxWidthValidator(String name, boolean isAdminOption) {
      super(name, isAdminOption);
    }

    public void loadDefault(DrillConfig bootConfig) {
      OptionValue value = OptionValue.createLong(OptionType.SYSTEM, getOptionName(), bootConfig.getLong(getConfigProperty()), OptionScope.BOOT);
      setDefaultValue(value);
    }

    public int computeMaxWidth(double cpuLoadAverage, long maxWidth) {
      // if maxwidth is already set return it
      if (maxWidth != 0) {
        return (int) maxWidth;
      }
      // else compute the value and return
      else {
        int availProc = Runtime.getRuntime().availableProcessors();
        long maxWidthPerNode = Math.max(1, Math.min(availProc, Math.round(availProc * cpuLoadAverage)));
        return (int) maxWidthPerNode;
      }
    }
  }

  public static abstract class TypeValidator extends OptionValidator {
    private final Kind kind;
    private OptionValue defaultValue = null;

    public TypeValidator(final String name, final Kind kind) {
      this(name, kind, false);
    }

    public TypeValidator(final String name, final Kind kind, final boolean isAdminOption) {
      super(name, isAdminOption);
      this.kind = kind;
    }

    @Override
    public OptionValue getDefault() {
      return defaultValue;
    }

    @Override
    public void validate(final OptionValue v, final OptionSet manager) {
      if (v.kind != kind) {
        throw UserException.validationError()
            .message(String.format("Option %s must be of type %s but you tried to set to %s.", getOptionName(),
              kind.name(), v.kind.name()))
            .build(logger);
      }
      if (isAdminOption() && v.type != OptionType.SYSTEM) {
        throw UserException.validationError()
            .message("Admin related settings can only be set at SYSTEM level scope. Given scope '%s'.", v.type)
            .build(logger);
      }
    }

    @Override
    public Kind getKind() {
      return kind;
    }

    protected void setDefaultValue(OptionValue defaultValue) {
      this.defaultValue = defaultValue;
    }

    public String getConfigProperty() {
      return OPTION_DEFAULTS_ROOT + getOptionName();
    }
  }
}
