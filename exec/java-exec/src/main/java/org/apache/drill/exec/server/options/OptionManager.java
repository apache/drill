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

import org.apache.drill.exec.server.options.OptionValue.OptionType;

/**
 * Manager for Drill {@link OptionValue options}. Implementations must be case-insensitive to the name of an option.
 */
public interface OptionManager extends Iterable<OptionValue> {

  /**
   * Sets an option value.
   *
   * @param value option value
   * @throws org.apache.drill.common.exceptions.UserException message to describe error with value
   */
  void setOption(OptionValue value);

  /**
   * Deletes the option. Unfortunately, the type is required given the fallback structure of option managers.
   * See {@link FallbackOptionManager}.
   *
   * If the option name is valid (exists in {@link SystemOptionManager#VALIDATORS}),
   * but the option was not set within this manager, calling this method should be a no-op.
   *
   * @param name option name
   * @param type option type
   * @throws org.apache.drill.common.exceptions.UserException message to describe error with value
   */
  void deleteOption(String name, OptionType type);

  /**
   * Deletes all options. Unfortunately, the type is required given the fallback structure of option managers.
   * See {@link FallbackOptionManager}.
   *
   * If no options are set, calling this method should be no-op.
   *
   * @param type option type
   * @throws org.apache.drill.common.exceptions.UserException message to describe error with value
   */
  void deleteAllOptions(OptionType type);

  /**
   * Gets the option value for the given option name.
   *
   * This interface also provides convenient methods to get typed option values:
   * {@link #getOption(TypeValidators.BooleanValidator validator)},
   * {@link #getOption(TypeValidators.DoubleValidator validator)},
   * {@link #getOption(TypeValidators.LongValidator validator)}, and
   * {@link #getOption(TypeValidators.StringValidator validator)}.
   *
   * @param name option name
   * @return the option value, null if the option does not exist
   */
  OptionValue getOption(String name);

  /**
   * Gets the boolean value (from the option value) for the given boolean validator.
   *
   * @param validator the boolean validator
   * @return the boolean value
   */
  boolean getOption(TypeValidators.BooleanValidator validator);

  /**
   * Gets the double value (from the option value) for the given double validator.
   *
   * @param validator the double validator
   * @return the double value
   */
  double getOption(TypeValidators.DoubleValidator validator);

  /**
   * Gets the long value (from the option value) for the given long validator.
   *
   * @param validator the long validator
   * @return the long value
   */
  long getOption(TypeValidators.LongValidator validator);

  /**
   * Gets the string value (from the option value) for the given string validator.
   *
   * @param validator the string validator
   * @return the string value
   */
  String getOption(TypeValidators.StringValidator validator);

  /**
   * Gets the list of options managed this manager.
   *
   * @return the list of options
   */
  OptionList getOptionList();
}
