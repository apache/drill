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

import org.apache.calcite.sql.SqlLiteral;

/**
 * Manager for Drill options. Implementations must be case-insensitive to the name of the option, specifically the
 * {@link #setOption}, {@link #getOption} and {@link #getDefault} methods must ignore the case of the option name.
 */
public interface OptionManager extends Iterable<OptionValue> {

  /**
   * Gets the option value for the given option name.
   *
   * @param name option name
   * @return the option value, null if the option does not exist
   */
  OptionValue getOption(String name);

  /**
   * Sets an option value.
   *
   * @param value option value
   * @throws SetOptionException message to describe error with value
   */
  void setOption(OptionValue value);

  /**
   * Sets the option value built from the given parameters.
   *
   * @param name    option name
   * @param literal sql literal
   * @param type    option type
   * @throws SetOptionException message to describe error with value
   */
  void setOption(String name, SqlLiteral literal, OptionValue.OptionType type);

  /**
   * Gets the option admin for this manager.
   *
   * @return the option admin
   */
  OptionAdmin getAdmin();

  /**
   * Gets the {@link SystemOptionManager} for this manager.
   *
   * @return the system option manager
   */
  OptionManager getSystemManager();

  /**
   * Gets the list of options managed this manager.
   *
   * @return the list of options
   */
  OptionList getOptionList();

  /**
   * Gets the default option value for the given option name.
   *
   * @param name option name
   * @return the default option value, or null if the option does not exist
   */
  OptionValue getDefault(String name);

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
   * Administrator that is used to validate the options.
   */
  public interface OptionAdmin {

    /**
     * Gets the validator for the given option name. Implementations must be case insensitive to the name.
     *
     * @param name option name
     * @return the option validator, or null if the validator does not exist
     * @throws SetOptionException message to describe error with value
     */
    OptionValidator getValidator(String name);

    /**
     * Validates the option value.
     *
     * @param value option value
     * @throws SetOptionException message to describe error with value
     */
    void validate(OptionValue value);

    /**
     * Validate the option value built from the parameters. For options that support some ambiguity in their settings,
     * such as case-insensitivity for string options, this method returns a modified version of the passed value that
     * is considered the standard format of the option that should be used for system-internal representation.
     *
     * @param name       option name
     * @param value      sql literal
     * @param optionType option type
     * @return the value requested, in its standard format to be used for representing the value within Drill
     * Example: all lower case values for strings, to avoid ambiguities in how values are stored
     * while allowing some flexibility for users
     * @throws SetOptionException message to describe error with value
     */
    OptionValue validate(String name, SqlLiteral value, OptionValue.OptionType optionType);
  }
}
