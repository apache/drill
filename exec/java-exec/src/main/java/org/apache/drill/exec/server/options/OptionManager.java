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
public interface OptionManager extends OptionSet, Iterable<OptionValue> {

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
   * Gets the list of options managed this manager.
   *
   * @return the list of options
   */
  OptionList getOptionList();
}
