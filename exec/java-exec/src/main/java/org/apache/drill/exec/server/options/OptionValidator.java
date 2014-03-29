/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.server.options;


import org.apache.drill.common.exceptions.ExpressionParsingException;
import org.eigenbase.sql.SqlLiteral;

/**
 * Validates the values provided to Drill options.
 *
 * @param <E>
 */
public abstract class OptionValidator {

  // Stored here as well as in the option static class to allow insertion of option optionName into
  // the error messages produced by the validator
  private String optionName;

  public OptionValidator(String optionName){
    this.optionName = optionName;
  }

  /**
   * This method determines if a given value is a valid setting for an option. For options that support some
   * ambiguity in their settings, such as case-insensitivity for string options, this method returns a modified
   * version of the passed value that is considered the standard format of the option that should be used for
   * system-internal representation.
   *
   * @param value - the value to validate
   * @return - the value requested, in its standard format to be used for representing the value within Drill
   *            Example: all lower case values for strings, to avoid ambiguities in how values are stored
   *            while allowing some flexibility for users
   * @throws ExpressionParsingException - message to describe error with value, including range or list of expected values
   */
  public abstract OptionValue validate(SqlLiteral value) throws ExpressionParsingException;

  public String getOptionName() {
    return optionName;
  }

  public String getDefaultString(){
    return null;
  }


  public abstract OptionValue getDefault();

  public abstract void validate(OptionValue v) throws ExpressionParsingException;
}
