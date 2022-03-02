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

package org.apache.drill.exec.expr.fn.impl;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

//import static java.lang.Float.NaN;

public class MathFunctionsVarcharUtils {

  public String validateInput(String input) {

    String regex = "^[-]*[0-9.]*[0-9]*+$";
    Pattern pattern = java.util.regex.Pattern.compile(regex);
    input = input.trim();

    if (input != null) {
      Matcher matcher = pattern.matcher(input);

      if (!input.equals("") && matcher.matches()) {
        return input;
      }
      else {
        return null;
      }
    }
    else {
      return null;
    }
  }
}
