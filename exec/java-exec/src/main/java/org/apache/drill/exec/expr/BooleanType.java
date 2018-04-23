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
package org.apache.drill.exec.expr;

import org.apache.drill.common.map.CaseInsensitiveMap;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Enum that contains two boolean types: TRUE and FALSE.
 * Each has numeric representation and list of allowed literals.
 * List of literals if formed according to
 * {@link <a href="https://www.postgresql.org/docs/9.6/static/datatype-boolean.html">Postgre Documentation</a>}
 */
public enum BooleanType {
  TRUE(1, Arrays.asList("t", "true", "y", "yes", "on", "1")),
  FALSE(0, Arrays.asList("f", "false", "n", "no", "off", "0"));

  private final int numericValue;
  private final List<String> literals;

  BooleanType(int numericValue, List<String> literals) {
    this.numericValue = numericValue;
    this.literals = literals;
  }

  public int getNumericValue() {
    return numericValue;
  }

  public List<String> getLiterals() {
    return literals;
  }

  /** Contains all literals that are allowed to represent boolean type. */
  private static final Map<String, BooleanType> allLiterals = CaseInsensitiveMap.newHashMap();
  static {
    for (BooleanType booleanType : BooleanType.values()) {
      for (String literal : booleanType.getLiterals()) {
        allLiterals.put(literal, booleanType);
      }
    }
  }

  /**
   * Finds boolean by passed literal.
   * Leading or trailing whitespace is ignored, and case does not matter.
   *
   * @param literal boolean string representation
   * @return boolean type
   * @throws IllegalArgumentException if boolean type is not found
   */
  public static BooleanType get(String literal) {
    final String value = literal.trim();
    final BooleanType booleanType = allLiterals.get(value);
    if (booleanType == null) {
      throw new IllegalArgumentException("Invalid value for boolean: " + literal);
    }
    return booleanType;
  }

}