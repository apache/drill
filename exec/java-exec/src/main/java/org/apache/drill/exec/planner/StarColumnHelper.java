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

package org.apache.drill.exec.planner;

import java.util.List;
import java.util.Set;

import org.eigenbase.reltype.RelDataType;

public class StarColumnHelper {

  public final static String PREFIX_DELIMITER = "\u00a6\u00a6";

  public final static String STAR_COLUMN = "*";

  public final static String PREFIXED_STAR_COLUMN = PREFIX_DELIMITER + STAR_COLUMN;

  public static boolean containsStarColumn(RelDataType type) {
    List<String> fieldNames = type.getFieldNames();

    for (String s : fieldNames) {
      if (s.startsWith(STAR_COLUMN))
        return true;
    }

    return false;
  }

  public static boolean isPrefixedStarColumn(String fieldName) {
    return fieldName.indexOf(PREFIXED_STAR_COLUMN) > 0 ; // the delimiter * starts at none-zero position.
  }

  public static boolean isNonPrefixedStarColumn(String fieldName) {
    return fieldName.startsWith("*");
  }

  public static boolean isStarColumn(String fieldName) {
    return isPrefixedStarColumn(fieldName) || isNonPrefixedStarColumn(fieldName);
  }

  public static String extractStarColumnPrefix(String fieldName) {

    assert (isPrefixedStarColumn(fieldName));

    return fieldName.substring(0, fieldName.indexOf(PREFIXED_STAR_COLUMN));
  }

  public static String extractColumnPrefix(String fieldName) {
    if (fieldName.indexOf(PREFIX_DELIMITER) >=0) {
      return fieldName.substring(0, fieldName.indexOf(PREFIX_DELIMITER));
    } else {
      return "";
    }
  }

  // Given a set of prefixes, check if a regular column is subsumed by any of the prefixed star column in the set.
  public static boolean subsumeRegColumn(Set<String> prefixes, String fieldName) {
    if (isPrefixedStarColumn(fieldName))
      return false;  // only applies to regular column.

    return prefixes.contains(extractColumnPrefix(fieldName));
  }

}
