/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to you under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.drill.common.expression.fn;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.map.CaseInsensitiveMap;

import java.util.Comparator;
import java.util.Set;

public class JodaDateValidator {

  private static final Set<String> postgresValuesForDeleting = Sets.newTreeSet(new LengthDescComparator());
  private static final CaseInsensitiveMap<String> postgresToJodaMap = CaseInsensitiveMap.newTreeMap(new LengthDescComparator());

  // tokens for deleting
  public static final String SUFFIX_SP = "sp";
  public static final String PREFIX_FM = "fm";
  public static final String PREFIX_FX = "fx";
  public static final String PREFIX_TM = "tm";

  // postgres patterns
  public static final String POSTGRES_FULL_NAME_OF_DAY = "day";
  public static final String POSTGRES_DAY_OF_YEAR = "ddd";
  public static final String POSTGRES_DAY_OF_MONTH = "dd";
  public static final String POSTGRES_DAY_OF_WEEK = "d";
  public static final String POSTGRES_NAME_OF_MONTH = "month";
  public static final String POSTGRES_ABR_NAME_OF_MONTH = "mon";
  public static final String POSTGRES_FULL_ERA_NAME = "ee";
  public static final String POSTGRES_NAME_OF_DAY = "dy";
  public static final String POSTGRES_TIME_ZONE_NAME = "tz";
  public static final String POSTGRES_HOUR_12_NAME = "hh";
  public static final String POSTGRES_HOUR_12_OTHER_NAME = "hh12";
  public static final String POSTGRES_HOUR_24_NAME = "hh24";
  public static final String POSTGRES_MINUTE_OF_HOUR_NAME = "mi";
  public static final String POSTGRES_SECOND_OF_MINUTE_NAME = "ss";
  public static final String POSTGRES_MILLISECOND_OF_MINUTE_NAME = "ms";
  public static final String POSTGRES_WEEK_OF_YEAR = "ww";
  public static final String POSTGRES_MONTH = "mm";
  public static final String POSTGRES_HALFDAY_AM = "am";
  public static final String POSTGRES_HALFDAY_PM = "pm";

  // jodaTime patterns
  public static final String JODA_FULL_NAME_OF_DAY = "EEEE";
  public static final String JODA_DAY_OF_YEAR = "D";
  public static final String JODA_DAY_OF_MONTH = "d";
  public static final String JODA_DAY_OF_WEEK = "e";
  public static final String JODA_NAME_OF_MONTH = "MMMM";
  public static final String JODA_ABR_NAME_OF_MONTH = "MMM";
  public static final String JODA_FULL_ERA_NAME = "G";
  public static final String JODA_NAME_OF_DAY = "E";
  public static final String JODA_TIME_ZONE_NAME = "TZ";
  public static final String JODA_HOUR_12_NAME = "h";
  public static final String JODA_HOUR_12_OTHER_NAME = "h";
  public static final String JODA_HOUR_24_NAME = "H";
  public static final String JODA_MINUTE_OF_HOUR_NAME = "m";
  public static final String JODA_SECOND_OF_MINUTE_NAME = "s";
  public static final String JODA_MILLISECOND_OF_MINUTE_NAME = "SSS";
  public static final String JODA_WEEK_OF_YEAR = "w";
  public static final String JODA_MONTH = "MM";
  public static final String JODA_HALFDAY = "aa";

  static {
    postgresToJodaMap.put(POSTGRES_FULL_NAME_OF_DAY, JODA_FULL_NAME_OF_DAY);
    postgresToJodaMap.put(POSTGRES_DAY_OF_YEAR, JODA_DAY_OF_YEAR);
    postgresToJodaMap.put(POSTGRES_DAY_OF_MONTH, JODA_DAY_OF_MONTH);
    postgresToJodaMap.put(POSTGRES_DAY_OF_WEEK, JODA_DAY_OF_WEEK);
    postgresToJodaMap.put(POSTGRES_NAME_OF_MONTH, JODA_NAME_OF_MONTH);
    postgresToJodaMap.put(POSTGRES_ABR_NAME_OF_MONTH, JODA_ABR_NAME_OF_MONTH);
    postgresToJodaMap.put(POSTGRES_FULL_ERA_NAME, JODA_FULL_ERA_NAME);
    postgresToJodaMap.put(POSTGRES_NAME_OF_DAY, JODA_NAME_OF_DAY);
    postgresToJodaMap.put(POSTGRES_TIME_ZONE_NAME, JODA_TIME_ZONE_NAME);
    postgresToJodaMap.put(POSTGRES_HOUR_12_NAME, JODA_HOUR_12_NAME);
    postgresToJodaMap.put(POSTGRES_HOUR_12_OTHER_NAME, JODA_HOUR_12_OTHER_NAME);
    postgresToJodaMap.put(POSTGRES_HOUR_24_NAME, JODA_HOUR_24_NAME);
    postgresToJodaMap.put(POSTGRES_MINUTE_OF_HOUR_NAME, JODA_MINUTE_OF_HOUR_NAME);
    postgresToJodaMap.put(POSTGRES_SECOND_OF_MINUTE_NAME, JODA_SECOND_OF_MINUTE_NAME);
    postgresToJodaMap.put(POSTGRES_MILLISECOND_OF_MINUTE_NAME, JODA_MILLISECOND_OF_MINUTE_NAME);
    postgresToJodaMap.put(POSTGRES_WEEK_OF_YEAR, JODA_WEEK_OF_YEAR);
    postgresToJodaMap.put(POSTGRES_MONTH, JODA_MONTH);
    postgresToJodaMap.put(POSTGRES_HALFDAY_AM, JODA_HALFDAY);
    postgresToJodaMap.put(POSTGRES_HALFDAY_PM, JODA_HALFDAY);
  }

  static {
    postgresValuesForDeleting.add(SUFFIX_SP);
    postgresValuesForDeleting.add(PREFIX_FM);
    postgresValuesForDeleting.add(PREFIX_FX);
    postgresValuesForDeleting.add(PREFIX_TM);
  }

  /**
   * Replaces all postgres patterns from {@param pattern},
   * available in postgresToJodaMap keys to jodaTime equivalents.
   *
   * @param pattern date pattern in postgres format
   * @return date pattern with replaced patterns in joda format
   */
  public static String toJodaFormat(String pattern) {
    String preparedString = deleteFromPostgres(pattern);  // deletes all ansi prefix and suffix patterns from string
    StringBuilder builder = new StringBuilder(preparedString);

    int start = 0;    // every time search of postgres token in pattern will start from this index.
    int minPos;       // min position of the longest postgres token
    do {
      // finds first value with max length
      minPos = builder.length();
      String firstMatch = null;
      for (String postgresPattern : postgresToJodaMap.keySet()) {
        // keys sorted in length decreasing
        // at first search longer tokens to consider situation where some tokens are the parts of large tokens
        // example: if pattern contains a token "DDD", token "DD" would be skipped, as a part of "DDD".
        int pos;
        // some tokens can't be in upper camel casing, so we ignore them here.
        // example: DD, DDD, MM, etc.
        if (StringUtils.countMatches(postgresPattern, postgresPattern.substring(0, 0)) < 2) {
          // finds postgres tokens in upper camel casing
          // example: Month, Mon, Day, Dy, etc.
          pos = builder.indexOf(StringUtils.capitalize(postgresPattern), start);
          if (pos >= 0 && pos < minPos) {
            firstMatch = postgresPattern;
            minPos = pos;
            if (minPos == start) {
              break;
            }
          }
        }
        // finds postgres tokens in lower casing
        pos = builder.indexOf(postgresPattern.toLowerCase(), start);
        if (pos >= 0 && pos < minPos) {
          firstMatch = postgresPattern;
          minPos = pos;
          if (minPos == start) {
            break;
          }
        }
        // finds postgres tokens in upper casing
        pos = builder.indexOf(postgresPattern.toUpperCase(), start);
        if (pos >= 0 && pos < minPos) {
          firstMatch = postgresPattern;
          minPos = pos;
          if (minPos == start) {
            break;
          }
        }
      }
      // replaces postgres token, if found
      if (minPos < builder.length()) {
        int offset = minPos + firstMatch.length();
        String jodaToken = postgresToJodaMap.get(firstMatch.toLowerCase());
        builder.replace(minPos, offset, jodaToken);
        start = minPos + jodaToken.length();
      }
    } while (minPos < builder.length());
    return builder.toString();
  }

  /**
   * Deletes all postgres patterns from {@param pattern}, available in postgresValuesForDeleting set
   *
   * @param pattern date pattern in postgres format
   * @return date pattern in postgres format without patterns in postgresValuesForDeleting
   */
  private static String deleteFromPostgres(String pattern) {
    StringBuilder builder = new StringBuilder(pattern);
    boolean mark;
    for (String postgresPattern : postgresValuesForDeleting) {
      do {
        mark = false;
        int pos = builder.indexOf(postgresPattern.toLowerCase());
        if (pos >= 0) {
          builder.delete(pos, postgresPattern.length() + pos);
          mark = true;
        }

        pos = builder.indexOf(postgresPattern.toUpperCase());
        if (pos >= 0) {
          builder.delete(pos, postgresPattern.length() + pos);
          mark = true;
        }
      } while (mark);
    }
    return builder.toString();
  }

  /**
   * Length decreasing comparator.
   * Compares strings by length, if they have the same length, compares them lexicographically.
   */
  private static class LengthDescComparator implements Comparator<String> {

    public int compare(String o1, String o2) {
      int result = o2.length() - o1.length();
      if (result == 0) {
        return o1.compareTo(o2);
      }
      return result;
    }
  }

}
