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

package org.apache.drill.exec.udfs;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateUtilFunctions {
  private static final Logger logger = LoggerFactory.getLogger(DateUtilFunctions.class);
  // Date Matcher Regexes
  // yyyy-mm-dd
  private static final Pattern DB_DATE = Pattern.compile("^(\\d{4})-(\\d{2})-(\\d{2})$");

  // Matches various dates which use slashes
  private static final Pattern DATE_SLASH = Pattern.compile("^(\\d{1,2})/(\\d{1,2})/(\\d{4})$");

  // Year first with slashes
  private static final Pattern LEADING_SLASH_DATE = Pattern.compile("^(\\d{4})/(\\d{1,2})/" +
    "(\\d{1,2})$");

  private static final Pattern TIMESTAMP_PATTERN = Pattern.compile("^(\\d{4})-(\\d{2})-(\\d{2})" +
    "(?:T|\\s)(\\d{1,2}):(\\d{1,2}):(\\d{1,2})\\.?(\\d*)");

  /** Parses common date strings and returns a {@link LocalDate} of that string.
   * If the method is unable to parse the string, an error will be logged.
   * Supports the following formats:
   *
   * <ul>
   *   <li>yyyy-MM-dd</li>
   *   <li>MM/dd/yyyy</li>
   *   <li>M/d/yyyy</li>
   *   <li>yyyy/MM/dd</li>
   * </ul>
   *
   * @param inputString An input string containing a date.
   * @return A {@link LocalDate} of the input string.
   */
  public static LocalDate getDateFromString(String inputString) {
    return getDateFromString(inputString, false);
  }

  /** Parses common date strings and returns a {@link LocalDate} of that string.
   * If the method is unable to parse the string, an error will be logged.
   * Supports the following formats:
   *
   * <ul>
   *   <li>yyyy-MM-dd</li>
   *   <li>MM/dd/yyyy</li>
   *   <li>dd/MM/yyyy</li>
   *   <li>M/d/yyyy</li>
   *   <li>yyyy/MM/dd</li>
   * </ul>
   *
   * If the matcher is unable to convert the string, the function returns null.
   * @param inputString An input string containing a date.
   * @param leadingDay True if the format has the day first.
   * @return A {@link LocalDate} of the input string.
   */
  public static LocalDate getDateFromString(String inputString, boolean leadingDay) {
    int year;
    int month;
    int day;
    if (StringUtils.isEmpty(inputString)) {
      return null;
    }

    // Clean up input string:
    inputString = inputString.trim();
    Matcher dateMatcher;
    if (DB_DATE.matcher(inputString).matches()) {
      dateMatcher = DB_DATE.matcher(inputString);
      dateMatcher.find();
      year = Integer.parseInt(dateMatcher.group(1));
      month = Integer.parseInt(dateMatcher.group(2));
      day = Integer.parseInt(dateMatcher.group(3));
    } else if (DATE_SLASH.matcher(inputString).matches()) {
      dateMatcher = DATE_SLASH.matcher(inputString);
      dateMatcher.find();
      year = Integer.parseInt(dateMatcher.group(3));
      if (leadingDay) {
        month = Integer.parseInt(dateMatcher.group(2));
        day = Integer.parseInt(dateMatcher.group(1));
      } else {
        month = Integer.parseInt(dateMatcher.group(1));
        day = Integer.parseInt(dateMatcher.group(2));
      }
    } else if (LEADING_SLASH_DATE.matcher(inputString).matches()) {
      dateMatcher = LEADING_SLASH_DATE.matcher(inputString);
      dateMatcher.find();
      year = Integer.parseInt(dateMatcher.group(1));
      month = Integer.parseInt(dateMatcher.group(2));
      day = Integer.parseInt(dateMatcher.group(3));
    } else {
      logger.warn("Unable to parse date {}.", inputString);
      return null;
    }
    try {
      return LocalDate.of(year,month,day);
    } catch (DateTimeException e) {
      logger.warn("Unable to parse date {}.", inputString);
      return null;
    }
  }

  public static LocalDateTime getTimestampFromString(String inputString) {
    if (StringUtils.isEmpty(inputString)) {
      return null;
    }
    // Clean up input string:
    inputString = inputString.trim();

    if (inputString.length() <= 10) {
      LocalDate localDate = getDateFromString(inputString);
      if (localDate == null) {
        logger.warn("Unable to parse date {}.", inputString);
        return null;
      }
      LocalTime localTime = LocalTime.of(0,0,0);
      return LocalDateTime.of(localDate, localTime);
    }

    Matcher timestampMatcher = TIMESTAMP_PATTERN.matcher(inputString);
    if (timestampMatcher.find()) {
      int year = Integer.parseInt(timestampMatcher.group(1));
      int month = Integer.parseInt(timestampMatcher.group(2));
      int day = Integer.parseInt(timestampMatcher.group(3));
      int hour = Integer.parseInt(timestampMatcher.group(4));
      int minute = Integer.parseInt(timestampMatcher.group(5));
      int second = Integer.parseInt(timestampMatcher.group(6));
      int nanos = 0;
      if (StringUtils.isNotEmpty(timestampMatcher.group(7))) {
        nanos = Integer.parseInt(timestampMatcher.group(7)) * 1000000;
      }
      return LocalDateTime.of(year,month,day,hour,minute,second,nanos);
    } else {
      logger.warn("Unable to parse date {}.", inputString);
      return null;
    }
  }
}
