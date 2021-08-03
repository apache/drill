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
package org.apache.drill.exec.vector;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.ZoneId;

import org.joda.time.Period;

/**
 * Utility class for Date, DateTime, TimeStamp, Interval data types.
 * <p>
 * WARNING: This class is included from the JDBC driver. There is another, similar
 * class called <tt>org.apache.drill.exec.expr.fn.impl.DateUtility</tt>. If vectors refer
 * to that class, they will fail when called from JDBC. So, place code here if
 * it is needed by JDBC, in the other class if only needed by the Drill engine.
 * (This is a very poor design, but it is what it is.)
 */

public class DateUtilities {

  public static final int yearsToMonths = 12;
  public static final int hoursToMillis = 60 * 60 * 1000;
  public static final int minutesToMillis = 60 * 1000;
  public static final int secondsToMillis = 1000;
  public static final int monthToStandardDays = 30;
  public static final long monthsToMillis = 2592000000L; // 30 * 24 * 60 * 60 * 1000
  public static final int daysToStandardMillis = 24 * 60 * 60 * 1000;

  public static int monthsFromPeriod(Period period){
    return (period.getYears() * yearsToMonths) + period.getMonths();
  }

  public static int periodToMillis(final Period period){
    return (period.getHours() * hoursToMillis) +
           (period.getMinutes() * minutesToMillis) +
           (period.getSeconds() * secondsToMillis) +
           (period.getMillis());
  }

  public static int toMonths(int years, int months) {
    return years * yearsToMonths + months;
  }

  public static int periodToMonths(Period value) {
    return value.getYears() * yearsToMonths + value.getMonths();
  }

  public static Period fromIntervalYear(int value) {
    final int years  = (value / yearsToMonths);
    final int months = (value % yearsToMonths);
    return new Period()
        .plusYears(years)
        .plusMonths(months);
  }

  public static StringBuilder intervalYearStringBuilder(int months) {
    final int years = months / yearsToMonths;
    months %= yearsToMonths;

    return new StringBuilder()
           .append(years)
           .append(pluralify("year", years))
           .append(" ")
           .append(months)
           .append(pluralify("month", months));
  }

  public static StringBuilder intervalYearStringBuilder(Period value) {
    return intervalYearStringBuilder(
        value.getYears() * 12 + value.getMonths());
  }

  public static String pluralify(String term, int value) {
    term = (Math.abs(value) == 1) ? term : term + "s";
    return " " + term;
  }

  public static Period fromIntervalDay(int days, int millis) {
    return new Period()
        .plusDays(days)
        .plusMillis(millis);
  }

  public static StringBuilder intervalDayStringBuilder(int days, int millis) {

    final int hours  = millis / (hoursToMillis);
    millis %= (hoursToMillis);

    final int minutes = millis / (minutesToMillis);
    millis %= (minutesToMillis);

    final int seconds = millis / (secondsToMillis);
    millis %= (secondsToMillis);

    final StringBuilder buf = new StringBuilder()
            .append(days)
            .append(pluralify("day", days))
            .append(" ")
            .append(hours)
            .append(":")
            .append(asTwoDigits(minutes))
            .append(":")
            .append(asTwoDigits(seconds));
    if (millis != 0) {
      buf.append(".")
         .append(millis);
    }
    return buf;
  }

  public static StringBuilder intervalDayStringBuilder(Period value) {
    return intervalDayStringBuilder(
        value.getDays(),
        periodToMillis(value));
  }

  public static Period fromInterval(int months, int days, int millis) {
    return new Period()
        .plusMonths(months)
        .plusDays(days)
        .plusMillis(millis);
  }

  public static String asTwoDigits(int value) {
    return String.format("%02d", value);
  }

  public static StringBuilder intervalStringBuilder(int months, int days, int millis) {

    final int years = months / yearsToMonths;
    months %= yearsToMonths;

    final int hours  = millis / hoursToMillis;
    millis %= hoursToMillis;

    final int minutes = millis / minutesToMillis;
    millis %= minutesToMillis;

    final int seconds = millis / secondsToMillis;
    millis %= secondsToMillis;

    final StringBuilder buf = new StringBuilder()
           .append(years)
           .append(pluralify("year", years))
           .append(" ")
           .append(months)
           .append(pluralify("month", months))
           .append(" ")
           .append(days)
           .append(pluralify("day", days))
           .append(" ")
           .append(hours)
           .append(":")
           .append(asTwoDigits(minutes))
           .append(":")
           .append(asTwoDigits(seconds));
    if (millis != 0) {
      buf.append(".")
         .append(millis);
    }
    return buf;
  }

  public static StringBuilder intervalStringBuilder(Period value) {
    return intervalStringBuilder(
        value.getYears() * 12 + value.getMonths(),
        value.getDays(),
        periodToMillis(value));
  }

  public static int timeToMillis(int hours, int minutes, int seconds, int millis) {
    return ((hours * 60 +
             minutes) * 60 +
            seconds) * 1000 +
           millis;
  }

  /**
   * Convert from Java LocalTime to the ms-since-midnight format which Drill uses
   * @param localTime Java local time
   * @return Drill form of the time
   */
  public static int toDrillTime(LocalTime localTime) {
    return (int) ((localTime.toNanoOfDay() + 500_000L) / 1_000_000L); // round to milliseconds
  }

  public static LocalTime fromDrillTime(int value) {
    return LocalTime.ofNanoOfDay(value * 1_000_000L);
  }

  public static long toDrillDate(LocalDate localDate) {
    return localDate.toEpochDay() * daysToStandardMillis;
  }

  public static LocalDate fromDrillDate(long value) {
    return LocalDate.ofEpochDay(value / daysToStandardMillis);
  }

  /**
   * Convert to seconds since the _local_ Unix epoch, midnight 1970-01-01
   * in the configured time zone.
   * @param instant an Instant representing a moment in time.
   * @param zoneId Drill's configured time zone.
   * @return the Drill timestamp value corresponding to instant.
   */
  public static long toDrillTimestamp(Instant instant, ZoneId zoneId) {
    LocalDateTime ldt = instant.atZone(zoneId).toLocalDateTime();
    return ldt.atZone(ZoneId.of("UTC")).toEpochSecond()*1000;
  }

  /**
   * Convert from seconds since the _local_ Unix epoch, midnight 1970-01-01
   * in the configured time zone.
   * @param value a Drill timestamp in milliseconds
   * @param zoneId Drill's configured time zone.
   * @return an Instant corresponding to the the Drill timestamp in value.
   */
  public static Instant fromDrillTimestamp(long value, ZoneId zoneId) {
     ZonedDateTime zdt = Instant.ofEpochMilli(value).atZone(ZoneId.of("UTC"));
     return zdt.toLocalDateTime().atZone(zoneId).toInstant();
  }
}
