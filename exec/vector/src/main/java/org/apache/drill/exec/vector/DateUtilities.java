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

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.joda.time.Period;

/**
 * Utility class for Date, DateTime, TimeStamp, Interval data types.
 * <p>
 * WARNING: This class is included in the JDBC driver. There is another, similar
 * class called {@code org.apache.drill.exec.expr.fn.impl.DateUtility}. If vectors refer
 * to that class, they will fail when called from JDBC. So, place code here if
 * it is needed by JDBC, in the other class if only needed by the Drill engine.
 * (This is a very poor design, but it is what it is.)
 */
public class DateUtilities {

  public static final int yearsToMonths = 12;
  public static final int daysToWeeks = 7;
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

  // NOTE: This assumes the instant is already in the local time zone:
  // it does not do the required local-to-UTC conversion.
  public static long toDrillTimestamp(Instant instant) {
    return instant.toEpochMilli();
  }

  // NOTE: This produces an instant in the local time zone:
  // it does not do the required local-to-UTC conversion.
  public static Instant fromDrillTimestamp(long value) {
    return Instant.ofEpochMilli(value);
  }

  public static final LocalDateTime LOCAL_EPOCH = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
  public static final ZoneId LOCAL_ZONE_ID = ZoneId.systemDefault();

  /**
   * Convert from a Java {@code LocalDateTime} to Drill's form for storage
   * in a TIMESTAMP vector.
   */
  public static long localDateTimeToDrillTimetamp(LocalDateTime localDT) {
    return Duration.between(LOCAL_EPOCH, localDT).toMillis();
  }

  // The Drill "local timestamp" is ill-defined in java.time. There is no java.time
  // representation of the number of seconds (or ms.) since an epoch in local time.
  // Further, the conversion is non-uniform: in time zones with daylight savings
  // time, the offset is different depending on the date. java.time will get
  // the offset at a given UTC timestamp, but going from local time is much
  // harder.

  /**
   * Convert an {@code Instant} (number of ms. since the epoch, UTC) to
   * a Drill "local timestamp" the number of ms since the "local epoch".
   *
   * @param instant UTC timestamp
   * @return "Drill timestamp"
   */
  public static long utcInstantToDrillTimetamp(Instant instant) {
    // The instant is in UTC, which is needed to get the TZ offset at
    // that specific instant. (Won't work the other way, alas.)
    return instant.toEpochMilli() +
        LOCAL_ZONE_ID.getRules().getOffset(instant).getTotalSeconds() * 1000;
  }

  /**
   * Convert a UTC timestamp, in ms since the epoch UTC to a Drill
   * timestamp (in local time).
   * @param utcTimestampMs
   * @return
   */
  public static long utcTimestampToDrillTimestamp(long utcTimestampMs) {
    return utcInstantToDrillTimetamp(Instant.ofEpochMilli(utcTimestampMs));
  }

  public static Instant drillTimestampToUTCInstant(long drillTimestamp) {
    long sec = drillTimestamp / 1000;
    int ns = (int) ((drillTimestamp % 1000) * 1_000_000);
    return localDateTimeToUTCInstant(DateUtilities.LOCAL_EPOCH.plusSeconds(sec).plusNanos(ns));
  }

  public static Instant localDateTimeToUTCInstant(LocalDateTime localDt) {
    ZonedDateTime localZdt = localDt.atZone(DateUtilities.LOCAL_ZONE_ID);
    return localZdt.toInstant();
  }

  public static long drillTimestampToUTCMs(long drillTimestamp) {
     return drillTimestampToUTCInstant(drillTimestamp).toEpochMilli();
  }

  public static LocalDateTime timestampLiteralToLocalDateTime(String literal) {
    return LocalDateTime.parse(literal);
  }
}
