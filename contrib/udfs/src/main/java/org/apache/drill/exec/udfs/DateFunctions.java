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

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableTimeStampHolder;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;


public class DateFunctions {

  /**
   * This function takes two arguments, an input date object, and an interval and returns
   * the previous date that is the first date in that period.  This function is intended to be used in time series analysis to
   * aggregate by various units of time.
   * Usage is:
   * <p>
   * SELECT <date_field>, COUNT(*) AS event_count
   * FROM ...
   * GROUP BY nearestDate(`date_field`, 'QUARTER')
   * <p>
   * Currently supports the following time units:
   * <p>
   * YEAR
   * QUARTER
   * MONTH
   * WEEK_SUNDAY
   * WEEK_MONDAY
   * DAY
   * HOUR
   * HALF_HOUR
   * QUARTER_HOUR
   * MINUTE
   * 30SECOND
   * 15SECOND
   * SECOND
   */
  @FunctionTemplate(names = {"nearestDate","nearest_date"},
          scope = FunctionTemplate.FunctionScope.SIMPLE,
          nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class NearestDateFunction implements DrillSimpleFunc {

    @Param
    TimeStampHolder inputDate;

    @Param
    VarCharHolder interval;

    @Output
    TimeStampHolder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {

      String input = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(interval.start, interval.end, interval.buffer);
      java.time.LocalDateTime ld = java.time.LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(inputDate.value), java.time.ZoneId.of("UTC"));

      java.time.LocalDateTime td = org.apache.drill.exec.udfs.DateConversionUtils.getDate(ld, input);
      out.value = td.atZone(java.time.ZoneId.of("UTC")).toInstant().toEpochMilli();
    }
  }

  /**
   * This function takes three arguments, an input date string, an input date format string, and an interval and returns
   * the previous date that is the first date in that period.  This function is intended to be used in time series analysis to
   * aggregate by various units of time.
   * Usage is:
   * <p>
   * SELECT <date_field>, COUNT(*) AS event_count
   * FROM ...
   * GROUP BY nearestDate(`date_field`, 'yyyy-mm-dd', 'QUARTER')
   * <p>
   * Currently supports the following time units:
   * <p>
   * YEAR
   * QUARTER
   * MONTH
   * WEEK_SUNDAY
   * WEEK_MONDAY
   * DAY
   * HOUR
   * HALF_HOUR
   * QUARTER_HOUR
   * MINUTE
   * 30SECOND
   * 15SECOND
   * SECOND
   */
  @FunctionTemplate(names = {"nearestDate","nearest_date"},
          scope = FunctionTemplate.FunctionScope.SIMPLE,
          nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class NearestDateFunctionWithString implements DrillSimpleFunc {

    @Param
    VarCharHolder input;

    @Param
    VarCharHolder formatString;

    @Param
    VarCharHolder interval;

    @Output
    TimeStampHolder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      String inputDate = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(input.start, input.end, input.buffer);

      String format = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(formatString.start, formatString.end, formatString.buffer);

      String intervalString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(interval.start, interval.end, interval.buffer);

      java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern(format);
      java.time.LocalDateTime dateTime = java.time.LocalDateTime.parse(inputDate, formatter);

      java.time.LocalDateTime td = org.apache.drill.exec.udfs.DateConversionUtils.getDate(dateTime, intervalString);
      out.value = td.atZone(java.time.ZoneId.of("UTC")).toInstant().toEpochMilli();
    }
  }

  @FunctionTemplate(names = {"yearweek","year_week"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class YearWeekFunction implements DrillSimpleFunc {
    @Param
    VarCharHolder inputHolder;

    @Output
    IntHolder out;

    @Override
    public void setup() {
      // noop
    }

    @Override
    public void eval() {
      String input = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputHolder.start, inputHolder.end, inputHolder.buffer);
      java.time.LocalDateTime dt = org.apache.drill.exec.udfs.DateUtilFunctions.getTimestampFromString(input);
      int week = dt.get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR);
      int year = dt.getYear();
      out.value = (year * 100) + week;
    }
  }

  @FunctionTemplate(names = {"to_timestamp"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class TimestampFunction implements DrillSimpleFunc {
    /**
     * This version of the TO_TIMESTAMP function converts strings into timestamps
     * without the need for a format string.  The function will attempt to determine
     * the date format automatically.  If it cannot, the function will return null.
     */
    @Param
    VarCharHolder inputHolder;

    @Output
    NullableTimeStampHolder out;

    @Override
    public void setup() {
      // noop
    }

    @Override
    public void eval() {
      String input = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputHolder.start, inputHolder.end, inputHolder.buffer);
      java.time.LocalDateTime dt = org.apache.drill.exec.udfs.DateUtilFunctions.getTimestampFromString(input);
      if (dt != null) {
        out.value = dt.toEpochSecond(java.time.ZoneOffset.UTC) * 1000;
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }
  }
}
