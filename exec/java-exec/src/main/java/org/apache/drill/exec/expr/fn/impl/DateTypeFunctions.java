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

package org.apache.drill.exec.expr.fn.impl;

import io.netty.buffer.ByteBuf;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.DateHolder;
import org.apache.drill.exec.expr.holders.IntervalDayHolder;
import org.apache.drill.exec.expr.holders.IntervalHolder;
import org.apache.drill.exec.expr.holders.IntervalYearHolder;
import org.apache.drill.exec.expr.holders.TimeHolder;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.expr.holders.TimeStampTZHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.record.RecordBatch;

public class DateTypeFunctions {

    @FunctionTemplate(name = "intervaltype", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class IntervalType implements DrillSimpleFunc {

        @Param  BigIntHolder inputYears;
        @Param  BigIntHolder inputMonths;
        @Param  BigIntHolder inputDays;
        @Param  BigIntHolder inputHours;
        @Param  BigIntHolder inputMinutes;
        @Param  BigIntHolder inputSeconds;
        @Param  BigIntHolder inputMilliSeconds;
        @Output IntervalHolder out;

        public void setup(RecordBatch b) {
        }

        public void eval() {

            out.months       =  (int) ((inputYears.value * org.apache.drill.exec.expr.fn.impl.DateUtility.yearsToMonths) +
                                       (inputMonths.value));
            out.days         =  (int) inputDays.value;
            out.milliSeconds =  (int) ((inputHours.value * org.apache.drill.exec.expr.fn.impl.DateUtility.hoursToMillis) +
                                       (inputMinutes.value * org.apache.drill.exec.expr.fn.impl.DateUtility.minutesToMillis) +
                                       (inputSeconds.value * org.apache.drill.exec.expr.fn.impl.DateUtility.secondsToMillis) +
                                       (inputMilliSeconds.value));
        }
    }

    @FunctionTemplate(name = "interval_year", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class IntervalYearType implements DrillSimpleFunc {

        @Param  BigIntHolder inputYears;
        @Param  BigIntHolder inputMonths;
        @Output IntervalYearHolder out;

        public void setup(RecordBatch b) {
        }

        public void eval() {

            out.value       = (int) ((inputYears.value * org.apache.drill.exec.expr.fn.impl.DateUtility.yearsToMonths) +
                                      (inputMonths.value));
        }
    }

    @FunctionTemplate(name = "interval_day", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class IntervalDayType implements DrillSimpleFunc {

        @Param  BigIntHolder inputDays;
        @Param  BigIntHolder inputHours;
        @Param  BigIntHolder inputMinutes;
        @Param  BigIntHolder inputSeconds;
        @Param  BigIntHolder inputMillis;
        @Output IntervalDayHolder out;

        public void setup(RecordBatch b) {
        }

        public void eval() {

            out.days  = (int) inputDays.value;
            out.milliSeconds =  (int) ((inputHours.value * org.apache.drill.exec.expr.fn.impl.DateUtility.hoursToMillis) +
                                       (inputMinutes.value * org.apache.drill.exec.expr.fn.impl.DateUtility.minutesToMillis) +
                                       (inputSeconds.value * org.apache.drill.exec.expr.fn.impl.DateUtility.secondsToMillis) +
                                 (inputMillis.value));
        }
    }

    @FunctionTemplate(name = "datetype", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class DateType implements DrillSimpleFunc {

        @Param  BigIntHolder inputYears;
        @Param  BigIntHolder inputMonths;
        @Param  BigIntHolder inputDays;
        @Output DateHolder   out;

        public void setup(RecordBatch b) {
        }

        public void eval() {
            out.value = ((new org.joda.time.MutableDateTime((int) inputYears.value,
                                                            (int) inputMonths.value,
                                                            (int)inputDays.value,
                                                            0,
                                                            0,
                                                            0,
                                                            0,
                                                            org.joda.time.DateTimeZone.UTC))).getMillis();
        }
    }

    @FunctionTemplate(name = "timestamptype", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class TimeStampType implements DrillSimpleFunc {

        @Param  BigIntHolder inputYears;
        @Param  BigIntHolder inputMonths;
        @Param  BigIntHolder inputDays;
        @Param  BigIntHolder inputHours;
        @Param  BigIntHolder inputMinutes;
        @Param  BigIntHolder inputSeconds;
        @Param  BigIntHolder inputMilliSeconds;
        @Output TimeStampHolder out;

        public void setup(RecordBatch b) {
        }

        public void eval() {
            out.value = ((new org.joda.time.MutableDateTime((int)inputYears.value,
                                                            (int)inputMonths.value,
                                                            (int)inputDays.value,
                                                            (int)inputHours.value,
                                                            (int)inputMinutes.value,
                                                            (int)inputSeconds.value,
                                                            (int)inputMilliSeconds.value,
                                                            org.joda.time.DateTimeZone.UTC))).getMillis();
        }
    }

    @FunctionTemplate(name = "timestamptztype", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class TimeStampTZType implements DrillSimpleFunc {

        @Param  BigIntHolder inputYears;
        @Param  BigIntHolder inputMonths;
        @Param  BigIntHolder inputDays;
        @Param  BigIntHolder inputHours;
        @Param  BigIntHolder inputMinutes;
        @Param  BigIntHolder inputSeconds;
        @Param  BigIntHolder inputMilliSeconds;
        @Param  VarCharHolder inputTimeZone;
        @Output TimeStampTZHolder out;

        public void setup(RecordBatch b) {
        }

        public void eval() {

            String timeZone = (inputTimeZone.toString());
            out.value = ((new org.joda.time.MutableDateTime((int)inputYears.value,
                                                            (int)inputMonths.value,
                                                            (int)inputDays.value,
                                                            (int)inputHours.value,
                                                            (int)inputMinutes.value,
                                                            (int)inputSeconds.value,
                                                            (int)inputMilliSeconds.value,
                                                            org.joda.time.DateTimeZone.forID(timeZone)))).getMillis();

            out.index = org.apache.drill.exec.expr.fn.impl.DateUtility.getIndex(timeZone);
        }
    }

    @FunctionTemplate(name = "timetype", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class TimeType implements DrillSimpleFunc {

        @Param  BigIntHolder inputHours;
        @Param  BigIntHolder inputMinutes;
        @Param  BigIntHolder inputSeconds;
        @Param  BigIntHolder inputMilliSeconds;
        @Output TimeHolder   out;

        public void setup(RecordBatch b) {
        }

        public void eval() {
            out.value = (int) ((inputHours.value * org.apache.drill.exec.expr.fn.impl.DateUtility.hoursToMillis) +
                               (inputMinutes.value * org.apache.drill.exec.expr.fn.impl.DateUtility.minutesToMillis) +
                               (inputSeconds.value * org.apache.drill.exec.expr.fn.impl.DateUtility.secondsToMillis) +
                                inputMilliSeconds.value);
        }
    }

    @FunctionTemplate(name = "current_date", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class CurrentDate implements DrillSimpleFunc {
        @Workspace long queryStartDate;
        @Output DateHolder out;

        public void setup(RecordBatch incoming) {

            int timeZoneIndex = incoming.getContext().getRootFragmentTimeZone();
            org.joda.time.DateTimeZone timeZone = org.joda.time.DateTimeZone.forID(org.apache.drill.exec.expr.fn.impl.DateUtility.getTimeZone(timeZoneIndex));
            org.joda.time.DateTime now = new org.joda.time.DateTime(incoming.getContext().getQueryStartTime(), timeZone);
            queryStartDate = (new org.joda.time.DateMidnight(now.getYear(), now.getMonthOfYear(), now.getDayOfMonth(), timeZone)).getMillis();
        }

        public void eval() {
            out.value = queryStartDate;
        }

    }

    @FunctionTemplate(names = {"current_timestamp", "now", "statement_timestamp", "transaction_timestamp"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class CurrentTimeStamp implements DrillSimpleFunc {
        @Workspace long queryStartDate;
        @Workspace int timezoneIndex;
        @Output TimeStampTZHolder out;

        public void setup(RecordBatch incoming) {

            int timeZoneIndex = incoming.getContext().getRootFragmentTimeZone();
            org.joda.time.DateTimeZone timeZone = org.joda.time.DateTimeZone.forID(org.apache.drill.exec.expr.fn.impl.DateUtility.getTimeZone(timeZoneIndex));
            org.joda.time.DateTime now = new org.joda.time.DateTime(incoming.getContext().getQueryStartTime(), timeZone);
            queryStartDate = now.getMillis();
            timezoneIndex = org.apache.drill.exec.expr.fn.impl.DateUtility.getIndex(now.getZone().toString());
        }

        public void eval() {
            out.value = queryStartDate;
            out.index = timezoneIndex;
        }

    }

    @FunctionTemplate(name = "clock_timestamp", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL, isRandom = true)
    public static class ClockTimeStamp implements DrillSimpleFunc {
        @Workspace int timezoneIndex;
        @Output TimeStampTZHolder out;

        public void setup(RecordBatch incoming) {
            org.joda.time.DateTime now = new org.joda.time.DateTime();
            timezoneIndex = org.apache.drill.exec.expr.fn.impl.DateUtility.getIndex(now.getZone().toString());
        }

        public void eval() {
            org.joda.time.DateTime now = new org.joda.time.DateTime();
            out.value = now.getMillis();
            out.index = timezoneIndex;
        }
    }

    @FunctionTemplate(name = "timeofday", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL, isRandom = true)
    public static class TimeOfDay implements DrillSimpleFunc {
        @Workspace ByteBuf buffer;
        @Output VarCharHolder out;

        public void setup(RecordBatch incoming) {
            buffer = io.netty.buffer.Unpooled.wrappedBuffer(new byte[100]);
        }

        public void eval() {
            org.joda.time.DateTime temp = new org.joda.time.DateTime();
            String str = org.apache.drill.exec.expr.fn.impl.DateUtility.formatTimeStampTZ.print(temp);
            out.buffer = buffer;
            out.start = 0;
            out.end = Math.min(100,  str.length()); // truncate if target type has length smaller than that of input's string
            out.buffer.setBytes(0, str.substring(0,out.end).getBytes());
        }
    }

    @FunctionTemplate(name = "localtimestamp", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class LocalTimeStamp implements DrillSimpleFunc {
        @Workspace long queryStartDate;
        @Output TimeStampHolder out;

        public void setup(RecordBatch incoming) {

            org.joda.time.DateTime now = (new org.joda.time.DateTime(incoming.getContext().getQueryStartTime())).withZoneRetainFields(org.joda.time.DateTimeZone.UTC);
            queryStartDate = now.getMillis();
        }

        public void eval() {
            out.value = queryStartDate;
        }
    }

    @FunctionTemplate(names = {"current_time", "localtime"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class CurrentTime implements DrillSimpleFunc {
        @Workspace int queryStartTime;
        @Output TimeHolder out;

        public void setup(RecordBatch incoming) {

            int timeZoneIndex = incoming.getContext().getRootFragmentTimeZone();
            org.joda.time.DateTimeZone timeZone = org.joda.time.DateTimeZone.forID(org.apache.drill.exec.expr.fn.impl.DateUtility.getTimeZone(timeZoneIndex));
            org.joda.time.DateTime now = new org.joda.time.DateTime(incoming.getContext().getQueryStartTime(), timeZone);
            queryStartTime= (int) ((now.getHourOfDay() * org.apache.drill.exec.expr.fn.impl.DateUtility.hoursToMillis) +
                                   (now.getMinuteOfHour() * org.apache.drill.exec.expr.fn.impl.DateUtility.minutesToMillis) +
                                   (now.getSecondOfMinute() * org.apache.drill.exec.expr.fn.impl.DateUtility.secondsToMillis) +
                                   (now.getMillisOfSecond()));
        }

        public void eval() {
            out.value = queryStartTime;
        }
    }

    @SuppressWarnings("unused")
    @FunctionTemplate(names = {"date_add", "add"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
    public static class DateTimeAddFunction implements DrillSimpleFunc {
    @Param DateHolder left;
    @Param TimeHolder right;
    @Output TimeStampHolder out;

        public void setup(RecordBatch incoming) {
        }

        public void eval() {
            out.value = left.value + right.value;
        }
    }

    @SuppressWarnings("unused")
    @FunctionTemplate(names = {"date_add", "add"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
    public static class TimeDateAddFunction implements DrillSimpleFunc {
        @Param TimeHolder right;
        @Param DateHolder left;
        @Output TimeStampHolder out;

        public void setup(RecordBatch incoming) {
        }

        public void eval() {
            out.value = left.value + right.value;
        }
    }

    /* Dummy function template to allow Optiq to validate this function call.
     * At DrillOptiq time we rewrite all date_part() functions to extract functions,
     * since they are essentially the same
     */
    @SuppressWarnings("unused")
    @FunctionTemplate(names = "date_part", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
    public static class DatePartFunction implements DrillSimpleFunc {
        @Param VarCharHolder left;
        @Param DateHolder right;
        @Output BigIntHolder out;

        public void setup(RecordBatch incoming) {
        }

        public void eval() {
            if (1 == 1) {
                throw new UnsupportedOperationException("date_part function should be rewritten as extract() functions");
            }
        }
    }

    @SuppressWarnings("unused")
    @FunctionTemplate(name = "age", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class AgeTimeStampFunction implements DrillSimpleFunc {
        @Param TimeStampHolder left;
        @Param TimeStampHolder right;
        @Output IntervalHolder out;

        public void setup(RecordBatch incoming) {
        }

        public void eval() {
            long diff = left.value - right.value;
            long days = diff / org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis;
            out.months = (int) (days / org.apache.drill.exec.expr.fn.impl.DateUtility.monthToStandardDays);
            out.days = (int) (days % org.apache.drill.exec.expr.fn.impl.DateUtility.monthToStandardDays);
            out.milliSeconds = (int) (diff % org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis);
        }
    }

    @SuppressWarnings("unused")
    @FunctionTemplate(name = "age", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class AgeTimeStamp2Function implements DrillSimpleFunc {
        @Param TimeStampHolder right;
        @Workspace long queryStartDate;
        @Output IntervalHolder out;

        public void setup(RecordBatch incoming) {
            int timeZoneIndex = incoming.getContext().getRootFragmentTimeZone();
            org.joda.time.DateTimeZone timeZone = org.joda.time.DateTimeZone.forID(org.apache.drill.exec.expr.fn.impl.DateUtility.getTimeZone(timeZoneIndex));
            org.joda.time.DateTime now = new org.joda.time.DateTime(incoming.getContext().getQueryStartTime(), timeZone);
            queryStartDate = (new org.joda.time.DateMidnight(now.getYear(), now.getMonthOfYear(), now.getDayOfMonth(), timeZone)).getMillis();
        }

        public void eval() {
            long diff = queryStartDate - right.value;
            long days = diff / org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis;
            out.months = (int) (days / org.apache.drill.exec.expr.fn.impl.DateUtility.monthToStandardDays);
            out.days = (int) (days % org.apache.drill.exec.expr.fn.impl.DateUtility.monthToStandardDays);
            out.milliSeconds = (int) (diff % org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis);
        }
    }

    @SuppressWarnings("unused")
    @FunctionTemplate(name = "age", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class AgeDateFunction implements DrillSimpleFunc {
        @Param DateHolder left;
        @Param DateHolder right;
        @Output IntervalHolder out;

        public void setup(RecordBatch incoming) {
        }

        public void eval() {
          long diff = left.value - right.value;
          long days = diff / org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis;
          out.months = (int) (days / org.apache.drill.exec.expr.fn.impl.DateUtility.monthToStandardDays);
          out.days = (int) (days % org.apache.drill.exec.expr.fn.impl.DateUtility.monthToStandardDays);
          out.milliSeconds = (int) (diff % org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis);
        }
    }

    @SuppressWarnings("unused")
    @FunctionTemplate(name = "age", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class AgeDate2Function implements DrillSimpleFunc {
        @Param DateHolder right;
        @Workspace long queryStartDate;
        @Output IntervalHolder out;

        public void setup(RecordBatch incoming) {
            int timeZoneIndex = incoming.getContext().getRootFragmentTimeZone();
            org.joda.time.DateTimeZone timeZone = org.joda.time.DateTimeZone.forID(org.apache.drill.exec.expr.fn.impl.DateUtility.getTimeZone(timeZoneIndex));
            org.joda.time.DateTime now = new org.joda.time.DateTime(incoming.getContext().getQueryStartTime(), timeZone);
            queryStartDate = (new org.joda.time.DateMidnight(now.getYear(), now.getMonthOfYear(), now.getDayOfMonth(), timeZone)).getMillis();
        }

        public void eval() {
            long diff = queryStartDate - right.value;
            long days = diff / org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis;
            out.months = (int) (days / org.apache.drill.exec.expr.fn.impl.DateUtility.monthToStandardDays);
            out.days = (int) (days % org.apache.drill.exec.expr.fn.impl.DateUtility.monthToStandardDays);
            out.milliSeconds = (int) (diff % org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis);
        }
    }

    @FunctionTemplate(name = "castTIME", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class CastTimeStampToTime implements DrillSimpleFunc {
        @Param TimeStampHolder in;
        @Output TimeHolder out;

        @Override
        public void setup(RecordBatch incoming) {
        }

        @Override
        public void eval() {
            out.value = (int) (in.value % org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis);
        }
    }
}
