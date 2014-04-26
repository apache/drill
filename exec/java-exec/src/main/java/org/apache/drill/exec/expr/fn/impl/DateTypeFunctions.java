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

import org.apache.drill.common.expression.*;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.*;
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

    @FunctionTemplate(names = {"date_add", "add"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
    public static class DateIntervalAdd implements DrillSimpleFunc{

        @Param DateHolder dateType;
        @Param IntervalHolder intervalType;
        @Output DateHolder out;

        public void setup(RecordBatch b){}

        public void eval(){

            org.joda.time.MutableDateTime temp = new org.joda.time.MutableDateTime(dateType.value, org.joda.time.DateTimeZone.UTC);
            temp.addMonths(intervalType.months);
            temp.addDays(intervalType.days);
            temp.add(intervalType.milliSeconds);

            out.value = temp.getMillis();
        }
    }

    @FunctionTemplate(names = {"date_add", "add"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
    public static class IntervalDateAdd implements DrillSimpleFunc{

        @Param IntervalHolder intervalType;
        @Param DateHolder dateType;
        @Output DateHolder out;

        public void setup(RecordBatch b){}

        public void eval(){

            org.joda.time.MutableDateTime temp = new org.joda.time.MutableDateTime(dateType.value, org.joda.time.DateTimeZone.UTC);
            temp.addMonths(intervalType.months);
            temp.addDays(intervalType.days);
            temp.add(intervalType.milliSeconds);

            out.value = temp.getMillis();
        }
    }


    @FunctionTemplate(names = {"date_sub", "subtract"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
    public static class DateIntervalSub implements DrillSimpleFunc{

        @Param DateHolder dateType;
        @Param IntervalHolder intervalType;
        @Output DateHolder out;

        public void setup(RecordBatch b){}

        public void eval(){

            org.joda.time.MutableDateTime temp = new org.joda.time.MutableDateTime(dateType.value, org.joda.time.DateTimeZone.UTC);
            temp.addMonths(intervalType.months * -1);
            temp.addDays(intervalType.days * -1);
            temp.add(intervalType.milliSeconds * -1);

            out.value = temp.getMillis();
        }
    }

    @FunctionTemplate(names = {"date_sub", "subtract"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
    public static class IntervalDateSub implements DrillSimpleFunc{

        @Param IntervalHolder intervalType;
        @Param DateHolder dateType;
        @Output DateHolder out;

        public void setup(RecordBatch b){}

        public void eval(){

            org.joda.time.MutableDateTime temp = new org.joda.time.MutableDateTime(dateType.value, org.joda.time.DateTimeZone.UTC);
            temp.addMonths(intervalType.months * -1);
            temp.addDays(intervalType.days * -1);
            temp.add(intervalType.milliSeconds * -1);

            out.value = temp.getMillis();
        }
    }

    @FunctionTemplate(name = "current_date", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class CurrentDate implements DrillSimpleFunc {
        @Workspace long queryStartDate;
        @Output DateHolder out;

        public void setup(RecordBatch incoming) {

            org.joda.time.DateTime now = new org.joda.time.DateTime(incoming.getContext().getQueryStartTime());
            queryStartDate = (new org.joda.time.DateMidnight(now.getYear(), now.getMonthOfYear(), now.getDayOfMonth(), org.joda.time.DateTimeZone.UTC)).getMillis();
        }

        public void eval() {
            out.value = queryStartDate;
        }

    }

    @FunctionTemplate(name = "current_timestamptz", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class CurrentTimeStampTZ implements DrillSimpleFunc {
        @Workspace long queryStartDate;
        @Workspace int timezoneIndex;
        @Output TimeStampTZHolder out;

        public void setup(RecordBatch incoming) {

            org.joda.time.DateTime now = new org.joda.time.DateTime(incoming.getContext().getQueryStartTime());
            queryStartDate = now.getMillis();
            timezoneIndex = org.apache.drill.exec.expr.fn.impl.DateUtility.getIndex(now.getZone().toString());
        }

        public void eval() {
            out.value = queryStartDate;
            out.index = timezoneIndex;
        }

    }

    @FunctionTemplate(name = "current_time", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class CurrentTime implements DrillSimpleFunc {
        @Workspace int queryStartTime;
        @Output TimeHolder out;

        public void setup(RecordBatch incoming) {

            org.joda.time.DateTime now = new org.joda.time.DateTime(incoming.getContext().getQueryStartTime());
            queryStartTime= (int) ((now.getHourOfDay() * org.apache.drill.exec.expr.fn.impl.DateUtility.hoursToMillis) +
                                   (now.getMinuteOfHour() * org.apache.drill.exec.expr.fn.impl.DateUtility.minutesToMillis) +
                                   (now.getSecondOfMinute() * org.apache.drill.exec.expr.fn.impl.DateUtility.secondsToMillis) +
                                   (now.getMillisOfSecond()));
        }

        public void eval() {
            out.value = queryStartTime;
        }

    }
}
