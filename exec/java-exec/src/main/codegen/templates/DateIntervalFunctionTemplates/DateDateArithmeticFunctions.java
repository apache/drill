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

import org.apache.drill.exec.expr.annotations.Workspace;

<@pp.dropOutputFile />

<#list dateIntervalFunc.dates as type>

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/G${type}Arithmetic.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;

import io.netty.buffer.ByteBuf;

@SuppressWarnings("unused")
public class G${type}Arithmetic {
@SuppressWarnings("unused")

@FunctionTemplate(names = {"date_diff", "subtract", "date_sub"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
public static class G${type}Difference implements DrillSimpleFunc {

    @Param  ${type}Holder left;
    @Param  ${type}Holder right;
    @Output IntervalDayHolder out;

    public void setup(RecordBatch b) {
    }

    public void eval() {
        <#if type == "Time">
        out.milliseconds = left.value - right.value;
        <#elseif type == "Date">
        out.days = (int) ((left.value - right.value) / org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis);
        <#elseif type == "TimeStamp">
        long difference = (left.value - right.value);
        out.milliseconds = (int) (difference % org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis);
        out.days = (int) (difference / org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis);
        <#elseif type == "TimeStampTZ">
        if (left.index != right.index) {
            // The two inputs are in different time zones convert one of them
            org.joda.time.DateTime leftInput = new org.joda.time.DateTime(left.value, org.joda.time.DateTimeZone.forID(org.apache.drill.exec.expr.fn.impl.DateUtility.timezoneList[left.index]));
            // convert left timestamp to the time zone of the right timestamp
            left.value = (leftInput.withZone(org.joda.time.DateTimeZone.forID(org.apache.drill.exec.expr.fn.impl.DateUtility.timezoneList[right.index]))).getMillis();
        }
        long difference = (left.value - right.value);
        out.milliseconds = (int) (difference % org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis);
        out.days = (int) (difference / org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis);
        </#if>
    }
}

@SuppressWarnings("unused")
@FunctionTemplate(names = "date_trunc", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
public static class G${type}DateTrunc implements DrillSimpleFunc {

    @Param  VarCharHolder left;
    @Param  ${type}Holder right;
    @Output ${type}Holder out;
    @Workspace org.joda.time.MutableDateTime dateTime;

    public void setup(RecordBatch incoming) {
      dateTime = new org.joda.time.MutableDateTime(org.joda.time.DateTimeZone.UTC);
    }

    public void eval() {
        dateTime.setMillis(right.value);
        <#if type != "Time">
        if (left.toString().equalsIgnoreCase("YEAR")) dateTime.setRounding(dateTime.getChronology().year());
        else if (left.toString().equalsIgnoreCase("MONTH")) dateTime.setRounding(dateTime.getChronology().monthOfYear());
        else if (left.toString().equalsIgnoreCase("DAY")) dateTime.setRounding(dateTime.getChronology().dayOfMonth());
        else
        </#if>
        <#if type != "Date">
        if (left.toString().equalsIgnoreCase("HOUR")) dateTime.setRounding(dateTime.getChronology().hourOfDay());
        else if (left.toString().equalsIgnoreCase("MINUTE")) dateTime.setRounding(dateTime.getChronology().minuteOfHour());
        else if (left.toString().equalsIgnoreCase("SECOND")) dateTime.setRounding(dateTime.getChronology().secondOfMinute());
        else
        </#if>
        <#if type == "TimeStamp" || type == "TimeStampTZ">
            throw new UnsupportedOperationException("date_trunc function supports the following time units for TimeStamp(TZ): YEAR, MONTH, DAY, HOUR, MINUTE, SECOND");
        out.value = dateTime.getMillis();
        <#elseif type == "Date">
            throw new UnsupportedOperationException("date_trunc function supports the following time units for Date: YEAR, MONTH, DAY");
        out.value = dateTime.getMillis();
        <#elseif type == "Time">
            throw new UnsupportedOperationException("date_trunc function supports the following time units for Time: HOUR, MINUTE, SECOND");
        out.value = (int) dateTime.getMillis();
        </#if>
    }
}
}
</#list>
