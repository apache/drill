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

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/GDateTimeTruncateFunctions.java" />

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

public class GDateTimeTruncateFunctions {

<#list dateIntervalFunc.dates as type>

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
        if (org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(left.start, left.end, left.buffer).equalsIgnoreCase("YEAR")) dateTime.setRounding(dateTime.getChronology().year());
        else if (org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(left.start, left.end, left.buffer).equalsIgnoreCase("MONTH")) dateTime.setRounding(dateTime.getChronology().monthOfYear());
        else if (org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(left.start, left.end, left.buffer).equalsIgnoreCase("DAY")) dateTime.setRounding(dateTime.getChronology().dayOfMonth());
        else
        </#if>
        <#if type != "Date">
        if (org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(left.start, left.end, left.buffer).equalsIgnoreCase("HOUR")) dateTime.setRounding(dateTime.getChronology().hourOfDay());
        else if (org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(left.start, left.end, left.buffer).equalsIgnoreCase("MINUTE")) dateTime.setRounding(dateTime.getChronology().minuteOfHour());
        else if (org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(left.start, left.end, left.buffer).equalsIgnoreCase("SECOND")) dateTime.setRounding(dateTime.getChronology().secondOfMinute());
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
</#list>
}