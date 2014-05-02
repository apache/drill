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
<@pp.dropOutputFile />


<#list dateIntervalFunc.dates as datetype>
<#list dateIntervalFunc.intervals as intervaltype>

<#if datetype == "Date" || datetype == "TimeStamp" || datetype == "TimeStampTZ">
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/${datetype}${intervaltype}Functions.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl;

import io.netty.buffer.ByteBuf;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;
import org.joda.time.MutableDateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.DateMidnight;
import org.apache.drill.exec.expr.fn.impl.DateUtility;


public class ${datetype}${intervaltype}Functions {

<#macro dateIntervalArithmeticBlock left right temp op output intervaltype datetype>

    <#-- Throw exception if we are adding integer to a TIMESTAMP -->
    <#if (datetype == "TimeStamp" || datetype == "TimeStampTZ") && (intervaltype == "Int" || intervaltype == "BigInt")>
    if (1 == 1) {
        /* Since this will be included in the run time generated code, there might be other logic that follows this
         * if the exception is raised without a condition, we will hit compilation issues while compiling run time code
         * with the error: unreachable code.
         */
        throw new UnsupportedOperationException("Cannot add integer to TIMESTAMP, cast it to specific interval");
    }
    <#else>
    ${temp}.setMillis(${left}.value);

    <#if intervaltype == "Interval">
    ${temp}.addMonths(${right}.months <#if op == '-'> * -1 </#if>);
    ${temp}.addDays(${right}.days <#if op == '-'> * -1 </#if>);
    ${temp}.add(${right}.milliSeconds <#if op == '-'> * -1 </#if>);
    <#elseif intervaltype == "IntervalYear">
    ${temp}.addMonths(${right}.value <#if op == '-'> * -1 </#if>);
    <#elseif intervaltype == "IntervalDay">
    ${temp}.addDays(${right}.days <#if op == '-'> * -1 </#if>);
    ${temp}.add(${right}.milliSeconds <#if op == '-'> * -1 </#if>);
    <#elseif intervaltype == "Int" || intervaltype == "BigInt">
    ${temp}.addDays((int) ${right}.value <#if op == '-'> * -1 </#if>);
    </#if>

    <#-- copy the time zone index if its a timestamp -->
    <#if datetype == "TimeStampTZ">
    ${output}.index = ${left}.index;
    </#if>

    ${output}.value = ${temp}.getMillis();
    </#if>
</#macro>

    @SuppressWarnings("unused")
    @FunctionTemplate(names = {"date_add", "add"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
    public static class ${datetype}${intervaltype}AddFunction implements DrillSimpleFunc {
    @Param ${datetype}Holder left;
    @Param ${intervaltype}Holder right;
    @Workspace org.joda.time.MutableDateTime temp;
    @Output ${datetype}Holder out;

        public void setup(RecordBatch incoming) {
            <#if datetype == "TimeStampTZ">
            temp = new org.joda.time.MutableDateTime(org.joda.time.DateTimeZone.forID(org.apache.drill.exec.expr.fn.impl.DateUtility.timezoneList[left.index]));
            <#else>
            temp = new org.joda.time.MutableDateTime(org.joda.time.DateTimeZone.UTC);
            </#if>
        }

        public void eval() {
            <@dateIntervalArithmeticBlock left="left" right="right" temp = "temp" op = "+" output="out" intervaltype=intervaltype datetype = datetype/>
        }
    }

    <#-- Below function is the same as above except the arguments are in reverse order. We use macros to avoid having the logic in multiple places -->
    @SuppressWarnings("unused")
    @FunctionTemplate(names = {"date_add", "add"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
    public static class ${intervaltype}${datetype}AddFunction implements DrillSimpleFunc {

    @Param ${intervaltype}Holder right;
    @Param ${datetype}Holder left;
    @Workspace org.joda.time.MutableDateTime temp;
    @Output ${datetype}Holder out;

        public void setup(RecordBatch incoming) {
            <#if datetype == "TimeStampTZ">
            temp = new org.joda.time.MutableDateTime(org.joda.time.DateTimeZone.forID(org.apache.drill.exec.expr.fn.impl.DateUtility.timezoneList[left.index]));
            <#else>
            temp = new org.joda.time.MutableDateTime(org.joda.time.DateTimeZone.UTC);
            </#if>
        }

        public void eval() {

            <@dateIntervalArithmeticBlock left="left" right="right" temp = "temp" op = "+" output="out" intervaltype=intervaltype datetype = datetype/>
        }
    }

    @SuppressWarnings("unused")
    @FunctionTemplate(names = {"date_sub", "subtract"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
    public static class ${datetype}${intervaltype}SubtractFunction implements DrillSimpleFunc {
    @Param ${datetype}Holder left;
    @Param ${intervaltype}Holder right;
    @Workspace org.joda.time.MutableDateTime temp;
    @Output ${datetype}Holder out;

        public void setup(RecordBatch incoming) {
            <#if datetype == "TimeStampTZ">
            temp = new org.joda.time.MutableDateTime(org.joda.time.DateTimeZone.forID(org.apache.drill.exec.expr.fn.impl.DateUtility.timezoneList[left.index]));
            <#else>
            temp = new org.joda.time.MutableDateTime(org.joda.time.DateTimeZone.UTC);
            </#if>
        }

        public void eval() {
            <@dateIntervalArithmeticBlock left="left" right="right" temp = "temp" op = "-" output="out" intervaltype=intervaltype datetype = datetype/>
        }
    }
}
<#elseif datetype == "Time">
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/${datetype}${intervaltype}Functions.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl;

import io.netty.buffer.ByteBuf;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;
import org.joda.time.MutableDateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.DateMidnight;
import org.apache.drill.exec.expr.fn.impl.DateUtility;

public class ${datetype}${intervaltype}Functions {
<#macro timeIntervalArithmeticBlock left right temp op output intervaltype>
    <#if intervaltype == "Interval">
    if (${right}.months != 0 || ${right}.days != 0) {
        throw new UnsupportedOperationException("Cannot add interval type with months or days to TIME");
    }
    ${output} = ${left}.value ${op} ${right}.milliSeconds;
    <#elseif intervaltype == "IntervalYear">
    if (1 == 1) {
        throw new UnsupportedOperationException("Cannot add IntervalYear to TIME");
    }
    <#elseif intervaltype == "IntervalDay">
    if (${right}.days != 0) {
        throw new UnsupportedOperationException("Cannot add days to TIME");
    }
    ${output} = ${left}.value ${op} ${right}.milliSeconds;
    <#elseif intervaltype == "Int" || intervaltype == "BigInt">
    if (1 == 1) {
        throw new UnsupportedOperationException("Cannot add integer to TIME, cast it to specific interval");
    }
    </#if>
</#macro>

    @SuppressWarnings("unused")
    @FunctionTemplate(names = {"date_add", "add"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
    public static class ${datetype}${intervaltype}AddFunction implements DrillSimpleFunc {
    @Param ${datetype}Holder left;
    @Param ${intervaltype}Holder right;
    @Output ${datetype}Holder out;

        public void setup(RecordBatch incoming) {
        }

        public void eval() {
            <@timeIntervalArithmeticBlock left="left" right="right" temp = "temp" op = "+" output="out.value" intervaltype=intervaltype />
        }
    }

    @SuppressWarnings("unused")
    @FunctionTemplate(names = {"date_add", "add"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
    public static class ${intervaltype}${datetype}AddFunction implements DrillSimpleFunc {
    @Param ${intervaltype}Holder right;
    @Param ${datetype}Holder left;
    @Output ${datetype}Holder out;

        public void setup(RecordBatch incoming) {
        }
        public void eval() {
            <@timeIntervalArithmeticBlock left="left" right="right" temp = "temp" op = "+" output="out.value" intervaltype=intervaltype />
        }
    }

    @SuppressWarnings("unused")
    @FunctionTemplate(names = {"date_sub", "subtract"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
    public static class ${datetype}${intervaltype}SubtractFunction implements DrillSimpleFunc {
    @Param ${datetype}Holder left;
    @Param ${intervaltype}Holder right;
    @Output ${datetype}Holder out;

        public void setup(RecordBatch incoming) {
        }

        public void eval() {
            <@timeIntervalArithmeticBlock left="left" right="right" temp = "temp" op = "-" output="out.value" intervaltype=intervaltype />
        }
    }
}
</#if>
</#list>
</#list>