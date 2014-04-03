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
<#assign className="GExtract" />

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/${className}.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.*;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;

public class ${className} {

<#list extract.toTypes as toUnit>
<#list extract.fromTypes as fromUnit>
<#if fromUnit == "Date" || fromUnit == "Time" || fromUnit == "TimeStamp">
  @FunctionTemplate(name = "extract${toUnit}", scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class ${toUnit}From${fromUnit} implements DrillSimpleFunc {

    @Param ${fromUnit}Holder in;
    @Output BigIntHolder out;

    public void setup(RecordBatch incoming) { }

    public void eval() {
      org.joda.time.MutableDateTime temp = new org.joda.time.MutableDateTime(in.value, org.joda.time.DateTimeZone.UTC);
    <#if toUnit == "Second">
      out.value = temp.getSecondOfMinute();
    <#elseif toUnit = "Minute">
      out.value = temp.getMinuteOfHour();
    <#elseif toUnit = "Hour">
      out.value = temp.getHourOfDay();
    <#elseif toUnit = "Day">
      out.value = temp.getDayOfMonth();
    <#elseif toUnit = "Month">
      out.value = temp.getMonthOfYear();
    <#elseif toUnit = "Year">
      out.value = temp.getYear();
    </#if>
    }
  }
<#else>
  @FunctionTemplate(name = "extract${toUnit}", scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class ${toUnit}From${fromUnit} implements DrillSimpleFunc {

    @Param ${fromUnit}Holder in;
    @Output BigIntHolder out;

    public void setup(RecordBatch incoming) { }

    public void eval() {
    <#if fromUnit == "Interval">

      int years  = (in.months / org.apache.drill.exec.expr.fn.impl.DateUtility.yearsToMonths);
      int months = (in.months % org.apache.drill.exec.expr.fn.impl.DateUtility.yearsToMonths);

      int millis = in.milliSeconds;

      int hours  = millis / (org.apache.drill.exec.expr.fn.impl.DateUtility.hoursToMillis);
      millis     = millis % (org.apache.drill.exec.expr.fn.impl.DateUtility.hoursToMillis);

      int minutes = millis / (org.apache.drill.exec.expr.fn.impl.DateUtility.minutesToMillis);
      millis      = millis % (org.apache.drill.exec.expr.fn.impl.DateUtility.minutesToMillis);

      int seconds = millis / (org.apache.drill.exec.expr.fn.impl.DateUtility.secondsToMillis);
      millis      = millis % (org.apache.drill.exec.expr.fn.impl.DateUtility.secondsToMillis);
      org.joda.time.Period temp = new org.joda.time.Period(years, months, 0, in.days, hours, minutes, seconds, millis);

    <#elseif fromUnit == "IntervalDay">

      int millis = in.milliSeconds;

      int hours  = millis / (org.apache.drill.exec.expr.fn.impl.DateUtility.hoursToMillis);
      millis     = millis % (org.apache.drill.exec.expr.fn.impl.DateUtility.hoursToMillis);

      int minutes = millis / (org.apache.drill.exec.expr.fn.impl.DateUtility.minutesToMillis);
      millis      = millis % (org.apache.drill.exec.expr.fn.impl.DateUtility.minutesToMillis);

      int seconds = millis / (org.apache.drill.exec.expr.fn.impl.DateUtility.secondsToMillis);
      millis      = millis % (org.apache.drill.exec.expr.fn.impl.DateUtility.secondsToMillis);
      org.joda.time.Period temp = new org.joda.time.Period(0, 0, 0, in.days, hours, minutes, seconds, millis);

    <#else>

      int years  = (in.value / org.apache.drill.exec.expr.fn.impl.DateUtility.yearsToMonths);
      int months = (in.value % org.apache.drill.exec.expr.fn.impl.DateUtility.yearsToMonths);
      org.joda.time.Period temp = new org.joda.time.Period(years, months, 0, 0, 0, 0, 0, 0);

    </#if>
      out.value = temp.get${toUnit}s();
    }
  }
  </#if>
</#list>
</#list>
}