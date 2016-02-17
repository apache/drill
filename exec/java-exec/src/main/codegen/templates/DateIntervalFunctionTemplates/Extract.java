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

/* Dummy function template to allow Optiq to validate this function call.
 * At DrillOptiq time we rewrite all date_part() functions to extract functions,
 * since they are essentially the same
 */
@FunctionTemplate(name = "DATE_PART", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
public static class DummyDATE_PARTFromVarChar implements DrillSimpleFunc {
  @Param VarCharHolder in1;
  @Param VarCharHolder in;
  @Output BigIntHolder out;

  public void setup() {
  }

  public void eval() {
    if (1 == 1) {
      throw new UnsupportedOperationException("date_part function should be rewritten as extract() functions");
    }
  }
}

<#list extract.fromTypes as dummyFromUnit>
/* Dummy function template to allow Optiq to validate this function call.
 * At DrillOptiq time we rewrite all date_part() functions to extract functions,
 * since they are essentially the same
 */
@FunctionTemplate(name = "DATE_PART", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
public static class DummyDATE_PARTFrom${dummyFromUnit} implements DrillSimpleFunc {
  @Param VarCharHolder in1;
  @Param ${dummyFromUnit}Holder in;
  @Output BigIntHolder out;

  public void setup() {
  }

  public void eval() {
    if (1 == 1) {
      throw new UnsupportedOperationException("date_part function should be rewritten as extract() functions");
    }
  }
}
</#list>


<#list extract.fromTypes as fromUnit>
<#list extract.toTypes as toUnit>

<#if fromUnit == "Date" || fromUnit == "Time" || fromUnit == "TimeStamp">
<#if !(fromUnit == "Time" && (toUnit == "Year" || toUnit == "Month" || toUnit == "Day"))>
  @FunctionTemplate(name = "extract${toUnit}", scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class ${toUnit}From${fromUnit} implements DrillSimpleFunc {

    @Param ${fromUnit}Holder in;
    <#if toUnit == "Second">
    @Output Float8Holder out;
    <#else>
    @Output BigIntHolder out;
    </#if>
    @Workspace org.joda.time.MutableDateTime dateTime;

    public void setup() {
      dateTime = new org.joda.time.MutableDateTime(org.joda.time.DateTimeZone.UTC);
    }

    public void eval() {
      dateTime.setMillis(in.value);

    <#if toUnit == "Second">
      out.value = dateTime.getSecondOfMinute();
      out.value += ((double) dateTime.getMillisOfSecond()) / 1000;
    <#elseif toUnit = "Minute">
      out.value = dateTime.getMinuteOfHour();
    <#elseif toUnit = "Hour">
      out.value = dateTime.getHourOfDay();
    <#elseif toUnit = "Day">
      out.value = dateTime.getDayOfMonth();
    <#elseif toUnit = "Month">
      out.value = dateTime.getMonthOfYear();
    <#elseif toUnit = "Year">
      out.value = dateTime.getYear();
    </#if>
    }
  }
</#if>
<#else>
  @FunctionTemplate(name = "extract${toUnit}", scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class ${toUnit}From${fromUnit} implements DrillSimpleFunc {

    @Param ${fromUnit}Holder in;
    <#if toUnit == "Second">
    @Output Float8Holder out;
    <#else>
    @Output BigIntHolder out;
    </#if>

    public void setup() { }

    public void eval() {
  <#if fromUnit == "Interval">
    <#if toUnit == "Year">
      out.value = (in.months / org.apache.drill.exec.expr.fn.impl.DateUtility.yearsToMonths);
    <#elseif toUnit == "Month">
      out.value = (in.months % org.apache.drill.exec.expr.fn.impl.DateUtility.yearsToMonths);
    <#elseif toUnit == "Day">
      out.value = in.days;
    <#elseif toUnit == "Hour">
      out.value = in.milliseconds/(org.apache.drill.exec.expr.fn.impl.DateUtility.hoursToMillis);
    <#elseif toUnit == "Minute">
      int millis = in.milliseconds % (org.apache.drill.exec.expr.fn.impl.DateUtility.hoursToMillis);
      out.value = millis / (org.apache.drill.exec.expr.fn.impl.DateUtility.minutesToMillis);
    <#elseif toUnit == "Second">
      long millis = in.milliseconds % org.apache.drill.exec.expr.fn.impl.DateUtility.minutesToMillis;
      out.value = (double) millis / (org.apache.drill.exec.expr.fn.impl.DateUtility.secondsToMillis);
    </#if>
  <#elseif fromUnit == "IntervalDay">
    <#if toUnit == "Year" || toUnit == "Month">
      out.value = 0;
    <#elseif toUnit == "Day">
      out.value = in.days;
    <#elseif toUnit == "Hour">
      out.value = in.milliseconds/(org.apache.drill.exec.expr.fn.impl.DateUtility.hoursToMillis);
    <#elseif toUnit == "Minute">
      int millis = in.milliseconds % (org.apache.drill.exec.expr.fn.impl.DateUtility.hoursToMillis);
      out.value = millis / (org.apache.drill.exec.expr.fn.impl.DateUtility.minutesToMillis);
    <#elseif toUnit == "Second">
      long millis = in.milliseconds % org.apache.drill.exec.expr.fn.impl.DateUtility.minutesToMillis;
      out.value = (double) millis / (org.apache.drill.exec.expr.fn.impl.DateUtility.secondsToMillis);
    </#if>
  <#else> <#-- IntervalYear type -->
    <#if toUnit == "Year">
      out.value = (in.value / org.apache.drill.exec.expr.fn.impl.DateUtility.yearsToMonths);
    <#elseif toUnit == "Month">
      out.value = (in.value % org.apache.drill.exec.expr.fn.impl.DateUtility.yearsToMonths);
    <#else>
      out.value = 0;
    </#if>
  </#if>
    }
  }
</#if>
</#list>
</#list>
}