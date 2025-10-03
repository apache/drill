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
<@pp.dropOutputFile />
<#assign className="GTimestampAdd"/>

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/${className}.java"/>

<#include "/@includes/license.ftl"/>

package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */

public class ${className} {

<#list dateIntervalFunc.timestampAddUnits as unit>
<#list dateIntervalFunc.timestampAddInputTypes as inputType>
<#-- Determine output type based on DrillTimestampAddTypeInference rules:
     - NANOSECOND, DAY, WEEK, MONTH, QUARTER, YEAR: preserve input type
     - MICROSECOND, MILLISECOND: always TIMESTAMP
     - SECOND, MINUTE, HOUR: TIMESTAMP except TIME input stays TIME
-->
<#assign outType=inputType>
<#if unit == "Microsecond" || unit == "Millisecond">
<#assign outType="TimeStamp">
<#elseif (unit == "Second" || unit == "Minute" || unit == "Hour") && inputType != "Time">
<#assign outType="TimeStamp">
</#if>

  @FunctionTemplate(name = "timestampadd${unit}",
                    scope = FunctionTemplate.FunctionScope.SIMPLE,
                    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class TimestampAdd${unit}${inputType} implements DrillSimpleFunc {

    @Param IntHolder count;
    @Param ${inputType}Holder input;
    @Output ${outType}Holder out;

    public void setup() {
    }

    public void eval() {
    <#if inputType == "Time">
      <#-- For TIME inputs, check output type -->
      <#if outType == "Time">
      <#-- TIME input, TIME output (NANOSECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, YEAR) -->
      <#if unit == "Nanosecond">
      // NANOSECOND: TIME -> TIME (preserve time)
      out.value = (int)(input.value + (count.value / 1_000_000L));
      <#elseif unit == "Second">
      out.value = (int)(input.value + ((long) count.value * org.apache.drill.exec.vector.DateUtilities.secondsToMillis));
      <#elseif unit == "Minute">
      out.value = (int)(input.value + ((long) count.value * org.apache.drill.exec.vector.DateUtilities.minutesToMillis));
      <#elseif unit == "Hour">
      out.value = (int)(input.value + ((long) count.value * org.apache.drill.exec.vector.DateUtilities.hoursToMillis));
      <#elseif unit == "Day">
      // DAY: TIME -> TIME (preserve time)
      out.value = input.value;
      <#elseif unit == "Week">
      // WEEK: TIME -> TIME (preserve time)
      out.value = input.value;
      <#elseif unit == "Month" || unit == "Quarter" || unit == "Year">
      // Month-level: TIME -> TIME (preserve time)
      out.value = input.value;
      </#if>
      <#else>
      <#-- TIME input, TIMESTAMP output (all other units) -->
      long inputMillis = input.value;
      <#if unit == "Nanosecond">
      // NANOSECOND: TIME -> TIME
      out.value = inputMillis + (count.value / 1_000_000L);
      <#elseif unit == "Microsecond">
      // MICROSECOND: TIME -> TIMESTAMP
      out.value = inputMillis + (count.value / 1_000L);
      <#elseif unit == "Millisecond">
      // MILLISECOND: TIME -> TIMESTAMP
      out.value = inputMillis + count.value;
      <#elseif unit == "Day">
      // Day interval: TIME -> TIME
      out.value = inputMillis + ((long) count.value * org.apache.drill.exec.vector.DateUtilities.daysToStandardMillis);
      <#elseif unit == "Week">
      // Week interval: TIME -> TIME
      out.value = inputMillis + ((long) count.value * 604800000L); // 7 * 24 * 60 * 60 * 1000
      <#elseif unit == "Month" || unit == "Quarter" || unit == "Year">
      // Month-level intervals: TIME -> TIME (epoch + TIME + interval)
      java.time.LocalDateTime dateTime = java.time.Instant.ofEpochMilli(inputMillis).atZone(java.time.ZoneOffset.UTC).toLocalDateTime();
        <#if unit == "Month">
      dateTime = dateTime.plusMonths(count.value);
        <#elseif unit == "Quarter">
      dateTime = dateTime.plusMonths((long) count.value * 3);
        <#elseif unit == "Year">
      dateTime = dateTime.plusYears(count.value);
        </#if>
      out.value = dateTime.atZone(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();
      </#if>
      </#if>
    <#elseif inputType == "Date">
      <#-- For DATE inputs, check output type -->
      <#if outType == "Date">
      <#-- DATE input, DATE output (NANOSECOND, DAY, WEEK, MONTH, QUARTER, YEAR) -->
      <#if unit == "Nanosecond">
      // NANOSECOND: DATE -> DATE (preserve days)
      out.value = input.value;
      <#elseif unit == "Day">
      // DAY: DATE -> DATE (DATE stores milliseconds)
      out.value = input.value + ((long) count.value * org.apache.drill.exec.vector.DateUtilities.daysToStandardMillis);
      <#elseif unit == "Week">
      // WEEK: DATE -> DATE (DATE stores milliseconds)
      out.value = input.value + ((long) count.value * 7 * org.apache.drill.exec.vector.DateUtilities.daysToStandardMillis);
      <#elseif unit == "Month" || unit == "Quarter" || unit == "Year">
      // Month-level: DATE -> DATE (input.value is milliseconds since epoch)
      java.time.LocalDate date = java.time.Instant.ofEpochMilli(input.value).atZone(java.time.ZoneOffset.UTC).toLocalDate();
        <#if unit == "Month">
      date = date.plusMonths(count.value);
        <#elseif unit == "Quarter">
      date = date.plusMonths((long) count.value * 3);
        <#elseif unit == "Year">
      date = date.plusYears(count.value);
        </#if>
      out.value = date.atStartOfDay(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();
      </#if>
      <#else>
      <#-- DATE input, TIMESTAMP output (MICROSECOND, MILLISECOND, SECOND, MINUTE, HOUR) -->
      long inputMillis = input.value;
      <#if unit == "Microsecond">
      // MICROSECOND: DATE -> TIMESTAMP
      out.value = inputMillis + (count.value / 1_000L);
      <#elseif unit == "Millisecond">
      // MILLISECOND: DATE -> TIMESTAMP
      out.value = inputMillis + count.value;
      <#elseif unit == "Second">
      // SECOND: DATE -> TIMESTAMP
      out.value = inputMillis + ((long) count.value * org.apache.drill.exec.vector.DateUtilities.secondsToMillis);
      <#elseif unit == "Minute">
      // MINUTE: DATE -> TIMESTAMP
      out.value = inputMillis + ((long) count.value * org.apache.drill.exec.vector.DateUtilities.minutesToMillis);
      <#elseif unit == "Hour">
      // HOUR: DATE -> TIMESTAMP
      out.value = inputMillis + ((long) count.value * org.apache.drill.exec.vector.DateUtilities.hoursToMillis);
      </#if>
      </#if>
    <#elseif inputType == "TimeStamp">
      <#-- TIMESTAMP input always produces TIMESTAMP output -->
      <#if unit == "Nanosecond">
      out.value = input.value + (count.value / 1_000_000L);
      <#elseif unit == "Microsecond">
      out.value = input.value + (count.value / 1_000L);
      <#elseif unit == "Millisecond">
      out.value = input.value + count.value;
      <#elseif unit == "Second">
      out.value = input.value + ((long) count.value * org.apache.drill.exec.vector.DateUtilities.secondsToMillis);
      <#elseif unit == "Minute">
      out.value = input.value + ((long) count.value * org.apache.drill.exec.vector.DateUtilities.minutesToMillis);
      <#elseif unit == "Hour">
      out.value = input.value + ((long) count.value * org.apache.drill.exec.vector.DateUtilities.hoursToMillis);
      <#elseif unit == "Day">
      out.value = input.value + ((long) count.value * org.apache.drill.exec.vector.DateUtilities.daysToStandardMillis);
      <#elseif unit == "Week">
      out.value = input.value + ((long) count.value * 604800000L); // 7 * 24 * 60 * 60 * 1000
      <#elseif unit == "Month" || unit == "Quarter" || unit == "Year">
      java.time.LocalDateTime dateTime = java.time.Instant.ofEpochMilli(input.value).atZone(java.time.ZoneOffset.UTC).toLocalDateTime();
        <#if unit == "Month">
      dateTime = dateTime.plusMonths(count.value);
        <#elseif unit == "Quarter">
      dateTime = dateTime.plusMonths((long) count.value * 3);
        <#elseif unit == "Year">
      dateTime = dateTime.plusYears(count.value);
        </#if>
      out.value = dateTime.atZone(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();
      </#if>
    </#if>
    }
  }
</#list>

</#list>
}
