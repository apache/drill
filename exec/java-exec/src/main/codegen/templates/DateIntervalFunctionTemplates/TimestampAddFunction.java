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
<#list dateIntervalFunc.dates as toUnit>


@FunctionTemplate(name = "timestampadd${unit}",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
public static class TimestampAdd${unit}to${toUnit} implements DrillSimpleFunc {

@Param BigIntHolder count;
@Param ${toUnit}Holder right;
<#if toUnit == "Time">
@Output TimeHolder out;
<#else>
@Output TimeStampHolder out;
</#if>
public void setup() {
    }
public void eval() {
    long countValue = count.value;
<#if unit == "Nanosecond">
    java.time.temporal.TemporalUnit unit = java.time.temporal.ChronoUnit.NANOS;
<#elseif unit == "Microsecond">
    java.time.temporal.TemporalUnit unit = java.time.temporal.ChronoUnit.MICROS;
<#elseif unit == "Millisecond">
    java.time.temporal.TemporalUnit unit = java.time.temporal.ChronoUnit.MILLIS;
<#elseif unit == "Second">
    java.time.temporal.TemporalUnit unit = java.time.temporal.ChronoUnit.SECONDS;
<#elseif unit == "Minute">
    java.time.temporal.TemporalUnit unit = java.time.temporal.ChronoUnit.MINUTES;
<#elseif unit == "Hour">
    java.time.temporal.TemporalUnit unit = java.time.temporal.ChronoUnit.HOURS;
<#elseif unit == "Day">
    java.time.temporal.TemporalUnit unit = java.time.temporal.ChronoUnit.DAYS;
<#elseif unit == "Week">
    java.time.temporal.TemporalUnit unit = java.time.temporal.ChronoUnit.WEEKS;
<#elseif unit == "Month">
    java.time.temporal.TemporalUnit unit = java.time.temporal.ChronoUnit.MONTHS;
<#elseif unit == "Year">
    java.time.temporal.TemporalUnit unit = java.time.temporal.ChronoUnit.YEARS;
<#elseif unit == "Quarter">
    java.time.temporal.TemporalUnit unit = java.time.temporal.ChronoUnit.MONTHS;
    // Quarter has 3 month
    countValue *= 3;
</#if>
    long result = java.time.LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(right.value), java.time.ZoneOffset.UTC).plus(countValue, unit).toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
<#if toUnit == "Time">
    out.value = (int) (result % org.apache.drill.exec.vector.DateUtilities.daysToStandardMillis);
<#else>
    out.value = result;
</#if>
    }
    }

</#list>
</#list>
}
