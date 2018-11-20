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
<#assign className="GTimestampDiff"/>

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

<#list dateIntervalFunc.timestampDiffUnits as unit>

<#list dateIntervalFunc.dates as fromUnit>
<#list dateIntervalFunc.dates as toUnit>

  @FunctionTemplate(name = "timestampdiff${unit}",
                    scope = FunctionTemplate.FunctionScope.SIMPLE,
                    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class TimestampDiff${unit}${fromUnit}To${toUnit} implements DrillSimpleFunc {

    @Param ${fromUnit}Holder left;
    @Param ${toUnit}Holder right;
    @Output BigIntHolder out;

    public void setup() {
    }

    public void eval() {
    <#if unit == "Nanosecond">
      out.value = (right.value - left.value) * 1000000;
    <#elseif unit == "Microsecond">
      out.value = (right.value - left.value) * 1000;
    <#elseif unit == "Second">
      out.value = (right.value - left.value) / org.apache.drill.exec.vector.DateUtilities.secondsToMillis;
    <#elseif unit == "Minute">
      out.value = (right.value - left.value) / org.apache.drill.exec.vector.DateUtilities.minutesToMillis;
    <#elseif unit == "Hour">
      out.value = (right.value - left.value) / org.apache.drill.exec.vector.DateUtilities.hoursToMillis;
    <#elseif unit == "Day">
      out.value = (right.value - left.value) / org.apache.drill.exec.vector.DateUtilities.daysToStandardMillis;
    <#elseif unit == "Week">
      out.value = (right.value - left.value) / 604800000; // 7 * 24 * 60 * 60 * 1000
    <#elseif unit == "Month" || unit == "Quarter" || unit == "Year">
      long timeMilliseconds = left.value % org.apache.drill.exec.vector.DateUtilities.daysToStandardMillis
          - right.value % org.apache.drill.exec.vector.DateUtilities.daysToStandardMillis;

      java.time.Period between = java.time.Period.between(
          java.time.Instant.ofEpochMilli(left.value).atZone(java.time.ZoneOffset.UTC).toLocalDate(),
          java.time.Instant.ofEpochMilli(right.value).atZone(java.time.ZoneOffset.UTC).toLocalDate());
      int days = between.getDays();
      if (timeMilliseconds < 0 && days > 0) {
        // in the case of negative time value increases left operand days value
        between = java.time.Period.between(
            java.time.Instant.ofEpochMilli(left.value + org.apache.drill.exec.vector.DateUtilities.daysToStandardMillis).atZone(java.time.ZoneOffset.UTC).toLocalDate(),
            java.time.Instant.ofEpochMilli(right.value).atZone(java.time.ZoneOffset.UTC).toLocalDate());
      } else if (timeMilliseconds > 0 && days < 0) {
        // in the case of negative days value decreases it for the right operand
        between = java.time.Period.between(
            java.time.Instant.ofEpochMilli(left.value - org.apache.drill.exec.vector.DateUtilities.daysToStandardMillis).atZone(java.time.ZoneOffset.UTC).toLocalDate(),
            java.time.Instant.ofEpochMilli(right.value).atZone(java.time.ZoneOffset.UTC).toLocalDate());
      }
      int months = between.getMonths() + between.getYears() * org.apache.drill.exec.vector.DateUtilities.yearsToMonths;

        <#if unit == "Month">
      out.value = months;
        <#elseif unit == "Quarter">
      out.value = months / 4;
        <#elseif unit == "Year">
      out.value = months / org.apache.drill.exec.vector.DateUtilities.yearsToMonths;
        </#if>
    </#if>
    }
  }
</#list>
</#list>

</#list>
}
