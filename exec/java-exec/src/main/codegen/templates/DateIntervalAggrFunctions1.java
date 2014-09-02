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



<#list aggrtypes1.aggrtypes as aggrtype>
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/gaggr/${aggrtype.className}DateTypeFunctions.java" />

<#include "/@includes/license.ftl" />

<#-- A utility class that is used to generate java code for aggr functions for Date, Time, Interval types -->
<#--  that maintain a single running counter to hold the result.  This includes: MIN, MAX, SUM, COUNT. -->

/*
 * This class is automatically generated from AggrTypeFunctions1.tdd using FreeMarker.
 */

package org.apache.drill.exec.expr.fn.impl.gaggr;

import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;

@SuppressWarnings("unused")

public class ${aggrtype.className}DateTypeFunctions {

<#list aggrtype.types as type>
<#if type.major == "Date">
@FunctionTemplate(name = "${aggrtype.funcName}", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
public static class ${type.inputType}${aggrtype.className} implements DrillAggFunc{

  @Param ${type.inputType}Holder in;
  @Workspace ${type.runningType}Holder value;
  @Output ${type.outputType}Holder out;

  public void setup(RecordBatch b) {
	value = new ${type.runningType}Holder();
    <#if type.runningType == "Interval">
    value.months = ${type.initialValue};
    value.days= ${type.initialValue};
    value.milliseconds = ${type.initialValue};
    <#elseif type.runningType == "IntervalDay">
    value.days= ${type.initialValue};
    value.milliseconds = ${type.initialValue};
    <#else>
    value.value = ${type.initialValue};
    </#if>
  }

  @Override
  public void add() {
	  <#if type.inputType?starts_with("Nullable")>
	    sout: {
	    if (in.isSet == 0) {
		    // processing nullable input and the value is null, so don't do anything...
		    break sout;
	    }
	  </#if>

	  <#if aggrtype.funcName == "min">

    <#if type.outputType == "TimeStampTZ">
    if (in.value < value.value) {
      value.value = in.value;
      value.index = in.index;
    }
    <#elseif type.outputType == "Interval">

    long inMS = (long) in.months * org.apache.drill.exec.expr.fn.impl.DateUtility.monthsToMillis+
                       in.days * (org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis) +
                       in.milliseconds;

    value.value = Math.min(value.value, inMS);

    <#elseif type.outputType == "IntervalDay">
    long inMS = (long) in.days * (org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis) +
                       in.milliseconds;

    value.value = Math.min(value.value, inMS);


    <#else>
    value.value = Math.min(value.value, in.value);
    </#if>
	  <#elseif aggrtype.funcName == "max">
    <#if type.outputType == "TimeStampTZ">
    if (in.value < value.value) {
      value.value = in.value;
      value.index = in.index;
    }
    <#elseif type.outputType == "Interval">
    long inMS = (long) in.months * org.apache.drill.exec.expr.fn.impl.DateUtility.monthsToMillis+
                       in.days * (org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis) +
                       in.milliseconds;

    value.value = Math.max(value.value, inMS);
    <#elseif type.outputType == "IntervalDay">
    long inMS = (long) in.days * (org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis) +
                       in.milliseconds;

    value.value = Math.max(value.value, inMS);
    <#else>
    value.value = Math.max(value.value, in.value);
    </#if>

	  <#elseif aggrtype.funcName == "sum">
    <#if type.outputType == "Interval">
    value.days += in.days;
    value.months += in.months;
    value.milliseconds += in.milliseconds;
    <#elseif type.outputType == "IntervalDay">
    value.days += in.days;
    value.milliseconds += in.milliseconds;
    <#else>
	    value.value += in.value;
    </#if>
	  <#elseif aggrtype.funcName == "count">
	    value.value++;
	  <#else>
	  // TODO: throw an error ?
	  </#if>
	<#if type.inputType?starts_with("Nullable")>
    } // end of sout block
	</#if>
  }

  @Override
  public void output() {

    <#if aggrtype.funcName == "max" || aggrtype.funcName == "min">
    <#if type.outputType == "Interval">
    out.months = (int) (value.value / org.apache.drill.exec.expr.fn.impl.DateUtility.monthsToMillis);
    value.value = value.value % org.apache.drill.exec.expr.fn.impl.DateUtility.monthsToMillis;
    out.days = (int) (value.value / org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis);
    out.milliseconds = (int) (value.value % org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis);
    <#elseif type.outputType == "IntervalDay">
    out.days = (int) (value.value / org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis);
    out.milliseconds = (int) (value.value % org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis);
    <#else>
    out.value = value.value;
    </#if>
    <#else>
    <#if type.outputType == "Interval">
    out.months = value.months;
    out.days = value.days;
    out.milliseconds = value.milliseconds;
    <#elseif type.outputType == "IntervalDay">
    out.days = value.days;
    out.milliseconds = value.milliseconds;
    <#else>
    out.value = value.value;
    </#if>
    </#if>
  }

  @Override
  public void reset() {
    <#if type.runningType == "Interval">
    value.months = ${type.initialValue};
    value.days= ${type.initialValue};
    value.milliseconds = ${type.initialValue};
    <#elseif type.runningType == "IntervalDay">
    value.days= ${type.initialValue};
    value.milliseconds = ${type.initialValue};
    <#else>
    value.value = ${type.initialValue};
    </#if>
  }

 }

</#if>
</#list>
}
</#list>

