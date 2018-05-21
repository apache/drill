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

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/gaggr/SingleValueFunctions.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl.gaggr;

import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.*;

import javax.inject.Inject;
import io.netty.buffer.DrillBuf;

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */
@SuppressWarnings("unused")
public class SingleValueFunctions {
<#list singleValue.types as type>

  @FunctionTemplate(name = "single_value",
                  <#if type.major == "VarDecimal">
                    returnType = FunctionTemplate.ReturnType.DECIMAL_AVG_AGGREGATE,
                  </#if>
                    scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class ${type.inputType}SingleValue implements DrillAggFunc {
    @Param ${type.inputType}Holder in;
    @Workspace ${type.runningType}Holder value;
    @Output ${type.outputType}Holder out;
    @Workspace BigIntHolder nonNullCount;
    <#if type.major == "VarDecimal" || type.major == "bytes">
    @Inject DrillBuf buffer;
    </#if>

    public void setup() {
      nonNullCount = new BigIntHolder();
      nonNullCount.value = 0;
      value = new ${type.runningType}Holder();
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
      if (nonNullCount.value == 0) {
        nonNullCount.value = 1;
      } else {
        throw org.apache.drill.common.exceptions.UserException.functionError()
            .message("Input for single_value function has more than one row")
            .build();
      }
    <#if type.major == "primitive">
      value.value = in.value;
    <#elseif type.major == "IntervalDay">
      value.days = in.days;
      value.milliseconds = in.milliseconds;
    <#elseif type.major == "Interval">
      value.days = in.days;
      value.milliseconds = in.milliseconds;
      value.months = in.months;
    <#elseif type.major == "VarDecimal">
      value.start = in.start;
      value.end = in.end;
      value.buffer = in.buffer;
      value.scale = in.scale;
      value.precision = in.precision;
    <#elseif type.major == "bytes">
      value.start = in.start;
      value.end = in.end;
      value.buffer = in.buffer;
    </#if>
    <#if type.inputType?starts_with("Nullable")>
      } // end of sout block
	  </#if>
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.isSet = 1;
      <#if type.major == "primitive">
        out.value = value.value;
      <#elseif type.major == "IntervalDay">
        out.days = value.days;
        out.milliseconds = value.milliseconds;
      <#elseif type.major == "Interval">
        out.days = value.days;
        out.milliseconds = value.milliseconds;
        out.months = value.months;
      <#elseif type.major == "VarDecimal">
        out.start = value.start;
        out.end = value.end;
        out.buffer = buffer.reallocIfNeeded(value.end - value.start);
        out.buffer.writeBytes(value.buffer, value.start, value.end - value.start);
        out.scale = value.scale;
        out.precision = value.precision;
      <#elseif type.major == "bytes">
        out.start = value.start;
        out.end = value.end;
        out.buffer = buffer.reallocIfNeeded(value.end - value.start);
        out.buffer.writeBytes(value.buffer, value.start, value.end - value.start);
      </#if>
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      value = new ${type.runningType}Holder();
      nonNullCount.value = 0;
    }
  }
</#list>
}

