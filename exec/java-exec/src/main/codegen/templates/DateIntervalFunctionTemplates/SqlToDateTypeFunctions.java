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

<@pp.dropOutputFile/>

<#list dateIntervalFunc.dates as type>

<@pp.changeOutputFile name = "/org/apache/drill/exec/expr/fn/impl/SqlTo${type}.java"/>

<#include "/@includes/license.ftl"/>

package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.*;

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */

@FunctionTemplate(name = "sql_to_${type?lower_case}",
                  scope = FunctionTemplate.FunctionScope.SIMPLE,
                  nulls = NullHandling.NULL_IF_NULL)
public class SqlTo${type} implements DrillSimpleFunc {

  @Param  VarCharHolder left;
  @Param  VarCharHolder right;
  @Workspace org.joda.time.format.DateTimeFormatter format;
  @Output ${type}Holder out;

  public void setup() {
    // Get the desired output format
    String formatString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(right);
    String pattern = org.apache.drill.common.expression.fn.JodaDateValidator.toJodaFormat(formatString);
    try {
      format = org.joda.time.format.DateTimeFormat.forPattern(pattern);
    } catch (IllegalArgumentException e) {
      throw org.apache.drill.common.exceptions.UserException.functionError(e)
        .message("Error parsing formatter %s in %s function", formatString, "sql_to_${type?lower_case}")
        .build();
    }
  }

  public void eval() {
    // Get the input
    String input = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(left);
    try {
      <#if type == "Date">
      out.value = org.joda.time.DateMidnight.parse(input, format).withZoneRetainFields(org.joda.time.DateTimeZone.UTC).getMillis();
      <#elseif type == "TimeStamp">
      out.value = org.joda.time.DateTime.parse(input, format).withZoneRetainFields(org.joda.time.DateTimeZone.UTC).getMillis();
      <#elseif type == "Time">
      out.value = (int) format.parseDateTime(input).withZoneRetainFields(org.joda.time.DateTimeZone.UTC).getMillis();
      </#if>
    } catch (IllegalArgumentException e) {
      throw org.apache.drill.common.exceptions.UserException.functionError(e)
        .message("Error parsing date-time %s in %s function", input, "sql_to_${type?lower_case}")
        .build();
    }
  }
}
</#list>