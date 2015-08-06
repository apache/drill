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



<#list holdTypes.holdtypes as holdtype>
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/holdlast/${holdtype.className}NumericFunctions.java" />

<#include "/@includes/license.ftl" />

// Source code generated using FreeMarker template ${.template_name}

<#-- A utility class that is used to generate java code for hold last functions that maintain a single value. -->

package org.apache.drill.exec.expr.fn.impl.holdlast;

import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.*;

@SuppressWarnings("unused")

public class ${holdtype.className}NumericFunctions {

<#list holdtype.types as type>
<#if type.major == "Numeric">

@FunctionTemplate(name = "${holdtype.funcName}", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
public static class ${type.inputType}${holdtype.className} implements DrillAggFunc {

  @Param ${type.inputType}Holder in;
  @Workspace ${type.inputType}Holder value;
  @Output ${type.inputType}Holder out;

  public void setup() {
    value = new ${type.inputType}Holder();
  }
  
  @Override
  public void add() {
	<#if type.inputType?starts_with("Nullable")>
    value.isSet = in.isSet;
    if (in.isSet == 1) {
      value.value = in.value;
    }
	<#else>
    value.value = in.value;
	</#if>
  }

  @Override
  public void output() {
	<#if type.inputType?starts_with("Nullable")>
    out.isSet = value.isSet;
    if (value.isSet == 1) {
      out.value = value.value;
    }
	<#else>
    out.value = value.value;
	</#if>
  }

  @Override
  public void reset() {
	<#if type.inputType?starts_with("Nullable")>
    value.isSet = 0;
	</#if>
  }

 }

</#if>
</#list>
}
</#list>

