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



<#list aggrtypes4.aggrtypes as aggrtype>
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/gaggr/${aggrtype.className}Functions.java" />

<#include "/@includes/license.ftl" />

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */

<#-- A utility class that is used to generate java code for median functions -->

package org.apache.drill.exec.expr.fn.impl.gaggr;

import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.expr.fn.impl.StreamingMedianHelpers.StreamingMedianHelper;
import org.apache.drill.exec.expr.fn.impl.StreamingMedianHelpers.StreamingIntMedianHelper;
import org.apache.drill.exec.expr.fn.impl.StreamingMedianHelpers.StreamingDoubleMedianHelper;

@SuppressWarnings("unused")

public class ${aggrtype.className}Functions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${aggrtype.className}Functions.class);

<#list aggrtype.types as type>

<#if aggrtype.aliasName == "">
  @FunctionTemplate(name = "${aggrtype.funcName}", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
<#else>
  @FunctionTemplate(names = {"${aggrtype.funcName}", "${aggrtype.aliasName}"}, scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
</#if>

  public static class ${type.inputType}${aggrtype.className} implements DrillAggFunc {

    @Param ${type.inputType}Holder input;
    @Output ${type.outputType}Holder median;
    @Workspace ObjectHolder utils;

    public void setup() {
      // Initialize the ObjectHolder
      utils = new ObjectHolder();
      utils.obj = new org.apache.drill.exec.expr.fn.impl.StreamingMedianHelpers.${type.medianHelper}();
    }

    @Override
    public void add() {
      org.apache.drill.exec.expr.fn.impl.StreamingMedianHelpers.${type.medianHelper} medianHelper = (org.apache.drill.exec.expr.fn.impl.StreamingMedianHelpers.${type.medianHelper}) utils.obj;
      <#if type.inputType?starts_with("Nullable")>
          sout: {
      if (input.isSet == 0) {
        // processing nullable input and the value is null, so don't do anything...
        break sout;
      }
	    </#if>
      medianHelper.addNextNumber(input.value);

      <#if type.inputType?starts_with("Nullable")>
      } // end of sout block
    </#if>
    }

    @Override
    public void output() {
      org.apache.drill.exec.expr.fn.impl.StreamingMedianHelpers.${type.medianHelper} medianHelper = (org.apache.drill.exec.expr.fn.impl.StreamingMedianHelpers.${type.medianHelper}) utils.obj;
      median.value = medianHelper.getMedian();
      median.isSet = 1;
    }

    @Override
    public void reset() {
      org.apache.drill.exec.expr.fn.impl.StreamingMedianHelpers.StreamingMedianHelper medianHelper = (org.apache.drill.exec.expr.fn.impl.StreamingMedianHelpers.${type.medianHelper}) utils.obj;
      medianHelper.reset();
    }
  }


</#list>
}
</#list>
