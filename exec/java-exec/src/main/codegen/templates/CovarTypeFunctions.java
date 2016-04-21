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



<#list covarTypes.covarianceTypes as aggrtype>
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/gaggr/${aggrtype.className}Functions.java" />

<#include "/@includes/license.ftl" />

<#-- A utility class that is used to generate java code for covariance functions -->

/*
 * This class is automatically generated from CovarType.tdd using FreeMarker.
 */

package org.apache.drill.exec.expr.fn.impl.gaggr;

import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.IntHolder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.SmallIntHolder;
import org.apache.arrow.vector.holders.NullableSmallIntHolder;
import org.apache.arrow.vector.holders.TinyIntHolder;
import org.apache.arrow.vector.holders.NullableTinyIntHolder;
import org.apache.arrow.vector.holders.UInt1Holder;
import org.apache.arrow.vector.holders.NullableUInt1Holder;
import org.apache.arrow.vector.holders.UInt2Holder;
import org.apache.arrow.vector.holders.NullableUInt2Holder;
import org.apache.arrow.vector.holders.UInt4Holder;
import org.apache.arrow.vector.holders.NullableUInt4Holder;
import org.apache.arrow.vector.holders.UInt8Holder;
import org.apache.arrow.vector.holders.NullableUInt8Holder;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.Float8Holder;
import org.apache.arrow.vector.holders.Float4Holder;

@SuppressWarnings("unused")

public class ${aggrtype.className}Functions {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${aggrtype.className}Functions.class);

<#list aggrtype.types as type>

<#if aggrtype.aliasName == "">
@FunctionTemplate(name = "${aggrtype.funcName}", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
<#else>
@FunctionTemplate(names = {"${aggrtype.funcName}", "${aggrtype.aliasName}"}, scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
</#if>

public static class ${type.inputType}${aggrtype.className} implements DrillAggFunc{

  @Param ${type.inputType}Holder xIn;
  @Param ${type.inputType}Holder yIn;

  @Workspace ${type.movingAverageType}Holder xMean;
  @Workspace ${type.movingAverageType}Holder yMean;
  @Workspace ${type.movingAverageType}Holder xyMean;

  @Workspace ${type.movingDeviationType}Holder covar;

  @Workspace ${type.countRunningType}Holder count;
  @Output ${type.outputType}Holder out;

  public void setup() {
    xMean = new ${type.movingAverageType}Holder();
    yMean = new ${type.movingAverageType}Holder();
    xyMean = new ${type.movingDeviationType}Holder();
    count = new ${type.countRunningType}Holder();
    covar = new ${type.movingDeviationType}Holder();

    // Initialize the workspace variables
    xMean.value = 0;
    yMean.value = 0;
    xyMean.value = 0;
    count.value = 1;
    covar.value = 0;
  }

  @Override
  public void add() {
	<#if type.inputType?starts_with("Nullable")>
	  sout: {
	  if (xIn.isSet == 0 || yIn.isSet == 0) {
	   // processing nullable input and the value is null, so don't do anything...
	   break sout;
	  }
	</#if>

    // compute covariance
    xMean.value += ((xIn.value - xMean.value) / count.value);
    yMean.value += ((yIn.value - yMean.value) / count.value);

    xyMean.value += ((xIn.value * yIn.value) - xyMean.value) / count.value;
    count.value++;
    <#if type.inputType?starts_with("Nullable")>
    } // end of sout block
    </#if>
  }

  @Override
  public void output() {
	  <#if aggrtype.funcName == "covar_pop">
      out.value = (xyMean.value - (xMean.value * yMean.value));
      <#elseif aggrtype.funcName == "covar_samp">
      out.value = (xyMean.value - (xMean.value * yMean.value))*(count.value - 1)/(count.value - 2);
      </#if>
  }

  @Override
  public void reset() {
    xMean.value = 0;
    yMean.value = 0;
    xyMean.value = 0;
    count.value = 1;
    covar.value = 0;
  }
}


</#list>
}
</#list>