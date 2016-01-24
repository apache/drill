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



<#list decimalaggrtypes1.aggrtypes as aggrtype>
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/gaggr/Decimal${aggrtype.className}Functions.java" />

<#include "/@includes/license.ftl" />

<#-- A utility class that is used to generate java code for aggr functions for decimal data type that maintain a single -->
<#-- running counter to hold the result.  This includes: MIN, MAX, COUNT. -->

/*
 * This class is automatically generated from AggrTypeFunctions1.tdd using FreeMarker.
 */

package org.apache.drill.exec.expr.fn.impl.gaggr;

<#include "/@includes/vv_imports.ftl" />

import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import javax.inject.Inject;
import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;
import io.netty.buffer.ByteBuf;

@SuppressWarnings("unused")

public class Decimal${aggrtype.className}Functions {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${aggrtype.className}Functions.class);

<#list aggrtype.types as type>

@FunctionTemplate(name = "${aggrtype.funcName}", <#if aggrtype.funcName == "sum"> scope = FunctionTemplate.FunctionScope.DECIMAL_SUM_AGGREGATE <#else>scope = FunctionTemplate.FunctionScope.DECIMAL_AGGREGATE</#if>)
public static class ${type.inputType}${aggrtype.className} implements DrillAggFunc{

  @Param ${type.inputType}Holder in;
  <#if aggrtype.funcName == "sum">
  @Inject DrillBuf buffer;
  @Workspace ObjectHolder value;
  @Workspace IntHolder outputScale;
  <#elseif type.outputType.endsWith("Sparse")>
  @Inject DrillBuf buffer;
  @Workspace IntHolder scale;
  @Workspace IntHolder precision;
  @Workspace ObjectHolder value;
  <#else>
  @Workspace ${type.runningType}Holder value;
  </#if>
  @Output ${type.outputType}Holder out;

  public void setup() {
	<#if aggrtype.funcName == "count">
  	value = new ${type.runningType}Holder();
    value.value = 0;
	<#elseif aggrtype.funcName == "max" || aggrtype.funcName == "min">
    <#if type.outputType.endsWith("Sparse")>
    scale.value = 0;
    precision.value = 0;
    value = new ObjectHolder();
    //${type.runningType}Holder tmp = new ${type.runningType}Holder();
    byte[] byteArray = new byte[${type.runningType}Holder.WIDTH];
    org.apache.drill.exec.util.Text tmp = new org.apache.drill.exec.util.Text(byteArray);
    value.obj = tmp;
    <#if aggrtype.funcName == "max">
    for (int i = 0; i < ${type.runningType}Holder.nDecimalDigits; i++) {
      org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers.setInteger(tmp.getBytes(), i, 0xFFFFFFFF);
    }
    org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers.setSign(tmp.getBytes(), true);
    <#elseif aggrtype.funcName == "min">
    for (int i = 0; i < ${type.runningType}Holder.nDecimalDigits; i++) {
      org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers.setInteger(tmp.getBytes(), i, 0x7FFFFFFF);
    }
    // Set sign to be positive so initial value is maximum
    org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers.setSign(tmp.getBytes(), false);
    </#if>
    <#elseif type.outputType == "Decimal9" || type.outputType == "Decimal18">
    value = new ${type.runningType}Holder();
    value.value = ${type.initValue};
    </#if>
  <#elseif aggrtype.funcName == "sum">
    buffer = buffer.reallocIfNeeded(${type.outputType}Holder.WIDTH);
    value = new ObjectHolder();
    value.obj = java.math.BigDecimal.ZERO;
    outputScale = new IntHolder();
    outputScale.value = Integer.MIN_VALUE;
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
    <#if aggrtype.funcName == "count">
    value.value++;
    <#elseif aggrtype.funcName == "max">
    <#if type.outputType.endsWith("Sparse")>
      //${type.runningType}Holder tmp = (${type.runningType}Holder) value.obj;
      org.apache.drill.exec.util.Text tmp = (org.apache.drill.exec.util.Text) value.obj;
      int cmp = org.apache.drill.exec.util.DecimalUtility.compareSparseSamePrecScale(in.buffer, in.start, tmp.getBytes(), tmp.getLength());
    if (cmp == 1) {
      //in.buffer.getBytes(in.start, tmp.getBytes(), 0, ${type.runningType}Holder.WIDTH);
      for (int i = 0; i < ${type.runningType}Holder.nDecimalDigits; i++) {
        org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers.setInteger(tmp.getBytes(), i, ${type.runningType}Holder.getInteger(i, in.start, in.buffer));
      }
      org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers.setSign(tmp.getBytes(), in.getSign(in.start, in.buffer));
      scale.value = in.scale;
      precision.value = in.precision;
    }
    <#elseif type.outputType == "Decimal9" || type.outputType == "Decimal18">
    value.value = Math.max(value.value, in.value);
    </#if>
    <#elseif aggrtype.funcName == "min">
    <#if type.outputType.endsWith("Sparse")>
    //${type.runningType}Holder tmp = (${type.runningType}Holder) value.obj;
    org.apache.drill.exec.util.Text tmp = (org.apache.drill.exec.util.Text) value.obj;
    int cmp = org.apache.drill.exec.util.DecimalUtility.compareSparseSamePrecScale(in.buffer, in.start, tmp.getBytes(), tmp.getLength());
    if (cmp == -1) {
      //in.buffer.getBytes(in.start, tmp.getBytes(), 0, ${type.runningType}Holder.WIDTH);
      for (int i = 0; i < ${type.runningType}Holder.nDecimalDigits; i++) {
        org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers.setInteger(tmp.getBytes(), i, ${type.runningType}Holder.getInteger(i, in.start, in.buffer));
      }
      org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers.setSign(tmp.getBytes(), in.getSign(in.start, in.buffer));
      scale.value = in.scale;
      precision.value = in.precision;
    }
    <#elseif type.outputType == "Decimal9" || type.outputType == "Decimal18">
    value.value = Math.min(value.value, in.value);
    </#if>
    <#elseif aggrtype.funcName == "sum">
   <#if type.inputType.endsWith("Decimal9") || type.inputType.endsWith("Decimal18")>
    java.math.BigDecimal currentValue = org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromPrimitiveTypes(in.value, in.scale, in.precision);
    <#else>
    java.math.BigDecimal currentValue = org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromSparse(in.buffer, in.start, in.nDecimalDigits, in.scale);
    </#if>
    value.obj = ((java.math.BigDecimal)(value.obj)).add(currentValue);
    if (outputScale.value == Integer.MIN_VALUE) {
      outputScale.value = in.scale;
    }
    </#if>
	<#if type.inputType?starts_with("Nullable")>
    } // end of sout block
	</#if>
  }

  @Override
  public void output() {
    <#if aggrtype.funcName == "count">
    out.value = value.value;
    <#elseif aggrtype.funcName == "sum">
    out.buffer = buffer;
    out.start  = 0;
    out.scale = outputScale.value;
    out.precision = 38;
    value.obj = ((java.math.BigDecimal) (value.obj)).setScale(out.scale, java.math.BigDecimal.ROUND_HALF_UP);
    org.apache.drill.exec.util.DecimalUtility.getSparseFromBigDecimal((java.math.BigDecimal) value.obj, out.buffer, out.start, out.scale, out.precision, out.nDecimalDigits);
   <#else>
    <#if type.outputType.endsWith("Sparse")>
    org.apache.drill.exec.util.Text tmp = (org.apache.drill.exec.util.Text) value.obj;
    buffer = buffer.reallocIfNeeded(tmp.getLength());
    //buffer.setBytes(0, tmp.getBytes(), 0, tmp.getLength());
    for (int i = 0; i < ${type.runningType}Holder.nDecimalDigits; i++) {
      ${type.runningType}Holder.setInteger(i, org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers.getInteger(tmp.getBytes(), i), 0, buffer);
    }
    out.buffer = buffer;
    out.start = 0;
    out.setSign(org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers.getSign(tmp.getBytes()), out.start, out.buffer);
    out.scale = scale.value;
    out.precision = precision.value;
    <#elseif type.outputType == "Decimal9" || type.outputType == "Decimal18">
    out.value = value.value;
    out.scale = value.scale;
    out.precision = value.precision;
    </#if>
    </#if>
  }

  @Override
  public void reset() {

	<#if aggrtype.funcName == "count">
	  value.value = 0;
	<#elseif aggrtype.funcName == "max" || aggrtype.funcName == "min">
    <#if type.outputType.endsWith("Sparse")>
    org.apache.drill.exec.util.Text tmp = (org.apache.drill.exec.util.Text) value.obj;
    for (int i = 0; i < ${type.runningType}Holder.nDecimalDigits; i++) {
      org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers.setInteger(tmp.getBytes(), i, 0xFFFFFFFF);
    }
    scale.value = 0;
    precision.value = 0;
    <#if aggrtype.funcName == "min">
    // Set sign to be positive so initial value is maximum
    org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers.setSign(tmp.getBytes(), false);
    <#elseif aggrtype.funcName == "max">
    org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers.setSign(tmp.getBytes(), true);
    </#if>
    <#elseif type.outputType == "Decimal9" || type.outputType == "Decimal18">
    value = new ${type.runningType}Holder();
    value.value = ${type.initValue};
    </#if>
  <#elseif aggrtype.funcName == "sum">
    value = new ObjectHolder();
    value.obj = java.math.BigDecimal.ZERO;
    outputScale = new IntHolder();
    outputScale.value = Integer.MIN_VALUE;
	</#if>

  }

 }


</#list>
}
</#list>

