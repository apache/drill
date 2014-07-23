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

import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
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
  @Workspace ObjectHolder value;
  @Workspace IntHolder outputScale;
  <#else>
  @Workspace ${type.runningType}Holder value;
  @Workspace ByteBuf buffer;
  </#if>
  @Output ${type.outputType}Holder out;

  public void setup(RecordBatch b) {
	<#if aggrtype.funcName == "count">
  	value = new ${type.runningType}Holder();
    value.value = 0;
	<#elseif aggrtype.funcName == "max" || aggrtype.funcName == "min">
  	value = new ${type.runningType}Holder();
    <#if type.outputType.endsWith("Dense") || type.outputType.endsWith("Sparse")>
    buffer = io.netty.buffer.Unpooled.wrappedBuffer(new byte[value.WIDTH]);
    buffer = new io.netty.buffer.SwappedByteBuf(buffer);
    value.buffer = buffer;
    value.start  = 0;
    <#if aggrtype.funcName == "max">
    for (int i = 0; i < value.nDecimalDigits; i++) {
      value.setInteger(i, 0xFFFFFFFF);
    }
    value.setSign(true);
    <#elseif aggrtype.funcName == "min">
    for (int i = 0; i < value.nDecimalDigits; i++) {
      value.setInteger(i, 0x7FFFFFFF);
    }
    // Set sign to be positive so initial value is maximum
    value.setSign(false);
    value.precision = ${type.runningType}Holder.maxPrecision;
    </#if>
    <#elseif type.outputType == "Decimal9" || type.outputType == "Decimal18">
    value.value = ${type.initValue};
    </#if>
  <#elseif aggrtype.funcName == "sum">
    value.obj = java.math.BigDecimal.ZERO;
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
    <#if type.outputType.endsWith("Dense")>
    int cmp = org.apache.drill.common.util.DecimalUtility.compareDenseBytes(in.buffer, in.start, in.getSign(), value.buffer, value.start, value.getSign(), in.WIDTH);
    if (cmp == 1) {
      in.buffer.getBytes(in.start, value.buffer, 0, value.WIDTH);
      value.setSign(in.getSign());
      value.scale = in.scale;
      value.precision = in.precision;
    }
    <#elseif type.outputType.endsWith("Sparse")>
    int cmp = org.apache.drill.common.util.DecimalUtility.compareSparseBytes(in.buffer, in.start, in.getSign(),
      in.scale, in.precision, value.buffer,
      value.start, value.getSign(), value.precision,
      value.scale, in.WIDTH, in.nDecimalDigits, false);
    if (cmp == 1) {
      in.buffer.getBytes(in.start, value.buffer, 0, value.WIDTH);
      value.setSign(in.getSign());
      value.scale = in.scale;
      value.precision = in.precision;
    }
    <#elseif type.outputType == "Decimal9" || type.outputType == "Decimal18">
    value.value = Math.max(value.value, in.value);
    </#if>
    <#elseif aggrtype.funcName == "min">
    <#if type.outputType.endsWith("Dense")>
    int cmp = org.apache.drill.common.util.DecimalUtility.compareDenseBytes(in.buffer, in.start, in.getSign(), value.buffer, value.start, value.getSign(), in.WIDTH);
    if (cmp == -1) {
      in.buffer.getBytes(in.start, value.buffer, 0, value.WIDTH);
      value.setSign(in.getSign());
      value.scale = in.scale;
      value.precision = in.precision;
    }
    <#elseif type.outputType.endsWith("Sparse")>
    int cmp = org.apache.drill.common.util.DecimalUtility.compareSparseBytes(in.buffer, in.start, in.getSign(),
      in.scale, in.precision, value.buffer,
      value.start, value.getSign(), value.precision,
      value.scale, in.WIDTH, in.nDecimalDigits, false);
    if (cmp == -1) {
      in.buffer.getBytes(in.start, value.buffer, 0, value.WIDTH);
      value.setSign(in.getSign());
      value.scale = in.scale;
      value.precision = in.precision;
    }
    <#elseif type.outputType == "Decimal9" || type.outputType == "Decimal18">
    value.value = Math.min(value.value, in.value);
    </#if>
    <#elseif aggrtype.funcName == "sum">
   <#if type.inputType.endsWith("Decimal9") || type.inputType.endsWith("Decimal18")>
    java.math.BigDecimal currentValue = org.apache.drill.common.util.DecimalUtility.getBigDecimalFromPrimitiveTypes(in.value, in.scale, in.precision);
    <#else>
    java.math.BigDecimal currentValue = org.apache.drill.common.util.DecimalUtility.getBigDecimalFromSparse(in.buffer, in.start, in.nDecimalDigits, in.scale);
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
    io.netty.buffer.ByteBuf buffer = io.netty.buffer.Unpooled.wrappedBuffer(new byte[out.WIDTH]);
    buffer = new io.netty.buffer.SwappedByteBuf(buffer);
    out.buffer = buffer;
    out.start  = 0;
    out.scale = outputScale.value;
    out.precision = 38;
    value.obj = ((java.math.BigDecimal) (value.obj)).setScale(out.scale, java.math.BigDecimal.ROUND_HALF_UP);
    org.apache.drill.common.util.DecimalUtility.getSparseFromBigDecimal((java.math.BigDecimal) value.obj, out.buffer, out.start, out.scale, out.precision, out.nDecimalDigits);
   <#else>
    <#if type.outputType.endsWith("Dense") || type.outputType.endsWith("Sparse") || aggrtype.funcName == "sum">
    out.buffer = value.buffer;
    out.start = value.start;
    out.setSign(value.getSign());
    <#elseif type.outputType == "Decimal9" || type.outputType == "Decimal18">
    out.value = value.value;
    </#if>
    out.scale = value.scale;
    out.precision = value.precision;
    </#if>
  }

  @Override
  public void reset() {

	<#if aggrtype.funcName == "count">
	  value.value = 0;
	<#elseif aggrtype.funcName == "max" || aggrtype.funcName == "min">
    <#if type.outputType.endsWith("Dense") || type.outputType.endsWith("Sparse")>
    buffer = io.netty.buffer.Unpooled.wrappedBuffer(new byte[value.WIDTH]);
    buffer = new io.netty.buffer.SwappedByteBuf(buffer);
    value.buffer = buffer;
    value.start  = 0;
    for (int i = 0; i < value.nDecimalDigits; i++) {
      value.setInteger(i, 0xFFFFFFFF);
    }
    <#if aggrtype.funcName == "min">
    // Set sign to be positive so initial value is maximum
    value.setSign(false);
    <#elseif aggrtype.funcName == "max">
    value.setSign(true);
    </#if>
    <#elseif type.outputType == "Decimal9" || type.outputType == "Decimal18">
    value.value = ${type.initValue};
    </#if>
  <#elseif aggrtype.funcName == "sum">
    value.obj = java.math.BigDecimal.ZERO;
	</#if>

  }

 }


</#list>
}
</#list>

