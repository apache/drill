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



<#list logicalTypes.logicalAggrTypes as aggrtype>
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/gaggr/${aggrtype.className}Functions.java" />

<#include "/@includes/license.ftl" />

<#-- A utility class that is used to generate java code for aggr functions bit_and / bit_or -->

/*
 * This class is automatically generated from AggrBitwiseLogicalTypes.tdd using FreeMarker.
 */

package org.apache.drill.exec.expr.fn.impl.gaggr;

import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableIntHolder;
import org.apache.drill.exec.expr.holders.SmallIntHolder;
import org.apache.drill.exec.expr.holders.NullableSmallIntHolder;
import org.apache.drill.exec.expr.holders.TinyIntHolder;
import org.apache.drill.exec.expr.holders.NullableTinyIntHolder;
import org.apache.drill.exec.expr.holders.UInt1Holder;
import org.apache.drill.exec.expr.holders.NullableUInt1Holder;
import org.apache.drill.exec.expr.holders.UInt2Holder;
import org.apache.drill.exec.expr.holders.NullableUInt2Holder;
import org.apache.drill.exec.expr.holders.UInt4Holder;
import org.apache.drill.exec.expr.holders.NullableUInt4Holder;
import org.apache.drill.exec.expr.holders.UInt8Holder;
import org.apache.drill.exec.expr.holders.NullableUInt8Holder;
import org.apache.drill.exec.record.RecordBatch;

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

  @Param ${type.inputType}Holder in;
  @Workspace ${type.outputType}Holder inter;
  @Output ${type.outputType}Holder out;

  public void setup(RecordBatch b) {
  inter = new ${type.outputType}Holder();

    // Initialize the workspace variables
  <#if aggrtype.funcName == "bit_and">
    inter.value = ${type.maxval}.MAX_VALUE;
    <#elseif aggrtype.funcName == "bit_or">
    inter.value = 0;
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

  <#if aggrtype.funcName == "bit_and">
    inter.value = <#if type.extraCast ??>(${type.extraCast})</#if>(inter.value & in.value);
    <#elseif aggrtype.funcName == "bit_or">
    inter.value = <#if type.extraCast ??>(${type.extraCast})</#if>(inter.value | in.value);
  </#if>

    <#if type.inputType?starts_with("Nullable")>
    } // end of sout block
    </#if>
  }

  @Override
  public void output() {
    <#if aggrtype.funcName == "bit_and">
      out.value = inter.value;
      <#elseif aggrtype.funcName == "bit_or">
      out.value = inter.value;
    </#if>
  }

  @Override
  public void reset() {
    <#if aggrtype.funcName == "bit_and">
      inter.value = ${type.maxval}.MAX_VALUE;
      <#elseif aggrtype.funcName == "bit_or">
      inter.value = 0;
    </#if>
  }
}


</#list>
}
</#list>
