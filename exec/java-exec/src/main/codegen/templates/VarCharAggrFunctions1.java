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
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/gaggr/${aggrtype.className}VarBytesFunctions.java" />

<#include "/@includes/license.ftl" />

<#-- A utility class that is used to generate java code for aggr functions that maintain a single -->
<#-- running counter to hold the result.  This includes: MIN, MAX, COUNT. -->

/*
 * This class is automatically generated from VarCharAggrFunctions1.java using FreeMarker.
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

public class ${aggrtype.className}VarBytesFunctions {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${aggrtype.className}Functions.class);

<#list aggrtype.types as type>
<#if type.major == "VarBytes">

@FunctionTemplate(name = "${aggrtype.funcName}", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
public static class ${type.inputType}${aggrtype.className} implements DrillAggFunc{

  @Param ${type.inputType}Holder in;
  <#if aggrtype.funcName == "max" || aggrtype.funcName == "min">
  @Workspace ObjectHolder value;
  <#else>
  @Workspace  ${type.runningType}Holder value;
  </#if>
  @Output ${type.outputType}Holder out;

  public void setup(RecordBatch b) {
    <#if aggrtype.funcName == "max" || aggrtype.funcName == "min">
    value = new ObjectHolder();
    ${type.runningType}Holder tmp = new ${type.runningType}Holder();
    tmp.start = 0;
    tmp.end = 0;
    tmp.buffer = null;
    value.obj = tmp;

    <#else>
    value = new ${type.runningType}Holder();
    value.value = 0;
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
    <#if aggrtype.funcName == "max" || aggrtype.funcName == "min">
    ${type.runningType}Holder tmp = (${type.runningType}Holder)value.obj;
    int cmp = 0;
    boolean swap = false;

    // if buffer is null then swap
    if (tmp.buffer == null) {
      swap = true;
    } else {
      // Compare the bytes
      for (int l = in.start, r = tmp.start; l < in.end && r < tmp.end; l++, r++) {
        byte leftByte = in.buffer.getByte(l);
        byte rightByte = tmp.buffer.getByte(r);
        if (leftByte != rightByte) {
          cmp = ((leftByte & 0xFF) - (rightByte & 0xFF)) > 0 ? 1 : -1;
          break;
        }
      }

      if (cmp == 0) {
        int l = (in.end - in.start) - (tmp.end - tmp.start);
        if (l > 0) {
          cmp = 1;
        } else {
          cmp = -1;
        }
      }

      <#if aggrtype.className == "Min">
      swap = (cmp == -1);
      <#elseif aggrtype.className == "Max">
      swap = (cmp == 1);
      </#if>
    }
    if (swap) {
      int length = in.end - in.start;
      if (length > (tmp.end - tmp.start)) {
        // if workspace buffer is smaller, release and allocate a new one
        if (tmp.buffer != null) {
          tmp.buffer.release();
        }
        tmp.buffer = io.netty.buffer.Unpooled.wrappedBuffer(new byte [length]);
      }
      in.buffer.getBytes(in.start, tmp.buffer, 0, length);
      tmp.end = length;
    }
    <#else>
    value.value++;
    </#if>
    <#if type.inputType?starts_with("Nullable")>
    } // end of sout block
	  </#if>
  }

  @Override
  public void output() {
    <#if aggrtype.funcName == "max" || aggrtype.funcName == "min">
    ${type.runningType}Holder tmp = (${type.runningType}Holder)value.obj;
    out.start  = tmp.start;
    out.end    = tmp.end;
    out.buffer = tmp.buffer;
    <#else>
    out.value = value.value;
    </#if>
  }

  @Override
  public void reset() {
    <#if aggrtype.funcName == "max" || aggrtype.funcName == "min">
    value = new ObjectHolder();
    ${type.runningType}Holder tmp = new ${type.runningType}Holder();
    tmp.start = 0;
    tmp.end = 0;
    if (tmp.buffer != null) {
      tmp.buffer.release();
      tmp.buffer = null;
    }
    value.obj = tmp;
    <#else>
    value = new ${type.runningType}Holder();
    value.value = 0;
    </#if>
  }
}
</#if>
</#list>
}
</#list>
