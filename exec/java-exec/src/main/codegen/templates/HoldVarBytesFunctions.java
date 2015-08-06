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
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/holdlast/${holdtype.className}VarBytesFunctions.java" />

<#include "/@includes/license.ftl" />

// Source code generated using FreeMarker template ${.template_name}

<#-- A utility class that is used to generate java code for hold last functions that maintain a single value. -->

package org.apache.drill.exec.expr.fn.impl.holdlast;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.*;

import javax.inject.Inject;

@SuppressWarnings("unused")

public class ${holdtype.className}VarBytesFunctions {

<#list holdtype.types as type>
<#if type.major == "VarBytes">

@FunctionTemplate(name = "${holdtype.funcName}", scope = FunctionScope.POINT_AGGREGATE)
public static class ${type.inputType}${holdtype.className} implements DrillAggFunc {

  @Param ${type.inputType}Holder in;
  @Workspace ObjectHolder value;
  @Output ${type.inputType}Holder out;
  @Inject DrillBuf buf;
<#if type.inputType?starts_with("Nullable")>
  @Workspace BitHolder isNull;
</#if>

  public void setup() {
    value = new ObjectHolder();
    org.apache.drill.exec.expr.fn.impl.DrillByteArray tmp = new org.apache.drill.exec.expr.fn.impl.DrillByteArray();
    value.obj = tmp;

<#if type.inputType?starts_with("Nullable")>
    isNull.value = 0;
</#if>
  }
  
  @Override
  public void add() {
<#if type.inputType?starts_with("Nullable")>
    if (in.isSet == 0) {
      isNull.value = 1;
      return;
    }

</#if>
    int inputLength = in.end - in.start;
    org.apache.drill.exec.expr.fn.impl.DrillByteArray tmp = (org.apache.drill.exec.expr.fn.impl.DrillByteArray) value.obj;
    if (tmp.getLength() >= inputLength) {
      in.buffer.getBytes(in.start, tmp.getBytes(), 0, inputLength);
      tmp.setLength(inputLength);
    } else {
      byte[] tempArray = new byte[in.end - in.start];
      in.buffer.getBytes(in.start, tempArray, 0, in.end - in.start);
      tmp.setBytes(tempArray);
    }
  }

  @Override
  public void output() {
<#if type.inputType?starts_with("Nullable")>
    if (isNull.value == 1) {
      out.isSet = 0;
      return;
    }

    out.isSet = 1;
</#if>
    org.apache.drill.exec.expr.fn.impl.DrillByteArray tmp = (org.apache.drill.exec.expr.fn.impl.DrillByteArray) value.obj;
    buf = buf.reallocIfNeeded(tmp.getLength());
    buf.setBytes(0, tmp.getBytes(), 0, tmp.getLength());
    out.start  = 0;
    out.end    = tmp.getLength();
    out.buffer = buf;
  }

  @Override
  public void reset() {
    value = new ObjectHolder();
<#if type.inputType?starts_with("Nullable")>
    isNull.value = 0;
</#if>
  }

 }

</#if>
</#list>
}
</#list>

