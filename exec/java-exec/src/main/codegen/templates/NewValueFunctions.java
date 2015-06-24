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


<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/GNewValueFunctions.java" />
<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.*;
import javax.inject.Inject;
import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.record.RecordBatch;

public class GNewValueFunctions {
<#list vv.types as type>
<#if type.major == "Fixed" || type.major = "Bit">

<#list type.minor as minor>
<#list vv.modes as mode>
  <#if mode.name != "Repeated">

<#if !minor.class.startsWith("Decimal28") && !minor.class.startsWith("Decimal38") && !minor.class.startsWith("Interval")>
@SuppressWarnings("unused")
@FunctionTemplate(name = "newPartitionValue", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.INTERNAL)
public static class NewValue${minor.class}${mode.prefix} implements DrillSimpleFunc{

  @Param ${mode.prefix}${minor.class}Holder in;
  @Workspace ${mode.prefix}${minor.class}Holder previous;
  @Workspace Boolean initialized;
  @Output BitHolder out;

  public void setup() {
    initialized = false;
  }

  <#if mode.name == "Required">
  public void eval() {
    if (initialized) {
      if (in.value == previous.value) {
        out.value = 0;
      } else {
        previous.value = in.value;
        out.value = 1;
      }
    } else {
      previous.value = in.value;
      out.value = 1;
      initialized = true;
    }
  }
  </#if>
  <#if mode.name == "Optional">
  public void eval() {
    if (initialized) {
      if (in.isSet == 0 && previous.isSet == 0) {
        out.value = 0;
      } else if (in.value == previous.value) {
        out.value = 0;
      } else {
        previous.value = in.value;
        previous.isSet = in.isSet;
        out.value = 1;
      }
    } else {
      previous.value = in.value;
      previous.isSet = in.isSet;
      out.value = 1;
      initialized = true;
    }
  }
  </#if>
}
</#if> <#-- minor.class.startWith -->

</#if> <#-- mode.name -->
</#list>
</#list>
</#if> <#-- type.major -->
</#list>
}
