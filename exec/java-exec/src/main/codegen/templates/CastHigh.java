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

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/gcast/CastHighFunctions.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl.gcast;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.*;
import javax.inject.Inject;
import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.record.RecordBatch;

public class CastHighFunctions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CastHighFunctions.class);

  <#list casthigh.types as type>

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "casthigh", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastHigh${type.from} implements DrillSimpleFunc {

    @Param ${type.from}Holder in;
    <#if type.from.contains("Decimal")>
      @Output ${type.from}Holder out;
    <#else>
      @Output ${type.to}Holder out;
    </#if>

    public void setup() {}

    public void eval() {
      <#if type.value >
        out.value = (double) in.value;
      <#else>
        out = in;
      </#if>
    }
  }
</#list>
}

