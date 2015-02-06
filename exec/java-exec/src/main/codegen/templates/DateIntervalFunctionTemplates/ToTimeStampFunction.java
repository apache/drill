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

<#list numericTypes.numeric as numerics>

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/G${numerics}ToTimeStamp.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;

// This class is generated using freemarker template ToTimeStampFunction.java

@FunctionTemplate(name = "to_timestamp" , scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
public class G${numerics}ToTimeStamp implements DrillSimpleFunc {


    @Param  ${numerics}Holder left;
    <#if numerics.startsWith("Decimal")>
    @Workspace java.math.BigInteger millisConstant;
    </#if>
    @Output TimeStampHolder out;

    public void setup() {
      <#if numerics.startsWith("Decimal")>
      millisConstant = java.math.BigInteger.valueOf(1000);
      </#if>
    }

    public void eval() {
        long inputMillis = 0;

        <#if (numerics.startsWith("Decimal"))>
        <#if (numerics == "Decimal9") || (numerics == "Decimal18")>
        java.math.BigInteger value = java.math.BigInteger.valueOf(left.value);
        value = value.multiply(millisConstant);
        inputMillis = (new java.math.BigDecimal(value, left.scale)).longValue();
        <#elseif (numerics == "Decimal28Sparse") || (numerics == "Decimal38Sparse")>
        java.math.BigDecimal input = org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromSparse(left.buffer, left.start, left.nDecimalDigits, left.scale);
        inputMillis = input.multiply(new java.math.BigDecimal(1000)).longValue();
        </#if>
        <#else>
        inputMillis = (long) (left.value * 1000l);
        </#if>
        out.value = new org.joda.time.DateTime(inputMillis).withZoneRetainFields(org.joda.time.DateTimeZone.UTC).getMillis();
    }
}
</#list>