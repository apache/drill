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


<#list cast.types as type>

<#-- Cast function template for conversion from Float to Decimal9, Decimal18, Decimal28, Decimal38 -->
<#if type.major == "FloatDecimalComplex" || type.major == "DoubleDecimalComplex" || type.major == "FloatDecimalSimple" || type.major == "DoubleDecimalSimple">

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/gcast/Cast${type.from}${type.to}.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl.gcast;

<#include "/@includes/vv_imports.ftl" />

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.util.DecimalUtility;
import org.apache.drill.exec.expr.annotations.Workspace;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;

import java.nio.ByteBuffer;

@SuppressWarnings("unused")
@FunctionTemplate(name = "cast${type.to?upper_case}", scope = FunctionTemplate.FunctionScope.DECIMAL_CAST, nulls=NullHandling.NULL_IF_NULL)
public class Cast${type.from}${type.to} implements DrillSimpleFunc {

@Param ${type.from}Holder in;
<#if type.major == "FloatDecimalComplex" || type.major == "DoubleDecimalComplex">
@Inject DrillBuf buffer;
</#if>
@Param BigIntHolder precision;
@Param BigIntHolder scale;
@Output ${type.to}Holder out;

    public void setup() {
        <#if type.major == "FloatDecimalComplex" || type.major == "DoubleDecimalComplex">
        int size = ${type.arraySize} * (org.apache.drill.exec.util.DecimalUtility.INTEGER_SIZE);
        buffer = buffer.reallocIfNeeded(size);
        </#if>
    }

    public void eval() {

        out.scale = (int) scale.value;
        out.precision = (int) precision.value;

        <#if type.major == "FloatDecimalComplex" || type.major == "DoubleDecimalComplex">
        out.start = 0;
        out.buffer = buffer;

       // Assign the integer part of the decimal to the output holder
        org.apache.drill.exec.util.DecimalUtility.getSparseFromBigDecimal(new java.math.BigDecimal(String.valueOf(in.value)), out.buffer, out.start, out.scale, out.precision, out.nDecimalDigits);

        <#elseif type.to.endsWith("Decimal9")>
        out.value = org.apache.drill.exec.util.DecimalUtility.getDecimal9FromBigDecimal(new java.math.BigDecimal(String.valueOf(in.value)), out.scale, out.precision);
        <#elseif type.to.endsWith("Decimal18")>
        out.value = org.apache.drill.exec.util.DecimalUtility.getDecimal18FromBigDecimal(new java.math.BigDecimal(String.valueOf(in.value)), out.scale, out.precision);
        </#if>
    }
}
</#if>
</#list>
