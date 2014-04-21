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
<#if type.major == "DecimalSimpleFloat" || type.major == "DecimalSimpleDouble"> <#-- Cast function template for conversion from Decimal9, Decimal18 to Float4 and Float8-->

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/gcast/Cast${type.from}${type.to}.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl.gcast;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.common.util.DecimalUtility;
import org.apache.drill.exec.expr.annotations.Workspace;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;

@SuppressWarnings("unused")
@FunctionTemplate(name = "cast${type.to?upper_case}", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
public class Cast${type.from}${type.to} implements DrillSimpleFunc {

@Param ${type.from}Holder in;
@Output ${type.to}Holder out;

    public void setup(RecordBatch incoming) {
    }

    public void eval() {

        // Divide the decimal with the scale to get the floating point value
        out.value = (${type.javatype}) (in.value / Math.pow(10, in.scale));
    }
}
<#elseif type.major == "DecimalComplexFloat" || type.major == "DecimalComplexDouble"> <#-- Cast function template for conversion from Decimal9, Decimal18 to Float4 -->

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/gcast/Cast${type.from}${type.to}.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl.gcast;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.common.util.DecimalUtility;
import org.apache.drill.exec.expr.annotations.Workspace;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;

@SuppressWarnings("unused")
@FunctionTemplate(name = "cast${type.to?upper_case}", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
public class Cast${type.from}${type.to} implements DrillSimpleFunc {

@Param ${type.from}Holder in;
@Output ${type.to}Holder out;

    public void setup(RecordBatch incoming) {
    }

    public void eval() {

        java.math.BigDecimal bigDecimal = org.apache.drill.common.util.DecimalUtility.getBigDecimalFromByteBuf(in.buffer, in.start, in.nDecimalDigits, in.scale, true);
        out.value = bigDecimal.${type.javatype}Value();
    }
}
</#if>
</#list>
