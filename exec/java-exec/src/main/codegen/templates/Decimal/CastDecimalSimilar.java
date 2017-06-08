/*
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

<#-- Template for converting between similar types of decimal. Decimal28Dense -> Decimal38Dense & Decimal28Sparse -> Decimal38Sparse -->

<#list cast.types as type>
<#if type.major == "DecimalSimilar">

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
import org.apache.drill.exec.expr.annotations.Workspace;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;

import java.nio.ByteBuffer;

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */

@SuppressWarnings("unused")
@FunctionTemplate(name = "cast${type.to?upper_case}",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    returnType = FunctionTemplate.ReturnType.DECIMAL_CAST,
    nulls = NullHandling.NULL_IF_NULL)
public class Cast${type.from}${type.to} implements DrillSimpleFunc{

    @Param ${type.from}Holder in;
    @Inject DrillBuf buffer;
    @Param BigIntHolder precision;
    @Param BigIntHolder scale;
    @Output ${type.to}Holder out;

    public void setup() {
        int size = (${type.arraySize} * (org.apache.drill.exec.util.DecimalUtility.INTEGER_SIZE));
        buffer = buffer.reallocIfNeeded(size);
    }

    public void eval() {

        out.buffer = buffer;
        out.start = 0;
        out.scale = (int) scale.value;
        out.precision = (int) precision.value;
        boolean sign = (in.getSign(in.start, in.buffer));

        // Re initialize the buffer everytime
        for (int i = 0; i < ${type.arraySize}; i++) {
            out.setInteger(i, 0, out.start, out.buffer);
        }

        int inputIdx = in.nDecimalDigits - 1;
        int outputIdx = out.nDecimalDigits - 1;

        for (; inputIdx >= 0; inputIdx--, outputIdx--) {
            out.setInteger(outputIdx, in.getInteger(inputIdx, in.start, in.buffer), out.start, out.buffer);
        }

        // round up or down the scale
        if (in.scale != out.scale) {
          org.apache.drill.exec.util.DecimalUtility.roundDecimal(out.buffer, out.start, out.nDecimalDigits, out.scale, in.scale);
        }
        out.setSign(sign, out.start, out.buffer);
    }
}
</#if> <#-- type.major -->
</#list>
