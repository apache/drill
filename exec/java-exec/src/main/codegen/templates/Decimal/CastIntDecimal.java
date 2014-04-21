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
<#if type.major == ("IntDecimal") || type.major == ("BigIntDecimal")>
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
@FunctionTemplate(name = "cast${type.to?upper_case}", scope = FunctionTemplate.FunctionScope.DECIMAL_CAST, nulls=NullHandling.NULL_IF_NULL)
public class Cast${type.from}${type.to} implements DrillSimpleFunc {

    @Param ${type.from}Holder in;
    <#if type.to.startsWith("Decimal28") || type.to.startsWith("Decimal38")>
    @Workspace ByteBuf buffer;
    </#if>
    @Param BigIntHolder precision;
    @Param BigIntHolder scale;
    @Output ${type.to}Holder out;

    public void setup(RecordBatch incoming) {
        <#if type.to.startsWith("Decimal28") || type.to.startsWith("Decimal38")>
        int size = ${type.arraySize} * (org.apache.drill.common.util.DecimalUtility.integerSize);
        buffer = io.netty.buffer.Unpooled.wrappedBuffer(new byte[size]);
        buffer = new io.netty.buffer.SwappedByteBuf(buffer);
        </#if>

    }

    public void eval() {

        out.scale = (int) scale.value;
        out.precision = (int) precision.value;

        <#if type.to == "Decimal9" || type.to == "Decimal18">
        out.value = (${type.javatype}) in.value;

        // converting from integer to decimal, pad zeroes if scale is non zero
        out.value = (${type.javatype}) (out.value * Math.pow(10, scale.value));

        <#else>
        out.start = 0;
        out.buffer = buffer;

        // Initialize the buffer
        for (int i = 0; i < ${type.arraySize}; i++) {
            out.setInteger(i, 0);
        }

        // check if input is a negative number and store the sign
        if (in.value < 0) {
            out.sign = true;
            /* we are going to split the input into base 1 billion numbers
             * by dividing, if we leave the input to be negative all the splits
             * will be negative. We store the sign in the output at the end in the
             * most significant bit
             */
            in.value = in.value * -1;
        }

        // Figure out how many array positions to be left for the scale part
        int scaleSize = org.apache.drill.common.util.DecimalUtility.roundUp((int) scale.value);
        int integerIndex = (${type.arraySize} - scaleSize - 1);

        while (in.value > 0 && integerIndex >= 0) {
            out.setInteger(integerIndex, (int) (in.value % org.apache.drill.common.util.DecimalUtility.DIGITS_BASE));
            in.value = in.value / org.apache.drill.common.util.DecimalUtility.DIGITS_BASE;
        }

        </#if>
    }
}
</#if> <#-- type.major -->
</#list>