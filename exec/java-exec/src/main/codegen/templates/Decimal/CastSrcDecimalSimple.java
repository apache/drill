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

<#list cast.types as type>

<#-- Template code for converting from Decimal9, Decimal18 to Decimal28Dense and Decimal38Dense -->
<#if type.major == "DecimalSimpleDecimalDense">

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
public class Cast${type.from}${type.to} implements DrillSimpleFunc {

    @Param ${type.from}Holder in;
    <#if type.to.startsWith("Decimal28") || type.to.startsWith("Decimal38")>
    @Inject DrillBuf buffer;
    </#if>
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

        // Re initialize the buffer everytime
        for (int i = 0; i < ${type.arraySize}; i++) {
            out.setInteger(i, 0, out.start, out.buffer);
        }

        out.scale = (int) scale.value;
        out.precision = (int) precision.value;

        out.buffer = buffer;
        out.start = 0;
        out.setSign((in.value < 0), out.start, out.buffer);

        /* Since we will be dividing the decimal value with base 1 billion
         * we don't want negative results if the decimal is negative.
         */
        long value = (in.value < 0) ? (in.value * -1) : in.value;

        int index = out.nDecimalDigits - 1;

        // store the decimal value as sequence of integers of base 1 billion.
        while (value > 0) {

            out.setInteger(index, (int) (value % org.apache.drill.exec.util.DecimalUtility.DIGITS_BASE), out.start, out.buffer);
            value = value/org.apache.drill.exec.util.DecimalUtility.DIGITS_BASE;
            index--;
        }

        /* We have stored the decimal value in the intermediate format, which is basically that the
         * scale and integer part of the decimal together, with no additional zeroes padded to the
         * scale. Now we simply need to shift the bits around to get a more compact representation
         */
        int[] mask = {0x03, 0x0F, 0x3F, 0xFF};
        int maskIndex = 0;
        int shiftOrder = 2;

        // Start shifting bits just after the first integer
        int byteIndex = in.WIDTH - (org.apache.drill.exec.util.DecimalUtility.INTEGER_SIZE + 1);

        while (byteIndex >= 0) {

            /* get the last bits that need to shifted to the next byte */
            byte shiftBits = (byte) ((out.buffer.getByte(byteIndex) & mask[maskIndex]) << (8 - shiftOrder));

            int shiftOrder1 = ((byteIndex % 4) == 0) ? shiftOrder - 2 : shiftOrder;

            /* transfer the bits from the left to the right */
            out.buffer.setByte(byteIndex + 1,  (byte) (((out.buffer.getByte(byteIndex + 1) & 0xFF) >>> (shiftOrder1)) | shiftBits));

            byteIndex--;

            if (byteIndex % 4 == 0) {
                /* We are on a border */
                shiftOrder += 2;
                maskIndex++;
            }
        }
    }
}

<#-- Template code for converting from Decimal9, Decimal18 to Decimal28Sparse and Decimal38Sparse -->
<#elseif type.major == "DecimalSimpleDecimalSparse">
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

        // Re initialize the buffer everytime
        for (int i = 0; i < ${type.arraySize}; i++) {
            out.setInteger(i, 0, out.start, out.buffer);
        }
        out.scale = (int) scale.value;
        out.precision = (int) precision.value;

        out.buffer = buffer;
        out.start = 0;

        /* Since we will be dividing the decimal value with base 1 billion
         * we don't want negative results if the decimal is negative.
         */
        long value = (in.value < 0) ? (in.value * -1) : in.value;

        int index = out.nDecimalDigits - 1;

        // Separate out the scale part and store it
        int remainingScale = in.scale;

        while(remainingScale > 0) {

            int power = (remainingScale % org.apache.drill.exec.util.DecimalUtility.MAX_DIGITS);
            int padding = 1;

            if (power == 0) {
                power = 9;
            } else {
                padding = (int) (org.apache.drill.exec.util.DecimalUtility.getPowerOfTen((int) (org.apache.drill.exec.util.DecimalUtility.MAX_DIGITS - power)));
            }

            int mask = (int) org.apache.drill.exec.util.DecimalUtility.getPowerOfTen(power);

            out.setInteger(index, (int) ((value % mask) * padding), out.start, out.buffer);

            value = value/mask;

            remainingScale -= power;

            index--;
        }

        while (value > 0) {
            out.setInteger(index, (int) (value % org.apache.drill.exec.util.DecimalUtility.DIGITS_BASE), out.start, out.buffer);
            value = value/org.apache.drill.exec.util.DecimalUtility.DIGITS_BASE;
            index--;
        }

        // Round up or down the scale
        if (in.scale != out.scale) {
          org.apache.drill.exec.util.DecimalUtility.roundDecimal(out.buffer, out.start, out.nDecimalDigits, out.scale, in.scale);
        }
        // Set the sign
        out.setSign((in.value < 0), out.start, out.buffer);
    }
}

<#-- Template code for converting from Decimal9 to Decimal18 -->
<#elseif type.major == "DecimalSimpleDecimalSimple">

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

import java.nio.ByteBuffer;

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */

@SuppressWarnings("unused")
@FunctionTemplate(name = "cast${type.to?upper_case}",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    returnType = FunctionTemplate.ReturnType.DECIMAL_CAST,
    nulls = NullHandling.NULL_IF_NULL)
public class Cast${type.from}${type.to} implements DrillSimpleFunc {

    @Param ${type.from}Holder in;
    @Param BigIntHolder precision;
    @Param BigIntHolder scale;
    @Output ${type.to}Holder out;

    public void setup() {
    }

    public void eval() {
        out.scale = (int) scale.value;
        out.precision = (int) precision.value;
        out.value = in.value;

        // Truncate or pad additional zeroes if the output scale is different from input scale
        out.value = (${type.javatype}) (org.apache.drill.exec.util.DecimalUtility.adjustScaleMultiply(out.value, (int) (out.scale - in.scale)));
    }
}
</#if>
</#list>