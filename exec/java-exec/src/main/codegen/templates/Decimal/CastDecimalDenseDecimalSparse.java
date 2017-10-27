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
<#if type.major == "DecimalDenseDecimalSparse">

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

        // Re initialize the buffer everytime
        for (int i = 0; i < ${type.arraySize}; i++) {
            out.setInteger(i, 0, out.start, out.buffer);
        }
        out.scale = (int) scale.value;
        out.precision = (int) precision.value;
        out.setSign(in.getSign(in.start, in.buffer), out.start, out.buffer);

        /* We store base 1 Billion integers in our representation, which requires
         * 30 bits, but a typical integer requires 32 bits. In our dense representation
         * we shift bits around to utilize the two available bits, to get back to our sparse
         * representation rearrange the bits so that we use 32 bits represent the digits.
         */

        byte[] intermediateBytes = new byte[(in.nDecimalDigits * org.apache.drill.exec.util.DecimalUtility.INTEGER_SIZE) + 1];

        int[] mask = {0x03, 0x0F, 0x3F, 0xFF};
        int[] reverseMask = {0xFC, 0xF0, 0xC0, 0x00};

        <#if (type.from == "Decimal38Dense")>
        int maskIndex = 0;
        int shiftOrder = 6;
        byte shiftBits = 0x00;
        intermediateBytes[0] = (byte) (in.buffer.getByte(0) & 0x7F);
        <#elseif (type.from == "Decimal28Dense")>
        int maskIndex = 1;
        int shiftOrder = 4;
        byte shiftBits = (byte) ((in.buffer.getByte(0) & 0x03) << shiftOrder);
        intermediateBytes[0] = (byte) (((in.buffer.getByte(0) & 0x3C) & 0xFF) >>> 2);
        </#if>

        int intermediateIndex = 1;
        int inputIndex = in.start + 1;

        while (intermediateIndex < in.WIDTH) {

            intermediateBytes[intermediateIndex] = (byte) ((shiftBits) | (((in.buffer.getByte(inputIndex) & reverseMask[maskIndex]) & 0xFF) >>> (8 - shiftOrder)));

            shiftBits = (byte) ((in.buffer.getByte(inputIndex) & mask[maskIndex]) << shiftOrder);

            inputIndex++;
            intermediateIndex++;

            if (((intermediateIndex - 1) % org.apache.drill.exec.util.DecimalUtility.INTEGER_SIZE) == 0) {
                shiftBits = (byte) ((shiftBits & 0xFF) >>> 2);
                maskIndex++;
                shiftOrder -= 2;
            }

        }

        /* copy the last byte */
        intermediateBytes[intermediateIndex] = shiftBits;

        /* We have shifted the bits around and now each digit is represented by 32 digits
         * Now we transfer the bytes into a integer array and separate out the scale and
         * integer part of the decimal. Also pad the scale part with zeroes if needed
         */
        int[] intermediate = new int[(intermediateBytes.length/org.apache.drill.exec.util.DecimalUtility.INTEGER_SIZE) + 1];

        java.nio.ByteBuffer wrapper = java.nio.ByteBuffer.wrap(intermediateBytes);
        intermediate[0] = wrapper.get(0);

        int intermediateIdx = 1;

        for (int i = 1; i < intermediate.length; i++) {
            intermediate[i] = wrapper.getInt(intermediateIdx);
            intermediateIdx += 4;
        }

        int actualDigits;
        int srcIndex = intermediate.length - 1;
        int dstIndex = out.nDecimalDigits - 1;

        // break the scale and integer part and pad zeroes
        if (in.scale > 0 && (actualDigits = (in.scale % org.apache.drill.exec.util.DecimalUtility.MAX_DIGITS)) > 0) {

            int paddedDigits = org.apache.drill.exec.util.DecimalUtility.MAX_DIGITS - actualDigits;
            int padding = (int) (Math.pow(10, paddedDigits));
            int transferDigitMask = (int) (Math.pow(10, actualDigits));

            /* copy the remaining scale over to the last deciml digit */
            out.setInteger(dstIndex, ((intermediate[srcIndex] % transferDigitMask) * (padding)), out.start, out.buffer);
            dstIndex--;

            while (srcIndex > 0) {
                out.setInteger(dstIndex, ((intermediate[srcIndex]/transferDigitMask) + ((intermediate[srcIndex - 1] % transferDigitMask) * padding)), out.start, out.buffer);

                dstIndex--;
                srcIndex--;
            }

            out.setInteger(dstIndex, (intermediate[0]/transferDigitMask), out.start, out.buffer);
        } else {
            for (; srcIndex >= 0; srcIndex--, dstIndex--)
                out.setInteger(dstIndex, intermediate[srcIndex], out.start, out.buffer);
        }
    }
}
</#if>
</#list>