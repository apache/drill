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

<#if type.major == "DecimalSimpleVarChar"> <#-- Cast function template for conversion from Decimal9, Decimal18 to VarChar -->

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
import io.netty.buffer.SwappedByteBuf;

import java.nio.ByteBuffer;

@SuppressWarnings("unused")
@FunctionTemplate(name = "cast${type.to?upper_case}", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
public class Cast${type.from}${type.to} implements DrillSimpleFunc {

    @Param ${type.from}Holder in;
    @Param BigIntHolder len;
    @Inject DrillBuf buffer;
    @Output ${type.to}Holder out;

    public void setup() {
        buffer = buffer.reallocIfNeeded(${type.bufferSize});
    }

    public void eval() {

        StringBuilder str = new StringBuilder();

        if (in.value < 0) {
            // Negative value, add '-' to the string
            str.append("-");

            // Negate the number
            in.value *= -1;
        }

        ${type.javatype} separator = (${type.javatype}) org.apache.drill.exec.util.DecimalUtility.getPowerOfTen((int) in.scale);

        str.append(in.value / separator);

        if (in.scale > 0) {
            str.append(".");

            String fractionalPart = String.valueOf(in.value % separator);

            /* Since we are taking modulus to find fractional part,
             * we will miss printing the leading zeroes in the fractional part
             * Account for those zeroes
             *
             * Eg: 1.0002
             * Scale: 3
             *
             * Stored as: 10002
             *
             * We print integer part by 10002/1000 = 1
             * We print fractional part by 10002 % 1000 = 2
             *
             * We missed the initial zeroes in the fractional part. Below logic accounts for this case
             */
            str.append(org.apache.drill.exec.util.DecimalUtility.toStringWithZeroes((in.value % separator), in.scale));
        }

        out.buffer = buffer;
        out.start = 0;
        out.end = Math.min((int)len.value, str.length()); // truncate if target type has length smaller than that of input's string
        out.buffer.setBytes(0, String.valueOf(str.substring(0,out.end)).getBytes());
    }
}
<#elseif type.major == "DecimalComplexVarChar"> <#-- Cast function template for conversion from Decimal28Sparse, Decimal38Sparse to VarChar -->

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

import java.nio.ByteBuffer;

@SuppressWarnings("unused")
@FunctionTemplate(name = "cast${type.to?upper_case}", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
public class Cast${type.from}${type.to} implements DrillSimpleFunc {

    @Param ${type.from}Holder in;
    @Param BigIntHolder len;
    @Inject DrillBuf buffer;
    @Output ${type.to}Holder out;

    public void setup() {
        buffer = buffer.reallocIfNeeded((int) len.value);
    }

    public void eval() {

        StringBuilder str = new StringBuilder();
        int index = 0;
        int fractionalStartIndex = ${type.arraySize} - org.apache.drill.exec.util.DecimalUtility.roundUp(in.scale);

        // Find the first non-zero value in the integer part of the decimal
        while (index < fractionalStartIndex && in.getInteger(index, in.start, in.buffer) == 0)  {
            index++;
        }


        // If we have valid digits print '-' sign
        if ((in.getSign(in.start, in.buffer) == true) && index < ${type.arraySize}) {
            str.append("-");
        }

        // If all the integer digits are zero, print a single zero
        if (index == fractionalStartIndex) {
            str.append("0");
        }

        boolean fillZeroes = false;

        // convert the integer part
        while (index < fractionalStartIndex) {
            int value =  in.getInteger(index++, in.start, in.buffer);

            if (fillZeroes == true) {
                str.append(org.apache.drill.exec.util.DecimalUtility.toStringWithZeroes(value, org.apache.drill.exec.util.DecimalUtility.MAX_DIGITS));
            } else {
                str.append(value);
                fillZeroes = true;
            }
            //str.append(value);
        }

        if (fractionalStartIndex < ${type.arraySize}) {
            // We have fractional part, print '.'
            str.append(".");

            /* convert the fractional part (except the last decimal digit,
             * as it might have padding that needs to be stripped
             */
            while (fractionalStartIndex < ${type.arraySize} - 1) {
                int value = in.getInteger(fractionalStartIndex++, in.start, in.buffer);

                // Fill zeroes at the beginning of the decimal digit
                str.append(org.apache.drill.exec.util.DecimalUtility.toStringWithZeroes(value, org.apache.drill.exec.util.DecimalUtility.MAX_DIGITS));
            }

            // Last decimal digit, strip the extra zeroes we may have padded
            int actualDigits = in.scale % org.apache.drill.exec.util.DecimalUtility.MAX_DIGITS;

            int lastFractionalDigit = in.getInteger(${type.arraySize} - 1, in.start, in.buffer);

            if (actualDigits != 0) {

                // Strip padded zeroes at the end that is not part of the scale
                lastFractionalDigit /= (int) (org.apache.drill.exec.util.DecimalUtility.getPowerOfTen((int) (org.apache.drill.exec.util.DecimalUtility.MAX_DIGITS - actualDigits)));
                str.append(org.apache.drill.exec.util.DecimalUtility.toStringWithZeroes(lastFractionalDigit, actualDigits));
            } else {
                // Last digit does not have any padding print as is
                str.append(org.apache.drill.exec.util.DecimalUtility.toStringWithZeroes(lastFractionalDigit, org.apache.drill.exec.util.DecimalUtility.MAX_DIGITS));
            }


        }

        out.buffer = buffer;
        out.start = 0;
        out.end = Math.min((int)len.value, str.length()); // truncate if target type has length smaller than that of input's string
        out.buffer.setBytes(0, String.valueOf(str.substring(0,out.end)).getBytes());
    }
}
</#if> <#-- type.major -->
</#list>