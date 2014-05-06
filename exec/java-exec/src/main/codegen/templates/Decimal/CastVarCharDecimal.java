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

<#if type.major == "VarCharDecimalSimple">  <#-- Cast function template for conversion from VarChar to Decimal9, Decimal18 -->
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
    @Param BigIntHolder precision;
    @Param BigIntHolder scale;
    @Output ${type.to}Holder out;

    public void setup(RecordBatch incoming) {
    }

    public void eval() {

        // Assign the scale and precision
        out.scale = (int) scale.value;
        out.precision = (int) precision.value;

        int readIndex = in.start;
        int endIndex  = in.end;

        // Check if its an empty string
        if (endIndex - readIndex == 0) {
            throw new org.apache.drill.common.exceptions.DrillRuntimeException("Empty String, cannot cast to Decimal");
        }
        // Starting position of fractional part
        int scaleIndex = -1;
        // true if we have a negative sign at the beginning
        boolean negative = false;

        // Check the first byte for '-'
        byte next = (in.buffer.getByte(readIndex));

        // If its a negative number
        if (next == '-') {
            negative = true;
            readIndex++;
        }


        /* Below two fields are used to compute if the precision is sufficient to store
         * the scale along with the integer digits in the string
         */
        int integerStartIndex = readIndex;
        int integerEndIndex = endIndex;

        int radix = 10;

        // Start parsing the digits
        while (readIndex < endIndex) {
            next = in.buffer.getByte(readIndex++);

            if (next == '.') {
                scaleIndex = readIndex;
                // Integer end index is just before the scale part begins
                integerEndIndex = scaleIndex - 1;
                // If the number of fractional digits is > scale specified we might have to truncate
                endIndex = (scaleIndex + out.scale) < endIndex ? (scaleIndex + out.scale) : endIndex;

                continue;
            } else {
                // If its not a '.' we expect only numbers
                next = (byte) Character.digit(next, radix);
            }

            if (next == -1) {
                // not a valid digit
                byte[] buf = new byte[in.end - in.start];
                in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
                throw new org.apache.drill.common.exceptions.DrillRuntimeException(new String(buf, com.google.common.base.Charsets.UTF_8));
            }
            out.value *= radix;
            out.value += next;
        }

        // Check if the provided precision is enough to store the given input
        if (((integerEndIndex - integerStartIndex) + out.scale) > out.precision) {
            byte[] buf = new byte[in.end - in.start];
            in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
            throw new org.apache.drill.common.exceptions.DrillRuntimeException("Precision is insufficient for the provided input: " + new String(buf, com.google.common.base.Charsets.UTF_8) + " Precision: " + out.precision +
                                                                               " Total Digits: " + (out.scale + (integerEndIndex - integerStartIndex)));
        }

        // Number of fractional digits in the input
        int fractionalDigits = (scaleIndex == -1) ? 0 : ((endIndex - scaleIndex));

        // Pad the number with zeroes if number of fractional digits is less than scale
        if (fractionalDigits < scale.value) {
            out.value *= Math.pow(10, scale.value - fractionalDigits);
        }

        // Negate the number if we saw a -ve sign
        if (negative == true) {
            out.value *= -1;
        }
    }
}

<#elseif type.major == "VarCharDecimalComplex">  <#-- Cast function template for conversion from VarChar to Decimal9, Decimal18 -->
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
    @Workspace ByteBuf buffer;
    @Param BigIntHolder precision;
    @Param BigIntHolder scale;
    @Output ${type.to}Holder out;

    public void setup(RecordBatch incoming) {
        int size = ${type.arraySize} * (org.apache.drill.common.util.DecimalUtility.integerSize);
        buffer = io.netty.buffer.Unpooled.wrappedBuffer(new byte[size]);
        buffer = new io.netty.buffer.SwappedByteBuf(buffer);
    }

    public void eval() {

        out.buffer = buffer;
        out.start  = 0;

        out.scale = (int) scale.value;
        out.precision = (int) precision.value;

        // Initialize the output buffer
        for (int i = 0; i < ${type.arraySize}; i++) {
            out.setInteger(i, 0);
        }

        int startIndex;
        int readIndex = in.start;
        int integerDigits = 0;
        int fractionalDigits = 0;
        int scaleIndex = -1;
        int scaleEndIndex = in.end;

        byte[] buf1 = new byte[in.end - in.start];
        in.buffer.getBytes(in.start, buf1, 0, in.end - in.start);

        Byte next = in.buffer.getByte(readIndex);


        if (next == '-') {
            readIndex++;
            out.sign = true;
        }

        if (next == '.') {
            readIndex++;
            scaleIndex = readIndex; // Fractional part starts at the first position
        }

        if (in.end - readIndex == 0) {
            throw new org.apache.drill.common.exceptions.DrillRuntimeException("Empty String, cannot cast to Decimal");
        }

        // Store start index for the second pass
        startIndex = readIndex;

        int radix = 10;

        /* This is the first pass, we get the number of integer digits and based on the provided scale
         * we compute which index into the ByteBuf we start storing the integer part of the Decimal
         */
        if (scaleIndex == -1) {

            while (readIndex < in.end) {
                next = in.buffer.getByte(readIndex++);

                if (next == '.') {

                    // We have found the decimal point. we can compute the starting index into the Decimal's bytebuf
                    scaleIndex = readIndex;
                    // We may have to truncate fractional part if > scale
                    scaleEndIndex = ((in.end - scaleIndex) <= out.scale) ? in.end : (scaleIndex + out.scale);
                    break;
                }

                // If its not a '.' we expect only numbers
                next = (byte) Character.digit(next, radix);

                if (next == -1) {
                    // not a valid digit
                    byte[] buf = new byte[in.end - in.start];
                    in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
                    throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
                }

                integerDigits++;
            }
        }

        /* Based on the number of integer digits computed and the scale throw an
         * exception if the provided precision is not sufficient to store the value
         */
        if (integerDigits + out.scale > out.precision) {
            byte[] buf = new byte[in.end - in.start];
            in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
            throw new org.apache.drill.common.exceptions.DrillRuntimeException("Precision is insufficient for the provided input: " + new String(buf, com.google.common.base.Charsets.UTF_8) + " Precision: " + out.precision + " Total Digits: " + (out.scale + integerDigits));
        }


        // Compute the number of slots needed in the ByteBuf to store the integer and fractional part
        int scaleRoundedUp   = org.apache.drill.common.util.DecimalUtility.roundUp(out.scale);
        int integerRoundedUp = org.apache.drill.common.util.DecimalUtility.roundUp(integerDigits);

        int ndigits = 0;

        int decimalBufferIndex = ${type.arraySize} - scaleRoundedUp - 1;

        /* Compute the end index of the integer part.
         * If we haven't seen a '.' then entire string is integer.
         * If we have seen a '.' it ends before the '.'
         */
        int integerEndIndex = (scaleIndex == -1) ? (in.end - 1) : (scaleIndex - 2);

        // Traverse and extract the integer part
        while (integerEndIndex >= startIndex) {
            next = in.buffer.getByte(integerEndIndex--);

            next = (byte) Character.digit(next, radix);

            int value = (((int) Math.pow(10, ndigits)) * next) + (out.getInteger(decimalBufferIndex));
            out.setInteger(decimalBufferIndex, value);

            ndigits++;

            /* We store the entire decimal as base 1 billion values, which has maximum of 9 digits (MAX_DIGITS)
             * Once we have stored MAX_DIGITS in a given slot move to the next slot.
             */
            if (ndigits >= org.apache.drill.common.util.DecimalUtility.MAX_DIGITS) {
                ndigits = 0;
                decimalBufferIndex--;
            }
        }

        // Traverse and extract the fractional part
        decimalBufferIndex = ${type.arraySize} - scaleRoundedUp;
        ndigits = 0;

        if (scaleIndex != -1 && out.scale > 0) {
            while (scaleIndex < scaleEndIndex) {

                // check if we have scanned MAX_DIGITS and we need to move to the next index
                if (ndigits >= org.apache.drill.common.util.DecimalUtility.MAX_DIGITS) {
                    ndigits = 0;
                    decimalBufferIndex++;
                }

                next = in.buffer.getByte(scaleIndex++);

                // We expect only numbers beyond this
                next = (byte) Character.digit(next, radix);

                if (next == -1) {
                    // not a valid digit
                    byte[] buf = new byte[in.end - in.start];
                    in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
                    throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
                }
                int value = (out.getInteger(decimalBufferIndex) * radix) + next;
                out.setInteger(decimalBufferIndex, value);

                // added another digit to the current index
                ndigits++;
            }
            // Pad zeroes in the fractional part so that number of digits = MAX_DIGITS
            int padding = (int) Math.pow(10, org.apache.drill.common.util.DecimalUtility.MAX_DIGITS - ndigits);
            out.setInteger(decimalBufferIndex, out.getInteger(decimalBufferIndex) * padding);
        }
    }
}
</#if> <#-- type.major -->
</#list>