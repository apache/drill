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

<#if type.major == "VarCharDecimalSimple" || type.major == "EmptyStringVarCharDecimalSimple">  <#-- Cast function template for conversion from VarChar to Decimal9, Decimal18 -->

<#if type.major == "VarCharDecimalSimple">
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/gcast/Cast${type.from}${type.to}.java"/>
<#elseif type.major == "EmptyStringVarCharDecimalSimple">
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/gcast/CastEmptyString${type.from}To${type.to}.java"/>
</#if>

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
<#if type.major == "VarCharDecimalSimple">
@FunctionTemplate(name ="cast${type.to?upper_case}", scope = FunctionTemplate.FunctionScope.DECIMAL_CAST, nulls=NullHandling.NULL_IF_NULL)
public class Cast${type.from}${type.to} implements DrillSimpleFunc {
<#elseif type.major == "EmptyStringVarCharDecimalSimple">
@FunctionTemplate(name ="castEmptyString${type.from}To${type.to?upper_case}", scope = FunctionTemplate.FunctionScope.DECIMAL_CAST, nulls=NullHandling.INTERNAL)
public class CastEmptyString${type.from}To${type.to} implements DrillSimpleFunc {
</#if>
    @Param ${type.from}Holder in;
    @Param BigIntHolder precision;
    @Param BigIntHolder scale;

    <#if type.major == "VarCharDecimalSimple">
    @Output ${type.to}Holder out;
    <#elseif type.major == "EmptyStringVarCharDecimalSimple">
    @Output ${type.to}Holder out;
    </#if>

    public void setup() {
    }

    public void eval() {
        <#if type.major == "EmptyStringVarCharDecimalSimple">
        // Check if the input is null or empty string
        if(<#if type.from == "NullableVarChar"> in.isSet == 0 || </#if> in.end == in.start) {
            out.isSet = 0;
            return;
        }
        out.isSet = 1;
        </#if>

        // Assign the scale and precision
        out.scale = (int) scale.value;
        out.precision = (int) precision.value;

        int readIndex = in.start;
        int endIndex  = in.end;

        <#if type.major == "VarCharDecimalSimple">
        // Check if its an empty string
        if (endIndex - readIndex == 0) {
            throw new org.apache.drill.common.exceptions.DrillRuntimeException("Empty String, cannot cast to Decimal");
        }
        </#if>

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
        boolean leadingDigitFound = false;
        boolean round = false;

        int radix = 10;

        // Start parsing the digits
        while (readIndex < endIndex) {
            next = in.buffer.getByte(readIndex++);

            if (next == '.') {
                scaleIndex = readIndex;
                // Integer end index is just before the scale part begins
                integerEndIndex = scaleIndex - 1;
                // If the number of fractional digits is > scale specified we might have to truncate
                if ((scaleIndex + out.scale) < endIndex ) {
                    endIndex = scaleIndex + out.scale;
                    round    = true;
                }
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
            } else if (leadingDigitFound == false) {
                if (next == 0) {
                    // Ignore the leading zeroes while validating if input digits will fit within the given precision
                    integerStartIndex++;
                } else {
                    leadingDigitFound = true;
                }
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
            // TODO:  Use JDK's java.nio.charset.StandardCharsets.UTF_8.
        }

        // Check if we need to round up
        if (round == true) {
            next = in.buffer.getByte(endIndex);
            next = (byte) Character.digit(next, radix);
            if (next == -1) {
                // not a valid digit
                byte[] buf = new byte[in.end - in.start];
                in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
                throw new org.apache.drill.common.exceptions.DrillRuntimeException(new String(buf, com.google.common.base.Charsets.UTF_8));
            }
            if (next > 4) {
                out.value++;
            }
        }

        // Number of fractional digits in the input
        int fractionalDigits = (scaleIndex == -1) ? 0 : ((endIndex - scaleIndex));

        // Pad the number with zeroes if number of fractional digits is less than scale
        if (fractionalDigits < scale.value) {
            out.value = (${type.javatype}) (org.apache.drill.exec.util.DecimalUtility.adjustScaleMultiply(out.value, (int) (scale.value - fractionalDigits)));
        }

        // Negate the number if we saw a -ve sign
        if (negative == true) {
            out.value *= -1;
        }
    }
}

<#elseif type.major == "VarCharDecimalComplex" || type.major == "EmptyStringVarCharDecimalComplex">  <#-- Cast function template for conversion from VarChar to Decimal28, Decimal38 -->

<#if type.major == "VarCharDecimalComplex">
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/gcast/Cast${type.from}${type.to}.java"/>
<#elseif type.major == "EmptyStringVarCharDecimalComplex">
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/gcast/CastEmptyString${type.from}To${type.to}.java"/>
</#if>

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
<#if type.major == "VarCharDecimalComplex">
@FunctionTemplate(name = "cast${type.to?upper_case}", scope = FunctionTemplate.FunctionScope.DECIMAL_CAST, nulls=NullHandling.NULL_IF_NULL)
public class Cast${type.from}${type.to} implements DrillSimpleFunc {
<#elseif type.major == "EmptyStringVarCharDecimalComplex">
@FunctionTemplate(name = "castEmptyString${type.from}To${type.to?upper_case}", scope = FunctionTemplate.FunctionScope.DECIMAL_CAST, nulls=NullHandling.INTERNAL)
public class CastEmptyString${type.from}To${type.to} implements DrillSimpleFunc {
</#if>
    @Param ${type.from}Holder in;
    @Inject DrillBuf buffer;
    @Param BigIntHolder precision;
    @Param BigIntHolder scale;

    <#if type.major == "VarCharDecimalComplex">
    @Output ${type.to}Holder out;
    <#elseif type.major == "EmptyStringVarCharDecimalComplex">
    @Output ${type.to}Holder out;
    </#if>

    public void setup() {
        int size = ${type.arraySize} * (org.apache.drill.exec.util.DecimalUtility.INTEGER_SIZE);
        buffer = buffer.reallocIfNeeded(size);
    }

    public void eval() {
        <#if type.major == "EmptyStringVarCharDecimalComplex">
        // Check if the input is null or empty string
        if(<#if type.from == "NullableVarChar"> in.isSet == 0 || </#if> in.end == in.start) {
            out.isSet = 0;
            return;
        }
        out.isSet = 1;
        </#if>

        out.buffer = buffer;
        out.start  = 0;

        out.scale = (int) scale.value;
        out.precision = (int) precision.value;
        boolean sign = false;

        // Initialize the output buffer
        for (int i = 0; i < ${type.arraySize}; i++) {
            out.setInteger(i, 0, out.start, out.buffer);
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
            sign = true;
        }

        if (next == '.') {
            readIndex++;
            scaleIndex = readIndex; // Fractional part starts at the first position
        }

        <#if type.major == "VarCharDecimalComplex">
        // Check if its an empty string
        if (in.end - readIndex == 0) {
            throw new org.apache.drill.common.exceptions.DrillRuntimeException("Empty String, cannot cast to Decimal");
        }
        </#if>

        // Store start index for the second pass
        startIndex = readIndex;

        int radix = 10;
        boolean leadingDigitFound = false;
        boolean round = false;

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
                    if ((in.end - scaleIndex) > out.scale) {
                      scaleEndIndex =  scaleIndex + out.scale;
                      round = true;
                    }
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

                if (leadingDigitFound == false && next != 0) {
                    leadingDigitFound = true;
                }

                if (leadingDigitFound == true) {
                    integerDigits++;
                }
            }
        }

        <#-- TODO:  Pull out much of this code into something parallel to
             ByteFunctionHelpers but for DECIMAL type implementations. -->

        /* Based on the number of integer digits computed and the scale throw an
         * exception if the provided precision is not sufficient to store the value
         */
        if (integerDigits + out.scale > out.precision) {
            byte[] buf = new byte[in.end - in.start];
            in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
            throw new org.apache.drill.common.exceptions.DrillRuntimeException("Precision is insufficient for the provided input: " + new String(buf, com.google.common.base.Charsets.UTF_8) + " Precision: " + out.precision + " Total Digits: " + (out.scale + integerDigits));
            <#-- TODO:  Revisit message.  (Message would be clearer and shorter
                 as something like "Precision of X digits is insufficient for
                 the provided input of "XXXXX.XXXXX" (X total digits)."  (An
                 occurrence of "Precision is insufficient for the provided input:
                 123456789.987654321 Precision: 5 Total Digits: 9" seemed to
                 mean that 5 post-decimal digits and 9 total digits were allowed.)
                 -->
        }


        // Compute the number of slots needed in the ByteBuf to store the integer and fractional part
        int scaleRoundedUp   = org.apache.drill.exec.util.DecimalUtility.roundUp(out.scale);
        int integerRoundedUp = org.apache.drill.exec.util.DecimalUtility.roundUp(integerDigits);

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

            int value = (((int) org.apache.drill.exec.util.DecimalUtility.getPowerOfTen(ndigits)) * next) + (out.getInteger(decimalBufferIndex, out.start, out.buffer));
            out.setInteger(decimalBufferIndex, value, out.start, out.buffer);

            ndigits++;

            /* We store the entire decimal as base 1 billion values, which has maximum of 9 digits (MAX_DIGITS)
             * Once we have stored MAX_DIGITS in a given slot move to the next slot.
             */
            if (ndigits >= org.apache.drill.exec.util.DecimalUtility.MAX_DIGITS) {
                ndigits = 0;
                decimalBufferIndex--;
            }
        }

        // Traverse and extract the fractional part
        decimalBufferIndex = (scaleRoundedUp > 0) ? (${type.arraySize} - scaleRoundedUp) : (${type.arraySize} - 1);
        ndigits = 0;

        if (scaleIndex != -1) {
            while (scaleIndex < scaleEndIndex) {

                // check if we have scanned MAX_DIGITS and we need to move to the next index
                if (ndigits >= org.apache.drill.exec.util.DecimalUtility.MAX_DIGITS) {
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
                int value = (out.getInteger(decimalBufferIndex, out.start, out.buffer) * radix) + next;
                out.setInteger(decimalBufferIndex, value, out.start, out.buffer);

                // added another digit to the current index
                ndigits++;
            }

            // round up the decimal if we had to chop off a part of it
            if (round == true) {
               next = in.buffer.getByte(scaleEndIndex);

                // We expect only numbers beyond this
                next = (byte) Character.digit(next, radix);

                if (next == -1) {
                    // not a valid digit
                    byte[] buf = new byte[in.end - in.start];
                    in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
                    throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
                }
                if (next > 4) {
                    // Need to round up
                    out.setInteger(decimalBufferIndex, out.getInteger(decimalBufferIndex, out.start, out.buffer)+1, out.start, out.buffer);
                }
            }
            // Pad zeroes in the fractional part so that number of digits = MAX_DIGITS
            if (out.scale > 0) {
              int padding = (int) org.apache.drill.exec.util.DecimalUtility.getPowerOfTen((int) (org.apache.drill.exec.util.DecimalUtility.MAX_DIGITS - ndigits));
              out.setInteger(decimalBufferIndex, out.getInteger(decimalBufferIndex, out.start, out.buffer) * padding, out.start, out.buffer);
            }

            int carry = 0;
            do {
                // propagate the carry
                int tempValue = out.getInteger(decimalBufferIndex, out.start, out.buffer) + carry;
                if (tempValue >= org.apache.drill.exec.util.DecimalUtility.DIGITS_BASE) {
                    carry = tempValue / org.apache.drill.exec.util.DecimalUtility.DIGITS_BASE;
                    tempValue = (tempValue % org.apache.drill.exec.util.DecimalUtility.DIGITS_BASE);
                } else {
                    carry = 0;
                }
                out.setInteger(decimalBufferIndex--, tempValue, out.start, out.buffer);
            } while (carry > 0 && decimalBufferIndex >= 0);
        }
        out.setSign(sign, out.start, out.buffer);
    }
}
</#if> <#-- type.major -->
</#list>