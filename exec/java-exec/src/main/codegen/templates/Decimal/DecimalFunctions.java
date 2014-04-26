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

<#macro denseCompareBlock left right output>

            int invert = 1;

            outside: {

                /* If signs are different then simply look at the
                 * sign of the two inputs and determine which is greater
                 */
                if (left.sign != right.sign) {

                    ${output} = (left.sign == true) ? -1 : 1;
                    break outside;
                } else if(left.sign == true) {
                    /* Both inputs are negative, at the end we will
                     * have to invert the comparison
                     */
                    invert = -1;
                }

                ${output} = 0;
                for (int i = 0; i < left.WIDTH; i++) {
                    byte leftByte  = left.buffer.getByte(left.start + i);
                    byte rightByte = right.buffer.getByte(right.start + i);

                    // Unsigned byte comparison
                    if ((leftByte & 0xFF) > (rightByte & 0xFF)) {
                        ${output} = 1;
                        break;
                    } else if ((leftByte & 0xFF) < (rightByte & 0xFF)) {
                        ${output} = -1;
                        break;
                    }
                }
                ${output} *= invert; // invert the comparison if both were negative values
            }
</#macro>

<#macro compareBlock holderType left right absCompare output>

        outside:{

            <#if absCompare == "false">
            if (left.sign != right.sign) {
                /* signs are different, we can simply look at the sign
                 * and determine which decimal is greater
                 */
                ${output} = left.sign == true ? -1 : 1;
                break outside;
            } else if (left.sign == true) {
                /* Because both decimals are negative, we swap them
                 * and go ahead with the regular comparison
                 */
                left.swap(right);
            }
            </#if>
            /* compute the number of integer digits in each decimal */
            int leftInt  = left.precision - left.scale;
            int rightInt = right.precision - right.scale;

            /* compute the number of indexes required for storing integer digits */
            int leftIntRoundedUp = org.apache.drill.common.util.DecimalUtility.roundUp(leftInt);
            int rightIntRoundedUp = org.apache.drill.common.util.DecimalUtility.roundUp(rightInt);

            /* compute number of indexes required for storing scale */
            int leftScaleRoundedUp = org.apache.drill.common.util.DecimalUtility.roundUp(left.scale);
            int rightScaleRoundedUp = org.apache.drill.common.util.DecimalUtility.roundUp(right.scale);

            /* compute index of the most significant integer digits */
            int leftIndex1 = left.nDecimalDigits - leftScaleRoundedUp - leftIntRoundedUp;
            int rightIndex1 = right.nDecimalDigits - rightScaleRoundedUp - rightIntRoundedUp;

            int leftStopIndex = left.nDecimalDigits - leftScaleRoundedUp;
            int rightStopIndex = right.nDecimalDigits - rightScaleRoundedUp;

            /* Discard the zeroes in the integer part */
            while (leftIndex1 < leftStopIndex) {
                if (left.getInteger(leftIndex1) != 0) {
                     break;
                }

                /* Digit in this location is zero, decrement the actual number
                 * of integer digits
                 */
                leftIntRoundedUp--;
                leftIndex1++;
            }

            /* If we reached the stop index then the number of integers is zero */
            if (leftIndex1 == leftStopIndex) {
                leftIntRoundedUp = 0;
            }

            while (rightIndex1 < rightStopIndex) {
                if (right.getInteger(rightIndex1) != 0) {
                    break;
                }

                /* Digit in this location is zero, decrement the actual number
                 * of integer digits
                 */
                rightIntRoundedUp--;
                rightIndex1++;
            }

            if (rightIndex1 == rightStopIndex) {
                rightIntRoundedUp = 0;
            }

            /* We have the accurate number of non-zero integer digits,
             * if the number of integer digits are different then we can determine
             * which decimal is larger and needn't go down to comparing individual values
             */
            if (leftIntRoundedUp > rightIntRoundedUp) {
                ${output }= 1;
                break outside;
            }
            else if (rightIntRoundedUp > leftIntRoundedUp) {
                ${output} =  -1;
                break outside;
            }

            /* The number of integer digits are the same, set the each index
             * to the first non-zero integer and compare each digit
             */
            leftIndex1 = left.nDecimalDigits - leftScaleRoundedUp - leftIntRoundedUp;
            rightIndex1 = right.nDecimalDigits - rightScaleRoundedUp - rightIntRoundedUp;

            while (leftIndex1 < leftStopIndex && rightIndex1 < rightStopIndex) {
                if (left.getInteger(leftIndex1) > right.getInteger(rightIndex1)) {
                    ${output} = 1;
                    break outside;
                }
                else if (right.getInteger(rightIndex1) > left.getInteger(leftIndex1)) {
                    ${output} =  -1;
                    break outside;
                }

                leftIndex1++;
                rightIndex1++;
            }

            /* The integer part of both the decimal's are equal, now compare
             * each individual fractional part. Set the index to be at the
             * beginning of the fractional part
             */
            leftIndex1 = leftStopIndex;
            rightIndex1 = rightStopIndex;

            /* Stop indexes will be the end of the array */
            leftStopIndex = left.nDecimalDigits;
            rightStopIndex = right.nDecimalDigits;

            /* compare the two fractional parts of the decimal */
            while (leftIndex1 < leftStopIndex && rightIndex1 < rightStopIndex) {
                if (left.getInteger(leftIndex1) > right.getInteger(rightIndex1)) {
                    ${output} = 1;
                    break outside;
                }
                else if (right.getInteger(rightIndex1) > left.getInteger(leftIndex1)) {
                    ${output} = -1;
                    break outside;
                }

                leftIndex1++;
                rightIndex1++;
            }

            /* Till now the fractional part of the decimals are equal, check
             * if one of the decimal has fractional part that is remaining
             * and is non-zero
             */
            while (leftIndex1 < leftStopIndex) {
                if (left.getInteger(leftIndex1) != 0) {
                    ${output} = 1;
                    break outside;
                }
                leftIndex1++;
            }

            while(rightIndex1 < rightStopIndex) {
                if (right.getInteger(rightIndex1) != 0) {
                    ${output} = -1;
                    break outside;
                }
                rightIndex1++;
            }

            /* Both decimal values are equal */
            ${output} = 0;

        }
</#macro>

<#macro subtractBlock holderType left right result>

            /* compute the result's size, integer part and fractional part */
            result.scale   = Math.max(left.scale, right.scale);
            result.precision = result.maxPrecision;


            int resultScaleRoundedUp = org.apache.drill.common.util.DecimalUtility.roundUp(result.scale);
            int resultIndex = result.nDecimalDigits- 1;

            int leftScaleRoundedUp  = org.apache.drill.common.util.DecimalUtility.roundUp(left.scale);
            int leftIntRoundedUp    = org.apache.drill.common.util.DecimalUtility.roundUp(left.precision - left.scale);
            int rightScaleRoundedUp = org.apache.drill.common.util.DecimalUtility.roundUp(right.scale);

            int leftIndex  = left.nDecimalDigits - 1;
            int rightIndex = right.nDecimalDigits - 1;

            /* If the left scale is bigger, simply copy over the digits into result */
            while (leftScaleRoundedUp > rightScaleRoundedUp) {
                result.setInteger(resultIndex, left.getInteger(leftIndex));
                leftIndex--;
                resultIndex--;
                leftScaleRoundedUp--;
            }

            /* If the right scale is bigger, subtract with zero at each array location */
            int carry = 0;
            while(rightScaleRoundedUp > leftScaleRoundedUp) {

                int difference = 0 - right.getInteger(rightIndex) - carry;
                rightIndex--;

                if (difference < 0) {
                    carry = 1;
                    result.setInteger(resultIndex, (difference + org.apache.drill.common.util.DecimalUtility.DIGITS_BASE));
                } else {
                    result.setInteger(resultIndex, difference);
                    carry = 0;
                }
                resultIndex--;
                rightScaleRoundedUp--;

            }

            /* Now both the scales are equal perform subtraction use one of the scales
             * for terminal condition in the while loop
             */
            while (leftScaleRoundedUp > 0) {

                int difference = left.getInteger(leftIndex) - right.getInteger(rightIndex) - carry;
                leftIndex--;
                rightIndex--;

                if (difference < 0) {
                    carry = 1;
                    result.setInteger(resultIndex, (difference + org.apache.drill.common.util.DecimalUtility.DIGITS_BASE));
                } else {
                    result.setInteger(resultIndex, difference);
                    carry = 0;
                }
                resultIndex--;
                leftScaleRoundedUp--;
            }

            /* Since we are gurranteed to have the left input >= right input, iterate
             * over the remaining left input's integers
             */
            while(leftIntRoundedUp > 0) {

                int difference = left.getInteger(leftIndex);
                leftIndex--;

                if (rightIndex >= 0) {
                    difference -= right.getInteger(rightIndex);
                    rightIndex--;
                }

                difference -= carry;

                if (difference < 0) {
                    carry = 1;
                    result.setInteger(resultIndex, (difference + org.apache.drill.common.util.DecimalUtility.DIGITS_BASE));
                } else {
                    carry = 0;
                    result.setInteger(resultIndex, difference);
                }
                resultIndex--;
                leftIntRoundedUp--;
            }

</#macro>

<#macro addBlock holderType left right result>

        /* compute the result scale */
        result.scale = Math.max(left.scale, right.scale);
        result.precision = result.maxPrecision;

        int resultScaleRoundedUp = org.apache.drill.common.util.DecimalUtility.roundUp(result.scale);

        int leftScaleRoundedUp  = org.apache.drill.common.util.DecimalUtility.roundUp(left.scale);
        int rightScaleRoundedUp = org.apache.drill.common.util.DecimalUtility.roundUp(right.scale);

        /* starting index for each decimal */
        int leftIndex  = left.nDecimalDigits - 1;
        int rightIndex = right.nDecimalDigits - 1;
        int resultIndex = result.nDecimalDigits - 1;

        /* If one of the scale is larger then simply copy it over
         * to the result digits
         */
        while (leftScaleRoundedUp > rightScaleRoundedUp) {

            result.setInteger(resultIndex, left.getInteger(leftIndex));
            leftIndex--;
            resultIndex--;
            leftScaleRoundedUp--;
            resultScaleRoundedUp--;
        }

        while (rightScaleRoundedUp > leftScaleRoundedUp) {
            result.setInteger((resultIndex), right.getInteger(rightIndex));
            rightIndex--;
            resultIndex--;
            rightScaleRoundedUp--;
            resultScaleRoundedUp--;
        }

        int sum = 0;

        /* now the two scales are at the same level, we can add them */
        while (resultScaleRoundedUp > 0) {

            sum += left.getInteger(leftIndex) + right.getInteger(rightIndex);
            leftIndex--;
            rightIndex--;

            if (sum >= org.apache.drill.common.util.DecimalUtility.DIGITS_BASE) {
                result.setInteger(resultIndex, (sum - org.apache.drill.common.util.DecimalUtility.DIGITS_BASE));
                sum = 1;
            } else {
                result.setInteger(resultIndex, sum);
                sum = 0;
            }
            resultIndex--;
            resultScaleRoundedUp--;
        }

        /* add the integer part */
        while (leftIndex >= 0 && rightIndex >= 0) {

            sum += left.getInteger(leftIndex) + right.getInteger(rightIndex);
            leftIndex--;
            rightIndex--;

            if (sum > org.apache.drill.common.util.DecimalUtility.DIGITS_BASE) {
                result.setInteger(resultIndex, (sum - org.apache.drill.common.util.DecimalUtility.DIGITS_BASE));
                sum = 1;
            } else {
                result.setInteger(resultIndex, sum);
                sum = 0;
            }
            resultIndex--;
        }

        while (resultIndex >= 0 && leftIndex >= 0) {
            sum += left.getInteger(leftIndex);
            leftIndex--;

            if (sum > org.apache.drill.common.util.DecimalUtility.DIGITS_BASE) {
                result.setInteger(resultIndex, (sum - org.apache.drill.common.util.DecimalUtility.DIGITS_BASE));
                sum = 1;
            } else {
                result.setInteger(resultIndex, sum);
                sum = 0;
            }
        }

        while (resultIndex >= 0 && rightIndex >= 0) {
            sum += right.getInteger(rightIndex);
            rightIndex--;

            if (sum > org.apache.drill.common.util.DecimalUtility.DIGITS_BASE) {
                result.setInteger(resultIndex, (sum - org.apache.drill.common.util.DecimalUtility.DIGITS_BASE));
                sum = 1;
            } else {
                result.setInteger(resultIndex, sum);
                sum = 0;
            }
        }

        /* store the last carry */
        if (sum > 0)
        result.setInteger(resultIndex, sum);

</#macro>


<#macro adjustScale holderType javaType left right>

            // Adjust the scale of the two inputs to be the same

            int adjustment = 0;

            if (left.scale < right.scale) {
                left.value = (${javaType}) (left.value * Math.pow(10, (right.scale - left.scale)));
            } else if (right.scale < left.scale) {
                right.value = (${javaType}) (right.value * Math.pow(10, (left.scale - right.scale)));
            }
</#macro>

<#list decimal.decimalTypes as type>

<#if type.name.endsWith("Sparse")>
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/${type.name}Functions.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;

@SuppressWarnings("unused")
public class ${type.name}Functions {

    @FunctionTemplate(name = "subtract", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}SubtractFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Workspace ByteBuf buffer;
        @Output ${type.name}Holder result;

        public void setup(RecordBatch incoming) {
            int size = (${type.storage} * (org.apache.drill.common.util.DecimalUtility.integerSize));
            buffer = io.netty.buffer.Unpooled.wrappedBuffer(new byte[size]);
            buffer = new io.netty.buffer.SwappedByteBuf(buffer);
        }

        public void eval() {

            result.buffer = buffer;
            result.start = 0;

            // Re initialize the buffer everytime
            for (int i = 0; i < ${type.storage}; i++) {
                result.setInteger(i, 0);
            }

            /* If the sign of the two inputs is different, then the subtract
             * causes the sign of one of the inputs to change and hence it effectively
             * becomes addition
             */
            if (left.sign != right.sign) {
                <@addBlock holderType=type.name left="left" right="right" result="result"/>
                result.sign = left.sign;
            } else {
                /* Sign of the inputs are the same, meaning we have to perform subtraction
                 * For subtraction we need left input to be greater than right input
                 * Compare the two inputs, swap if necessary
                 */
                int cmp;
                <@compareBlock holderType=type.name left="left" right="right" absCompare="true" output="cmp"/>

                if (cmp == -1) {
                    left.swap(right);
                }

                //Determine the sign of the result
                if ((left.sign == false && cmp == -1) || (left.sign == true && cmp == 1)) {
                    result.sign = true;
                } else {
                    result.sign = false;
                }

                // Perform the subtraction
                <@subtractBlock holderType=type.name left="left" right="right" result="result"/>

            }

        }
    }

    @FunctionTemplate(name = "add", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}AddFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Workspace ByteBuf buffer;
        @Output ${type.name}Holder result;

        public void setup(RecordBatch incoming) {
            int size = (${type.storage} * (org.apache.drill.common.util.DecimalUtility.integerSize));
            buffer = io.netty.buffer.Unpooled.wrappedBuffer(new byte[size]);
            buffer = new io.netty.buffer.SwappedByteBuf(buffer);
        }

        public void eval() {

            result.buffer = buffer;
            result.start = 0;

            // Re initialize the buffer everytime
            for (int i = 0; i < ${type.storage}; i++) {
                result.setInteger(i, 0);
            }

            /* If sign is different use the subtraction logic */
            if (left.sign != right.sign) {

                /* Subtract logic assumes, left input is greater than right input
                 * swap if necessary
                 */
                int cmp;
                <@compareBlock holderType=type.name left="left" right="right" absCompare="true" output="cmp"/>

                if (cmp == -1) {
                    left.swap(right);
                }
                /* Perform the subtraction */
                <@subtractBlock holderType=type.name left="left" right="right" result="result"/>
            } else {
                /* Sign of the two input decimals is the same, use the add logic */
                <@addBlock holderType=type.name left="left" right="right" result="result"/>
            }

            /* Assign the result to be the sign of the left input
             * If the two input signs are the same, we can choose either to be the resulting sign
             * If the two input signs are different, we assign left input to be the greater absolute value
             * hence result will have the same sign as left
             */
            result.sign = left.sign;
        }
    }

    @FunctionTemplate(name = "multiply", scope = FunctionTemplate.FunctionScope.DECIMAL_SUM_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}MultiplyFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Workspace ByteBuf buffer;
        @Output ${type.name}Holder result;

        public void setup(RecordBatch incoming) {
            int size = (${type.storage} * (org.apache.drill.common.util.DecimalUtility.integerSize));
            buffer = io.netty.buffer.Unpooled.wrappedBuffer(new byte[size]);
            buffer = new io.netty.buffer.SwappedByteBuf(buffer);
        }

        public void eval() {

            result.buffer = buffer;
            result.start = 0;

            // Re initialize the buffer everytime
            for (int i = 0; i < ${type.storage}; i++) {
                result.setInteger(i, 0);
            }

            /* Remove the leading zeroes from the integer part of the input */
            int leftIndex = 0;
            int leftStopIndex = left.nDecimalDigits - org.apache.drill.common.util.DecimalUtility.roundUp(left.scale);

            while (leftIndex < leftStopIndex) {
                if (left.getInteger(leftIndex) > 0)
                    break;
                leftIndex++;
            }

            int leftIntegerSize = leftStopIndex - leftIndex;

            /* Remove the leaing zeroes from the integer part of the input */
            int rightIndex = 0;
            int rightStopIndex = right.nDecimalDigits - org.apache.drill.common.util.DecimalUtility.roundUp(right.scale);

            while(rightIndex < rightStopIndex) {
                if (right.getInteger(rightIndex) > 0)
                    break;
                rightIndex++;
            }

            int rightIntegerSize = rightStopIndex - rightIndex;

            int resultIntegerSize = leftIntegerSize + rightIntegerSize;
            int resultScaleSize = org.apache.drill.common.util.DecimalUtility.roundUp(left.scale + right.scale);

            if ((resultIntegerSize + resultScaleSize) > result.nDecimalDigits) {
                throw new org.apache.drill.common.exceptions.DrillRuntimeException("Cannot fit multiplication result in the given Decimal type");
            }

            int leftSize  = left.nDecimalDigits - 1;
            int rightSize = right.nDecimalDigits - 1;

            //int resultIndex = resultSize - 1;
            int resultIndex = result.nDecimalDigits - 1;

            for (int i = leftSize; i >= leftIndex; i--) {

                int currentIndex = resultIndex;
                int carry = 0;

                for (int j = rightSize; j >= rightIndex; j--) {

                    long mulResult = (long) right.getInteger(j) * (long) left.getInteger(i);

                    //long tempSum = mulResultDigits[currentIndex] + mulResult + carry;
                    long tempSum = result.getInteger(currentIndex) + mulResult + carry;

                    if (tempSum >= org.apache.drill.common.util.DecimalUtility.DIGITS_BASE) {
                        result.setInteger(currentIndex, (int) (tempSum % org.apache.drill.common.util.DecimalUtility.DIGITS_BASE));
                        carry = (int) (tempSum / org.apache.drill.common.util.DecimalUtility.DIGITS_BASE);
                    } else {
                        result.setInteger(currentIndex, (int) tempSum);
                        carry = 0;
                    }

                    currentIndex--;
                }
                /* propogate the carry */
                if (carry > 0)
                    result.setInteger(currentIndex,  (result.getInteger(currentIndex) + carry));

                resultIndex--;
            }


            // Set the scale and precision
            result.scale = left.scale + right.scale;
            result.precision = result.maxPrecision;

            result.sign = (left.sign == right.sign) ? false : true;
        }
    }


    @FunctionTemplate(name = "compare_to", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}CompareTo implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output IntHolder out;
        public void setup(RecordBatch incoming) {}

        public void eval() {
             <@compareBlock holderType=type.name left="left" right="right" absCompare="false" output="out.value"/>
        }
    }


    @FunctionTemplate(name = "less than", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}LessThan implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup(RecordBatch incoming) {}

        public void eval() {
            int cmp;
            <@compareBlock holderType=type.name left="left" right="right" absCompare="false" output="cmp"/>
            out.value = cmp == -1 ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "less than or equal to", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}LessThanEq implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup(RecordBatch incoming) {}

        public void eval() {
            int cmp;
            <@compareBlock holderType=type.name left="left" right="right" absCompare="false" output="cmp"/>
            out.value = cmp < 1 ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "greater than", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}GreaterThan implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup(RecordBatch incoming) {}

        public void eval() {
            int cmp;
            <@compareBlock holderType=type.name left="left" right="right" absCompare="false" output="cmp"/>
            out.value = cmp == 1 ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "greater than or equal to", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}GreaterThanEq implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup(RecordBatch incoming) {}

        public void eval() {
            int cmp;
            <@compareBlock holderType=type.name left="left" right="right" absCompare="false" output="cmp"/>
            out.value = cmp > -1 ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "Equal", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}Equal implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup(RecordBatch incoming) {}

        public void eval() {
            int cmp;
            <@compareBlock holderType=type.name left="left" right="right" absCompare="false" output="cmp"/>
            out.value = cmp == 0 ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "not equal", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}NotEqual implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup(RecordBatch incoming) {}

        public void eval() {
            int cmp;
            <@compareBlock holderType=type.name left="left" right="right" absCompare="false" output="cmp"/>
            out.value = cmp != 0 ? 1 : 0;
        }
    }
}

<#elseif type.name.endsWith("Dense")>
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/${type.name}Functions.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;

@SuppressWarnings("unused")
public class ${type.name}Functions {


    @FunctionTemplate(name = "compare_to", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}CompareTo implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output IntHolder out;
        public void setup(RecordBatch incoming) {}

        public void eval() {
            int cmp;
            <@denseCompareBlock left="left" right="right" output="cmp"/>
            out.value = cmp;
        }
    }

    @FunctionTemplate(name = "less than", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}LessThan implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup(RecordBatch incoming) {}

        public void eval() {
            int cmp;
            <@denseCompareBlock left="left" right="right" output="cmp"/>
            out.value = cmp == -1 ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "less than or equal to", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}LessThanEq implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup(RecordBatch incoming) {}

        public void eval() {
            int cmp;
            <@denseCompareBlock left="left" right="right" output="cmp"/>
            out.value = cmp < 1 ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "greater than", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}GreaterThan implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup(RecordBatch incoming) {}

        public void eval() {
            int cmp;
            <@denseCompareBlock left="left" right="right" output="cmp"/>
            out.value = cmp == 1 ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "greater than or equal to", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}GreaterThanEq implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup(RecordBatch incoming) {}

        public void eval() {
            int cmp;
            <@denseCompareBlock left="left" right="right" output="cmp"/>
            out.value = cmp > -1 ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "Equal", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}Equal implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup(RecordBatch incoming) {}

        public void eval() {
            int cmp;
            <@denseCompareBlock left="left" right="right" output="cmp"/>
            out.value = cmp == 0 ? 1 : 0;
        }
    }


    @FunctionTemplate(name = "not equal", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}NotEqual implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup(RecordBatch incoming) {}

        public void eval() {

            int cmp;
            <@denseCompareBlock left="left" right="right" output="cmp"/>
            out.value = cmp != 0 ? 1 : 0;
        }
    }
}
<#elseif type.name.endsWith("Decimal9") || type.name.endsWith("Decimal18")>

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/${type.name}Functions.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;

@SuppressWarnings("unused")
public class ${type.name}Functions {

    @FunctionTemplate(name = "add", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}AddFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output ${type.name}Holder result;

        public void setup(RecordBatch incoming) {}

        public void eval() {

            <@adjustScale holderType=type.name javaType=type.storage left="left" right="right"/>

            result.value = left.value + right.value;
            result.precision = result.maxPrecision;
            result.scale = Math.max(left.scale, right.scale);
        }
    }

    @FunctionTemplate(name = "subtract", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}SubtractFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output ${type.name}Holder result;

        public void setup(RecordBatch incoming) {}

        public void eval() {

            <@adjustScale holderType=type.name javaType=type.storage left="left" right="right"/>

            result.value = left.value - right.value;
            result.precision = result.maxPrecision;
            result.scale = Math.max(left.scale, right.scale);
        }
    }
    @FunctionTemplate(name = "multiply", scope = FunctionTemplate.FunctionScope.DECIMAL_SUM_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}MultiplyFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output ${type.name}Holder result;

        public void setup(RecordBatch incoming) {}

        public void eval() {

            result.value = left.value * right.value;
            result.precision = result.maxPrecision;
            result.scale = left.scale + right.scale;
        }
    }


    @FunctionTemplate(name = "compare_to", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}CompareTo implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output IntHolder out;
        public void setup(RecordBatch incoming) {}

        public void eval() {
            <@adjustScale holderType=type.name javaType=type.storage left="left" right="right"/>
            out.value = (left.value < right.value) ? -1 : (left.value > right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "less than", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}LessThan implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup(RecordBatch incoming) {}

        public void eval() {
            int cmp;
            <@adjustScale holderType=type.name javaType=type.storage left="left" right="right"/>
            out.value = (left.value < right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "less than or equal to", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}LessThanEq implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup(RecordBatch incoming) {}

        public void eval() {
            int cmp;
            <@adjustScale holderType=type.name javaType=type.storage left="left" right="right"/>
            out.value = (left.value <= right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "greater than", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}GreaterThan implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup(RecordBatch incoming) {}

        public void eval() {
            int cmp;
            <@adjustScale holderType=type.name javaType=type.storage left="left" right="right"/>
            out.value = (left.value > right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "greater than or equal to", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}GreaterThanEq implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup(RecordBatch incoming) {}

        public void eval() {
            int cmp;
            <@adjustScale holderType=type.name javaType=type.storage left="left" right="right"/>
            out.value = (left.value >= right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "Equal", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}Equal implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup(RecordBatch incoming) {}

        public void eval() {
            int cmp;
            <@adjustScale holderType=type.name javaType=type.storage left="left" right="right"/>
            out.value = (left.value == right.value) ? 1 : 0;
        }
    }


    @FunctionTemplate(name = "not equal", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}NotEqual implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup(RecordBatch incoming) {}

        public void eval() {

            int cmp;
            <@adjustScale holderType=type.name javaType=type.storage left="left" right="right"/>
            out.value = (left.value != right.value) ? 1 : 0;
        }
    }
}

</#if>
</#list>