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

<#macro compareBlock holderType left right absCompare output>

        outside:{
            ${output} = org.apache.drill.common.util.DecimalUtility.compareSparseBytes(left.buffer, left.start, left.getSign(),
                            left.scale, left.precision, right.buffer,
                            right.start, right.getSign(), right.precision,
                            right.scale, left.WIDTH, left.nDecimalDigits, ${absCompare});

    }
</#macro>

<#macro subtractBlock holderType in1 in2 result>

            int resultScaleRoundedUp = org.apache.drill.common.util.DecimalUtility.roundUp(result.scale);
            int resultIndex = result.nDecimalDigits- 1;

            int leftScaleRoundedUp  = org.apache.drill.common.util.DecimalUtility.roundUp(${in1}.scale);
            int leftIntRoundedUp    = org.apache.drill.common.util.DecimalUtility.roundUp(${in1}.precision - ${in1}.scale);
            int rightScaleRoundedUp = org.apache.drill.common.util.DecimalUtility.roundUp(${in2}.scale);

            int leftIndex  = ${in1}.nDecimalDigits - 1;
            int rightIndex = ${in2}.nDecimalDigits - 1;

            /* If the left scale is bigger, simply copy over the digits into result */
            while (leftScaleRoundedUp > rightScaleRoundedUp) {
                result.setInteger(resultIndex, ${in1}.getInteger(leftIndex));
                leftIndex--;
                resultIndex--;
                leftScaleRoundedUp--;
            }

            /* If the right scale is bigger, subtract with zero at each array location */
            int carry = 0;
            while(rightScaleRoundedUp > leftScaleRoundedUp) {

                int difference = 0 - ${in2}.getInteger(rightIndex) - carry;
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

                int difference = ${in1}.getInteger(leftIndex) - ${in2}.getInteger(rightIndex) - carry;
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

                int difference = ${in1}.getInteger(leftIndex);
                leftIndex--;

                if (rightIndex >= 0) {
                    difference -= ${in2}.getInteger(rightIndex);
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

<#macro addBlock holderType in1 in2 result>

        int resultScaleRoundedUp = org.apache.drill.common.util.DecimalUtility.roundUp(result.scale);

        int leftScaleRoundedUp  = org.apache.drill.common.util.DecimalUtility.roundUp(${in1}.scale);
        int rightScaleRoundedUp = org.apache.drill.common.util.DecimalUtility.roundUp(${in2}.scale);

        /* starting index for each decimal */
        int leftIndex  = ${in1}.nDecimalDigits - 1;
        int rightIndex = ${in2}.nDecimalDigits - 1;
        int resultIndex = result.nDecimalDigits - 1;

        /* If one of the scale is larger then simply copy it over
         * to the result digits
         */
        while (leftScaleRoundedUp > rightScaleRoundedUp) {

            result.setInteger(resultIndex, ${in1}.getInteger(leftIndex));
            leftIndex--;
            resultIndex--;
            leftScaleRoundedUp--;
            resultScaleRoundedUp--;
        }

        while (rightScaleRoundedUp > leftScaleRoundedUp) {
            result.setInteger((resultIndex), ${in2}.getInteger(rightIndex));
            rightIndex--;
            resultIndex--;
            rightScaleRoundedUp--;
            resultScaleRoundedUp--;
        }

        int sum = 0;

        /* now the two scales are at the same level, we can add them */
        while (resultScaleRoundedUp > 0) {

            sum += ${in1}.getInteger(leftIndex) + ${in2}.getInteger(rightIndex);
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

            sum += ${in1}.getInteger(leftIndex) + ${in2}.getInteger(rightIndex);
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
        }

        while (resultIndex >= 0 && leftIndex >= 0) {
            sum += ${in1}.getInteger(leftIndex);
            leftIndex--;

            if (sum >= org.apache.drill.common.util.DecimalUtility.DIGITS_BASE) {
                result.setInteger(resultIndex, (sum - org.apache.drill.common.util.DecimalUtility.DIGITS_BASE));
                sum = 1;
            } else {
                result.setInteger(resultIndex, sum);
                sum = 0;
            }
        }

        while (resultIndex >= 0 && rightIndex >= 0) {
            sum += ${in2}.getInteger(rightIndex);
            rightIndex--;

            if (sum >= org.apache.drill.common.util.DecimalUtility.DIGITS_BASE) {
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
                left.value = (${javaType}) (org.apache.drill.common.util.DecimalUtility.adjustScaleMultiply(left.value, (int) (right.scale - left.scale)));
                left.scale = right.scale;
            } else if (right.scale < left.scale) {
                right.value = (${javaType}) (org.apache.drill.common.util.DecimalUtility.adjustScaleMultiply(right.value, (int) (left.scale - right.scale)));
                right.scale = left.scale;
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

    @FunctionTemplate(name = "subtract", scope = FunctionTemplate.FunctionScope.DECIMAL_ADD_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}SubtractFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Workspace ByteBuf buffer;
        @Workspace int outputScale;
        @Workspace int outputPrecision;
        @Output ${type.name}Holder result;

        public void setup(RecordBatch incoming) {
            int size = (${type.storage} * (org.apache.drill.common.util.DecimalUtility.integerSize));
            buffer = io.netty.buffer.Unpooled.wrappedBuffer(new byte[size]);
            buffer = new io.netty.buffer.SwappedByteBuf(buffer);
            outputPrecision = Integer.MIN_VALUE;
        }

        public void eval() {
            if (outputPrecision == Integer.MIN_VALUE) {
                org.apache.drill.common.util.DecimalScalePrecisionAddFunction resultScalePrec =
                new org.apache.drill.common.util.DecimalScalePrecisionAddFunction((int) left.precision, (int) left.scale, (int) right.precision, (int) right.scale);
                outputScale = resultScalePrec.getOutputScale();
                outputPrecision = resultScalePrec.getOutputPrecision();
            }
            result.precision = outputPrecision;
            result.scale = outputScale;
            result.buffer = buffer;
            result.start = 0;

            java.math.BigDecimal leftInput = org.apache.drill.common.util.DecimalUtility.getBigDecimalFromSparse(left.buffer, left.start, left.nDecimalDigits, left.scale);
            java.math.BigDecimal rightInput = org.apache.drill.common.util.DecimalUtility.getBigDecimalFromSparse(right.buffer, right.start, right.nDecimalDigits, right.scale);
            java.math.BigDecimal addResult = leftInput.subtract(rightInput);

            // set the scale
            addResult.setScale(result.scale, java.math.BigDecimal.ROUND_HALF_UP);
            org.apache.drill.common.util.DecimalUtility.getSparseFromBigDecimal(addResult, result.buffer, result.start, result.scale, result.precision, result.nDecimalDigits);
        }
    }

    @FunctionTemplate(name = "add", scope = FunctionTemplate.FunctionScope.DECIMAL_ADD_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}AddFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Workspace int outputScale;
        @Workspace int outputPrecision;
        @Workspace ByteBuf buffer;
        @Output ${type.name}Holder result;

        public void setup(RecordBatch incoming) {
            int size = (${type.storage} * (org.apache.drill.common.util.DecimalUtility.integerSize));
            buffer = io.netty.buffer.Unpooled.wrappedBuffer(new byte[size]);
            buffer = new io.netty.buffer.SwappedByteBuf(buffer);
            outputPrecision = Integer.MIN_VALUE;
        }

        public void eval() {
            if (outputPrecision == Integer.MIN_VALUE) {
                org.apache.drill.common.util.DecimalScalePrecisionAddFunction resultScalePrec =
                new org.apache.drill.common.util.DecimalScalePrecisionAddFunction((int) left.precision, (int) left.scale, (int) right.precision, (int) right.scale);
                outputScale = resultScalePrec.getOutputScale();
                outputPrecision = resultScalePrec.getOutputPrecision();
            }
            result.precision = outputPrecision;
            result.scale = outputScale;
            result.buffer = buffer;
            result.start = 0;

            java.math.BigDecimal leftInput = org.apache.drill.common.util.DecimalUtility.getBigDecimalFromSparse(left.buffer, left.start, left.nDecimalDigits, left.scale);
            java.math.BigDecimal rightInput = org.apache.drill.common.util.DecimalUtility.getBigDecimalFromSparse(right.buffer, right.start, right.nDecimalDigits, right.scale);
            java.math.BigDecimal addResult = leftInput.add(rightInput);

            // set the scale
            addResult.setScale(result.scale, java.math.BigDecimal.ROUND_HALF_UP);
            org.apache.drill.common.util.DecimalUtility.getSparseFromBigDecimal(addResult, result.buffer, result.start, result.scale, result.precision, result.nDecimalDigits);
        }
    }

    @FunctionTemplate(name = "multiply", scope = FunctionTemplate.FunctionScope.DECIMAL_MUL_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}MultiplyFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Workspace ByteBuf buffer;
        @Workspace int[] tempResult;
        @Workspace int outputScale;
        @Workspace int outputPrecision;
        @Output ${type.name}Holder result;

        public void setup(RecordBatch incoming) {
            int size = (${type.storage} * (org.apache.drill.common.util.DecimalUtility.integerSize));
            buffer = io.netty.buffer.Unpooled.wrappedBuffer(new byte[size]);
            buffer = new io.netty.buffer.SwappedByteBuf(buffer);
            tempResult = new int[${type.storage} * ${type.storage}];
            outputPrecision = Integer.MIN_VALUE;
        }

        public void eval() {
            if (outputPrecision == Integer.MIN_VALUE) {
                org.apache.drill.common.util.DecimalScalePrecisionMulFunction resultScalePrec =
                new org.apache.drill.common.util.DecimalScalePrecisionMulFunction((int) left.precision, (int) left.scale, (int) right.precision, (int) right.scale);
                outputScale = resultScalePrec.getOutputScale();
                outputPrecision = resultScalePrec.getOutputPrecision();
            }
            // Set the scale and precision
            result.scale = outputScale;
            result.precision = outputPrecision;
            result.buffer = buffer;
            result.start = 0;

            // Re initialize the temporary array
            for (int i = 0; i < ${type.storage} * ${type.storage}; i++) {
                tempResult[i] = 0;
            }

            // Remove the leading zeroes from the integer part of the input
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

            int leftSize  = left.nDecimalDigits - 1;
            int rightSize = right.nDecimalDigits - 1;

            int resultIndex = tempResult.length - 1;
            int currentIndex = 0;

            for (int i = leftSize; i >= leftIndex; i--) {

                currentIndex = resultIndex;
                int carry = 0;

                for (int j = rightSize; j >= rightIndex; j--) {

                    long mulResult = (long) right.getInteger(j) * (long) left.getInteger(i);

                    long tempSum = tempResult[currentIndex] + mulResult + carry;

                    if (tempSum >= org.apache.drill.common.util.DecimalUtility.DIGITS_BASE) {
                        tempResult[currentIndex] = (int) (tempSum % org.apache.drill.common.util.DecimalUtility.DIGITS_BASE);
                        carry = (int) (tempSum / org.apache.drill.common.util.DecimalUtility.DIGITS_BASE);
                    } else {
                        tempResult[currentIndex] = (int) tempSum;
                        carry = 0;
                    }

                    currentIndex--;
                }
                /* propogate the carry */
                if (carry > 0)
                    tempResult[currentIndex] += carry;

                resultIndex--;
            }

            /* We have computed the result of the multiplication; check if we need to
             * round a portion of the fractional part
             */
            resultScaleSize = org.apache.drill.common.util.DecimalUtility.roundUp(result.scale);

            if (result.scale < (left.scale + right.scale)) {
              /* The scale of the output data type is lesser than the scale
               * we obtained as a result of multiplication, we need to round
               * a chunk of the fractional part
               */
              int lastScaleIndex = currentIndex + resultIntegerSize + resultScaleSize - 1;

              // compute the power of 10 necessary to find if we need to round up
              int roundFactor = (int) (org.apache.drill.common.util.DecimalUtility.getPowerOfTen(
                                        org.apache.drill.common.util.DecimalUtility.MAX_DIGITS - ((result.scale + 1) % org.apache.drill.common.util.DecimalUtility.MAX_DIGITS)));

              // index of rounding digit
              int roundIndex = currentIndex + resultIntegerSize + org.apache.drill.common.util.DecimalUtility.roundUp(result.scale + 1) - 1;

              // Check the first chopped digit to see if we need to round up
              int carry = ((tempResult[roundIndex] / roundFactor) % 10) > 4 ? 1 : 0;

              if (result.scale > 0) {

                // Compute the power of 10 necessary to chop of the fractional part
                int scaleFactor = (int) (org.apache.drill.common.util.DecimalUtility.getPowerOfTen(
                                         org.apache.drill.common.util.DecimalUtility.MAX_DIGITS - (result.scale % org.apache.drill.common.util.DecimalUtility.MAX_DIGITS)));
                // Chop the unwanted fractional part
                tempResult[lastScaleIndex] /=  scaleFactor;
                tempResult[lastScaleIndex] *= scaleFactor;

                // Adjust the carry so that it gets added to the correct digit
                carry *= scaleFactor;
              }

              // propogate the carry
              while (carry > 0 && lastScaleIndex >= 0) {
                int tempSum = tempResult[lastScaleIndex] + carry;
                if (tempSum >= org.apache.drill.common.util.DecimalUtility.DIGITS_BASE) {
                  tempResult[lastScaleIndex] = (tempSum % org.apache.drill.common.util.DecimalUtility.DIGITS_BASE);
                  carry = (int) (tempSum / org.apache.drill.common.util.DecimalUtility.DIGITS_BASE);
                } else {
                  tempResult[lastScaleIndex] = tempSum;
                  carry = 0;
                }
                lastScaleIndex--;
              }

              // check if carry has increased integer digit
              if ((lastScaleIndex + 1) < currentIndex) {
                resultIntegerSize++;
                currentIndex = lastScaleIndex + 1;
              }
            }

            if (resultIntegerSize > result.nDecimalDigits) {
              throw new org.apache.drill.common.exceptions.DrillRuntimeException("Cannot fit multiplication result in the given decimal type");
            }

            int outputIndex = result.nDecimalDigits - 1;

            for (int i = (currentIndex + resultIntegerSize + resultScaleSize - 1); i >= currentIndex; i--) {
                result.setInteger(outputIndex--, tempResult[i]);
            }

            // Set the remaining digits to be zero
            while(outputIndex >= 0) {
              result.setInteger(outputIndex--, 0);
            }
            result.setSign(left.getSign() != right.getSign());
        }
    }

    @FunctionTemplate(name = "exact_divide", scope = FunctionTemplate.FunctionScope.DECIMAL_DIV_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}DivideFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output ${type.name}Holder result;
        @Workspace ByteBuf buffer;
        @Workspace int outputScale;
        @Workspace int outputPrecision;

        public void setup(RecordBatch incoming) {
            int size = (${type.storage} * (org.apache.drill.common.util.DecimalUtility.integerSize));
            buffer = io.netty.buffer.Unpooled.wrappedBuffer(new byte[size]);
            buffer = new io.netty.buffer.SwappedByteBuf(buffer);
            outputPrecision = Integer.MIN_VALUE;
        }

        public void eval() {
            if (outputPrecision == Integer.MIN_VALUE) {
                org.apache.drill.common.util.DecimalScalePrecisionDivideFunction resultScalePrec =
                new org.apache.drill.common.util.DecimalScalePrecisionDivideFunction((int) left.precision, (int) left.scale, (int) right.precision, (int) right.scale);
                outputScale = resultScalePrec.getOutputScale();
                outputPrecision = resultScalePrec.getOutputPrecision();
            }
            result.scale = outputScale;
            result.precision = outputPrecision;
            result.buffer = buffer;
            result.start = 0;

            java.math.BigDecimal numerator = org.apache.drill.common.util.DecimalUtility.getBigDecimalFromByteBuf(left.buffer, left.start, left.nDecimalDigits, left.scale, true);
            java.math.BigDecimal denominator = org.apache.drill.common.util.DecimalUtility.getBigDecimalFromByteBuf(right.buffer, right.start, right.nDecimalDigits, right.scale, true);

            java.math.BigDecimal output = numerator.divide(denominator, (int) result.scale, java.math.BigDecimal.ROUND_HALF_UP);

            org.apache.drill.common.util.DecimalUtility.getSparseFromBigDecimal(output, result.buffer, result.start, result.scale, result.precision, result.nDecimalDigits);
        }
    }

    @FunctionTemplate(name = "mod", scope = FunctionTemplate.FunctionScope.DECIMAL_MOD_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}ModFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output ${type.name}Holder result;
        @Workspace ByteBuf buffer;
        @Workspace int outputScale;
        @Workspace int outputPrecision;

        public void setup(RecordBatch incoming) {
            int size = (${type.storage} * (org.apache.drill.common.util.DecimalUtility.integerSize));
            buffer = io.netty.buffer.Unpooled.wrappedBuffer(new byte[size]);
            buffer = new io.netty.buffer.SwappedByteBuf(buffer);
            outputPrecision = Integer.MIN_VALUE;
        }

        public void eval() {
            if (outputPrecision == Integer.MIN_VALUE) {
                org.apache.drill.common.util.DecimalScalePrecisionModFunction resultScalePrec =
                new org.apache.drill.common.util.DecimalScalePrecisionModFunction((int) left.precision, (int) left.scale, (int) right.precision, (int) right.scale);
                outputScale = resultScalePrec.getOutputScale();
                outputPrecision = resultScalePrec.getOutputPrecision();
            }
            result.scale = outputScale;
            result.precision = outputPrecision;
            result.buffer = buffer;
            result.start = 0;

            java.math.BigDecimal numerator = org.apache.drill.common.util.DecimalUtility.getBigDecimalFromByteBuf(left.buffer, left.start, left.nDecimalDigits, left.scale, true);
            java.math.BigDecimal denominator = org.apache.drill.common.util.DecimalUtility.getBigDecimalFromByteBuf(right.buffer, right.start, right.nDecimalDigits, right.scale, true);

            java.math.BigDecimal output = numerator.remainder(denominator);
            output.setScale(result.scale, java.math.BigDecimal.ROUND_HALF_UP);

            org.apache.drill.common.util.DecimalUtility.getSparseFromBigDecimal(output, result.buffer, result.start, result.scale, result.precision, result.nDecimalDigits);
        }
    }

    @FunctionTemplate(name = "abs", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}AbsFunction implements DrillSimpleFunc {

        @Param  ${type.name}Holder in;
        @Output ${type.name}Holder out;
        public void setup(RecordBatch incoming) {

        }

        public void eval() {
          out.scale = in.scale;
          out.precision = in.precision;
          out.buffer = in.buffer;
          out.start = in.start;

          // set the output buffer with the positive sign
          out.buffer.setInt(out.start, (out.buffer.getInt(out.start) & 0x7fffffff));
        }
    }

    @FunctionTemplate(name = "sign", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}SignFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder in;
        @Output IntHolder out;

        public void setup(RecordBatch incoming) {}

        public void eval() {

          boolean zeroValue = true;

          if (in.getSign() == true) {
            out.value = -1;
          } else {
            for (int i = 0; i < ${type.storage}; i++) {
              if (in.getInteger(i) != 0) {
                zeroValue = false;
                break;
              }
            }
            out.value = (zeroValue == true) ? 0 : 1;
          }
        }
    }

    @FunctionTemplate(names = {"ceil", "ceiling"}, scope = FunctionTemplate.FunctionScope.DECIMAL_ZERO_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}CeilFunction implements DrillSimpleFunc {

        @Param  ${type.name}Holder in;
        @Output ${type.name}Holder out;
        public void setup(RecordBatch incoming) {

        }

        public void eval() {
          out.scale = 0;
          out.precision = in.precision;
          out.buffer = in.buffer;
          out.start = in.start;
          boolean sign = in.getSign();

          // Indicates whether we need to add 1 to the integer part, while performing ceil
          int carry = 0;

          int scaleStartIndex = ${type.storage} - org.apache.drill.common.util.DecimalUtility.roundUp(in.scale);
          int srcIntIndex = scaleStartIndex - 1;

          if (sign == false) {
            // For negative values ceil we don't need to increment the integer part
            while (scaleStartIndex < ${type.storage}) {
              if (out.getInteger(scaleStartIndex) != 0) {
                carry = 1;
                break;
              }
              scaleStartIndex++;
            }
          }

          // Truncate the fractional part, move the integer part
          int destIndex = ${type.storage} - 1;
          while (srcIntIndex >= 0) {
            out.setInteger(destIndex--, out.getInteger(srcIntIndex--));
          }

          // Set the remaining portion of the decimal to be zeroes
          while (destIndex >= 0) {
            out.setInteger(destIndex--, 0);
          }

          // Add the carry
          if (carry != 0) {
            destIndex = ${type.storage} - 1;

            while (destIndex >= 0) {
              int intValue = out.getInteger(destIndex);
              intValue += carry;

              if (intValue >= org.apache.drill.common.util.DecimalUtility.DIGITS_BASE) {
                out.setInteger(destIndex--, intValue % org.apache.drill.common.util.DecimalUtility.DIGITS_BASE);
                carry = intValue / org.apache.drill.common.util.DecimalUtility.DIGITS_BASE;
              } else {
                out.setInteger(destIndex--, intValue);
                break;
              }
            }
          }
          // set the sign
          out.setSign(sign);
        }
    }

    @FunctionTemplate(name = "floor", scope = FunctionTemplate.FunctionScope.DECIMAL_ZERO_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}FloorFunction implements DrillSimpleFunc {

        @Param  ${type.name}Holder in;
        @Output ${type.name}Holder out;
        public void setup(RecordBatch incoming) {

        }

        public void eval() {
          out.scale = 0;
          out.precision = in.precision;
          out.buffer = in.buffer;
          out.start = in.start;
          boolean sign = in.getSign();

          // Indicates whether we need to decrement 1 from the integer part, while performing floor, done for -ve values
          int carry = 0;

          int scaleStartIndex = ${type.storage} - org.apache.drill.common.util.DecimalUtility.roundUp(in.scale);
          int srcIntIndex = scaleStartIndex - 1;

          if (sign == true) {
            // For negative values ceil we don't need to increment the integer part
            while (scaleStartIndex < ${type.storage}) {
              if (out.getInteger(scaleStartIndex) != 0) {
                carry = 1;
                break;
              }
              scaleStartIndex++;
            }
          }

          // Truncate the fractional part, move the integer part
          int destIndex = ${type.storage} - 1;
          while (srcIntIndex >= 0) {
            out.setInteger(destIndex--, out.getInteger(srcIntIndex--));
          }

          // Set the remaining portion of the decimal to be zeroes
          while (destIndex >= 0) {
            out.setInteger(destIndex--, 0);
          }
          // Add the carry
          if (carry != 0) {
            destIndex = ${type.storage} - 1;

            while (destIndex >= 0) {
              int intValue = out.getInteger(destIndex);
              intValue += carry;

              if (intValue >= org.apache.drill.common.util.DecimalUtility.DIGITS_BASE) {
                out.setInteger(destIndex--, intValue % org.apache.drill.common.util.DecimalUtility.DIGITS_BASE);
                carry = intValue / org.apache.drill.common.util.DecimalUtility.DIGITS_BASE;
              } else {
                out.setInteger(destIndex--, intValue);
                break;
              }
            }
          }
          // set the sign
          out.setSign(sign);
        }
    }

    @FunctionTemplate(names = {"trunc", "truncate"}, scope = FunctionTemplate.FunctionScope.DECIMAL_ZERO_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}TruncateFunction implements DrillSimpleFunc {

        @Param  ${type.name}Holder in;
        @Output ${type.name}Holder out;
        public void setup(RecordBatch incoming) {

        }

        public void eval() {
          out.scale = 0;
          out.precision = in.precision;
          out.buffer = in.buffer;
          out.start = in.start;
          boolean sign = in.getSign();

          // Integer part's src index
          int srcIntIndex = ${type.storage} - org.apache.drill.common.util.DecimalUtility.roundUp(in.scale) - 1;

          // Truncate the fractional part, move the integer part
          int destIndex = ${type.storage} - 1;
          while (srcIntIndex >= 0) {
            out.setInteger(destIndex--, out.getInteger(srcIntIndex--));
          }

          // Set the remaining portion of the decimal to be zeroes
          while (destIndex >= 0) {
            out.setInteger(destIndex--, 0);
          }
            // set the sign
            out.setSign(sign);
        }
    }

    @FunctionTemplate(names = {"trunc", "truncate"}, scope = FunctionTemplate.FunctionScope.DECIMAL_SET_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}TruncateScaleFunction implements DrillSimpleFunc {

        @Param  ${type.name}Holder left;
        @Param  IntHolder right;
        @Output ${type.name}Holder result;
        public void setup(RecordBatch incoming) {

        }

        public void eval() {
          result.scale = right.value;
          result.precision = left.precision;
          result.buffer = left.buffer;
          result.start = left.start;
          boolean sign = left.getSign();

          int newScaleRoundedUp  = org.apache.drill.common.util.DecimalUtility.roundUp(right.value);
          int origScaleRoundedUp = org.apache.drill.common.util.DecimalUtility.roundUp(left.scale);

          if (right.value < left.scale) {
            // Get the source index beyond which we will truncate
            int srcIntIndex = ${type.storage} - origScaleRoundedUp - 1;
            int srcIndex = srcIntIndex + newScaleRoundedUp;

            // Truncate the remaining fractional part, move the integer part
            int destIndex = ${type.storage} - 1;
            if (srcIndex != destIndex) {
              while (srcIndex >= 0) {
                result.setInteger(destIndex--, result.getInteger(srcIndex--));
              }

              // Set the remaining portion of the decimal to be zeroes
              while (destIndex >= 0) {
                result.setInteger(destIndex--, 0);
              }
            }

            // We truncated the decimal digit. Now we need to truncate within the base 1 billion fractional digit
            int truncateFactor = org.apache.drill.common.util.DecimalUtility.MAX_DIGITS - (right.value % org.apache.drill.common.util.DecimalUtility.MAX_DIGITS);
            if (truncateFactor != org.apache.drill.common.util.DecimalUtility.MAX_DIGITS) {
              truncateFactor = (int) org.apache.drill.common.util.DecimalUtility.getPowerOfTen(truncateFactor);
              int fractionalDigits = result.getInteger(${type.storage} - 1);
              fractionalDigits /= truncateFactor;
              result.setInteger(${type.storage} - 1, fractionalDigits * truncateFactor);
            }
          } else if (right.value > left.scale) {
            // Add fractional digits to the decimal

            // Check if we need to shift the decimal digits to the left
            if (newScaleRoundedUp > origScaleRoundedUp) {
              int srcIndex  = 0;
              int destIndex = newScaleRoundedUp - origScaleRoundedUp;

              // Check while extending scale, we are not overwriting integer part
              while (srcIndex < destIndex) {
                if (result.getInteger(srcIndex++) != 0) {
                  throw new org.apache.drill.common.exceptions.DrillRuntimeException("Truncate resulting in loss of integer part, reduce scale specified");
                }
              }

              srcIndex = 0;
              while (destIndex < ${type.storage}) {
                result.setInteger(srcIndex++, result.getInteger(destIndex++));
              }

              // Clear the remaining part
              while (srcIndex < ${type.storage}) {
                result.setInteger(srcIndex++, 0);
              }
            }
          }
            // set the sign
            result.setSign(sign);
        }
    }

    @FunctionTemplate(name = "round", scope = FunctionTemplate.FunctionScope.DECIMAL_ZERO_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}RoundFunction implements DrillSimpleFunc {

        @Param  ${type.name}Holder in;
        @Output ${type.name}Holder out;
        public void setup(RecordBatch incoming) {

        }

        public void eval() {
          out.scale = 0;
          out.precision = in.precision;
          out.buffer = in.buffer;
          out.start = in.start;
          boolean sign = in.getSign();

          boolean roundUp = false;

          // Get the first fractional digit to see if want to round up or not
          int scaleIndex = ${type.storage} - org.apache.drill.common.util.DecimalUtility.roundUp(in.scale);
          if (scaleIndex < ${type.storage}) {
            int fractionalPart = out.getInteger(scaleIndex);
            int digit = fractionalPart / (org.apache.drill.common.util.DecimalUtility.DIGITS_BASE / 10);

            if (digit > 4) {
              roundUp = true;
            }
          }

          // Integer part's src index
          int srcIntIndex = scaleIndex - 1;

          // Truncate the fractional part, move the integer part
          int destIndex = ${type.storage} - 1;
          while (srcIntIndex >= 0) {
            out.setInteger(destIndex--, out.getInteger(srcIntIndex--));
          }

          // Set the remaining portion of the decimal to be zeroes
          while (destIndex >= 0) {
            out.setInteger(destIndex--, 0);
          }

          // Perform the roundup
          srcIntIndex = ${type.storage} - 1;
          if (roundUp == true) {
            while (srcIntIndex >= 0) {
              int value = out.getInteger(srcIntIndex) + 1;
              if (value >= org.apache.drill.common.util.DecimalUtility.DIGITS_BASE) {
                out.setInteger(srcIntIndex--, value % org.apache.drill.common.util.DecimalUtility.DIGITS_BASE);
                value = value / org.apache.drill.common.util.DecimalUtility.DIGITS_BASE;
              } else {
                out.setInteger(srcIntIndex--, value);
                break;
              }
            }
          }
            // set the sign
            out.setSign(sign);
        }
    }

    @FunctionTemplate(name = "round", scope = FunctionTemplate.FunctionScope.DECIMAL_SET_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}RoundScaleFunction implements DrillSimpleFunc {

        @Param  ${type.name}Holder left;
        @Param  IntHolder right;
        @Output ${type.name}Holder result;
        public void setup(RecordBatch incoming) {

        }

        public void eval() {
          result.scale = right.value;
          result.precision = left.precision;
          result.buffer = left.buffer;
          result.start = left.start;
          boolean sign = left.getSign();

          org.apache.drill.common.util.DecimalUtility.roundDecimal(result.buffer, result.start, result.nDecimalDigits, result.scale, left.scale);
          // set the sign
          result.setSign(sign);
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
            out.value = org.apache.drill.common.util.DecimalUtility.compareDenseBytes(left.buffer, left.start, left.getSign(), right.buffer, right.start, right.getSign(), left.WIDTH);
        }
    }

    @FunctionTemplate(name = "less than", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}LessThan implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup(RecordBatch incoming) {}

        public void eval() {
            int cmp  = org.apache.drill.common.util.DecimalUtility.compareDenseBytes(left.buffer, left.start, left.getSign(), right.buffer, right.start, right.getSign(), left.WIDTH);
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
            int cmp = org.apache.drill.common.util.DecimalUtility.compareDenseBytes(left.buffer, left.start, left.getSign(), right.buffer, right.start, right.getSign(), left.WIDTH);
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
            int cmp = org.apache.drill.common.util.DecimalUtility.compareDenseBytes(left.buffer, left.start, left.getSign(), right.buffer, right.start, right.getSign(), left.WIDTH);
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
            int cmp = org.apache.drill.common.util.DecimalUtility.compareDenseBytes(left.buffer, left.start, left.getSign(), right.buffer, right.start, right.getSign(), left.WIDTH);
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
            int cmp = org.apache.drill.common.util.DecimalUtility.compareDenseBytes(left.buffer, left.start, left.getSign(), right.buffer, right.start, right.getSign(), left.WIDTH);
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

            int cmp = org.apache.drill.common.util.DecimalUtility.compareDenseBytes(left.buffer, left.start, left.getSign(), right.buffer, right.start, right.getSign(), left.WIDTH);
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
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;

@SuppressWarnings("unused")
public class ${type.name}Functions {

    @FunctionTemplate(name = "add", scope = FunctionTemplate.FunctionScope.DECIMAL_ADD_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}AddFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Workspace int outputScale;
        @Workspace int outputPrecision;
        @Output ${type.name}Holder result;

        public void setup(RecordBatch incoming) {
            outputPrecision = Integer.MIN_VALUE;
        }


        public void eval() {
            if (outputPrecision == Integer.MIN_VALUE) {
                org.apache.drill.common.util.DecimalScalePrecisionAddFunction resultScalePrec =
                new org.apache.drill.common.util.DecimalScalePrecisionAddFunction((int) left.precision, (int) left.scale, (int) right.precision, (int) right.scale);
                outputScale = resultScalePrec.getOutputScale();
                outputPrecision = resultScalePrec.getOutputPrecision();
            }
            <@adjustScale holderType=type.name javaType=type.storage left="left" right="right"/>

            result.value = left.value + right.value;
            result.precision = outputPrecision;
            result.scale = outputScale;
        }
    }

    @FunctionTemplate(name = "subtract", scope = FunctionTemplate.FunctionScope.DECIMAL_ADD_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}SubtractFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Workspace int outputScale;
        @Workspace int outputPrecision;
        @Output ${type.name}Holder result;

        public void setup(RecordBatch incoming) {
            outputPrecision = Integer.MIN_VALUE;
        }

        public void eval() {
            if (outputPrecision == Integer.MIN_VALUE) {
                org.apache.drill.common.util.DecimalScalePrecisionAddFunction resultScalePrec =
                new org.apache.drill.common.util.DecimalScalePrecisionAddFunction((int) left.precision, (int) left.scale, (int) right.precision, (int) right.scale);
                outputScale = resultScalePrec.getOutputScale();
                outputPrecision = resultScalePrec.getOutputPrecision();
            }
            <@adjustScale holderType=type.name javaType=type.storage left="left" right="right"/>

            result.value = left.value - right.value;
            result.precision = outputPrecision;
            result.scale = outputScale;
        }
    }
    @FunctionTemplate(name = "multiply", scope = FunctionTemplate.FunctionScope.DECIMAL_MUL_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}MultiplyFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Workspace int outputScale;
        @Workspace int outputPrecision;
        @Output ${type.name}Holder result;

        public void setup(RecordBatch incoming) {
            outputPrecision = Integer.MIN_VALUE;
        }

        public void eval() {
            if (outputPrecision == Integer.MIN_VALUE) {
                org.apache.drill.common.util.DecimalScalePrecisionMulFunction resultScalePrec =
                new org.apache.drill.common.util.DecimalScalePrecisionMulFunction((int) left.precision, (int) left.scale, (int) right.precision, (int) right.scale);
                outputScale = resultScalePrec.getOutputScale();
                outputPrecision = resultScalePrec.getOutputPrecision();
            }
            result.value = left.value * right.value;
            result.precision = outputPrecision;
            result.scale = outputScale;
        }
    }

    @FunctionTemplate(name = "abs", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}AbsFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder in;
        @Output ${type.name}Holder out;

        public void setup(RecordBatch incoming) {}

        public void eval() {
            out.precision = out.maxPrecision;
            out.scale = in.scale;

            out.value = in.value;

            if (out.value < 0){
                out.value *= -1;
            }
        }
    }

    @FunctionTemplate(name = "exact_divide", scope = FunctionTemplate.FunctionScope.DECIMAL_DIV_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}DivideFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output ${type.name}Holder result;
        @Workspace int outputScale;
        @Workspace int outputPrecision;

        public void setup(RecordBatch incoming) {
            outputPrecision = Integer.MIN_VALUE;
        }

        public void eval() {

            if (outputPrecision == Integer.MIN_VALUE) {
                org.apache.drill.common.util.DecimalScalePrecisionDivideFunction resultScalePrec =
                new org.apache.drill.common.util.DecimalScalePrecisionDivideFunction((int) left.precision, (int) left.scale, (int) right.precision, (int) right.scale);
                outputScale = resultScalePrec.getOutputScale();
                outputPrecision = resultScalePrec.getOutputPrecision();
            }
            result.scale = outputScale;
            result.precision = outputPrecision;

            java.math.BigDecimal numerator = new java.math.BigDecimal(java.math.BigInteger.valueOf(left.value), left.scale);
            java.math.BigDecimal denominator = new java.math.BigDecimal(java.math.BigInteger.valueOf(right.value), right.scale);

            java.math.BigDecimal output = numerator.divide(denominator, (int) result.scale, java.math.BigDecimal.ROUND_HALF_UP);

            result.value = output.unscaledValue().${type.storage}Value();
        }
    }

    @FunctionTemplate(name = "mod", scope = FunctionTemplate.FunctionScope.DECIMAL_MOD_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}ModFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Workspace int outputScale;
        @Workspace int outputPrecision;
        @Output ${type.name}Holder result;

        public void setup(RecordBatch incoming) {
            outputPrecision = Integer.MIN_VALUE;
        }

        public void eval() {
            if (outputPrecision == Integer.MIN_VALUE) {
                org.apache.drill.common.util.DecimalScalePrecisionModFunction resultScalePrec =
                new org.apache.drill.common.util.DecimalScalePrecisionModFunction((int) left.precision, (int) left.scale, (int) right.precision, (int) right.scale);
                outputScale = resultScalePrec.getOutputScale();
                outputPrecision = resultScalePrec.getOutputPrecision();
            }
            result.precision = outputPrecision;
            result.scale = outputScale;
            java.math.BigDecimal numerator = new java.math.BigDecimal(java.math.BigInteger.valueOf(left.value), left.scale);
            java.math.BigDecimal denominator = new java.math.BigDecimal(java.math.BigInteger.valueOf(right.value), right.scale);

            java.math.BigDecimal output = numerator.remainder(denominator);
            output.setScale(result.scale, java.math.BigDecimal.ROUND_HALF_UP);

            result.value = output.unscaledValue().${type.storage}Value();
        }
    }

    @FunctionTemplate(name = "sign", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}SignFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder in;
        @Output IntHolder out;

        public void setup(RecordBatch incoming) {}

        public void eval() {

            out.value = (in.value < 0) ? -1 : ((in.value > 0) ? 1 : 0);
        }
    }

    @FunctionTemplate(names = {"trunc", "truncate"}, scope = FunctionTemplate.FunctionScope.DECIMAL_ZERO_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}TruncFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder in;
        @Output ${type.name}Holder out;

        public void setup(RecordBatch incoming) {}

        public void eval() {

            out.value =(${type.storage}) (org.apache.drill.common.util.DecimalUtility.adjustScaleDivide(in.value, (int) in.scale));
            out.precision = out.maxPrecision;
            out.scale = 0;
        }
    }

    @FunctionTemplate(names = {"trunc", "truncate"}, scope = FunctionTemplate.FunctionScope.DECIMAL_SET_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}TruncateScaleFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param IntHolder right;
        @Output ${type.name}Holder out;

        public void setup(RecordBatch incoming) {}

        public void eval() {

            out.value = (${type.storage}) (org.apache.drill.common.util.DecimalUtility.adjustScaleDivide(left.value, (int) (left.scale - right.value)));
            out.precision = out.maxPrecision;
            out.scale = right.value;
        }
    }

    @FunctionTemplate(names = {"ceil", "ceiling"}, scope = FunctionTemplate.FunctionScope.DECIMAL_ZERO_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}CeilFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder in;
        @Output ${type.name}Holder out;

        public void setup(RecordBatch incoming) {

        }

        public void eval() {
          ${type.storage} scaleFactor = (${type.storage}) (org.apache.drill.common.util.DecimalUtility.getPowerOfTen((int) in.scale));

          // Get the integer part
          ${type.storage} integerPart = in.value / scaleFactor;

          // Get the fractional part, if its non-zero increment the integer part
          ${type.storage} fractionalPart = (${type.storage}) (in.value % scaleFactor);
          if (fractionalPart != 0 && in.value >= 0) {
            integerPart++;
          }

          out.scale = 0;
          out.value = integerPart;
        }
    }

    @FunctionTemplate(name = "floor", scope = FunctionTemplate.FunctionScope.DECIMAL_ZERO_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}FloorFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder in;
        @Output ${type.name}Holder out;

        public void setup(RecordBatch incoming) {
        }

        public void eval() {

          ${type.storage} scaleFactor = (${type.storage}) (org.apache.drill.common.util.DecimalUtility.getPowerOfTen((int) in.scale));
          out.scale = 0;
          out.value = (in.value / scaleFactor);

          // For negative values we have to decrement by 1
          if (in.value < 0) {
            ${type.storage} fractionalPart = (${type.storage}) (in.value % scaleFactor);
            if (fractionalPart != 0) {
              out.value--;
            }
          }
        }
    }

    @FunctionTemplate(name = "round", scope = FunctionTemplate.FunctionScope.DECIMAL_ZERO_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}RoundFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder in;
        @Output ${type.name}Holder out;

        public void setup(RecordBatch incoming) {
        }

        public void eval() {

          ${type.storage} scaleFactor = (${type.storage}) (org.apache.drill.common.util.DecimalUtility.getPowerOfTen((int) in.scale));
          ${type.storage} extractDigit = scaleFactor / 10;

          out.scale = 0;
          // Assign the integer part to the output
          out.value = in.value / scaleFactor;

          // Get the fractional part
          ${type.storage} fractionalPart = in.value % scaleFactor;
          // Get the first digit to check for rounding
          int digit = Math.abs((int) (fractionalPart / extractDigit));

          if (digit > 4) {
            if (in.value > 0) {
              out.value++;
            } else if (in.value < 0) {
              out.value--;
            }
          }
        }
    }

    @FunctionTemplate(name = "round", scope = FunctionTemplate.FunctionScope.DECIMAL_SET_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}RoundScaleFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param IntHolder right;
        @Output ${type.name}Holder out;


        public void setup(RecordBatch incoming) {
        }

        public void eval() {

          ${type.storage} scaleFactor = (${type.storage}) (org.apache.drill.common.util.DecimalUtility.getPowerOfTen((int) left.scale));
          ${type.storage} newScaleFactor = (${type.storage}) (org.apache.drill.common.util.DecimalUtility.getPowerOfTen((int) right.value));
          ${type.storage} truncScaleFactor = (${type.storage}) (org.apache.drill.common.util.DecimalUtility.getPowerOfTen( Math.abs(left.scale - right.value)));
          int truncFactor = (int) (left.scale - right.value);

          // If rounding scale is >= current scale
          if (right.value >= left.scale) {
            out.value = (${type.storage}) (org.apache.drill.common.util.DecimalUtility.adjustScaleMultiply(left.value, (int) (right.value - left.scale)));
          }
          else {
            out.scale = right.value;
            // Assign the integer part to the output
            out.value = left.value / scaleFactor;

            // Get the fractional part
            ${type.storage} fractionalPart = left.value % scaleFactor;

            // From the entire fractional part extract the digits upto which rounding is needed
            ${type.storage} newFractionalPart = (${type.storage}) (org.apache.drill.common.util.DecimalUtility.adjustScaleDivide(fractionalPart, truncFactor));
            ${type.storage} truncatedFraction = fractionalPart % truncScaleFactor;


            // Get the truncated fractional part and extract the first digit to see if we need to add 1
            int digit = Math.abs((int) org.apache.drill.common.util.DecimalUtility.adjustScaleDivide(truncatedFraction, truncFactor - 1));

            if (digit > 4) {
              if (left.value > 0) {
                newFractionalPart++;
              } else if (left.value < 0) {
                newFractionalPart--;
              }
            }

            out.value = (out.value * newScaleFactor) + newFractionalPart;
          }
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