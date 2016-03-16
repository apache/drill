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

<#-- TODO:  Refactor comparison code from here into ComparisonFunctions (to
     eliminate duplicate template code and so that ComparisonFunctions actually
     as all comparison functions. -->

<#macro compareNullsSubblock leftType rightType output breakTarget nullCompare nullComparesHigh>
  <#if nullCompare>
    <#if nullComparesHigh>
      <#assign leftNullResult = 1> <#-- if only L is null and nulls are high, then "L > R" (1) -->
      <#assign rightNullResult = -1>
    <#else>
      <#assign leftNullResult = -1> <#-- if only L is null and nulls are low, then "L < R" (-1) -->
      <#assign rightNullResult = 1>
    </#if>

    <#if leftType?starts_with("Nullable")>
      <#if rightType?starts_with("Nullable")>
        <#-- Both are nullable. -->
        if ( left.isSet == 0 ) {
          if ( right.isSet == 0 ) {
            <#-- Both are null--result is "L = R". -->
            ${output} = 0;
            break ${breakTarget};
          } else {
            <#-- Only left is null--result is "L < R" or "L > R" per null ordering. -->
            ${output} = ${leftNullResult};
            break ${breakTarget};
          }
        } else if ( right.isSet == 0 ) {
          <#-- Only right is null--result is "L > R" or "L < R" per null ordering. -->
          ${output} = ${rightNullResult};
          break ${breakTarget};
        }
      <#else>
        <#-- Left is nullable but right is not. -->
        if ( left.isSet == 0 ) {
          <#-- Only left is null--result is "L < R" or "L > R" per null ordering. -->
          ${output} = ${leftNullResult};
          break ${breakTarget};
        }
      </#if>
    <#elseif rightType?starts_with("Nullable")>
      <#-- Left is not nullable but right is. -->
      if ( right.isSet == 0 ) {
        <#-- Only right is null--result is "L > R" or "L < R" per null ordering. -->
        ${output} = ${rightNullResult};
        break ${breakTarget};
      }
    </#if>
  </#if>

</#macro>


<#macro compareBlock leftType rightType absCompare output nullCompare nullComparesHigh>
         outside:
          {
            <@compareNullsSubblock leftType=leftType rightType=rightType output=output breakTarget="outside" nullCompare=nullCompare nullComparesHigh=nullComparesHigh />

            ${output} = org.apache.drill.exec.util.DecimalUtility.compareSparseBytes(left.buffer, left.start, left.getSign(left.start, left.buffer),
                            left.scale, left.precision, right.buffer,
                            right.start, right.getSign(right.start, right.buffer), right.precision,
                            right.scale, left.WIDTH, left.nDecimalDigits, ${absCompare});

          } // outside
</#macro>

<#macro adjustScale javaType leftType rightType>

            // Adjust the scale of the two inputs to be the same

            if (left.scale < right.scale) {
                left.value = (${javaType}) (org.apache.drill.exec.util.DecimalUtility.adjustScaleMultiply(left.value, (int) (right.scale - left.scale)));
                left.scale = right.scale;
            } else if (right.scale < left.scale) {
                right.value = (${javaType}) (org.apache.drill.exec.util.DecimalUtility.adjustScaleMultiply(right.value, (int) (left.scale - right.scale)));
                right.scale = left.scale;
            }
</#macro>

<#-- For each DECIMAL... type (in DecimalTypes.tdd) ... -->
<#list comparisonTypesDecimal.decimalTypes as type>

<#if type.name.endsWith("Sparse")>

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/${type.name}Functions.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl;

<#include "/@includes/vv_imports.ftl" />

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;

import java.nio.ByteBuffer;

@SuppressWarnings("unused")
public class ${type.name}Functions {

    @FunctionTemplate(name = "subtract", scope = FunctionTemplate.FunctionScope.DECIMAL_ADD_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}SubtractFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Inject DrillBuf buffer;
        @Workspace int outputScale;
        @Workspace int outputPrecision;
        @Output ${type.name}Holder result;

        public void setup() {
            int size = (${type.storage} * (org.apache.drill.exec.util.DecimalUtility.INTEGER_SIZE));
            buffer = buffer.reallocIfNeeded(size);
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

            java.math.BigDecimal leftInput = org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromSparse(left.buffer, left.start, left.nDecimalDigits, left.scale);
            java.math.BigDecimal rightInput = org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromSparse(right.buffer, right.start, right.nDecimalDigits, right.scale);
            java.math.BigDecimal addResult = leftInput.subtract(rightInput);

            // Set the scale
            addResult.setScale(result.scale, java.math.BigDecimal.ROUND_HALF_UP);
            org.apache.drill.exec.util.DecimalUtility.getSparseFromBigDecimal(addResult, result.buffer, result.start, result.scale, result.precision, result.nDecimalDigits);
        }
    }

    @FunctionTemplate(name = "add", scope = FunctionTemplate.FunctionScope.DECIMAL_ADD_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}AddFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Workspace int outputScale;
        @Workspace int outputPrecision;
        @Inject DrillBuf buffer;
        @Output ${type.name}Holder result;

        public void setup() {
            int size = (${type.storage} * (org.apache.drill.exec.util.DecimalUtility.INTEGER_SIZE));
            buffer = buffer.reallocIfNeeded(size);
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

            java.math.BigDecimal leftInput = org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromSparse(left.buffer, left.start, left.nDecimalDigits, left.scale);
            java.math.BigDecimal rightInput = org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromSparse(right.buffer, right.start, right.nDecimalDigits, right.scale);
            java.math.BigDecimal addResult = leftInput.add(rightInput);

            // Set the scale
            addResult.setScale(result.scale, java.math.BigDecimal.ROUND_HALF_UP);
            org.apache.drill.exec.util.DecimalUtility.getSparseFromBigDecimal(addResult, result.buffer, result.start, result.scale, result.precision, result.nDecimalDigits);
        }
    }

    @FunctionTemplate(name = "multiply", scope = FunctionTemplate.FunctionScope.DECIMAL_MUL_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}MultiplyFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Inject DrillBuf buffer;
        @Workspace int[] tempResult;
        @Workspace int outputScale;
        @Workspace int outputPrecision;
        @Output ${type.name}Holder result;

        public void setup() {
            int size = (${type.storage} * (org.apache.drill.exec.util.DecimalUtility.INTEGER_SIZE));
            buffer = buffer.reallocIfNeeded(size);
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
            int leftStopIndex = left.nDecimalDigits - org.apache.drill.exec.util.DecimalUtility.roundUp(left.scale);

            while (leftIndex < leftStopIndex) {
                if (left.getInteger(leftIndex, left.start, left.buffer) > 0)
                    break;
                leftIndex++;
            }

            int leftIntegerSize = leftStopIndex - leftIndex;

            /* Remove the leading zeroes from the integer part of the input */
            int rightIndex = 0;
            int rightStopIndex = right.nDecimalDigits - org.apache.drill.exec.util.DecimalUtility.roundUp(right.scale);

            while(rightIndex < rightStopIndex) {
                if (right.getInteger(rightIndex, right.start, right.buffer) > 0)
                    break;
                rightIndex++;
            }

            int rightIntegerSize = rightStopIndex - rightIndex;

            int resultIntegerSize = leftIntegerSize + rightIntegerSize;
            int resultScaleSize = org.apache.drill.exec.util.DecimalUtility.roundUp(left.scale + right.scale);

            int leftSize  = left.nDecimalDigits - 1;
            int rightSize = right.nDecimalDigits - 1;

            int resultIndex = tempResult.length - 1;
            int currentIndex = 0;

            for (int i = leftSize; i >= leftIndex; i--) {

                currentIndex = resultIndex;
                int carry = 0;

                for (int j = rightSize; j >= rightIndex; j--) {

                    long mulResult = (long) right.getInteger(j, right.start, right.buffer) * (long) left.getInteger(i, left.start, left.buffer);

                    long tempSum = tempResult[currentIndex] + mulResult + carry;

                    if (tempSum >= org.apache.drill.exec.util.DecimalUtility.DIGITS_BASE) {
                        tempResult[currentIndex] = (int) (tempSum % org.apache.drill.exec.util.DecimalUtility.DIGITS_BASE);
                        carry = (int) (tempSum / org.apache.drill.exec.util.DecimalUtility.DIGITS_BASE);
                    } else {
                        tempResult[currentIndex] = (int) tempSum;
                        carry = 0;
                    }

                    currentIndex--;
                }
                /* Propagate the carry */
                if (carry > 0)
                    tempResult[currentIndex] += carry;

                resultIndex--;
            }

            /* We have computed the result of the multiplication; check if we need to
             * round a portion of the fractional part
             */
            resultScaleSize = org.apache.drill.exec.util.DecimalUtility.roundUp(result.scale);

            if (result.scale < (left.scale + right.scale)) {
              /* The scale of the output data type is less than the scale
               * we obtained as a result of multiplication, we need to round
               * a chunk of the fractional part
               */
              int lastScaleIndex = currentIndex + resultIntegerSize + resultScaleSize - 1;

              // Compute the power of 10 necessary to find if we need to round up
              int roundFactor = (int) (org.apache.drill.exec.util.DecimalUtility.getPowerOfTen(
                                        org.apache.drill.exec.util.DecimalUtility.MAX_DIGITS - ((result.scale + 1) % org.apache.drill.exec.util.DecimalUtility.MAX_DIGITS)));

              // Index of rounding digit
              int roundIndex = currentIndex + resultIntegerSize + org.apache.drill.exec.util.DecimalUtility.roundUp(result.scale + 1) - 1;

              // Check the first chopped digit to see if we need to round up
              int carry = ((tempResult[roundIndex] / roundFactor) % 10) > 4 ? 1 : 0;

              if (result.scale > 0) {

                // Compute the power of 10 necessary to chop of the fractional part
                int scaleFactor = (int) (org.apache.drill.exec.util.DecimalUtility.getPowerOfTen(
                                         org.apache.drill.exec.util.DecimalUtility.MAX_DIGITS - (result.scale % org.apache.drill.exec.util.DecimalUtility.MAX_DIGITS)));
                // Chop the unwanted fractional part
                tempResult[lastScaleIndex] /=  scaleFactor;
                tempResult[lastScaleIndex] *= scaleFactor;

                // Adjust the carry so that it gets added to the correct digit
                carry *= scaleFactor;
              }

              // Propagate the carry
              while (carry > 0 && lastScaleIndex >= 0) {
                int tempSum = tempResult[lastScaleIndex] + carry;
                if (tempSum >= org.apache.drill.exec.util.DecimalUtility.DIGITS_BASE) {
                  tempResult[lastScaleIndex] = (tempSum % org.apache.drill.exec.util.DecimalUtility.DIGITS_BASE);
                  carry = (int) (tempSum / org.apache.drill.exec.util.DecimalUtility.DIGITS_BASE);
                } else {
                  tempResult[lastScaleIndex] = tempSum;
                  carry = 0;
                }
                lastScaleIndex--;
              }

              // Check if carry has increased integer digit
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
                result.setInteger(outputIndex--, tempResult[i], result.start, result.buffer);
            }

            // Set the remaining digits to be zero
            while(outputIndex >= 0) {
              result.setInteger(outputIndex--, 0, result.start, result.buffer);
            }
            result.setSign(left.getSign(left.start, left.buffer) != right.getSign(right.start, right.buffer), result.start, result.buffer);
        }
    }

    @FunctionTemplate(name = "exact_divide", scope = FunctionTemplate.FunctionScope.DECIMAL_DIV_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}DivideFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output ${type.name}Holder result;
        @Inject DrillBuf buffer;
        @Workspace int outputScale;
        @Workspace int outputPrecision;

        public void setup() {
            int size = (${type.storage} * (org.apache.drill.exec.util.DecimalUtility.INTEGER_SIZE));
            buffer = buffer.reallocIfNeeded(size);
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

            java.math.BigDecimal numerator = org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromDrillBuf(left.buffer, left.start, left.nDecimalDigits, left.scale, true);
            java.math.BigDecimal denominator = org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromDrillBuf(right.buffer, right.start, right.nDecimalDigits, right.scale, true);

            java.math.BigDecimal output = numerator.divide(denominator, (int) result.scale, java.math.BigDecimal.ROUND_HALF_UP);

            org.apache.drill.exec.util.DecimalUtility.getSparseFromBigDecimal(output, result.buffer, result.start, result.scale, result.precision, result.nDecimalDigits);
        }
    }

    @FunctionTemplate(name = "mod", scope = FunctionTemplate.FunctionScope.DECIMAL_MOD_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}ModFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output ${type.name}Holder result;
        @Inject DrillBuf buffer;
        @Workspace int outputScale;
        @Workspace int outputPrecision;

        public void setup() {
            int size = (${type.storage} * (org.apache.drill.exec.util.DecimalUtility.INTEGER_SIZE));
            buffer = buffer.reallocIfNeeded(size);
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

            java.math.BigDecimal numerator = org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromDrillBuf(left.buffer, left.start, left.nDecimalDigits, left.scale, true);
            java.math.BigDecimal denominator = org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromDrillBuf(right.buffer, right.start, right.nDecimalDigits, right.scale, true);

            java.math.BigDecimal output = numerator.remainder(denominator);
            output.setScale(result.scale, java.math.BigDecimal.ROUND_HALF_UP);

            org.apache.drill.exec.util.DecimalUtility.getSparseFromBigDecimal(output, result.buffer, result.start, result.scale, result.precision, result.nDecimalDigits);
        }
    }

    @FunctionTemplate(name = "abs", scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}AbsFunction implements DrillSimpleFunc {

        @Param  ${type.name}Holder in;
        @Output ${type.name}Holder out;
        public void setup() {

        }

        public void eval() {
          out.scale = in.scale;
          out.precision = in.precision;
          out.buffer = in.buffer;
          out.start = in.start;

          // Set the output buffer with the positive sign
          out.buffer.setInt(out.start, (out.buffer.getInt(out.start) & 0x7fffffff));
        }
    }

    @FunctionTemplate(name = "sign", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}SignFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder in;
        @Output IntHolder out;

        public void setup() {}

        public void eval() {

          boolean zeroValue = true;

          if (in.getSign(in.start, in.buffer) == true) {
            out.value = -1;
          } else {
            for (int i = 0; i < ${type.storage}; i++) {
              if (in.getInteger(i, in.start, in.buffer) != 0) {
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
        public void setup() {

        }

        public void eval() {
          out.scale = 0;
          out.precision = in.precision;
          out.buffer = in.buffer;
          out.start = in.start;
          boolean sign = in.getSign(in.start, in.buffer);

          // Indicates whether we need to add 1 to the integer part, while performing ceil
          int carry = 0;

          int scaleStartIndex = ${type.storage} - org.apache.drill.exec.util.DecimalUtility.roundUp(in.scale);
          int srcIntIndex = scaleStartIndex - 1;

          if (sign == false) {
            // For negative values ceil we don't need to increment the integer part
            while (scaleStartIndex < ${type.storage}) {
              if (out.getInteger(scaleStartIndex, out.start, out.buffer) != 0) {
                carry = 1;
                break;
              }
              scaleStartIndex++;
            }
          }

          // Truncate the fractional part, move the integer part
          int destIndex = ${type.storage} - 1;
          while (srcIntIndex >= 0) {
            out.setInteger(destIndex--, out.getInteger(srcIntIndex--, out.start, out.buffer), out.start, out.buffer);
          }

          // Set the remaining portion of the decimal to be zeroes
          while (destIndex >= 0) {
            out.setInteger(destIndex--, 0, out.start, out.buffer);
          }

          // Add the carry
          if (carry != 0) {
            destIndex = ${type.storage} - 1;

            while (destIndex >= 0) {
              int intValue = out.getInteger(destIndex, out.start, out.buffer);
              intValue += carry;

              if (intValue >= org.apache.drill.exec.util.DecimalUtility.DIGITS_BASE) {
                out.setInteger(destIndex--, intValue % org.apache.drill.exec.util.DecimalUtility.DIGITS_BASE, out.start, out.buffer);
                carry = intValue / org.apache.drill.exec.util.DecimalUtility.DIGITS_BASE;
              } else {
                out.setInteger(destIndex--, intValue, out.start, out.buffer);
                break;
              }
            }
          }
          // Set the sign
          out.setSign(sign, out.start, out.buffer);
        }
    }

    @FunctionTemplate(name = "floor", scope = FunctionTemplate.FunctionScope.DECIMAL_ZERO_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}FloorFunction implements DrillSimpleFunc {

        @Param  ${type.name}Holder in;
        @Output ${type.name}Holder out;
        public void setup() {

        }

        public void eval() {
          out.scale = 0;
          out.precision = in.precision;
          out.buffer = in.buffer;
          out.start = in.start;
          boolean sign = in.getSign(in.start, in.buffer);

          // Indicates whether we need to decrement 1 from the integer part, while performing floor, done for -ve values
          int carry = 0;

          int scaleStartIndex = ${type.storage} - org.apache.drill.exec.util.DecimalUtility.roundUp(in.scale);
          int srcIntIndex = scaleStartIndex - 1;

          if (sign == true) {
            // For negative values ceil we don't need to increment the integer part
            while (scaleStartIndex < ${type.storage}) {
              if (out.getInteger(scaleStartIndex, out.start, out.buffer) != 0) {
                carry = 1;
                break;
              }
              scaleStartIndex++;
            }
          }

          // Truncate the fractional part, move the integer part
          int destIndex = ${type.storage} - 1;
          while (srcIntIndex >= 0) {
            out.setInteger(destIndex--, out.getInteger(srcIntIndex--, out.start, out.buffer), out.start, out.buffer);
          }

          // Set the remaining portion of the decimal to be zeroes
          while (destIndex >= 0) {
            out.setInteger(destIndex--, 0, out.start, out.buffer);
          }
          // Add the carry
          if (carry != 0) {
            destIndex = ${type.storage} - 1;

            while (destIndex >= 0) {
              int intValue = out.getInteger(destIndex, out.start, out.buffer);
              intValue += carry;

              if (intValue >= org.apache.drill.exec.util.DecimalUtility.DIGITS_BASE) {
                out.setInteger(destIndex--, intValue % org.apache.drill.exec.util.DecimalUtility.DIGITS_BASE, out.start, out.buffer);
                carry = intValue / org.apache.drill.exec.util.DecimalUtility.DIGITS_BASE;
              } else {
                out.setInteger(destIndex--, intValue, out.start, out.buffer);
                break;
              }
            }
          }
          // Set the sign
          out.setSign(sign, out.start, out.buffer);
        }
    }

    @FunctionTemplate(names = {"trunc", "truncate"}, scope = FunctionTemplate.FunctionScope.DECIMAL_ZERO_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}TruncateFunction implements DrillSimpleFunc {

        @Param  ${type.name}Holder in;
        @Output ${type.name}Holder out;
        public void setup() {

        }

        public void eval() {
          out.scale = 0;
          out.precision = in.precision;
          out.buffer = in.buffer;
          out.start = in.start;
          boolean sign = in.getSign(in.start, in.buffer);

          // Integer part's src index
          int srcIntIndex = ${type.storage} - org.apache.drill.exec.util.DecimalUtility.roundUp(in.scale) - 1;

          // Truncate the fractional part, move the integer part
          int destIndex = ${type.storage} - 1;
          while (srcIntIndex >= 0) {
            out.setInteger(destIndex--, out.getInteger(srcIntIndex--, out.start, out.buffer), out.start, out.buffer);
          }

          // Set the remaining portion of the decimal to be zeroes
          while (destIndex >= 0) {
            out.setInteger(destIndex--, 0, out.start, out.buffer);
          }
          // Set the sign
          out.setSign(sign, out.start, out.buffer);
        }
    }

    @FunctionTemplate(names = {"trunc", "truncate"}, scope = FunctionTemplate.FunctionScope.DECIMAL_SET_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}TruncateScaleFunction implements DrillSimpleFunc {

        @Param  ${type.name}Holder left;
        @Param  IntHolder right;
        @Output ${type.name}Holder result;
        public void setup() {

        }

        public void eval() {
          result.scale = right.value;
          result.precision = left.precision;
          result.buffer = left.buffer;
          result.start = left.start;
          boolean sign = left.getSign(left.start, left.buffer);

          int newScaleRoundedUp  = org.apache.drill.exec.util.DecimalUtility.roundUp(right.value);
          int origScaleRoundedUp = org.apache.drill.exec.util.DecimalUtility.roundUp(left.scale);

          if (right.value < left.scale) {
            // Get the source index beyond which we will truncate
            int srcIntIndex = ${type.storage} - origScaleRoundedUp - 1;
            int srcIndex = srcIntIndex + newScaleRoundedUp;

            // Truncate the remaining fractional part, move the integer part
            int destIndex = ${type.storage} - 1;
            if (srcIndex != destIndex) {
              while (srcIndex >= 0) {
                result.setInteger(destIndex--, result.getInteger(srcIndex--, result.start, result.buffer), result.start, result.buffer);
              }

              // Set the remaining portion of the decimal to be zeroes
              while (destIndex >= 0) {
                result.setInteger(destIndex--, 0, result.start, result.buffer);
              }
            }

            // We truncated the decimal digit. Now we need to truncate within the base 1 billion fractional digit
            int truncateFactor = org.apache.drill.exec.util.DecimalUtility.MAX_DIGITS - (right.value % org.apache.drill.exec.util.DecimalUtility.MAX_DIGITS);
            if (truncateFactor != org.apache.drill.exec.util.DecimalUtility.MAX_DIGITS) {
              truncateFactor = (int) org.apache.drill.exec.util.DecimalUtility.getPowerOfTen(truncateFactor);
              int fractionalDigits = result.getInteger(${type.storage} - 1, result.start, result.buffer);
              fractionalDigits /= truncateFactor;
              result.setInteger(${type.storage} - 1, fractionalDigits * truncateFactor, result.start, result.buffer);
            }
          } else if (right.value > left.scale) {
            // Add fractional digits to the decimal

            // Check if we need to shift the decimal digits to the left
            if (newScaleRoundedUp > origScaleRoundedUp) {
              int srcIndex  = 0;
              int destIndex = newScaleRoundedUp - origScaleRoundedUp;

              // Check while extending scale, we are not overwriting integer part
              while (srcIndex < destIndex) {
                if (result.getInteger(srcIndex++, result.start, result.buffer) != 0) {
                  throw new org.apache.drill.common.exceptions.DrillRuntimeException("Truncate resulting in loss of integer part, reduce scale specified");
                }
              }

              srcIndex = 0;
              while (destIndex < ${type.storage}) {
                result.setInteger(srcIndex++, result.getInteger(destIndex++, result.start, result.buffer), result.start, result.buffer);
              }

              // Clear the remaining part
              while (srcIndex < ${type.storage}) {
                result.setInteger(srcIndex++, 0, result.start, result.buffer);
              }
            }
          }
          // Set the sign
          result.setSign(sign, result.start, result.buffer);
        }
    }

    @FunctionTemplate(name = "round", scope = FunctionTemplate.FunctionScope.DECIMAL_ZERO_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}RoundFunction implements DrillSimpleFunc {

        @Param  ${type.name}Holder in;
        @Output ${type.name}Holder out;
        public void setup() {

        }

        public void eval() {
          out.scale = 0;
          out.precision = in.precision;
          out.buffer = in.buffer;
          out.start = in.start;
          boolean sign = in.getSign(in.start, in.buffer);

          boolean roundUp = false;

          // Get the first fractional digit to see if want to round up or not
          int scaleIndex = ${type.storage} - org.apache.drill.exec.util.DecimalUtility.roundUp(in.scale);
          if (scaleIndex < ${type.storage}) {
            int fractionalPart = out.getInteger(scaleIndex, out.start, out.buffer);
            int digit = fractionalPart / (org.apache.drill.exec.util.DecimalUtility.DIGITS_BASE / 10);

            if (digit > 4) {
              roundUp = true;
            }
          }

          // Integer part's src index
          int srcIntIndex = scaleIndex - 1;

          // Truncate the fractional part, move the integer part
          int destIndex = ${type.storage} - 1;
          while (srcIntIndex >= 0) {
            out.setInteger(destIndex--, out.getInteger(srcIntIndex--, out.start, out.buffer), out.start, out.buffer);
          }

          // Set the remaining portion of the decimal to be zeroes
          while (destIndex >= 0) {
            out.setInteger(destIndex--, 0, out.start, out.buffer);
          }

          // Perform the roundup
          srcIntIndex = ${type.storage} - 1;
          if (roundUp == true) {
            while (srcIntIndex >= 0) {
              int value = out.getInteger(srcIntIndex, out.start, out.buffer) + 1;
              if (value >= org.apache.drill.exec.util.DecimalUtility.DIGITS_BASE) {
                out.setInteger(srcIntIndex--, value % org.apache.drill.exec.util.DecimalUtility.DIGITS_BASE, out.start, out.buffer);
                value = value / org.apache.drill.exec.util.DecimalUtility.DIGITS_BASE;
              } else {
                out.setInteger(srcIntIndex--, value, out.start, out.buffer);
                break;
              }
            }
          }
          // Set the sign
          out.setSign(sign, out.start, out.buffer);
        }
    }

    @FunctionTemplate(name = "round", scope = FunctionTemplate.FunctionScope.DECIMAL_SET_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}RoundScaleFunction implements DrillSimpleFunc {

        @Param  ${type.name}Holder left;
        @Param  IntHolder right;
        @Output ${type.name}Holder result;
        public void setup() {

        }

        public void eval() {
          result.scale = right.value;
          result.precision = left.precision;
          result.buffer = left.buffer;
          result.start = left.start;
          boolean sign = left.getSign(left.start, left.buffer);

          org.apache.drill.exec.util.DecimalUtility.roundDecimal(result.buffer, result.start, result.nDecimalDigits, result.scale, left.scale);
          // Set the sign
          result.setSign(sign, result.start, result.buffer);
        }
    }

 <#-- Handle 2 x 2 combinations of nullable and non-nullable arguments. -->
 <#list ["Nullable${type.name}", "${type.name}"] as leftType >
 <#list ["Nullable${type.name}", "${type.name}"] as rightType >

  <#-- Comparison function for sorting and grouping relational operators
       (not for comparison expression operators (=, <, etc.)). -->
  @FunctionTemplate(name = FunctionGenerationHelper.COMPARE_TO_NULLS_HIGH,
                    scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE,
                    nulls = NullHandling.INTERNAL)
  public static class GCompare${leftType}Vs${rightType}NullHigh implements DrillSimpleFunc {

    @Param ${leftType}Holder left;
    @Param ${rightType}Holder right;
    @Output IntHolder out;

    public void setup() {}

    public void eval() {
      <@compareBlock leftType=leftType rightType=rightType absCompare="false" output="out.value" nullCompare=true nullComparesHigh=true />
    }
  }



  <#-- Comparison function for sorting and grouping relational operators
        (not for comparison expression operators (=, <, etc.)). -->
  @FunctionTemplate(name = FunctionGenerationHelper.COMPARE_TO_NULLS_LOW,
                    scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE,
                    nulls = NullHandling.INTERNAL)
  public static class GCompare${leftType}Vs${rightType}NullLow implements DrillSimpleFunc {

    @Param ${leftType}Holder left;
    @Param ${rightType}Holder right;
    @Output IntHolder out;

    public void setup() {}

    public void eval() {
      <@compareBlock leftType=leftType rightType=rightType absCompare="false" output="out.value" nullCompare=true nullComparesHigh=false />
    }
  }

 </#list>
 </#list>


    <#-- Comparison function for comparison expression operator (=, &lt;, etc.),
         not for sorting and grouping relational operators.) -->
    @FunctionTemplate(name = "less than",
                      scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE,
                      nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}LessThan implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup() {}

        public void eval() {
            int cmp;
            <@compareBlock leftType="leftType" rightType="rightType" absCompare="false" output="cmp" nullCompare=false nullComparesHigh=false />
            out.value = cmp == -1 ? 1 : 0;
        }
    }


    // TODO:  RESOLVE:  Here there are spaces in function template names, but
    // elsewhere there are underlines.  Are things being looked up correctly?
    <#-- Comparison function for comparison expression operator (=, &lt;, etc.),
         not for sorting and grouping relational operators.) -->
    @FunctionTemplate(name = "less than or equal to",
                      scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE,
                      nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}LessThanEq implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup() {}

        public void eval() {
            int cmp;
            <@compareBlock leftType="leftType" rightType="rightType" absCompare="false" output="cmp" nullCompare=false nullComparesHigh=false />
            out.value = cmp < 1 ? 1 : 0;
        }
    }


    <#-- Comparison function for comparison expression operator (=, &lt;, etc.),
         not for sorting and grouping relational operators.) -->
    @FunctionTemplate(name = "greater than",
                      scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE,
                      nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}GreaterThan implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup() {}

        public void eval() {
            int cmp;
            <@compareBlock leftType="leftType" rightType="rightType" absCompare="false" output="cmp" nullCompare=false nullComparesHigh=false />
            out.value = cmp == 1 ? 1 : 0;
        }
    }


    <#-- Comparison function for comparison expression operator (=, &lt;, etc.),
         not for sorting and grouping relational operators.) -->
    @FunctionTemplate(name = "greater than or equal to",
                      scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE,
                      nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}GreaterThanEq implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup() {}

        public void eval() {
            int cmp;
            <@compareBlock leftType="leftType" rightType="rightType" absCompare="false" output="cmp" nullCompare=false nullComparesHigh=false />
            out.value = cmp > -1 ? 1 : 0;
        }
    }


    <#-- Comparison function for comparison expression operator (=, &lt;, etc.),
         not for sorting and grouping relational operators.) -->
    @FunctionTemplate(name = "Equal",
                      scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE,
                      nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}Equal implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup() {}

        public void eval() {
            int cmp;
            <@compareBlock leftType="leftType" rightType="rightType" absCompare="false" output="cmp" nullCompare=false nullComparesHigh=false />
            out.value = cmp == 0 ? 1 : 0;
        }
    }


    <#-- Comparison function for comparison expression operator (=, &lt;, etc.),
         not for sorting and grouping relational operators.) -->
    @FunctionTemplate(name = "not equal",
                      scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE,
                      nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}NotEqual implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup() {}

        public void eval() {
            int cmp;
            <@compareBlock leftType="leftType" rightType="rightType" absCompare="false" output="cmp" nullCompare=false nullComparesHigh=false />
            out.value = cmp != 0 ? 1 : 0;
        }
    }
}

<#elseif type.name.endsWith("Dense")>

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/${type.name}Functions.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl;

<#include "/@includes/vv_imports.ftl" />

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

@SuppressWarnings("unused")
public class ${type.name}Functions {


 <#-- Handle 2 x 2 combinations of nullable and non-nullable arguments. -->
 <#list ["Nullable${type.name}", "${type.name}"] as leftType >
 <#list ["Nullable${type.name}", "${type.name}"] as rightType >

  <#-- Comparison function for sorting and grouping relational operators
       (not for comparison expression operators (=, <, etc.)). -->
  @FunctionTemplate(name = FunctionGenerationHelper.COMPARE_TO_NULLS_HIGH,
                    scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE,
                    nulls = NullHandling.INTERNAL)
  public static class GCompare${leftType}Vs${rightType}NullHigh implements DrillSimpleFunc {

    @Param ${leftType}Holder left;
    @Param ${rightType}Holder right;
    @Output IntHolder out;

    public void setup() {}

    public void eval() {
     outside:
      {
        <@compareNullsSubblock leftType=leftType rightType=rightType output="out.value" breakTarget="outside" nullCompare=true nullComparesHigh=true />

        out.value = org.apache.drill.exec.util.DecimalUtility.compareDenseBytes(left.buffer, left.start, left.getSign(left.start, left.buffer), right.buffer, right.start, right.getSign(right.start, right.buffer), left.WIDTH);
      } // outside
    }
  }

  <#-- Comparison function for sorting and grouping relational operators
       (not for comparison expression operators (=, <, etc.)). -->
  @FunctionTemplate(name = FunctionGenerationHelper.COMPARE_TO_NULLS_LOW,
                    scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE,
                    nulls = NullHandling.INTERNAL)
  public static class GCompare${leftType}Vs${rightType}NullLow implements DrillSimpleFunc {

    @Param ${leftType}Holder left;
    @Param ${rightType}Holder right;
    @Output IntHolder out;

    public void setup() {}

    public void eval() {
     outside:
      {
        <@compareNullsSubblock leftType=leftType rightType=rightType output="out.value" breakTarget="outside" nullCompare=true nullComparesHigh=false />

        out.value = org.apache.drill.exec.util.DecimalUtility.compareDenseBytes(left.buffer, left.start, left.getSign(left.start, left.buffer), right.buffer, right.start, right.getSign(right.start, right.buffer), left.WIDTH);
      } // outside
    }
  }

 </#list>
 </#list>


    <#-- Comparison function for comparison expression operator (=, &lt;, etc.),
         not for sorting and grouping relational operators.) -->
    @FunctionTemplate(name = "less than",
                      scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE,
                      nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}LessThan implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup() {}

        public void eval() {
            int cmp  = org.apache.drill.exec.util.DecimalUtility.compareDenseBytes(left.buffer, left.start, left.getSign(left.start, left.buffer), right.buffer, right.start, right.getSign(right.start, right.buffer), left.WIDTH);
            out.value = cmp == -1 ? 1 : 0;
        }
    }

    <#-- Comparison function for comparison expression operator (=, &lt;, etc.),
         not for sorting and grouping relational operators.) -->
    @FunctionTemplate(name = "less than or equal to",
                      scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE,
                      nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}LessThanEq implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup() {}

        public void eval() {
            int cmp = org.apache.drill.exec.util.DecimalUtility.compareDenseBytes(left.buffer, left.start, left.getSign(left.start, left.buffer), right.buffer, right.start, right.getSign(right.start, right.buffer), left.WIDTH);
            out.value = cmp < 1 ? 1 : 0;
        }
    }

    <#-- Comparison function for comparison expression operator (=, &lt;, etc.),
         not for sorting and grouping relational operators.) -->
    @FunctionTemplate(name = "greater than",
                      scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE,
                      nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}GreaterThan implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup() {}

        public void eval() {
            int cmp = org.apache.drill.exec.util.DecimalUtility.compareDenseBytes(left.buffer, left.start, left.getSign(left.start, left.buffer), right.buffer, right.start, right.getSign(right.start, right.buffer), left.WIDTH);
            out.value = cmp == 1 ? 1 : 0;
        }
    }

    <#-- Comparison function for comparison expression operator (=, &lt;, etc.),
         not for sorting and grouping relational operators.) -->
    @FunctionTemplate(name = "greater than or equal to",
                      scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE,
                      nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}GreaterThanEq implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup() {}

        public void eval() {
            int cmp = org.apache.drill.exec.util.DecimalUtility.compareDenseBytes(left.buffer, left.start, left.getSign(left.start, left.buffer), right.buffer, right.start, right.getSign(right.start, right.buffer), left.WIDTH);
            out.value = cmp > -1 ? 1 : 0;
        }
    }

    <#-- Comparison function for comparison expression operator (=, &lt;, etc.),
         not for sorting and grouping relational operators.) -->
    @FunctionTemplate(name = "Equal",
                      scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE,
                      nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}Equal implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup() {}

        public void eval() {
            int cmp = org.apache.drill.exec.util.DecimalUtility.compareDenseBytes(left.buffer, left.start, left.getSign(left.start, left.buffer), right.buffer, right.start, right.getSign(right.start, right.buffer), left.WIDTH);
            out.value = cmp == 0 ? 1 : 0;
        }
    }


    <#-- Comparison function for comparison expression operator (=, &lt;, etc.),
         not for sorting and grouping relational operators.) -->
    @FunctionTemplate(name = "not equal",
                      scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE,
                      nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}NotEqual implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup() {}

        public void eval() {

            int cmp = org.apache.drill.exec.util.DecimalUtility.compareDenseBytes(left.buffer, left.start, left.getSign(left.start, left.buffer), right.buffer, right.start, right.getSign(right.start, right.buffer), left.WIDTH);
            out.value = cmp != 0 ? 1 : 0;
        }
    }
}

<#elseif type.name.endsWith("Decimal9") || type.name.endsWith("Decimal18")>

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/${type.name}Functions.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl;

<#include "/@includes/vv_imports.ftl" />

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
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

        public void setup() {
            outputPrecision = Integer.MIN_VALUE;
        }


        public void eval() {
            if (outputPrecision == Integer.MIN_VALUE) {
                org.apache.drill.common.util.DecimalScalePrecisionAddFunction resultScalePrec =
                new org.apache.drill.common.util.DecimalScalePrecisionAddFunction((int) left.precision, (int) left.scale, (int) right.precision, (int) right.scale);
                outputScale = resultScalePrec.getOutputScale();
                outputPrecision = resultScalePrec.getOutputPrecision();
            }
            <@adjustScale javaType=type.storage leftType="leftType" rightType="rightType"/>

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

        public void setup() {
            outputPrecision = Integer.MIN_VALUE;
        }

        public void eval() {
            if (outputPrecision == Integer.MIN_VALUE) {
                org.apache.drill.common.util.DecimalScalePrecisionAddFunction resultScalePrec =
                new org.apache.drill.common.util.DecimalScalePrecisionAddFunction((int) left.precision, (int) left.scale, (int) right.precision, (int) right.scale);
                outputScale = resultScalePrec.getOutputScale();
                outputPrecision = resultScalePrec.getOutputPrecision();
            }
            <@adjustScale javaType=type.storage leftType="leftType" rightType="rightType"/>

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

        public void setup() {
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

        public void setup() {}

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

        public void setup() {
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

        public void setup() {
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

        public void setup() {}

        public void eval() {

            out.value = (in.value < 0) ? -1 : ((in.value > 0) ? 1 : 0);
        }
    }

    @FunctionTemplate(names = {"trunc", "truncate"}, scope = FunctionTemplate.FunctionScope.DECIMAL_ZERO_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}TruncFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder in;
        @Output ${type.name}Holder out;

        public void setup() {}

        public void eval() {

            out.value =(${type.storage}) (org.apache.drill.exec.util.DecimalUtility.adjustScaleDivide(in.value, (int) in.scale));
            out.precision = out.maxPrecision;
            out.scale = 0;
        }
    }

    @FunctionTemplate(names = {"trunc", "truncate"}, scope = FunctionTemplate.FunctionScope.DECIMAL_SET_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}TruncateScaleFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param IntHolder right;
        @Output ${type.name}Holder out;

        public void setup() {}

        public void eval() {

            out.value = (${type.storage}) (org.apache.drill.exec.util.DecimalUtility.adjustScaleDivide(left.value, (int) (left.scale - right.value)));
            out.precision = out.maxPrecision;
            out.scale = right.value;
        }
    }

    @FunctionTemplate(names = {"ceil", "ceiling"}, scope = FunctionTemplate.FunctionScope.DECIMAL_ZERO_SCALE, nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}CeilFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder in;
        @Output ${type.name}Holder out;

        public void setup() {

        }

        public void eval() {
          ${type.storage} scaleFactor = (${type.storage}) (org.apache.drill.exec.util.DecimalUtility.getPowerOfTen((int) in.scale));

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

        public void setup() {
        }

        public void eval() {

          ${type.storage} scaleFactor = (${type.storage}) (org.apache.drill.exec.util.DecimalUtility.getPowerOfTen((int) in.scale));
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

        public void setup() {
        }

        public void eval() {

          ${type.storage} scaleFactor = (${type.storage}) (org.apache.drill.exec.util.DecimalUtility.getPowerOfTen((int) in.scale));
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

    @FunctionTemplate(name = "round",
                      scope = FunctionTemplate.FunctionScope.DECIMAL_SET_SCALE,
                      nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}RoundScaleFunction implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param IntHolder right;
        @Output ${type.name}Holder out;


        public void setup() {
        }

        public void eval() {

          ${type.storage} scaleFactor = (${type.storage}) (org.apache.drill.exec.util.DecimalUtility.getPowerOfTen((int) left.scale));
          ${type.storage} newScaleFactor = (${type.storage}) (org.apache.drill.exec.util.DecimalUtility.getPowerOfTen((int) right.value));
          ${type.storage} truncScaleFactor = (${type.storage}) (org.apache.drill.exec.util.DecimalUtility.getPowerOfTen( Math.abs(left.scale - right.value)));
          int truncFactor = (int) (left.scale - right.value);

          // If rounding scale is >= current scale
          if (right.value >= left.scale) {
            out.value = (${type.storage}) (org.apache.drill.exec.util.DecimalUtility.adjustScaleMultiply(left.value, (int) (right.value - left.scale)));
          }
          else {
            out.scale = right.value;
            // Assign the integer part to the output
            out.value = left.value / scaleFactor;

            // Get the fractional part
            ${type.storage} fractionalPart = left.value % scaleFactor;

            // From the entire fractional part extract the digits upto which rounding is needed
            ${type.storage} newFractionalPart = (${type.storage}) (org.apache.drill.exec.util.DecimalUtility.adjustScaleDivide(fractionalPart, truncFactor));
            ${type.storage} truncatedFraction = fractionalPart % truncScaleFactor;


            // Get the truncated fractional part and extract the first digit to see if we need to add 1
            int digit = Math.abs((int) org.apache.drill.exec.util.DecimalUtility.adjustScaleDivide(truncatedFraction, truncFactor - 1));

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

 <#-- Handle 2 x 2 combinations of nullable and non-nullable arguments. -->
 <#list ["Nullable${type.name}", "${type.name}"] as leftType >
 <#list ["Nullable${type.name}", "${type.name}"] as rightType >

  <#-- Comparison function for sorting and grouping relational operators
       (not for comparison expression operators (=, <, etc.)). -->
  @FunctionTemplate(name = FunctionGenerationHelper.COMPARE_TO_NULLS_HIGH,
                    scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE,
                    nulls = NullHandling.INTERNAL)
  public static class GCompare${leftType}Vs${rightType}NullHigh implements DrillSimpleFunc {

    @Param ${leftType}Holder left;
    @Param ${rightType}Holder right;
    @Output IntHolder out;

    public void setup() {}

    public void eval() {
     outside:
      {
        <@compareNullsSubblock leftType=leftType rightType=rightType output="out.value" breakTarget="outside" nullCompare=true nullComparesHigh=true />

        <@adjustScale javaType=type.storage leftType="leftType" rightType="rightType"/>
        out.value = (left.value < right.value) ? -1 : (left.value > right.value) ? 1 : 0;
      } // outside
    }
  }

  <#-- Comparison function for sorting and grouping relational operators
       (not for comparison expression operators (=, <, etc.)). -->
  @FunctionTemplate(name = FunctionGenerationHelper.COMPARE_TO_NULLS_LOW,
                    scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE,
                    nulls = NullHandling.INTERNAL)
  public static class GCompare${leftType}Vs${rightType}NullLow implements DrillSimpleFunc {

    @Param ${leftType}Holder left;
    @Param ${rightType}Holder right;
    @Output IntHolder out;

    public void setup() {}

    public void eval() {
     outside:
      {
        <@compareNullsSubblock leftType=leftType rightType=rightType output="out.value" breakTarget="outside" nullCompare=true nullComparesHigh=false />
        <@adjustScale javaType=type.storage leftType="leftType" rightType="rightType"/>
        out.value = (left.value < right.value) ? -1 : (left.value > right.value) ? 1 : 0;
      } // outside
    }
  }

 </#list>
 </#list>

    <#-- Comparison function for comparison expression operator (=, &lt;, etc.),
         not for sorting and grouping relational operators.) -->
    @FunctionTemplate(name = "less than",
                      scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE,
                      nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}LessThan implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup() {}

        public void eval() {
            <@adjustScale javaType=type.storage leftType="leftType" rightType="rightType"/>
            out.value = (left.value < right.value) ? 1 : 0;
        }
    }

     <#-- Comparison function for comparison expression operator (=, &lt;, etc.),
          not for sorting and grouping relational operators.) -->
    @FunctionTemplate(name = "less than or equal to",
                      scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE,
                      nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}LessThanEq implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup() {}

        public void eval() {
            <@adjustScale javaType=type.storage leftType="leftType" rightType="rightType"/>
            out.value = (left.value <= right.value) ? 1 : 0;
        }
    }

     <#-- Comparison function for comparison expression operator (=, &lt;, etc.),
          not for sorting and grouping relational operators.) -->
    @FunctionTemplate(name = "greater than",
                      scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE,
                      nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}GreaterThan implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup() {}

        public void eval() {
            <@adjustScale javaType=type.storage leftType="leftType" rightType="rightType"/>
            out.value = (left.value > right.value) ? 1 : 0;
        }
    }

     <#-- Comparison function for comparison expression operator (=, &lt;, etc.),
          not for sorting and grouping relational operators.) -->
    @FunctionTemplate(name = "greater than or equal to",
                      scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE,
                      nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}GreaterThanEq implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup() {}

        public void eval() {
            <@adjustScale javaType=type.storage leftType="leftType" rightType="rightType"/>
            out.value = (left.value >= right.value) ? 1 : 0;
        }
    }

     <#-- Comparison function for comparison expression operator (=, &lt;, etc.),
          not for sorting and grouping relational operators.) -->
    @FunctionTemplate(name = "Equal",
                      scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE,
                      nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}Equal implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup() {}

        public void eval() {
            <@adjustScale javaType=type.storage leftType="leftType" rightType="rightType"/>
            out.value = (left.value == right.value) ? 1 : 0;
        }
    }


     <#-- Comparison function for comparison expression operator (=, &lt;, etc.),
          not for sorting and grouping relational operators.) -->
    @FunctionTemplate(name = "not equal",
                      scope = FunctionTemplate.FunctionScope.DECIMAL_MAX_SCALE,
                      nulls = NullHandling.NULL_IF_NULL)
    public static class ${type.name}NotEqual implements DrillSimpleFunc {

        @Param ${type.name}Holder left;
        @Param ${type.name}Holder right;
        @Output BitHolder out;
        public void setup() {}

        public void eval() {
            <@adjustScale javaType=type.storage leftType="leftType" rightType="rightType"/>
            out.value = (left.value != right.value) ? 1 : 0;
        }
    }
}

</#if>

</#list>
