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
<#if type.major == "DecimalSparseDecimalDense">

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

        /* Before converting from a sparse representation to a dense representation
         * we need to convert it to an intermediate representation. In the sparse
         * representation we separate out the scale and the integer part of the decimal
         * and pad the scale part with additional zeroes for ease of performing arithmetic
         * operations. In the intermediate representation we strip out the extra zeroes and
         * combine the scale and integer part.
         */
        int[] intermediate = new int[in.nDecimalDigits - 1];

        int index = in.nDecimalDigits - 1;
        int actualDigits;

        if (in.scale > 0 && (actualDigits = (in.scale % org.apache.drill.exec.util.DecimalUtility.MAX_DIGITS)) > 0) {

            int paddedDigits = org.apache.drill.exec.util.DecimalUtility.MAX_DIGITS - actualDigits;

            int paddedMask = (int) Math.pow(10, paddedDigits);

            /* We have a scale that does not completely occupy a decimal
             * digit, so we have padded zeroes to it for ease of arithmetic
             * Truncate the extra zeroes added and move the digits to the right
             */
            int temp = (in.getInteger(index, in.start, in.buffer)/paddedMask);
            index--;

            while(index >= 0) {

                int transferDigits = (in.getInteger(index, in.start, in.buffer) % (paddedMask));

                intermediate[index] = (int) (temp + (Math.pow(10, actualDigits) * transferDigits));

                temp = (in.getInteger(index, in.start, in.buffer)/(paddedMask));

                index--;
            }
        } else {

            /* If the scale does not exist or it perfectly fits within a decimal digit
             * then we have padded no zeroes, which means there can atmost be only 38 digits, which
             * need only 5 decimal digit to be stored, simply copy over the integers
             */
            for (int i = 1; i < in.nDecimalDigits; i++)
                intermediate[i - 1] = in.getInteger(i, in.start, in.buffer);

        }

        /* Now we have an intermediate representation in the array intermediate[]
         * Every number in the intermediate representation is base 1 billion number
         * To represent it we require only 30 bits, but every integer has 32 bits.
         * By shifting the bits around we can utilize the extra two bits on every
         * number and create a dense representation
         */

          /* Allocate a byte array */
          int size = (((intermediate.length - 1) * org.apache.drill.exec.util.DecimalUtility.INTEGER_SIZE) + 1);
          byte[] intermediateBytes = new byte[size];
          java.nio.ByteBuffer wrapper = java.nio.ByteBuffer.wrap(intermediateBytes);

          wrapper.put((byte) intermediate[0]);

          for (int i = 1; i < intermediate.length; i++) {
            wrapper.put(java.nio.ByteBuffer.allocate(org.apache.drill.exec.util.DecimalUtility.INTEGER_SIZE).putInt(intermediate[i]).array());
          }

          final int[] mask = {0x03, 0x0F, 0x3F, 0xFF};
          int maskIndex = 0;
          int shiftOrder = 2;

          // Start just after the last integer and shift bits to the right
          index = size - (org.apache.drill.exec.util.DecimalUtility.INTEGER_SIZE+ 1);

          while (index >= 0) {

              /* get the last bits that need to shifted to the next byte */
              byte shiftBits = (byte) ((intermediateBytes[index] & mask[maskIndex]) << (8 - shiftOrder));

              int shiftOrder1 = ((index % org.apache.drill.exec.util.DecimalUtility.INTEGER_SIZE) == 0) ? shiftOrder - 2 : shiftOrder;

              /* transfer the bits from the left to the right */
              intermediateBytes[index + 1] = (byte) (((intermediateBytes[index + 1] & 0xFF) >>> (shiftOrder1)) | shiftBits);

              index--;

              if ((index % org.apache.drill.exec.util.DecimalUtility.INTEGER_SIZE) == 0) {
                  /* We are on a border */
                  shiftOrder += 2;
                  maskIndex++;
              }
          }

          <#if (type.from == "Decimal28Sparse") && (type.to == "Decimal38Dense")>
          /* Decimal38Dense representation has four bytes more than that needed to
           * represent Decimal28Dense. So our first four bytes are empty in that scenario
           */
          int dstIndex = 4;
          <#else>
          int dstIndex = 0;
          </#if>

          // Set the bytes in the buffer
          out.buffer.setBytes(dstIndex, intermediateBytes, 1, (size - 1));
          out.setSign(in.getSign(in.start, in.buffer), out.start, out.buffer);
    }
}
</#if>
</#list>