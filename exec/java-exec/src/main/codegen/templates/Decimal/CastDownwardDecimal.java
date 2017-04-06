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

<#if type.major == "DownwardDecimalSimpleDecimalSimple">  <#-- Cast function template for conversion from Decimal18, Decimal9 -->
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
      out.value = (int) in.value;
      // Check if we need to truncate or round up
      if (out.scale > in.scale) {
        out.value *= (int) org.apache.drill.exec.util.DecimalUtility.getPowerOfTen(out.scale - in.scale);
      } else if (out.scale < in.scale) {
        // need to round up since we are truncating fractional part
        int scaleFactor = (int) (org.apache.drill.exec.util.DecimalUtility.getPowerOfTen((int) in.scale));
        int newScaleFactor = (int) (org.apache.drill.exec.util.DecimalUtility.getPowerOfTen((int) scale.value));
        int truncScaleFactor = (int) (org.apache.drill.exec.util.DecimalUtility.getPowerOfTen( (int) (Math.abs(in.scale - scale.value))));
        int truncFactor = (int) (in.scale - scale.value);

        // Assign the integer part
        out.value = (int) (in.value / scaleFactor);

        // Get the fractional part
        int fractionalPart = (int) (in.value % scaleFactor);

        // From the entire fractional part extract the digits upto which rounding is needed
        int newFractionalPart = (int) (org.apache.drill.exec.util.DecimalUtility.adjustScaleDivide(fractionalPart, truncFactor));
        int truncatedFraction = fractionalPart % truncScaleFactor;

        // Get the truncated fractional part and extract the first digit to see if we need to add 1
        int digit = Math.abs((int) org.apache.drill.exec.util.DecimalUtility.adjustScaleDivide(truncatedFraction, truncFactor - 1));

        if (digit > 4) {
          if (in.value > 0) {
            newFractionalPart++;
          } else if (in.value < 0) {
            newFractionalPart--;
          }
        }
        out.value = (int) ((out.value * newScaleFactor) + newFractionalPart);
      }
    }
}
<#elseif type.major == "DownwardDecimalComplexDecimalSimple">  <#-- Cast function template for conversion from Decimal28/Decimal9 to Decimal18/Decimal9 -->
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
      java.math.BigDecimal temp = org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromSparse(in.buffer, in.start, in.nDecimalDigits, in.scale);
      temp = temp.setScale((int) scale.value, java.math.BigDecimal.ROUND_HALF_UP);
      out.value = temp.unscaledValue().${type.javatype}Value();
      out.precision = (int) precision.value;
      out.scale = (int) scale.value;
    }
}
<#elseif type.major == "DownwardDecimalComplexDecimalComplex">  <#-- Cast function template for conversion from Decimal28/Decimal9 to Decimal18/Decimal9 -->
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
    @Inject DrillBuf buffer;
    @Output ${type.to}Holder out;

    public void setup() {
      int size = (${type.arraySize} * (org.apache.drill.exec.util.DecimalUtility.INTEGER_SIZE));
      buffer = buffer.reallocIfNeeded(size);
    }

    public void eval() {
      java.math.BigDecimal temp = org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromSparse(in.buffer, in.start, in.nDecimalDigits, in.scale);
      temp = temp.setScale((int) scale.value, java.math.BigDecimal.ROUND_HALF_UP);
      out.precision = (int) precision.value;
      out.scale = (int) scale.value;
      out.buffer = buffer;
      out.start = 0;
      org.apache.drill.exec.util.DecimalUtility.getSparseFromBigDecimal(temp, out.buffer, out.start, out.scale, out.precision, out.nDecimalDigits);
    }
}
</#if> <#-- type.major -->
</#list>