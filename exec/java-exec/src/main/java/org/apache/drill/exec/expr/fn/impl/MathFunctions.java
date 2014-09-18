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
package org.apache.drill.exec.expr.fn.impl;

import java.text.DecimalFormat;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.record.RecordBatch;

public class MathFunctions{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MathFunctions.class);

  private MathFunctions(){}

  @FunctionTemplate(names = {"negative", "u-", "-"}, scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Negative implements DrillSimpleFunc{

    @Param BigIntHolder input;
    @Output BigIntHolder out;

    public void setup(RecordBatch b){}

    public void eval(){
      out.value = -input.value;
      return;
    }

  }

  @FunctionTemplate(name = "power", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Power implements DrillSimpleFunc{

    @Param Float8Holder a;
    @Param Float8Holder b;
    @Output  Float8Holder out;

    public void setup(RecordBatch b){}

    public void eval(){
      out.value = java.lang.Math.pow(a.value, b.value);
    }

  }

  @FunctionTemplate(name = "random", isRandom = true,
    scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Random implements DrillSimpleFunc{
    @Output  Float8Holder out;

    public void setup(RecordBatch b){}

    public void eval(){
      out.value = java.lang.Math.random();
    }

  }

  @FunctionTemplate(name = "to_number", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ToNumber implements DrillSimpleFunc {
    @Param  VarCharHolder left;
    @Param  VarCharHolder right;
    @Workspace java.text.DecimalFormat inputFormat;
    @Workspace int decimalDigits;
    @Output Float8Holder out;

    public void setup(RecordBatch b) {
      byte[] buf = new byte[right.end - right.start];
      right.buffer.getBytes(right.start, buf, 0, right.end - right.start);
      inputFormat = new DecimalFormat(new String(buf));
      decimalDigits = inputFormat.getMaximumFractionDigits();
    }

    public void eval() {
      byte[] buf1 = new byte[left.end - left.start];
      left.buffer.getBytes(left.start, buf1, 0, left.end - left.start);
      String input = new String(buf1);
      try {
        out.value = inputFormat.parse(input).doubleValue();
      }  catch(java.text.ParseException e) {
         throw new UnsupportedOperationException("Cannot parse input: " + input + " with pattern : " + inputFormat.toPattern());
      }

      // Round the value
      java.math.BigDecimal roundedValue = new java.math.BigDecimal(out.value);
      out.value = (roundedValue.setScale(decimalDigits, java.math.BigDecimal.ROUND_HALF_UP)).doubleValue();
    }
  }

  @FunctionTemplate(name = "pi", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Pi implements DrillSimpleFunc {

    @Output Float8Holder out;

    public void setup(RecordBatch b) {
    }

    public void eval() {
        out.value = java.lang.Math.PI;
    }
  }

}
