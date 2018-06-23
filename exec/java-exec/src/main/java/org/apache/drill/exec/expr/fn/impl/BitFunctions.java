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
package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.IntHolder;

/**
 * Function templates for Bit/BOOLEAN functions other than comparison
 * functions.  (Bit/BOOLEAN comparison functions are generated in
 * ComparisonFunctions.java template.)
 *
 */
public class BitFunctions {

  @FunctionTemplate(names = {"booleanOr", "or", "||", "orNoShortCircuit"},
                    scope = FunctionScope.SIMPLE,
                    nulls = NullHandling.NULL_IF_NULL)
  public static class BitOr implements DrillSimpleFunc {

    @Param BitHolder left;
    @Param BitHolder right;
    @Output BitHolder out;

    public void setup() {}

    public void eval() {
      out.value = left.value | right.value;
    }
  }

  @FunctionTemplate(names = {"booleanAnd", "and", "&&"},
                    scope = FunctionScope.SIMPLE,
                    nulls = NullHandling.NULL_IF_NULL)
  public static class BitAnd implements DrillSimpleFunc {

    @Param BitHolder left;
    @Param BitHolder right;
    @Output BitHolder out;

    public void setup() {}

    public void eval() {
      out.value = left.value & right.value;
    }
  }


  @FunctionTemplate(names = {"xor", "^"},
                    scope = FunctionScope.SIMPLE,
                    nulls = NullHandling.NULL_IF_NULL)
  public static class IntXor implements DrillSimpleFunc {

    @Param IntHolder left;
    @Param IntHolder right;
    @Output IntHolder out;

    public void setup() {}

    public void eval() {
      out.value = left.value ^ right.value;
    }
  }

}
