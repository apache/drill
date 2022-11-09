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
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.vector.complex.reader.FieldReader;


@SuppressWarnings("unused")
public class IsEmptyScalarFunctions {

  private IsEmptyScalarFunctions() {
  }

  @FunctionTemplate(names = {"isempty", "is_empty"},
    scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class IsEmptyVarcharFunctions implements DrillSimpleFunc {

    @Param
    NullableVarCharHolder input;

    @Output
    BitHolder out;

    @Override
    public void setup() {
      // no op
    }

    @Override
    public void eval() {
      if (input.start == 0 && input.end == 0) {
        out.value = 1;
      } else {
        String data = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(input.start, input.end, input.buffer);
        out.value = org.apache.commons.lang3.StringUtils.isEmpty(data) ? 1 : 0;
      }
    }
  }

  @FunctionTemplate(names = {"isempty", "is_empty"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = NullHandling.INTERNAL)
  public static class IsEmptyMapFunctions implements DrillSimpleFunc {

    @Param
    FieldReader input;

    @Output
    BitHolder out;

    @Override
    public void setup() {
      // no op
    }

    @Override
    public void eval() {
      out.value = org.apache.drill.exec.expr.fn.impl.IsEmptyUtils.mapIsEmpty(input) ? 1 : 0;
    }
  }
}
