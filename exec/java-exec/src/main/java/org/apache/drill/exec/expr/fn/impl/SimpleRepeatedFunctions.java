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

import org.apache.drill.common.expression.Arg;
import org.apache.drill.common.expression.BasicArgumentValidator;
import org.apache.drill.common.expression.CallProvider;
import org.apache.drill.common.expression.FunctionDefinition;
import org.apache.drill.common.expression.OutputTypeDeterminer;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.RepeatedBigIntHolder;
import org.apache.drill.exec.expr.holders.RepeatedIntHolder;
import org.apache.drill.exec.record.RecordBatch;

public class SimpleRepeatedFunctions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MathFunctions.class);

  private SimpleRepeatedFunctions() {
  }

  @FunctionTemplate(name = "repeated_count", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class RepeatedLengthBigInt implements DrillSimpleFunc {

    @Param
    RepeatedBigIntHolder input;
    @Output
    IntHolder out;

    public void setup(RecordBatch b) {
    }

    public void eval() {
      out.value = input.end - input.start;
    }
  }

  @FunctionTemplate(name = "repeated_count", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class RepeatedLengthInt implements DrillSimpleFunc {

    @Param RepeatedIntHolder input;
    @Output IntHolder out;

    public void setup(RecordBatch b) {
    }

    public void eval() {
      out.value = input.end - input.start;
    }
  }

  @FunctionTemplate(name = "repeated_contains", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class ContainsBigInt implements DrillSimpleFunc {

    @Param RepeatedBigIntHolder listToSearch;
    @Param BigIntHolder targetValue;
    @Output BitHolder out;

    public void setup(RecordBatch b) {
    }

    public void eval() {
      for (int i = listToSearch.start; i < listToSearch.end; i++) {
        if (listToSearch.vector.getAccessor().get(i) == targetValue.value) {
          out.value = 1;
          break;
        }
      }
    }

  }

  public static class Provider implements CallProvider {

    @Override
    public FunctionDefinition[] getFunctionDefintions() {
      return new FunctionDefinition[] {
          FunctionDefinition.simple("repeated_contains", new BasicArgumentValidator( //
              new Arg("repeatedToSearch", //
                  Types.repeated(MinorType.BIGINT), //
                  Types.repeated(MinorType.INT)), //
              new Arg("targetValue", Types.required(MinorType.BIGINT))), //
              OutputTypeDeterminer.FixedType.FIXED_BIT),

          FunctionDefinition.simple(
              "repeated_count",
              new BasicArgumentValidator(new Arg("repeatedToSearch", Types.repeated(MinorType.BIGINT), Types
                  .repeated(MinorType.INT))), new OutputTypeDeterminer.FixedType(Types.required(MinorType.INT)))

      };
    }

  }
}