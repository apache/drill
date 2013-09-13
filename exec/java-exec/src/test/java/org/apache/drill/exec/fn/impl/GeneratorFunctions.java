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
package org.apache.drill.exec.fn.impl;

import org.apache.drill.common.expression.ArgumentValidators;
import org.apache.drill.common.expression.CallProvider;
import org.apache.drill.common.expression.FunctionDefinition;
import org.apache.drill.common.expression.OutputTypeDeterminer;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;

import java.util.Random;

public class GeneratorFunctions {

  public static final Random random = new Random(1234L);
  public static final FunctionDefinition RANDOM_BIG_INT = FunctionDefinition.simple("randomBigInt", new ArgumentValidators.NumericTypeAllowed(1,2, true),
          OutputTypeDeterminer.FIXED_BIGINT, "randomBigInt");
  public static final FunctionDefinition RANDOM_FLOAT8 = FunctionDefinition.simple("randomFloat8", new ArgumentValidators.NumericTypeAllowed(1,2, true),
          OutputTypeDeterminer.FIXED_FLOAT8, "randomFloat8");

  public static class Provider implements CallProvider {

    @Override
    public FunctionDefinition[] getFunctionDefintions() {
      return new FunctionDefinition[] { RANDOM_BIG_INT,
                                        RANDOM_FLOAT8 };
    }

  }

  @FunctionTemplate(name = "randomBigInt", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class RandomBigIntGauss implements DrillSimpleFunc {

    @Param BigIntHolder range;
    @Output BigIntHolder out;

    public void setup(RecordBatch incoming) {
    }

    public void eval() {
      out.value = (long)(org.apache.drill.exec.fn.impl.GeneratorFunctions.random.nextGaussian() * range.value);
    }
  }

  @FunctionTemplate(name = "randomBigInt", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class RandomBigInt implements DrillSimpleFunc {

    @Param BigIntHolder min;
    @Param BigIntHolder max;
    @Output BigIntHolder out;

    public void setup(RecordBatch incoming) {
    }

    public void eval() {
      out.value = (long)(org.apache.drill.exec.fn.impl.GeneratorFunctions.random.nextFloat() * (max.value - min.value) + min.value);
    }
  }

  @FunctionTemplate(name = "randomFloat8", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class RandomFloat8Gauss implements DrillSimpleFunc {

    @Param BigIntHolder range;
    @Output
    Float8Holder out;

    public void setup(RecordBatch incoming) {
    }

    public void eval() {
      out.value = org.apache.drill.exec.fn.impl.GeneratorFunctions.random.nextGaussian() * range.value;
    }
  }

  @FunctionTemplate(name = "randomFloat8", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class RandomFloat8 implements DrillSimpleFunc {

    @Param BigIntHolder min;
    @Param BigIntHolder max;
    @Output Float8Holder out;

    public void setup(RecordBatch incoming) {
    }

    public void eval() {
      out.value = org.apache.drill.exec.fn.impl.GeneratorFunctions.random.nextFloat() * (max.value - min.value) + min.value;
    }
  }
}
