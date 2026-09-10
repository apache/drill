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

import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.expr.holders.VarDecimalHolder;

/**
 * LITERAL_AGG is an internal aggregate function introduced in Apache Calcite 1.35.
 * It returns a constant value regardless of the number of rows in the group.
 * This is used to optimize queries where constant values appear in the SELECT clause
 * of an aggregate query, avoiding the need for a separate Project operator.
 */
@SuppressWarnings("unused")
public class LiteralAggFunction {

  // BigInt (BIGINT) version
  @FunctionTemplate(name = "literal_agg", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class BigIntLiteralAgg implements DrillAggFunc {
    @Param BigIntHolder in;
    @Workspace BigIntHolder value;
    @Output BigIntHolder out;

    public void setup() {
      value = new BigIntHolder();
    }

    @Override
    public void add() {
      // Store the literal value on first call
      value.value = in.value;
    }

    @Override
    public void output() {
      out.value = value.value;
    }

    @Override
    public void reset() {
      value.value = 0;
    }
  }

  // Float8 (DOUBLE) version
  @FunctionTemplate(name = "literal_agg", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Float8LiteralAgg implements DrillAggFunc {
    @Param Float8Holder in;
    @Workspace Float8Holder value;
    @Output Float8Holder out;

    public void setup() {
      value = new Float8Holder();
    }

    @Override
    public void add() {
      value.value = in.value;
    }

    @Override
    public void output() {
      out.value = value.value;
    }

    @Override
    public void reset() {
      value.value = 0.0;
    }
  }

  // Bit (BOOLEAN) version
  @FunctionTemplate(name = "literal_agg", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class BitLiteralAgg implements DrillAggFunc {
    @Param BitHolder in;
    @Workspace BitHolder value;
    @Output BitHolder out;

    public void setup() {
      value = new BitHolder();
    }

    @Override
    public void add() {
      value.value = in.value;
    }

    @Override
    public void output() {
      out.value = value.value;
    }

    @Override
    public void reset() {
      value.value = 0;
    }
  }

  // VarChar (STRING) version
  @FunctionTemplate(name = "literal_agg", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class VarCharLiteralAgg implements DrillAggFunc {
    @Param VarCharHolder in;
    @Workspace VarCharHolder value;
    @Output VarCharHolder out;
    @Workspace org.apache.drill.exec.expr.holders.VarCharHolder tempHolder;

    public void setup() {
      value = new VarCharHolder();
      tempHolder = new VarCharHolder();
    }

    @Override
    public void add() {
      // Copy the input to workspace
      value.buffer = in.buffer;
      value.start = in.start;
      value.end = in.end;
    }

    @Override
    public void output() {
      out.buffer = value.buffer;
      out.start = value.start;
      out.end = value.end;
    }

    @Override
    public void reset() {
      value.start = 0;
      value.end = 0;
    }
  }

  // VarDecimal (DECIMAL) version
  @FunctionTemplate(name = "literal_agg", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class VarDecimalLiteralAgg implements DrillAggFunc {
    @Param VarDecimalHolder in;
    @Workspace VarDecimalHolder value;
    @Output VarDecimalHolder out;

    public void setup() {
      value = new VarDecimalHolder();
    }

    @Override
    public void add() {
      value.buffer = in.buffer;
      value.start = in.start;
      value.end = in.end;
      value.scale = in.scale;
      value.precision = in.precision;
    }

    @Override
    public void output() {
      out.buffer = value.buffer;
      out.start = value.start;
      out.end = value.end;
      out.scale = value.scale;
      out.precision = value.precision;
    }

    @Override
    public void reset() {
      value.start = 0;
      value.end = 0;
    }
  }
}
