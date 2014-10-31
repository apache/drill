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

import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.NullableBitHolder;
import org.apache.drill.exec.record.RecordBatch;

public class AggregateErrorFunctions {

  @FunctionTemplate(names = {"sum", "max", "avg", "stddev_pop", "stddev_samp", "stddev", "var_pop", "var_samp", "variance"}, scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class BitAggregateErrorFunctions implements DrillAggFunc {

    @Param BitHolder in;
    @Workspace BigIntHolder value;
    @Output BigIntHolder out;

    public void setup(RecordBatch b) {
      if (true) {
        throw new RuntimeException("Only COUNT aggregate function supported for Boolean type");
      }
    }

    @Override
    public void add() {
    }

    @Override
    public void output() {
    }

    @Override
    public void reset() {
    }

  }

  @FunctionTemplate(names = {"sum", "max", "avg", "stddev_pop", "stddev_samp", "stddev", "var_pop", "var_samp", "variance"}, scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableBitAggregateErrorFunctions implements DrillAggFunc{

    @Param NullableBitHolder in;
    @Workspace BigIntHolder value;
    @Output BigIntHolder out;

    public void setup(RecordBatch b) {
      if (true) {
        throw new RuntimeException("Only COUNT aggregate function supported for Boolean type");
      }
    }

    @Override
    public void add() {
    }

    @Override
    public void output() {
    }

    @Override
    public void reset() {
    }
  }
}
