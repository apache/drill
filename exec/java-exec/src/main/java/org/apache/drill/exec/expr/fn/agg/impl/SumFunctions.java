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
package org.apache.drill.exec.expr.fn.agg.impl;

import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.record.RecordBatch;

public class SumFunctions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SumFunctions.class);
  
  @FunctionTemplate(name = "sum", scope = FunctionScope.POINT_AGGREGATE)
  public static class BigIntSum implements DrillAggFunc{

    @Param BigIntHolder in;
    @Workspace long sum;
    @Output BigIntHolder out;
    
    public void setup(RecordBatch incoming) {
    }

    @Override
    public void add() {
      sum += in.value;
    }

    @Override
    public void output() {
      out.value = sum;
    }

    @Override
    public void reset() {
      sum = 0;
    }
    
  }
  
  @FunctionTemplate(name = "sum", scope = FunctionScope.POINT_AGGREGATE)
  public static class IntSum implements DrillAggFunc{

    @Param IntHolder in;
    @Workspace int sum;
    @Output IntHolder out;
    
    public void setup(RecordBatch incoming) {
    }

    @Override
    public void add() {
      sum += in.value;
    }

    @Override
    public void output() {
      out.value = sum;
    }

    @Override
    public void reset() {
      sum = 0;
    }
    
  }
}
