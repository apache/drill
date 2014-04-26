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
}