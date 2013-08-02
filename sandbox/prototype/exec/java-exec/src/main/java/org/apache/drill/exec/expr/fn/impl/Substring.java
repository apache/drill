/*******************************************************************************
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
 ******************************************************************************/

package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.common.expression.ArgumentValidators;
import org.apache.drill.common.expression.CallProvider;
import org.apache.drill.common.expression.FunctionDefinition;
import org.apache.drill.common.expression.OutputTypeDeterminer;
import org.apache.drill.exec.expr.DrillFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.*;

// TODO: implement optional length parameter
//       implement UTF-8 and UTF-16 support

/**
 * Evaluate a substring expression for a given value; specifying the start
 * position, and optionally the end position.
 *
 *  - If the start position is negative, start from abs(start) characters from
 *    the end of the buffer.
 *
 *  - If no length is specified, continue to the end of the string.
 *
 *  - If the substring expression's length exceeds the value's upward bound, the
 *    value's length will be used.
 *
 *  - If the substring is invalid, return an empty string.
 */
@FunctionTemplate(name = "substring",
                  scope = FunctionTemplate.FunctionScope.SIMPLE,
                  nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
public class Substring implements DrillFunc {

  @Param VarCharHolder string;
  @Param BigIntHolder offset;
  @Param BigIntHolder length;
  @Output VarCharHolder out;

  @Override
  public void setup(RecordBatch incoming) { }

  @Override
  public void eval() {
    out.buffer = string.buffer;

    // handle invalid values; e.g. SUBSTRING(value, 0, x) or SUBSTRING(value, x, 0)
    if (offset.value == 0 || length.value <= 0) {
      out.start = 0;
      out.end = 0;
      return;
    }

    // handle negative and positive offset values
    if (offset.value < 0)
      out.start = string.end + (int)offset.value;
    else
      out.start = (int)offset.value - 1;

    // calculate end position from length and truncate to upper value bounds
    if (out.start + length.value > string.end)
      out.end = string.end;
    else
      out.end = out.start + (int)length.value;
  }

  public static class Provider implements CallProvider {

    @Override
    public FunctionDefinition[] getFunctionDefintions() {
      return new FunctionDefinition[] {
          FunctionDefinition.simple("substring",
                                    new ArgumentValidators.AnyTypeAllowed(3),
                                    new OutputTypeDeterminer.SameAsFirstInput(),
                                    "substring",
                                    "substr")
      };
    }
  }
}
