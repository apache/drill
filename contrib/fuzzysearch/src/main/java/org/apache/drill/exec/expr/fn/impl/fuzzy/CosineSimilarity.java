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
package org.apache.drill.exec.expr.fn.impl.fuzzy;

import javax.inject.Inject;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.simmetrics.StringMetrics;

import io.netty.buffer.DrillBuf;

@FunctionTemplate(name = "cosine_similarity", scope = FunctionTemplate.FunctionScope.SIMPLE,
  nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
public class CosineSimilarity implements DrillSimpleFunc {
  @Param
  NullableVarCharHolder inputTextA;

  @Param
  NullableVarCharHolder inputTextB;

  @Output
  Float4Holder out;

  @Inject
  DrillBuf buffer;

  @Workspace
  org.simmetrics.StringMetric metric;

  public void setup() {
    metric = org.simmetrics.StringMetrics.cosineSimilarity();
  }

  public void eval() {
    String textA = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start,
        inputTextA.end, inputTextA.buffer);
    String textB = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextB.start,
        inputTextB.end, inputTextB.buffer);

    float result = metric.compare(textA, textB);

    out.value = result;
  }
}
