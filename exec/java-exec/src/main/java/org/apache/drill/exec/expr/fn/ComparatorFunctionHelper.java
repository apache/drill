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
package org.apache.drill.exec.expr.fn;

import com.google.common.collect.ImmutableList;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FunctionHolderExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.DrillFuncHolderExpr;
import org.apache.drill.exec.expr.HoldingContainerExpression;

public class ComparatorFunctionHelper {
  public static final String COMPARE_TO = "compare_to";

  /**
   * Given materialized arguments find the "compare_to" FunctionHolderExpression
   * @param left
   * @param right
   * @param registry
   * @return FunctionHolderExpression containing the function implementation
   */
  public static FunctionHolderExpression get(HoldingContainer left,
    HoldingContainer right,
    FunctionImplementationRegistry registry) {

    ImmutableList<TypeProtos.MajorType> args = ImmutableList.of(left.getMajorType(), right.getMajorType()) ;
    TypeProtos.MajorType returnType = Types.required(TypeProtos.MinorType.INT);

    for (DrillFuncHolder h : registry.getDrillRegistry().getMethods().get(COMPARE_TO)) {
      if (h.matches(returnType, args)) {
        return new DrillFuncHolderExpr(COMPARE_TO, h,
          ImmutableList.of((LogicalExpression)
            new HoldingContainerExpression(left),
            new HoldingContainerExpression(right)),
          ExpressionPosition.UNKNOWN);
      }
    }

    return null;
  }
}