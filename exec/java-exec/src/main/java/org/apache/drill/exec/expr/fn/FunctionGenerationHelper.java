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

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FunctionHolderExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.HoldingContainerExpression;

public class FunctionGenerationHelper {
  public static final String COMPARE_TO = "compare_to";

  /**
   * Given materialized arguments find the "compare_to" FunctionHolderExpression
   * @param left
   * @param right
   * @param registry
   * @return FunctionHolderExpression containing the function implementation
   */
  public static FunctionHolderExpression getComparator(HoldingContainer left,
    HoldingContainer right,
    FunctionImplementationRegistry registry) {
    if (! isComparableType(left.getMajorType()) || ! isComparableType(right.getMajorType()) ){
      throw new UnsupportedOperationException(formatCanNotCompareMsg(left.getMajorType(), right.getMajorType()));
    }

    return getFunctionExpression(COMPARE_TO, Types.required(MinorType.INT), registry, left, right);
  }

  public static FunctionHolderExpression getFunctionExpression(String name, MajorType returnType, FunctionImplementationRegistry registry, HoldingContainer... args) {
    List<MajorType> argTypes = new ArrayList<MajorType>(args.length);
    List<LogicalExpression> argExpressions = new ArrayList<LogicalExpression>(args.length);
    for(HoldingContainer c : args) {
      argTypes.add(c.getMajorType());
      argExpressions.add(new HoldingContainerExpression(c));
    }

    DrillFuncHolder holder = registry.findExactMatchingDrillFunction(name, argTypes, returnType);
    if (holder != null) {
      return holder.getExpr(name, argExpressions, ExpressionPosition.UNKNOWN);
    }

    StringBuilder sb = new StringBuilder();
    sb.append("Failure finding function that runtime code generation expected.  Signature: ");
    sb.append(name);
    sb.append("( ");
    for(int i =0; i < args.length; i++) {
      MajorType mt = args[i].getMajorType();
      appendType(mt, sb);
      if (i != 0) {
        sb.append(", ");
      }
    }
    sb.append(" ) returns ");
    appendType(returnType, sb);
    throw new UnsupportedOperationException(sb.toString());
  }

  private static final void appendType(MajorType mt, StringBuilder sb) {
    sb.append(mt.getMinorType().name());
    sb.append(":");
    sb.append(mt.getMode().name());
  }

  protected static boolean isComparableType(MajorType type) {
    if (type.getMinorType() == MinorType.MAP ||
        type.getMinorType() == MinorType.LIST ||
        type.getMode() == TypeProtos.DataMode.REPEATED ) {
      return false;
    } else {
      return true;
    }
  }

  private static String formatCanNotCompareMsg(MajorType left, MajorType right) {
    StringBuilder sb = new StringBuilder();
    sb.append("Map, Array or repeated scalar type should not be used in group by, order by or in a comparison operator. Drill does not support compare between ");

    appendType(left, sb);
    sb.append(" and ");
    appendType(right, sb);
    sb.append(".");

    return sb.toString();
  }

}
