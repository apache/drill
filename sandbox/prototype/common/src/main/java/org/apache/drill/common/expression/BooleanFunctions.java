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
package org.apache.drill.common.expression;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BooleanFunctions {

  static final Logger logger = LoggerFactory.getLogger(BooleanFunctions.class);

  public static final Class<?>[] SUB_TYPES = { Or.class, And.class };

  private static abstract class BooleanFunctionBase extends FunctionBase {

    protected BooleanFunctionBase(List<LogicalExpression> expressions) {
      super(expressions);
    }

    @Override
    public DataType getDataType() {
      return DataType.BOOLEAN;
    }
  }

  public static class Or extends BooleanFunctionBase {
    public Or(List<LogicalExpression> expressions) {
      super(expressions);
    }

    @Override
    public void addToString(StringBuilder sb) {
      this.opToString(sb, "||");
    }

  }

  public static class And extends BooleanFunctionBase {
    public And(List<LogicalExpression> expressions) {
      super(expressions);
    }

    @Override
    public void addToString(StringBuilder sb) {
      this.opToString(sb, "&&");
    }

  }

  @FunctionName("isNull")
  public static class IsNull extends BooleanFunctionBase {
    public IsNull(List<LogicalExpression> expressions) {
      super(expressions);
    }

    @Override
    public void addToString(StringBuilder sb) {
      this.funcToString(sb, "isNull");
    }

  }

  public static class Comparison extends LogicalExpressionBase {

    public final ComparisonType type;
    public final LogicalExpression left;
    public final LogicalExpression right;

    public static enum ComparisonType {
      GT(">"), LT("<"), EQ("=="), NOTEQ("!="), GTE(">="), LTE("<=");
      public final String val;

      ComparisonType(String str) {
        this.val = str;
      }
    }

    public Comparison(String s, LogicalExpression left, LogicalExpression right) {
      this.left = left;
      this.right = right;
      ComparisonType temp = null;
      for (ComparisonType ct : ComparisonType.values()) {
        if (ct.val.equals(s)) {
          temp = ct;
          break;
        }
      }
      if (temp == null)
        throw new IllegalArgumentException("Unknown comparison type of " + s);
      type = temp;

    }

    @Override
    public void addToString(StringBuilder sb) {
      sb.append(" ( ");
      left.addToString(sb);
      sb.append(" ");
      sb.append(type.val);
      sb.append(" ");
      right.addToString(sb);
      sb.append(" ) ");
    }

    public static LogicalExpression create(List<LogicalExpression> expressions, List<String> comparisonTypes) {
      // logger.debug("Generating new comparison expressions.");
      if (expressions.size() == 1) {
        return expressions.get(0);
      }

      if (expressions.size() - 1 != comparisonTypes.size())
        throw new IllegalArgumentException("Must receive one more expression then the provided number of comparisons.");

      LogicalExpression first = expressions.get(0);
      LogicalExpression second;
      for (int i = 0; i < comparisonTypes.size(); i++) {
        second = expressions.get(i + 1);
        first = new Comparison(comparisonTypes.get(i), first, second);
      }
      return first;
    }
  }

}
