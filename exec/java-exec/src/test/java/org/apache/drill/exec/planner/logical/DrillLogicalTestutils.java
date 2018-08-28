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
package org.apache.drill.exec.planner.logical;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.parser.ExprLexer;
import org.apache.drill.common.expression.parser.ExprParser;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Order;

import java.util.ArrayList;
import java.util.List;

public class DrillLogicalTestutils {
  public static Order.Ordering ordering(String expression,
                                        RelFieldCollation.Direction direction,
                                        RelFieldCollation.NullDirection nullDirection) {
    return new Order.Ordering(direction, parseExpr(expression), nullDirection);
  }

  public static JoinCondition joinCond(String leftExpr, String relationship, String rightExpr) {
    return new JoinCondition(relationship, parseExpr(leftExpr), parseExpr(rightExpr));
  }

  public static List<NamedExpression> parseExprs(String... expressionsAndOutputNames) {
    Preconditions.checkArgument(expressionsAndOutputNames.length % 2 == 0,
      "List of expressions and output field names" + " is not complete, each expression must explicitly give and output name,");
    List<NamedExpression> ret = new ArrayList<>();
    for (int i = 0; i < expressionsAndOutputNames.length; i += 2) {
      ret.add(new NamedExpression(parseExpr(expressionsAndOutputNames[i]),
        new FieldReference(new SchemaPath(new PathSegment.NameSegment(expressionsAndOutputNames[i + 1])))));
    }
    return ret;
  }

  public static LogicalExpression parseExpr(String expr) {
    ExprLexer lexer = new ExprLexer(new ANTLRStringStream(expr));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    ExprParser parser = new ExprParser(tokens);
    try {
      return parser.parse().e;
    } catch (RecognitionException e) {
      throw new RuntimeException("Error parsing expression: " + expr);
    }
  }
}