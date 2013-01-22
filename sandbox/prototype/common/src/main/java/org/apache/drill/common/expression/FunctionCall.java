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

import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.expression.types.DataType;
import org.apache.drill.common.expression.visitors.ExprVisitor;

import com.google.common.collect.ImmutableList;

public class FunctionCall extends LogicalExpressionBase implements Iterable<LogicalExpression> {
  private final FunctionDefinition func;
  public final ImmutableList<LogicalExpression> args;

  public FunctionCall(FunctionDefinition func, List<LogicalExpression> args) {
    this.func = func;
    if (!(args instanceof ImmutableList)) {
      args = ImmutableList.copyOf(args);
    }
    this.args = (ImmutableList<LogicalExpression>) args;
  }

  @Override
  public <T> T accept(ExprVisitor<T> visitor) {
    return visitor.visitFunctionCall(this);
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    return args.iterator();
  }

  public FunctionDefinition getDefinition(){
    return func;
  }
  
  @Override
  public DataType getDataType() {
    return func.getDataType(this.args);
  }

  @Override
  public void addToString(StringBuilder sb) {
    if (func.isOperator()) {
      if (args.size() == 1) { // unary
        func.addRegisteredName(sb);
        sb.append("(");
        args.get(0).addToString(sb);
        sb.append(")");
      } else {
        for (int i = 0; i < args.size(); i++) {
          if (i != 0) {
            sb.append(" ");
            func.addRegisteredName(sb);
          }
          sb.append(" (");
          args.get(i).addToString(sb);
          sb.append(") ");
        }
      }
    } else { // normal function

      func.addRegisteredName(sb);
      sb.append("(");
      for (int i = 0; i < args.size(); i++) {
        if (i != 0) sb.append(", ");
        args.get(i).addToString(sb);
      }
      sb.append(") ");
    }
  }
}
