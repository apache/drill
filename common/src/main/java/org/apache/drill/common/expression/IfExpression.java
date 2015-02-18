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
package org.apache.drill.common.expression;

import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class IfExpression extends LogicalExpressionBase {
  static final Logger logger = LoggerFactory.getLogger(IfExpression.class);

  public final IfCondition ifCondition;
  public final LogicalExpression elseExpression;

  private IfExpression(ExpressionPosition pos, IfCondition conditions, LogicalExpression elseExpression) {
    super(pos);
    this.ifCondition = conditions;
    this.elseExpression = elseExpression;
  }

  public static class IfCondition{
    public final LogicalExpression condition;
    public final LogicalExpression expression;

    public IfCondition(LogicalExpression condition, LogicalExpression expression) {
      //logger.debug("Generating IfCondition {}, {}", condition, expression);

      this.condition = condition;
      this.expression = expression;
    }

  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitIfExpression(this, value);
  }

  public static class Builder {
    IfCondition conditions;
    private LogicalExpression elseExpression;
    private ExpressionPosition pos = ExpressionPosition.UNKNOWN;

    public Builder setPosition(ExpressionPosition pos) {
      this.pos = pos;
      return this;
    }

    public Builder setElse(LogicalExpression elseExpression) {
      this.elseExpression = elseExpression;
            return this;
    }

    public Builder setIfCondition(IfCondition conditions) {
      this.conditions = conditions;
      return this;
    }

    public IfExpression build(){
      Preconditions.checkNotNull(pos);
      Preconditions.checkNotNull(conditions);
      return new IfExpression(pos, conditions, elseExpression);
    }

  }

  @Override
  public MajorType getMajorType() {
    // If the return type of one of the "then" expression or "else" expression is nullable, return "if" expression
    // type as nullable
    MajorType majorType = elseExpression.getMajorType();
    if (majorType.getMode() == DataMode.OPTIONAL) {
      return majorType;
    }

    if (ifCondition.expression.getMajorType().getMode() == DataMode.OPTIONAL) {
      assert ifCondition.expression.getMajorType().getMinorType() == majorType.getMinorType();

      return ifCondition.expression.getMajorType();
    }

    return majorType;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    List<LogicalExpression> children = Lists.newLinkedList();

    children.add(ifCondition.condition);
    children.add(ifCondition.expression);
    children.add(this.elseExpression);
    return children.iterator();
  }

  @Override
  public int getCumulativeCost() {
    // return the average cost of operands for a boolean "and" | "or"
    int cost = this.getSelfCost();

    int i = 0;
    for (LogicalExpression e : this) {
      cost += e.getCumulativeCost();
      i++;
    }

    return (int) (cost / i) ;
  }

}
