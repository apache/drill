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
package org.apache.drill.exec.physical.base;

import org.apache.calcite.rel.core.JoinRelType;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.JoinCondition;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public abstract class AbstractJoinPop extends AbstractBase {

  protected final PhysicalOperator left;

  protected final PhysicalOperator right;

  protected final JoinRelType joinType;

  protected final LogicalExpression condition;

  protected final List<JoinCondition> conditions;

  public AbstractJoinPop(PhysicalOperator leftOp, PhysicalOperator rightOp,
                         JoinRelType joinType, LogicalExpression joinCondition,
                         List<JoinCondition> joinConditions) {
    left = leftOp;
    right = rightOp;
    this.joinType = joinType;
    condition = joinCondition;
    conditions = joinConditions;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitOp(this, value);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Arrays.asList(left, right).iterator();
  }

  public PhysicalOperator getLeft() {
    return left;
  }

  public PhysicalOperator getRight() {
    return right;
  }

  public JoinRelType getJoinType() {
    return joinType;
  }

  public LogicalExpression getCondition() { return condition; }

  public List<JoinCondition> getConditions() {
    return conditions;
  }

}
