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

package org.apache.drill.exec.physical.config;

import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.calcite.rel.core.JoinRelType;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

@JsonTypeName("nested-loop-join")
public class NestedLoopJoinPOP extends AbstractBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NestedLoopJoinPOP.class);


  private final PhysicalOperator left;
  private final PhysicalOperator right;
  private final JoinRelType joinType;
  private final LogicalExpression condition;

  @JsonCreator
  public NestedLoopJoinPOP(
      @JsonProperty("left") PhysicalOperator left,
      @JsonProperty("right") PhysicalOperator right,
      @JsonProperty("joinType") JoinRelType joinType,
      @JsonProperty("condition") LogicalExpression condition
  ) {
    this.left = left;
    this.right = right;
    this.joinType = joinType;
    this.condition = condition;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitNestedLoopJoin(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.size() == 2, "Nested loop join should have two physical operators");
    return new NestedLoopJoinPOP(children.get(0), children.get(1), joinType, condition);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.forArray(left, right);
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

  @Override
  public int getOperatorType() {
    return CoreOperatorType.NESTED_LOOP_JOIN_VALUE;
  }
}

