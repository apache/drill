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
package org.apache.drill.common.logical.data;

import org.apache.drill.common.expression.LogicalExpression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("join")
public class Join extends LogicalOperatorBase {
  private final LogicalOperator left;
  private final LogicalOperator right;
  private final JoinCondition[] conditions;

  @JsonCreator
  public Join(@JsonProperty("left") LogicalOperator left, @JsonProperty("right") LogicalOperator right, @JsonProperty("conditions") JoinCondition[] conditions) {
    super();
    this.conditions = conditions;
    this.left = left;
    this.right = right;
    left.registerAsSubscriber(this);
    right.registerAsSubscriber(this);

  }

  public LogicalOperator getLeft() {
    return left;
  }

  public LogicalOperator getRight() {
    return right;
  }

  public JoinCondition[] getConditions() {
    return conditions;
  }

  public static class JoinCondition {
    private final String relationship;
    private final LogicalExpression left;
    private final LogicalExpression right;

    @JsonCreator
    public JoinCondition(@JsonProperty("relationship") String relationship,
        @JsonProperty("left") LogicalExpression left, @JsonProperty("right") LogicalExpression right) {
      super();
      this.relationship = relationship;
      this.left = left;
      this.right = right;
    }

    public String getRelationship() {
      return relationship;
    }

    public LogicalExpression getLeft() {
      return left;
    }

    public LogicalExpression getRight() {
      return right;
    }

  }
}
