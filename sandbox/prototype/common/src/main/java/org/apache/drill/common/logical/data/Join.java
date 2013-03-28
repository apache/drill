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

import org.apache.drill.common.exceptions.ExpressionParsingException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("join")
public class Join extends LogicalOperatorBase {
  private LogicalOperator left;
  private LogicalOperator right;
  private final JoinType type;
  private final JoinCondition[] conditions;

  public static enum JoinType{
    LEFT, INNER, OUTER;
    
    public static JoinType resolve(String val){
      for(JoinType jt : JoinType.values()){
        if(jt.name().equalsIgnoreCase(val)) return jt;
      }
      throw new ExpressionParsingException(String.format("Unable to determine join type for value '%s'.", val));
    }
  }
  
  @JsonCreator
  public Join(@JsonProperty("left") LogicalOperator left, @JsonProperty("right") LogicalOperator right, @JsonProperty("conditions") JoinCondition[] conditions, @JsonProperty("type") String type) {
    super();
    this.conditions = conditions;
    this.left = left;
    this.right = right;
    this.type = JoinType.resolve(type);
    left.registerAsSubscriber(this);
    right.registerAsSubscriber(this);

  }

  public LogicalOperator getLeft() {
    return left;
  }

  public LogicalOperator getRight() {
    return right;
  }

  public void setLeft(LogicalOperator left) {
    if(this.left != null){
      this.left.unregisterSubscriber(this);      
    }
    this.left = left;
    this.left.registerAsSubscriber(this);
  }

  public void setRight(LogicalOperator right) {
    if(this.right != null){
      this.right.unregisterSubscriber(this);
    }
    this.right = right;
    this.right.registerAsSubscriber(this);
  }

  public JoinCondition[] getConditions() {
    return conditions;
  }

  @JsonIgnore
  public JoinType getJointType(){
    return type;
  }
  
  public String getType(){
    return type.name();
  }
}
