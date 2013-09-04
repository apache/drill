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
package org.apache.drill.common.defs;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.Order.Direction;
import org.apache.drill.common.logical.data.Order.Ordering;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class OrderDef {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OrderDef.class);

  private final Direction direction;
  private final LogicalExpression expr;

  @JsonCreator
  public OrderDef(@JsonProperty("order") Direction direction, @JsonProperty("expr") LogicalExpression expr) {
    this.expr = expr;
    // default to ascending unless desc is provided.
    this.direction = direction == null ? Direction.ASC : direction;
  }
  
  @JsonIgnore
  public Direction getDirection() {
    return direction;
  }

  public LogicalExpression getExpr() {
    return expr;
  }

  public String getOrder() {
    return direction.description;
  }

  public static OrderDef create(Ordering o){
    return new OrderDef(o.getDirection(), o.getExpr());
  }
}
