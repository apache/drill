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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("order")
public class Order extends SingleInputOperator {

  private final Ordering[] orderings;

  @JsonCreator
  public Order(@JsonProperty("orders") Ordering... orderings) {
    this.orderings = orderings;
  }
  
  public Ordering[] getOrderings() {
    return orderings;
  }

  public static class Ordering {

    private final Direction direction;
    private final LogicalExpression expr;

    public Ordering(String strOrder, LogicalExpression expr) {
      this.expr = expr;
      this.direction = Direction.DESC.description.equals(strOrder) ? Direction.DESC : Direction.ASC; // default
                                                                                                     // to
                                                                                                     // ascending
                                                                                                     // unless
                                                                                                     // desc
                                                                                                     // is
                                                                                                     // provided.
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

  }

  public static enum Direction {
    ASC("asc"), DESC("desc");
    public final String description;

    Direction(String d) {
      description = d;
    }
  }
  
  
}
