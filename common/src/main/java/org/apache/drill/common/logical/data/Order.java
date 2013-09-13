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
package org.apache.drill.common.logical.data;

import com.google.common.collect.Iterators;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.data.visitors.LogicalVisitor;

import java.util.Iterator;

@JsonTypeName("order")
public class Order extends SingleInputOperator {

  private final Ordering[] orderings;
  private final FieldReference within;

  @JsonCreator
  public Order(@JsonProperty("within") FieldReference within, @JsonProperty("orderings") Ordering... orderings) {
    this.orderings = orderings;
    this.within = within;
  }
  
  public Ordering[] getOrderings() {
    return orderings;
  }
  
  public FieldReference getWithin() {
    return within;
  }

    @Override
    public <T, X, E extends Throwable> T accept(LogicalVisitor<T, X, E> logicalVisitor, X value) throws E {
        return logicalVisitor.visitOrder(this, value);
    }

    @Override
    public Iterator<LogicalOperator> iterator() {
        return Iterators.singletonIterator(getInput());
    }


    public static class Ordering {

    private final Direction direction;
    private final LogicalExpression expr;
    private final NullCollation nulls;
    
    @JsonCreator
    public Ordering(@JsonProperty("order") String strOrder, @JsonProperty("expr") LogicalExpression expr, @JsonProperty("nullCollation") String nullCollation) {
      this.expr = expr;
      this.nulls = NullCollation.NULLS_LAST.description.equals(nullCollation) ? NullCollation.NULLS_LAST :  NullCollation.NULLS_FIRST; // default first
      this.direction = Direction.DESC.description.equalsIgnoreCase(strOrder) ? Direction.DESC : Direction.ASC; // default asc
                                                                                                     
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

    public NullCollation getNullCollation() {
      return nulls;
    }
    
    

  }
  public static enum NullCollation {
    NULLS_FIRST("first"), NULLS_LAST("last");
    
    public final String description;

    NullCollation(String d) {
      description = d;
    }
  }

  public static enum Direction {
    ASC("ASC"), DESC("DESC");
    public final String description;

    Direction(String d) {
      description = d;
    }
  }
  
  
}
