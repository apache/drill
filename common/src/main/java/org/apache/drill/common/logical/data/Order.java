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

import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.visitors.LogicalVisitor;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelFieldCollation.Direction;
import org.eigenbase.rel.RelFieldCollation.NullDirection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

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

    private final RelFieldCollation.Direction direction;
    private final LogicalExpression expr;
    private final RelFieldCollation.NullDirection nulls;
    
    @JsonCreator
    public Ordering(@JsonProperty("order") String strOrder, @JsonProperty("expr") LogicalExpression expr, @JsonProperty("nullDirection") String nullCollation) {
      this.expr = expr;
      this.nulls = NullDirection.LAST.name().equalsIgnoreCase(nullCollation) ? NullDirection.LAST :  NullDirection.FIRST; // default first
      this.direction = Order.getDirectionFromString(strOrder);
    }

    public Ordering(Direction direction, LogicalExpression e, NullDirection nullCollation) {
      this.expr = e;
      this.nulls = nullCollation;
      this.direction = direction;
    }
    
    public Ordering(Direction direction, LogicalExpression e) {
      this(direction, e, NullDirection.FIRST);
    }

    @JsonIgnore
    public Direction getDirection() {
      return direction;
    }

    public LogicalExpression getExpr() {
      return expr;
    }

    public String getOrder() {
      
      switch(direction){
      case DESCENDING: return "DESC";
      default: return "ASC";
      }
    }

    public NullDirection getNullDirection() {
      return nulls;
    }
    
    

  }
  
  public static Builder builder(){
    return new Builder();
  }
  
  public static class Builder extends AbstractSingleBuilder<Order, Builder>{
    private List<Ordering> orderings = Lists.newArrayList();
    private FieldReference within;
    
    public Builder setWithin(FieldReference within){
      this.within = within;
      return this;
    }
    
    public Builder addOrdering(Direction direction, LogicalExpression e, NullDirection collation){
      orderings.add(new Ordering(direction, e, collation));
      return this;
    }

    @Override
    public Order internalBuild() {
      return new Order(within, orderings.toArray(new Ordering[orderings.size()]));
    }
    
    
  }
  
  public static Direction getDirectionFromString(String direction){
    return "DESC".equalsIgnoreCase(direction) ? Direction.DESCENDING : Direction.ASCENDING;
  }
  
  public static String getStringFromDirection(Direction direction){
    return direction == Direction.DESCENDING ? "DESC" : "ASC";
  }
}
