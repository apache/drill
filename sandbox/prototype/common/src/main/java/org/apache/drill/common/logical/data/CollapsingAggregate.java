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

import com.google.common.collect.Iterators;
import org.apache.drill.common.expression.FieldReference;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.data.visitors.LogicalVisitor;

import java.util.Iterator;
import java.util.List;

@JsonTypeName("collapsingaggregate")
public class CollapsingAggregate extends SingleInputOperator{
  
  private final FieldReference within;
  private final FieldReference target;
  private final FieldReference[] carryovers;
  private final NamedExpression[]  aggregations;

  @JsonCreator
  public CollapsingAggregate(@JsonProperty("within") FieldReference within, @JsonProperty("target") FieldReference target, @JsonProperty("carryovers") FieldReference[] carryovers, @JsonProperty("aggregations") NamedExpression[] aggregations) {
    super();
    this.within = within;
    this.target = target;
    this.carryovers = carryovers;
    this.aggregations = aggregations;
  }

  public FieldReference getWithin() {
    return within;
  }

  public FieldReference getTarget() {
    return target;
  }

  public FieldReference[] getCarryovers() {
    return carryovers;
  }

  public NamedExpression[] getAggregations() {
    return aggregations;
  }

    @Override
    public <T, X, E extends Throwable> T accept(LogicalVisitor<T, X, E> logicalVisitor, X value) throws E {
        return logicalVisitor.visitCollapsingAggregate(this, value);
    }

    @Override
    public Iterator<LogicalOperator> iterator() {
        return Iterators.singletonIterator(getInput());
    }
}
