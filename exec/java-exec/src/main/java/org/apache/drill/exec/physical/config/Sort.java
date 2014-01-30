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
package org.apache.drill.exec.physical.config;

import java.util.List;

import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.AbstractSingle;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.Size;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("sort")
public class Sort extends AbstractSingle{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Sort.class);
  
  private final List<Ordering> orderings;
  private boolean reverse = false;
  
  @JsonCreator
  public Sort(@JsonProperty("child") PhysicalOperator child, @JsonProperty("orderings") List<Ordering> orderings, @JsonProperty("reverse") boolean reverse) {
    super(child);
    this.orderings = orderings;
    this.reverse = reverse;
  }

  public List<Ordering> getOrderings() {
    return orderings;
  }

  public boolean getReverse() {
    return reverse;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E{
    return physicalVisitor.visitSort(this, value);
  }

  @Override
  public OperatorCost getCost() {
    Size childSize = child.getSize();
    long n = childSize.getRecordCount();
    long width = childSize.getRecordSize();

    //TODO: Magic Number, let's assume 1/10 of data can fit in memory. 
    int k = 10;
    long n2 = n/k;
    double cpuCost = 
        k * n2 * (Math.log(n2)/Math.log(2)) + // 
        n * (Math.log(k)/Math.log(2));
    double diskCost = n*width*2;
    
    return new OperatorCost(0, (float) diskCost, (float) n2*width, (float) cpuCost);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new Sort(child, orderings, reverse);
  }

    
  
  
}
