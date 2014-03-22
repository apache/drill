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

import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.AbstractSingle;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.physical.impl.common.HashTableConfig;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("hash-aggregate")
public class HashAggregate extends AbstractSingle {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashAggregate.class);

  private final NamedExpression[] groupByExprs;
  private final NamedExpression[] aggrExprs;

  private final float cardinality;

  // configuration parameters for the hash table
  private final HashTableConfig htConfig;
  
  @JsonCreator
  public HashAggregate(@JsonProperty("child") PhysicalOperator child, @JsonProperty("keys") NamedExpression[] groupByExprs, @JsonProperty("exprs") NamedExpression[] aggrExprs, @JsonProperty("cardinality") float cardinality) {
    super(child);
    this.groupByExprs = groupByExprs;
    this.aggrExprs = aggrExprs;
    this.cardinality = cardinality;

    int initial_capacity = cardinality > HashTable.DEFAULT_INITIAL_CAPACITY ? 
      (int) cardinality : HashTable.DEFAULT_INITIAL_CAPACITY;

    this.htConfig = new HashTableConfig(initial_capacity,                                        
                                        HashTable.DEFAULT_LOAD_FACTOR, 
                                        groupByExprs, 
                                        null /* no probe exprs */) ;
  }

  public NamedExpression[] getGroupByExprs() {
    return groupByExprs;
  }

  public NamedExpression[] getAggrExprs() {
    return aggrExprs;
  }

  public double getCardinality() {
    return cardinality;
  }

  public HashTableConfig getHtConfig() {
    return htConfig;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E{
    return physicalVisitor.visitHashAggregate(this, value);
  }

  @Override
  public OperatorCost getCost() {

    final float hashCpuCost = (float) 10000.0; // temporarily set this high until we calibrate hashaggr vs. streaming-aggr costs.
    Size childSize = child.getSize();
    long n = childSize.getRecordCount();
    long width = childSize.getRecordSize();
    int numExprs = getGroupByExprs().length;

    double cpuCost = n * numExprs * hashCpuCost;
    double diskCost = 0;      // for now assume hash table fits in memory 
        
    return new OperatorCost(0, (float) diskCost, (float) n*width, (float) cpuCost);
  }

  public void logCostInfo(OperatorCost HACost, OperatorCost SACost) {
	  logger.debug("HashAggregate cost: cpu = {}, disk = {}, memory = {}, network = {}.", HACost.getCpu(), HACost.getDisk(), HACost.getMemory(), HACost.getNetwork());
	  logger.debug("Streaming aggregate cost: cpu = {}, disk = {}, memory = {}, network = {}.", SACost.getCpu(), SACost.getDisk(), SACost.getMemory(), SACost.getNetwork());
  }
  
  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new HashAggregate(child, groupByExprs, aggrExprs, cardinality);
  }

  @Override
  public Size getSize() {
    // not a great hack...
    return new Size( (long) (child.getSize().getRecordCount()*cardinality), child.getSize().getRecordSize());
  }
  
  

  
  
  
}
