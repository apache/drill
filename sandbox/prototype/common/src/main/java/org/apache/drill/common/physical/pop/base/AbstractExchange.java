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
package org.apache.drill.common.physical.pop.base;

import java.util.List;

import org.apache.drill.common.physical.OperatorCost;
import org.apache.drill.common.proto.CoordinationProtos.DrillbitEndpoint;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class AbstractExchange extends AbstractSingle implements Exchange {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractExchange.class);

  private final ExchangeCost cost;

  public AbstractExchange(PhysicalOperator child, ExchangeCost cost) {
    super(child);
    this.cost = cost;
  }

  /**
   * Exchanges are not executable. The Execution layer first has to set their parallelization and convert them into
   * something executable
   */
  @Override
  public boolean isExecutable() {
    return false;
  }

  @Override
  public OperatorCost getAggregateSendCost() {
    return cost.getSend();
  }

  @Override
  public OperatorCost getAggregateReceiveCost() {
    return cost.getReceive();
  }

  @Override
  public ExchangeCost getExchangeCost() {
    return cost;
  }

  @JsonIgnore
  @Override
  public OperatorCost getCost() {
    return cost.getCombinedCost();
  }

}
