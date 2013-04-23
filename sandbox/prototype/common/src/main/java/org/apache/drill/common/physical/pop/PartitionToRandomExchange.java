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
package org.apache.drill.common.physical.pop;

import java.util.List;

import org.apache.drill.common.defs.PartitionDef;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.physical.OperatorCost;
import org.apache.drill.common.physical.pop.base.AbstractExchange;
import org.apache.drill.common.physical.pop.base.ExchangeCost;
import org.apache.drill.common.physical.pop.base.PhysicalOperator;
import org.apache.drill.common.physical.pop.base.PhysicalVisitor;
import org.apache.drill.common.physical.pop.base.Receiver;
import org.apache.drill.common.physical.pop.base.Sender;
import org.apache.drill.common.proto.CoordinationProtos.DrillbitEndpoint;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("partition-to-random-exchange")
public class PartitionToRandomExchange extends AbstractExchange{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PartitionToRandomExchange.class);

  private final PartitionDef partition;
  private final int maxWidth;
  
  @JsonCreator
  public PartitionToRandomExchange(@JsonProperty("child") PhysicalOperator child, @JsonProperty("partition") PartitionDef partition, @JsonProperty("cost") ExchangeCost cost) {
    super(child, cost);
    this.partition = partition;
    
    LogicalExpression[] parts = partition.getStarts();
    if(parts != null && parts.length > 0){
      this.maxWidth = parts.length+1;
    }else{
      this.maxWidth = Integer.MAX_VALUE;
    }
  }

  public PartitionDef getPartition() {
    return partition;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E{
    return physicalVisitor.visitExchange(this,  value);
  }

  @Override
  public int getMaxSendWidth() {
    return maxWidth;
  }

  @Override
  public void setupSenders(List<DrillbitEndpoint> senderLocations) {
  }

  @Override
  public void setupReceivers(List<DrillbitEndpoint> receiverLocations) {
  }

  @Override
  public Sender getSender(int minorFragmentId, PhysicalOperator child) {
    return null;
  }

  @Override
  public Receiver getReceiver(int minorFragmentId) {
    return null;
  }


  
  
}
