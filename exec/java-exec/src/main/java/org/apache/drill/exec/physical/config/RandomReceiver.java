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
package org.apache.drill.exec.physical.config;

import java.util.List;

import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.AbstractReceiver;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;

@JsonTypeName("random-receiver")
public class RandomReceiver extends AbstractReceiver{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RandomReceiver.class);

  private List<DrillbitEndpoint> senders;
  
  @JsonCreator
  public RandomReceiver(@JsonProperty("sender-major-fragment") int oppositeMajorFragmentId,
                        @JsonProperty("senders") List<DrillbitEndpoint> senders) {
    super(oppositeMajorFragmentId);
    this.senders = senders;
  }
  
  @Override
  @JsonProperty("senders")
  public List<DrillbitEndpoint> getProvidingEndpoints() {
    return senders;
  }

  @Override
  public boolean supportsOutOfOrderExchange() {
    return true;
  }

  @Override
  public OperatorCost getCost() {
    //TODO: deal with receiver cost through exchange.
    return new OperatorCost(1,1,1,1);
  }

  
  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitRandomReceiver(this, value);
  }

  @Override
  public Size getSize() {
    //TODO: deal with size info through exchange.
    return new Size(1,1);
  }

  

  
}
