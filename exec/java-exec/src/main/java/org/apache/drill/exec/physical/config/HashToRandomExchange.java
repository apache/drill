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

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.physical.base.AbstractExchange;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Receiver;
import org.apache.drill.exec.physical.base.Sender;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("hash-to-random-exchange")
public class HashToRandomExchange extends AbstractExchange{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashToRandomExchange.class);


  private final LogicalExpression expr;

  //ephemeral for setup tasks.
  private List<DrillbitEndpoint> senderLocations;
  private List<DrillbitEndpoint> receiverLocations;

  @JsonCreator
  public HashToRandomExchange(@JsonProperty("child") PhysicalOperator child, @JsonProperty("expr") LogicalExpression expr) {
    super(child);
    this.expr = expr;
  }

  @Override
  public int getMaxSendWidth() {
    return Integer.MAX_VALUE;
  }


  @Override
  protected void setupSenders(List<DrillbitEndpoint> senderLocations) {
    this.senderLocations = senderLocations;
  }

  @Override
  protected void setupReceivers(List<DrillbitEndpoint> receiverLocations) {
    this.receiverLocations = receiverLocations;
  }

  @Override
  public Sender getSender(int minorFragmentId, PhysicalOperator child) {
    return new HashPartitionSender(receiverMajorFragmentId, child, expr, receiverLocations);
  }

  @Override
  public Receiver getReceiver(int minorFragmentId) {
    return new UnorderedReceiver(senderMajorFragmentId, senderLocations);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new HashToRandomExchange(child, expr);
  }

  @JsonProperty("expr")
  public LogicalExpression getExpression(){
    return expr;
  }



}
