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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.defs.OrderDef;
import org.apache.drill.common.exceptions.PhysicalOperatorSetupException;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.physical.base.AbstractExchange;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Receiver;
import org.apache.drill.exec.physical.base.Sender;
import org.apache.drill.exec.proto.CoordinationProtos;

import java.util.List;

@JsonTypeName("single-merge-exchange")
public class SingleMergeExchange extends AbstractExchange {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SingleMergeExchange.class);

  private final List<OrderDef> orderExpr;

  // ephemeral for setup tasks
  private List<CoordinationProtos.DrillbitEndpoint> senderLocations;
  private CoordinationProtos.DrillbitEndpoint receiverLocation;

  @JsonCreator
  public SingleMergeExchange(@JsonProperty("child") PhysicalOperator child,
                             @JsonProperty("orderings") List<OrderDef> orderExpr) {
    super(child);
    this.orderExpr = orderExpr;
  }

  @Override
  public int getMaxSendWidth() {
    return Integer.MAX_VALUE;
  }

  @Override
  protected void setupSenders(List<CoordinationProtos.DrillbitEndpoint> senderLocations) {
    this.senderLocations = senderLocations;
  }

  @Override
  protected void setupReceivers(List<CoordinationProtos.DrillbitEndpoint> receiverLocations)
      throws PhysicalOperatorSetupException {

    if (receiverLocations.size() != 1)
      throw new PhysicalOperatorSetupException("SingleMergeExchange only supports a single receiver endpoint");
    receiverLocation = receiverLocations.iterator().next();

  }

  @Override
  public Sender getSender(int minorFragmentId, PhysicalOperator child) {
    return new SingleSender(receiverMajorFragmentId, child, receiverLocation);
  }

  @Override
  public Receiver getReceiver(int minorFragmentId) {
    return new MergingReceiverPOP(senderMajorFragmentId, senderLocations, orderExpr);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new SingleMergeExchange(child, orderExpr);
  }

  @Override
  public boolean supportsSelectionVector() {
    return true;
  }

}