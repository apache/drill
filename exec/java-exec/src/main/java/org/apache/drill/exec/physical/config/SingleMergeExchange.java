/*
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

import com.google.common.base.Preconditions;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractExchange;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalOperatorUtil;
import org.apache.drill.exec.physical.base.Receiver;
import org.apache.drill.exec.physical.base.Sender;
import org.apache.drill.exec.planner.fragment.ParallelizationInfo;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("single-merge-exchange")
public class SingleMergeExchange extends AbstractExchange {

  private final List<Ordering> orderExpr;

  @JsonCreator
  public SingleMergeExchange(@JsonProperty("child") PhysicalOperator child,
                             @JsonProperty("orderings") List<Ordering> orderExpr) {
    super(child);
    this.orderExpr = orderExpr;
  }

  @Override
  public ParallelizationInfo getReceiverParallelizationInfo(List<DrillbitEndpoint> senderFragmentEndpoints) {
    Preconditions.checkArgument(senderFragmentEndpoints != null && senderFragmentEndpoints.size() > 0,
        "Sender fragment endpoint list should not be empty");

    return ParallelizationInfo.create(1, 1, getDefaultAffinityMap(senderFragmentEndpoints));
  }

  @Override
  protected void setupReceivers(List<DrillbitEndpoint> receiverLocations)
      throws PhysicalOperatorSetupException {
    Preconditions.checkArgument(receiverLocations.size() == 1,
      "SingleMergeExchange only supports a single receiver endpoint.");

    super.setupReceivers(receiverLocations);
  }

  @Override
  public Sender getSender(int minorFragmentId, PhysicalOperator child) {
    return new SingleSender(receiverMajorFragmentId, child, receiverLocations.iterator().next());
  }

  @Override
  public Receiver getReceiver(int minorFragmentId) {
    return new MergingReceiverPOP(senderMajorFragmentId,
        PhysicalOperatorUtil.getIndexOrderedEndpoints(senderLocations), orderExpr, false);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new SingleMergeExchange(child, orderExpr);
  }

  @JsonProperty("orderings")
  public List<Ordering> getOrderings() {
    return this.orderExpr;
  }
}
