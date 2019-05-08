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
package org.apache.drill.exec.planner.fragment;

import org.apache.drill.common.DrillNode;
import org.apache.drill.common.util.function.CheckedConsumer;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.Exchange;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.AbstractOpWrapperVisitor;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.util.memory.ZKQueueMemoryAllocationUtilities;
import org.apache.drill.exec.work.foreman.rm.QueryResourceManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.function.BiFunction;

public class ZKQueueParallelizer extends SimpleParallelizer {

  private final boolean planHasMemory;
  private final QueryContext queryContext;
  private Map<String, Collection<PhysicalOperator>> endpointMap;
  private final QueryResourceManager resourceManager;

  public ZKQueueParallelizer(boolean memoryAvailableInPlan, QueryResourceManager rm, QueryContext queryContext) {
    super(queryContext);
    this.planHasMemory = memoryAvailableInPlan;
    this.queryContext = queryContext;
    this.resourceManager = rm;
  }

  @Override
  public void adjustMemory(PlanningSet planningSet, Set<Wrapper> roots,
                           Map<DrillbitEndpoint, String> onlineEndpointUUIDs) throws PhysicalOperatorSetupException {
    if (planHasMemory) {
      return;
    }

    Collector collector = new Collector();

    for (Wrapper wrapper : roots) {
      traverse(wrapper, CheckedConsumer.throwingConsumerWrapper((Wrapper fragment) -> {
        fragment.getNode().getRoot().accept(collector, fragment);
      }));
    }

    endpointMap = collector.getNodeMap();
    ZKQueueMemoryAllocationUtilities.planMemory(queryContext, this.resourceManager, endpointMap);
  }


  public class Collector extends AbstractOpWrapperVisitor<Void, RuntimeException> {
    private final Map<DrillNode, List<PhysicalOperator>> bufferedOperators;

    public Collector() {
      this.bufferedOperators = new HashMap<>();
    }

    private void getMinorFragCountPerDrillbit(Wrapper currFragment, PhysicalOperator operator) {
      for (DrillbitEndpoint endpoint : currFragment.getAssignedEndpoints()) {
        DrillNode node = new DrillNode(endpoint);
        bufferedOperators.putIfAbsent(node, new ArrayList<>());
        bufferedOperators.get(node).add(operator);
      }
    }

    @Override
    public Void visitSendingExchange(Exchange exchange, Wrapper fragment) throws RuntimeException {
      return visitOp(exchange, fragment);
    }

    @Override
    public Void visitReceivingExchange(Exchange exchange, Wrapper fragment) throws RuntimeException {
      return null;
    }

    @Override
    public Void visitOp(PhysicalOperator op, Wrapper fragment) {
      if (op.isBufferedOperator(queryContext)) {
        getMinorFragCountPerDrillbit(fragment, op);
      }
      for (PhysicalOperator child : op) {
        child.accept(this, fragment);
      }
      return null;
    }

    public Map<String, Collection<PhysicalOperator>> getNodeMap() {
      Map<DrillNode, List<PhysicalOperator>> endpointCollectionMap = bufferedOperators;
      Map<String, Collection<PhysicalOperator>> nodeMap = new HashMap<>();
      for (Map.Entry<DrillNode, List<PhysicalOperator>> entry : endpointCollectionMap.entrySet()) {
        nodeMap.put(entry.getKey().toString(), entry.getValue());
      }

      return nodeMap;
    }
  }

  @Override
  protected BiFunction<DrillbitEndpoint, PhysicalOperator, Long> getMemory() {
    return (endpoint, operator) -> operator.getMaxAllocation();
  }
}
